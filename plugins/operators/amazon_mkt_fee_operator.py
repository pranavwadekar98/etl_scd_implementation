import json
import logging
import os
import duckdb
import re

import pandas as pd
from dateutil import parser
from datetime import datetime, timedelta
from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook


class AmazonMktFeeExtractOperator(BaseOperator):

    def __init__(self,
                 amz_connection_id,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.amz_connection_id = amz_connection_id

    @staticmethod
    def get_fee_data(filename, start_date, end_date):
        df = pd.read_csv(filename)
        # Considering posted_date as last updated_date keys on which CDC data is being capture
        df['posted_date'] = pd.to_datetime(df['posted_date'])
        # Used DuckDB for faster performance than pandas.
        df1 = duckdb.query(f"""
                           SELECT * FROM df
                           where posted_date >= '{start_date}' 
                           and posted_date <= '{end_date}'""").to_df()
        return df1

    # Operator flow starts with execute statement
    def execute(self, context):
        logging.info("Collecting Data From Source")
        # if external API call invokes this DAG for custom dag run
        if context['dag_run'].conf:
            start_date = context['dag_run'].conf['start_date']
            end_date = context['dag_run'].conf['end_date']
        else:
            if not context['task_instance'].previous_ti:
                # When the process will run 1st time, all last 1-year data will get captured
                # After than we will get only data which gets updated/changed
                end_date = (context['execution_date']).strftime("%Y-%m-%d")
                start_date = (context['execution_date'] - timedelta(weeks=52)).strftime("%Y-%m-%d")
            else:
                start_date = (context['execution_date']).strftime("%Y-%m-%d")
                end_date = (context['execution_date'] + timedelta(weeks=2)).strftime("%Y-%m-%d")
        logging.info(start_date)
        logging.info(end_date)
        conn = BaseHook.get_connection(self.amz_connection_id)
        # Here we are getting CSV sheet, but we can also pull this data from the API.
        file_name = json.loads(conn.extra)['amz_fee_filename']
        data = AmazonMktFeeExtractOperator.get_fee_data(file_name, start_date, end_date)
        # We are creating raw/extracted data file on local (Ubuntu) machine in /tmp folder
        # but we can also put this file on Amazon S3 and pass the filename to XCOM.
        if not os.path.exists('/tmp/raw_data'):
            os.mkdir('/tmp/raw_data')
        extracted_data_file = '/tmp/raw_data/amz_mkt_fee_' + str(
            datetime.utcnow().replace(microsecond=0)) + '.csv'
        data.to_csv(extracted_data_file, index=False)
        # Passing data file to the next task by airflow communication channel.
        context['task_instance'].xcom_push('extracted_data_file', extracted_data_file, context['execution_date'])
        logging.info("Pushed Variable to Xcom")


class AmazonMktFeeTransformOperator(BaseOperator):

    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        upstream_task = list(self.upstream_task_ids)
        # Getting last task file for transformation.
        filename = context['task_instance'].xcom_pull(task_ids=upstream_task[0], key='extracted_data_file')
        transform = FeeTransformations(pd.read_csv(filename))
        # Creating transformed file on local machine
        if not os.path.exists('/tmp/transformed_data'):
            os.mkdir('/tmp/transformed_data')
        transformed_filename = '/tmp/transformed_data/amz_mkt_fee_' + str(
            datetime.utcnow().replace(microsecond=0)) + '.csv'
        df = transform.transform_data()
        df.to_csv(transformed_filename, index=False)
        # Passing transformed data file to the next task by airflow communication channel.
        context['task_instance'].xcom_push('transformed_data_file', transformed_filename, context['execution_date'])


class FeeTransformations(object):
    def __init__(self, df):
        self.df = df

    def drop_rows_with_null(self):
        # Dropping rows with NULL values from the primary key columns.
        self.df = self.df.dropna(subset=['sku', 'order_id'])

    def fill_null_and_format_type(self):
        # Replacing special characters with '' for SKU.
        self.df['sku'] = self.df['sku'].apply(lambda x: re.sub('[^A-Za-z0-9]+', '', x))
        # Filling NULL values with Not Available and formatted the type for each column.
        self.df['ob_mws_seller_id'] = self.df['ob_mws_seller_id'].fillna('N/A').astype(str)
        self.df['ob_mws_marketplace_id'] = self.df['ob_mws_marketplace_id'].fillna('N/A').astype(str)
        self.df['settlement_id'] = self.df['settlement_id'].fillna('N/A').astype(str)
        self.df['ob_settlement_index'] = self.df['ob_settlement_index'].fillna('N/A').astype(str)
        self.df['transaction_type'] = self.df['transaction_type'].fillna('N/A').astype(str)
        self.df['shipment_id'] = self.df['shipment_id'].fillna('N/A').astype(str)
        self.df['amount_type'] = self.df['amount_type'].fillna('N/A').astype(str)
        self.df['amount_description'] = self.df['amount_description'].fillna('N/A').astype(str)
        self.df['currency'] = self.df['currency'].fillna('N/A').astype(str)
        self.df['amount'] = self.df['amount'].fillna(0)
        self.df['amount'] = self.df['amount'].apply(lambda x: x.replace(',', '.')).astype(float)
        self.df['order_item_code'] = self.df['order_item_code'].fillna('N/A').astype(str)
        self.df['sku'] = self.df['sku'].fillna('N/A').astype(str)
        self.df['quantity_purchased'] = self.df['quantity_purchased'].fillna(0).astype(float)
        self.df['posted_date'] = pd.to_datetime(self.df['posted_date'])

    def rename_columns(self):
        # Renamed columns according to our table columns
        self.df.rename(columns=dict(ob_mws_seller_id='mws_seller_id', ob_mws_marketplace_id='marketplace_id',
                                    ob_settlement_index='settlement_index', amount='amount',
                                    order_item_code='order_item_code', currency='currency_code'), inplace=True)

    def get_relevant_data(self):
        # Getting only columns which we will store in our table
        return self.df[['mws_seller_id'
            , 'marketplace_id'
            , 'settlement_id'
            , 'settlement_index'
            , 'currency_code'
            , 'transaction_type'
            , 'order_id'
            , 'shipment_id'
            , 'amount_type'
            , 'amount_description'
            , 'amount'
            , 'posted_date'
            , 'order_item_code'
            , 'sku'
            , 'quantity_purchased']]

    def transform_data(self):
        self.drop_rows_with_null()
        self.fill_null_and_format_type()
        self.rename_columns()
        df = self.get_relevant_data()
        return df
