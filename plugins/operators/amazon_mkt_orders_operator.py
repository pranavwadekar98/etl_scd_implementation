import json
import logging
import os
import re
import duckdb

import pandas as pd
from dateutil import parser
from datetime import datetime, timedelta
from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook


class AmazonMktOrdersExtractOperator(BaseOperator):

    def __init__(self,
                 amz_connection_id,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.amz_connection_id = amz_connection_id

    @staticmethod
    def get_orders_data(filename, start_date, end_date):
        df = pd.read_csv(filename)
        # Considering last_updated_at as last updated_date keys on which CDC data is being capture
        df['last_updated_date'] = df['last_updated_date'].apply(lambda x:
                                                                parser.parse(x).strftime('%Y-%m-%d'))
        df['last_updated_date'] = pd.to_datetime(df['last_updated_date'], format='%Y-%m-%d')
        # Used DuckDB for faster performance than pandas.
        # Getting only data which has been updated in last 1 day.
        df1 = duckdb.query(f"""
                           SELECT * FROM df
                           where last_updated_date >= '{start_date}' 
                           and last_updated_date <= '{end_date}'""").to_df()
        return df1

    # Operator flow starts with execute statement
    def execute(self, context):
        logging.info("Collecting Data From Source")
        # if external API call invokes this DAG for custom dag run
        if context['dag_run'].conf:
            start_date = context['dag_run'].conf['start_date']
            end_date = context['dag_run'].conf['end_date']
        else:
            # When the process will run 1st time, all last 1-year data will get captured
            # After than we will get only data which gets updated/changed
            if not context['task_instance'].previous_ti:
                end_date = (context['execution_date']).strftime("%Y-%m-%d")
                start_date = (context['execution_date']-timedelta(weeks=52)).strftime("%Y-%m-%d")
            else:
                start_date = (context['execution_date']).strftime("%Y-%m-%d")
                end_date = (context['execution_date'] + timedelta(weeks=2)).strftime("%Y-%m-%d")
        logging.info(start_date)
        logging.info(end_date)
        # Here we are getting CSV sheet but we can also pull this data from the API.
        conn = BaseHook.get_connection(self.amz_connection_id)
        file_name = json.loads(conn.extra)['amz_orders_filename']
        data = AmazonMktOrdersExtractOperator.get_orders_data(file_name, start_date, end_date)
        # We are creating raw/extracted data file on local (Ubuntu) machine in /tmp folder
        # but we can also put this file on Amazon S3 and pass the filename to XCOM.
        if not os.path.exists('/tmp/raw_data'):
            os.mkdir('/tmp/raw_data')
        extracted_data_file = '/tmp/raw_data/amz_mkt_orders_' + str(
            datetime.utcnow().replace(microsecond=0)) + '.csv'
        data.to_csv(extracted_data_file, index=False)
        # Passing data file to the next task by airflow communication channel.
        context['task_instance'].xcom_push('extracted_data_file', extracted_data_file, context['execution_date'])
        logging.info("Pushed Variable to Xcom")


class AmazonMktOrdersTransformOperator(BaseOperator):

    def __init__(self,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        upstream_task = list(self.upstream_task_ids)
        # Getting last task file for transformation.
        filename = context['task_instance'].xcom_pull(task_ids=upstream_task[0], key='extracted_data_file')
        transform = OrdersTransformations(pd.read_csv(filename))
        # Creating transformed file on local machine
        if not os.path.exists('/tmp/transformed_data'):
            os.mkdir('/tmp/transformed_data')
        transformed_filename = '/tmp/transformed_data/amz_mkt_orders_' + str(
            datetime.utcnow().replace(microsecond=0)) + '.csv'
        df = transform.transform_data()
        df.to_csv(transformed_filename, index=False)
        # Passing transformed data file to the next task by airflow communication channel
        context['task_instance'].xcom_push('transformed_data_file', transformed_filename, context['execution_date'])


class OrdersTransformations(object):
    def __init__(self, df):
        self.df = df

    def drop_rows_with_null(self):
        # Dropping rows with NULL values from the primary key columns.
        self.df = self.df.dropna(subset=['sku', 'amazon_order_id'])

    def fill_null_and_format_type(self):
        # Replacing special characters with '' for SKU.
        self.df['sku'] = self.df['sku'].apply(lambda x: re.sub('[^A-Za-z0-9]+', '', x))
        # Filling NULL values with Not Available and formatted the type for each column.
        self.df['currency'] = self.df['currency'].fillna('N/A').astype(str)
        self.df['merchant_order_id'] = self.df['merchant_order_id'].fillna('N/A').astype(str)
        self.df['ship_country'] = self.df['ship_country'].fillna('N/A').astype(str)
        self.df['order_channel'] = self.df['order_channel'].fillna('N/A').astype(str)
        self.df['url'] = self.df['url'].fillna('N/A').astype(str)
        self.df['shipping_price'] = self.df['shipping_price'].fillna(0).astype(float)
        self.df['shipping_tax'] = self.df['shipping_tax'].fillna(0).astype(float)
        self.df['gift_wrap_price'] = self.df['gift_wrap_price'].fillna(0).astype(float)
        self.df['gift_wrap_tax'] = self.df['gift_wrap_tax'].fillna(0).astype(float)
        self.df['item_promotion_discount'] = self.df['item_promotion_discount'].fillna(0).astype(float)
        self.df['ship_promotion_discount'] = self.df['ship_promotion_discount'].fillna(0).astype(float)
        self.df['ship_city'] = self.df['ship_city'].fillna('N/A').astype(str)
        self.df['item_status'] = self.df['item_status'].fillna('N/A').astype(str)
        self.df['item_price'] = self.df['item_price'].fillna(0).astype(float)
        self.df['purchase_date'] = pd.to_datetime(self.df['purchase_date'])
        self.df['last_updated_date'] = pd.to_datetime(self.df['last_updated_date'])

    def rename_columns(self):
        # Renamed columns according to our table columns
        self.df.rename(columns=dict(amazon_order_id='amz_order_id', sales_channel='sales_channel',
                                    purchase_date='purchase_at', currency='currency_code', quantity='qty_ordered',
                                    item_price='item_price', ob_mws_marketplace_id='marketplace_id', sku='sku',
                                    last_updated_date='updated_at', ship_country='ship_country_code',
                                    item_status='order_item_status', item_promotion_discount='item_discount',
                                    ship_promotion_discount='ship_discount'), inplace=True)

    def get_relevant_data(self):
        # Getting only columns which we will store in our table
        return self.df[['amz_order_id'
                        , 'sales_channel'
                        , 'currency_code'
                        , 'qty_ordered'
                        , 'item_price'
                        , 'marketplace_id'
                        , 'sku'
                        , 'updated_at'
                        , 'ship_country_code'
                        , 'order_status'
                        , 'fulfillment_channel'
                        , 'ship_service_level'
                        , 'order_item_status'
                        , 'shipping_price'
                        , 'shipping_tax'
                        , 'item_discount'
                        , 'ship_discount'
                        , 'ship_city'
                        , 'purchase_at'
                        , 'item_tax'
                        , 'gift_wrap_price'
                        , 'gift_wrap_tax']]

    def transform_data(self):
        self.drop_rows_with_null()
        self.fill_null_and_format_type()
        self.rename_columns()
        df = self.get_relevant_data()
        return df
