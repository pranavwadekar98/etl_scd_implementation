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


class AmazonMktProductsExtractOperator(BaseOperator):

	def __init__(self,
				 amz_connection_id,
				 *args,
				 **kwargs):
		super().__init__(*args, **kwargs)

		self.amz_connection_id = amz_connection_id

	@staticmethod
	def get_unique_products_by_latest_updated_data(filename, start_date, end_date):
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
		"""
		Assumption & Implementation:
		We currently only have orders data, so for the current implementation we are pulling unique 
		sku's which have been updated in the last day and sorting them on updated_date and pulling out
		the latest records and also putting qty_ordered and price contained in the order data as we have
		mentioned these columns in the dim_product table.
		"""
		df1.sort_values('last_updated_date', axis=0, ascending=False, inplace=True)
		df1.drop_duplicates(subset=['sku'], keep='first', inplace=True)
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
				start_date = (context['execution_date'] - timedelta(weeks=52)).strftime("%Y-%m-%d")
			else:
				start_date = (context['execution_date']).strftime("%Y-%m-%d")
				end_date = (context['execution_date'] + timedelta(weeks=2)).strftime("%Y-%m-%d")
		logging.info(start_date)
		logging.info(end_date)
		# Here we are getting CSV sheet but we can also pull this data from the API.
		conn = BaseHook.get_connection(self.amz_connection_id)
		file_name = json.loads(conn.extra)['amz_orders_filename']
		data = AmazonMktProductsExtractOperator.get_unique_products_by_latest_updated_data(file_name,
																						   start_date,
																						   end_date)
		# We are creating raw/extracted data file on local (Ubuntu) machine in /tmp folder
		# but we can also put this file on Amazon S3 and pass the filename to XCOM.
		if not os.path.exists('/tmp/raw_data'):
			os.mkdir('/tmp/raw_data')
		extracted_data_file = '/tmp/raw_data/amz_mkt_products_' + str(
			datetime.utcnow().replace(microsecond=0)) + '.csv'
		data.to_csv(extracted_data_file, index=False)
		context['task_instance'].xcom_push('extracted_data_file', extracted_data_file, context['execution_date'])
		logging.info("Pushed Variable to Xcom")


class AmazonMktProductsTransformOperator(BaseOperator):

	def __init__(self,
				 *args,
				 **kwargs):
		super().__init__(*args, **kwargs)

	def execute(self, context):
		upstream_task = list(self.upstream_task_ids)
		# Getting last task file for transformation.
		filename = context['task_instance'].xcom_pull(task_ids=upstream_task[0], key='extracted_data_file')
		transform = ProductsTransformations(pd.read_csv(filename))
		# Creating transformed file on local machine
		if not os.path.exists('/tmp/transformed_data'):
			os.mkdir('/tmp/transformed_data')
		transformed_filename = '/tmp/transformed_data/amz_mkt_products_' + str(
			datetime.utcnow().replace(microsecond=0)) + '.csv'
		df = transform.transform_data()
		df.to_csv(transformed_filename, index=False)
		# Passing transformed data file to the next task by airflow communication channel
		context['task_instance'].xcom_push('transformed_data_file', transformed_filename, context['execution_date'])


class ProductsTransformations(object):
	def __init__(self, df):
		self.df = df

	def drop_rows_with_null(self):
		# Dropping rows with NULL values from the primary key columns.
		self.df = self.df.dropna(subset=['sku'])

	def fill_null_and_format_type(self):
		# Getting only columns which we will store in our table
		self.df = self.df[[
			  'sku'
			, 'asin'
			, 'product_name'
			, 'last_updated_date'
			, 'currency'
			, 'item_price'
			, 'quantity'
		]]
		# Replacing special characters with '' for SKU.
		self.df['sku'] = self.df['sku'].apply(lambda x: re.sub('[^A-Za-z0-9]+', '', x))
		# Filling NULL values with Not Available and formatted the type for each column.
		self.df['currency'] = self.df['currency'].fillna('N/A').astype(str)
		self.df['asin'] = self.df['asin'].fillna('N/A').astype(str)
		self.df['product_name'] = self.df['product_name'].fillna('N/A').astype(str)
		self.df['item_price'] = self.df['item_price'].fillna(0).astype(float)
		self.df['quantity'] = self.df['quantity'].fillna(0).astype(float)
		self.df['last_updated_date'] = pd.to_datetime(self.df['last_updated_date'])

	def rename_columns(self):
		self.df.rename(columns=
			dict(asin='product_parent_sku', sku='product_sku', last_updated_date='updated_at',
                 currency='currency_code',
				 item_price='price', quantity='stock_qty'), inplace=True)

	def get_relevant_data(self):
		# Renamed columns according to our table columns
		return self.df[['product_sku'
			, 'product_parent_sku'
			, 'product_name'
			, 'updated_at'
			, 'currency_code'
			, 'price'
			, 'stock_qty']]

	def transform_data(self):
		self.drop_rows_with_null()
		self.fill_null_and_format_type()
		self.rename_columns()
		df = self.get_relevant_data()
		return df
