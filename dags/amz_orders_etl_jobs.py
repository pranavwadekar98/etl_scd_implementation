# Last updated - Pranav Wadekar 2022-02-14 16:40:00

# This DAG is for extracting, transforming and loading orders data into a postgres table on a
# local machine

from airflow import DAG
from datetime import datetime


# sys.path.insert(0, os.path.abspath(os.path.dirname('airflow')))
from configs.amazon_table_config import *
from operators.load_data_postgres import LocalToPostgresOperator
from operators.amazon_mkt_orders_operator import AmazonMktOrdersExtractOperator, AmazonMktOrdersTransformOperator

dag = DAG('amz_mkt_orders_dag', description='Amazon Marketplace Orders DAG',
          schedule_interval='0 6 * * *', # This DAG will run 6AM UTC everyday
          default_args={"owner": "airflow"},
          start_date=datetime(2021, 1, 4, 10, 1, 0, 818988),
          catchup=False)

# 'amazon_mkt_orders_conn' this is the connection id with amazon file/API credentials.
# for this task purpose I have put orders csv file name in extras.

with dag:
    extract_task = AmazonMktOrdersExtractOperator(task_id='amz_mkt_extract',
                                            amz_connection_id='amazon_mkt_orders_conn',
                                            pool='amz_mkt_pool',
                                            provide_context=True)

    transform_task = AmazonMktOrdersTransformOperator(task_id='amz_mkt_transform',
                                                pool='amz_mkt_pool',
                                                provide_context=True)

    load_task = LocalToPostgresOperator(task_id='amz_mkt_load',
                                        table=amazon_mkt_order_table_name,
                                        incremental_keys=orders_incremental_keys,
                                        pool='amz_mkt_pool',
                                        postgres_conn_id='postgres_default',
                                        prev_task_id='amz_mkt_transform',
                                        provide_context=True)

    # These task will get run in the same flow mentioned below.
    extract_task >> transform_task >> load_task