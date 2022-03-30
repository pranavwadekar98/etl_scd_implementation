import json
import random
import string
import logging
from airflow.utils.db import provide_session
from airflow.models import Connection
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class LocalToPostgresOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 postgres_conn_id,
                 prev_task_id,
                 table,
                 incremental_keys,
                 load_type='upsert',
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        # We are doing upsert on the records. When the existing record gets updated then we will update
        # the same record with new information, else we will insert it as new
        self.temp_suffix = None
        self.table = table
        # incremental keys - keys which would be compared with the target data if the new data comes in
        # in the source data
        self.incremental_keys = incremental_keys
        self.postgres_conn_id = postgres_conn_id
        self.prev_task_id = prev_task_id

    # Operator flow starts with execute statement
    def execute(self, context):
        # Append a random string to the end of the staging table to ensure
        # no conflicts if multiple processes running concurrently.
        letters = string.ascii_lowercase
        print('\n ALL UPSTREAM TASKS', self.upstream_task_ids)
        upstream_task = list(self.upstream_task_ids)
        query_list = list()
        # For temp table created
        random_string = ''.join(random.choice(letters) for _ in range(7))
        self.temp_suffix = '_tmp_{0}'.format(random_string)
        # Getting filename which needs to be load
        filename = context['task_instance'].xcom_pull(task_ids=upstream_task[0], key='transformed_data_file')
        pg_hook = PostgresHook(self.postgres_conn_id)
        self.copy_data(pg_hook, query_list, filename)
        print("query_list", query_list)
        pg_hook.run(query_list)

    def copy_data(self, pg_hook, query_list, filename):
        @provide_session
        def get_conn(conn_id, session=None):
            conn = (
                session.query(Connection)
                    .filter(Connection.conn_id == conn_id)
                    .first())
            return conn

        # Delete records from the destination table where the incremental_key
        # is greater than or equal to the incremental_key of the source table
        # and the primary key is the same.
        # (e.g. Source: {"id": 1, "updated_at": "2017-01-02 00:00:00"};
        #       Destination: {"id": 1, "updated_at": "2017-01-01 00:00:00"})

        # Deleting data from the target table using incremental keys and comparing it with the temp table
        delete_sql = \
            '''
            DELETE FROM "{rs_table}"
            USING "{rs_table}{rs_suffix}"
            '''

        # Inserting data into target table after deleting it from the target table,
        # from the temp table
        insert_into_target = '''
            insert into "{rs_table}" 
            select * from "{rs_table}{rs_suffix}";
        '''

        # Copy data from the given CSV file into the temp table using copy command
        base_sql = \
            """
            COPY {table_name}{rs_suffix} FROM '{filename}' DELIMITERS ',' CSV HEADER;
            """.format(filename=filename, rs_suffix=self.temp_suffix, table_name=self.table)
        # For creating temp table
        self.create_if_not_exists(query_list)
        delete_qry = delete_sql.format(rs_table=self.table,
                                       rs_suffix=self.temp_suffix)
        for i, j in enumerate(self.incremental_keys):
            if i == 0:
                delete_qry = delete_qry +\
                             "WHERE"\
                             + """ "{rs_table}{rs_suffix}"."{rs_ik}" = "{rs_table}"."{rs_ik}" """.format(
                    rs_table=self.table,
                    rs_suffix=self.temp_suffix,
                    rs_ik=j)
            else:
                delete_qry = delete_qry +\
                             "AND"\
                             + """ "{rs_table}{rs_suffix}"."{rs_ik}" = "{rs_table}"."{rs_ik}" """.format(
                    rs_table=self.table,
                    rs_suffix=self.temp_suffix,
                    rs_ik=j)
        query_list.append(base_sql)
        logging.info("Delete - SQL QUERY")
        logging.info(delete_qry)
        query_list.append(delete_qry)
        query_list.append(insert_into_target.format(rs_table=self.table,
                                                    rs_suffix=self.temp_suffix
                                                         ))
        logging.info("Data loading successful")

    def create_if_not_exists(self, query_list):
        copy_table = '{0}{1}'.format(self.table, self.temp_suffix)
        CREATE_STAGING_TABLE = '''create temp table {stage} (like {target});'''
        # Create temp table like target table.
        create_stage_qry = CREATE_STAGING_TABLE.format(stage=copy_table, target=self.table)
        logging.info(create_stage_qry)
        query_list.append(create_stage_qry)
        # pg_hook.run(query_list)
