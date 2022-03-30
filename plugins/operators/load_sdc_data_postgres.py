import pandas as pd
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from hooks.postgres_hook import PostgresHook

from configs.amazon_table_config import source_target_mapping, product_table_insert_query,\
    dim_product_pk, product_update_query, amazon_mkt_product_table_name


class PostgresSCDOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 postgres_conn_id,
                 prev_task_id,
                 table,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        # Connecting to local postgres
        self.postgres_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.db = PostgresHook(self.postgres_conn)
        self.prev_task_id = prev_task_id

    def execute(self, context):
        # Append a random string to the end of the staging table to ensure
        # no conflicts if multiple processes running concurrently.

        print('\n ALL UPSTREAM TASKS', self.upstream_task_ids)
        upstream_task = list(self.upstream_task_ids)
        # Getting filename from the previous task
        filename = context['task_instance'].xcom_pull(task_ids=upstream_task[0], key='transformed_data_file')
        self.do_scd_loading(filename)

    def do_scd_loading(self, filename):
        # Getting source data/API Data
        source_data = pd.read_csv(filename)
        # Getting all the dimension data from target table
        target_data = self.get_target_data()
        # As we need to find updated data for all the columns, renaming columns
        # to resolve ambiguity
        source_data.rename(columns=self.rename_before_ambiguity('src'), inplace=True)
        target_data.rename(columns=self.rename_before_ambiguity('tgt'), inplace=True)
        print(target_data.info())
        if target_data.empty:
            print("Target data is empty")
            # If the target is empty insert all the new rows coming from the source
            self.insert_new_records(source_data, is_first=True)
            return
        else:
            target_data['updated_at_tgt'] = pd.to_datetime(target_data['updated_at_tgt']).dt.strftime('%Y-%m-%d')
            # Joing on source data to find out which new rows are added.
            join_df = pd.merge(source_data,
                               target_data,
                               left_on=f'{dim_product_pk}_src',
                               right_on=f'{dim_product_pk}_tgt', how='left')
            # If no new rows are found then return
            if join_df.empty:
                print("No new record found")
                return
            else:
                # Insert new rows which are missing in target and calculate the change of updated rows
                self.insert_new_records(join_df)
                # Checking for any updated record
                self.update_and_insert_records(join_df)
                return

    def update_and_insert_records(self, merge_df):
        stale_ids = list()
        ins_upd_rec = pd.DataFrame()
        # We are getting updated records for each column by comparing source_data and target data
        for i, j in source_target_mapping.items():
            ins_upd_rec = ins_upd_rec.append(self.get_updated_records(merge_df, i),
                                             ignore_index=True)
        # Data frame might get multiple rows if multiple column has changed
        ins_upd_rec.drop_duplicates(subset=['product_sku_src'], inplace=True)
        print(ins_upd_rec)
        # Adding ids/sku which has changed since last data pull
        for i, j in ins_upd_rec.iterrows():
            stale_ids.append(j.product_sku_src)
        print((set(stale_ids)))
        # Updating stale skus flag to False
        self.db.update_delete_query(product_update_query.format(table=amazon_mkt_product_table_name,
                                                                _bool=False,
                                                                stale_ids=tuple(list(set(stale_ids)))))
        ins_upd_rec = ins_upd_rec[list(self.rename_before_ambiguity('src').values())]
        ins_upd_rec['is_active'] = True
        # Inserting new updated rows
        self.db.insert_records(product_table_insert_query, list({tuple([
            i['product_sku_src'], i['product_parent_sku_src'],
            i['product_name_src'], i['updated_at_src'],
            i['currency_code_src'], i['price_src'],
            i['stock_qty_src'], bool(i['is_active'])]) for i in ins_upd_rec.to_dict('records')
        }))

    def get_updated_records(self, merged_df, source_column):
        # Check if primary key is same and if source_key != target_key, then it got updated else not
        merged_df['ins_upd_flag'] = merged_df[[f'{dim_product_pk}_src',
                                               f'{dim_product_pk}_tgt',
                                               f'{source_column}_src',
                                               f'{source_column}_tgt']].apply(
            lambda x: 'UI' if x[0] == x[1] and x[2] != x[3] else 'N', axis=1)
        # Return records which got updated
        return merged_df[merged_df['ins_upd_flag'] == 'UI']

    def insert_new_records(self, merge_df, is_first=False):
        # If 1st data load then inserted_df = merged_df
        # else calculate the new rows by checking null condition on right dataframe
        # because those value must be missing in the right/target dataframe
        if not is_first:
            merge_df['ins_flag'] = merge_df[[f'{dim_product_pk}_src', f'{dim_product_pk}_tgt']].apply(
                lambda x: 'I' if pd.isnull(x[1]) else 'N', axis=1)
            ins_rec = merge_df[merge_df['ins_flag'] == 'I']
        else:
            print("In else", merge_df.info())
            ins_rec = merge_df
        # Taking only source columns to insert new values
        ins_rec = ins_rec[list(self.rename_before_ambiguity('src').values())]
        # For new values is_active flag would be true
        ins_rec['is_active'] = True
        print(ins_rec.info())
        # Inserting new values into a table
        self.db.insert_records(product_table_insert_query, list({tuple([
            i['product_sku_src'], i['product_parent_sku_src'],
            i['product_name_src'], i['updated_at_src'],
            i['currency_code_src'], i['price_src'],
            i['stock_qty_src'], bool(i['is_active'])]) for i in ins_rec.to_dict('records')
        }))

    # Renaming the dataframe column values.
    def rename_before_ambiguity(self, type):
        temp = source_target_mapping.copy()
        for i, j in temp.items():
            temp[i] = j.format(prefix=type)
        return temp

    def get_target_data(self):
        target_data = self.db.select_query(f"select * from {self.table} where is_active = True")
        return pd.DataFrame(target_data)
