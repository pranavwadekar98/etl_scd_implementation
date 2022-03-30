
# Orders config
amazon_mkt_order_table_name = 'fact_order'
orders_incremental_keys = ['amz_order_id', 'sku']

# Product config
amazon_mkt_product_table_name = 'dim_product'
source_target_mapping = {
    'currency_code': 'currency_code_{prefix}',
    'stock_qty': 'stock_qty_{prefix}',
    'price': 'price_{prefix}',
    'product_sku': 'product_sku_{prefix}',
    'updated_at': 'updated_at_{prefix}',
    'product_parent_sku': 'product_parent_sku_{prefix}',
    'product_name': 'product_name_{prefix}',
}
dim_product_pk = 'product_sku'
product_table_insert_query = """
Insert into dim_product (
  product_sku
, product_parent_sku
, product_name
, updated_at
, currency_code
, price
, stock_qty
, is_active) values %s
"""
product_update_query = "UPDATE {table} set is_active = {_bool} where product_sku in {stale_ids}"

# Fee config
amazon_mkt_fee_table_name = 'fact_fee'
fee_incremental_keys = ['order_id', 'sku']