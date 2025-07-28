{{ config(materialized='table') }}

with source as (
    select * from {{ source('lnd', 'order_items') }}
)
select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    to_timestamp(shipping_limit_date) as shipping_limit_date,   
    price,
    freight_value,
from source
where order_id is not null
