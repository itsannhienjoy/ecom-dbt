{{ config(materialized='table') }}

with source as (
    select * from {{ source('lnd', 'orders') }}
)
select
    order_id,
    customer_id,
    order_status,
    to_timestamp(order_purchase_timestamp) as order_purchase_timestamp,
    to_timestamp(order_approved_at) as order_approved_at,
    to_timestamp(order_delivered_carrier_date) as order_delivered_carrier_date,
    to_timestamp(order_delivered_customer_date) as order_delivered_customer_date,
    to_timestamp(order_estimated_delivery_date) as order_estimated_delivery_date,
    already_shipped,
    already_delivered
from source
where order_id is not null and customer_id is not null
