{{ config(materialized='table') }}

with source as (
  select * from {{ source('lnd','order_payments') }}
)

select
  order_id,
  payment_sequential,
  payment_type,
  payment_installments,
  payment_value
from source
where order_id is not null and payment_sequential is not null
