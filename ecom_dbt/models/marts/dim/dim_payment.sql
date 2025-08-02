{{ config(materialized='table') }}

with src as (
  select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
  from {{ ref('stg_order_payments') }}
)

select
  payment_sequential,
  order_id,
  payment_type,
  payment_installments,
  payment_value
from src
