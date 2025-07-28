{{ config(materialized='table') }}

with src as (
  select distinct payment_type from {{ ref('stg_order_payments') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['payment_type']) }} as payment_type_pk,
  payment_type
from src
