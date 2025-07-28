{{ config(materialized='table') }}

with src as (
  select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    dbt_valid_from as valid_from,
    coalesce(dbt_valid_to, '9999-12-31') as valid_to,
  from {{ ref('customers_snapshot') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['customer_id','customer_unique_id']) }} as customer_pk,

  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state,
  valid_from,
  valid_to
from src
