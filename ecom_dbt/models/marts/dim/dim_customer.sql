{{ config(materialized='table') }}

with src as (
  select
    upper(trim(customer_id))              as customer_id_norm,
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    cast(dbt_valid_from as timestamp)  as valid_from,
    cast(coalesce(dbt_valid_to, '2999-12-31') as timestamp) as valid_to
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
  valid_to,
  customer_id_norm
from src
