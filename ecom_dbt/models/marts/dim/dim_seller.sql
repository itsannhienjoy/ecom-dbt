{{ config(materialized='table') }}

with src as (
  select
    upper(trim(seller_id)) as seller_id_norm,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    cast(dbt_valid_from as timestamp) as valid_from,
    cast(coalesce(dbt_valid_to,'2999-12-31') as timestamp) as valid_to
  from {{ ref('sellers_snapshot') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_pk,
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state,
  valid_from,
  valid_to,
  seller_id_norm
from src
