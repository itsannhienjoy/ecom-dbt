{{ config(materialized='table')}}

with src as (
    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        dbt_valid_from as valid_from,
        coalesce(dbt_valid_to, '9999-12-31') as valid_to
    from {{ ref('sellers_snapshot') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_pk,
  seller_id,
  seller_zip_code_prefix,
  seller_city,
  seller_state,
  valid_from,
  valid_to
from src