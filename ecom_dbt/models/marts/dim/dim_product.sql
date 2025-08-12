{{ config(materialized='table') }}

with src as (
  select
    upper(trim(product_id)) as product_id_norm,
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    cast(dbt_valid_from as timestamp) as valid_from,
    cast(coalesce(dbt_valid_to,'2999-12-31') as timestamp) as valid_to
  from {{ ref('products_snapshot') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_pk,
  product_id,
  product_category_name,
  product_name_length,
  product_description_length,
  product_photos_qty,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm,
  valid_from,
  valid_to,
  product_id_norm
from src