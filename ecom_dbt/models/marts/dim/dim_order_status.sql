{{ config(materialized='table') }}

with src as (
  select distinct order_status from {{ ref('stg_orders') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['order_status']) }}  as order_status_pk,
  order_status
from src