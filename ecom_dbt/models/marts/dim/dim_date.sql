{{ config(materialized='table') }}

with dates as (
  select distinct cast(order_purchase_timestamp as date) as dt from {{ ref('stg_orders') }}
  union
  select distinct cast(review_creation_date as date) from {{ ref('stg_order_reviews') }}
  union
  select distinct cast(review_answer_timestamp as date) from {{ ref('stg_order_reviews') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['dt']) }} as date_pk,
  dt   as date_key,
  extract(year from dt) as year,
  extract(month from dt) as month,
  extract(day from dt) as day,
  extract(week from dt) as week,
  to_char(dt,'Day') as day_name,
  to_char(dt,'Mon') as month_name,
  case when extract(dow from dt) in (0,6) then true else false end as is_weekend
from dates
