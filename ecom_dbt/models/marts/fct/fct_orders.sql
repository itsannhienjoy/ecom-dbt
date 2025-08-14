{{ config(materialized='incremental', on_schema_change='fail') }}

with payments_rollup as (
  select order_id,
         sum(payment_value) as total_paid,
         count(*)           as payment_count
  from {{ ref('stg_order_payments') }}
  group by 1
)
, revenue_rollup as (
  select order_id,
         sum(price + freight_value) as total_revenue
  from {{ ref('stg_order_items') }}
  group by 1
)
, src as (
  select
    o.order_id,
    o.customer_id,
    upper(trim(o.customer_id)) as customer_id_norm,
    upper(trim(o.order_status)) as order_status_norm,
    to_timestamp(o.order_purchase_timestamp) as purchase_ts
  from {{ ref('stg_orders') }} o
)

select
  s.order_id as order_pk,
  s.order_id,
  c.customer_pk,
  d.date_pk       as order_date_pk,
  {{ dbt_utils.generate_surrogate_key(['order_status_norm']) }} as order_status_pk,
  coalesce(rev.total_revenue, 0) as total_revenue,
  coalesce(pay.total_paid, 0)    as total_paid,
  coalesce(pay.payment_count, 0) as payment_count,
  t.time_pk                      as order_time_pk
from src s
left join payments_rollup pay on s.order_id = pay.order_id
left join revenue_rollup  rev on s.order_id = rev.order_id
left join {{ ref('dim_customer') }} c
  on upper(trim(s.customer_id)) = c.customer_id_norm
left join {{ ref('dim_date') }} d
  on cast(s.purchase_ts as date) = d.date_key
left join {{ ref('dim_order_status') }} os
  on s.order_status_norm = os.order_status 
left join {{ ref('dim_time') }} t
  on to_time(to_char(s.purchase_ts,'HH24:MI:SS')) = t.time_key
