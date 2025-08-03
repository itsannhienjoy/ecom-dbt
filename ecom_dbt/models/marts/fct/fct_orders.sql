{{ config(
    materialized='incremental',
    on_schema_change='fail'
) }}

with payments_rollup as (
  select
    order_id,
    sum(payment_value) as total_paid,
    count(*) as payment_count
  from {{ ref('stg_order_payments') }}
  group by order_id
),

revenue_rollup as (
  select
    order_id,
    sum(price + freight_value) as total_revenue
  from {{ ref('stg_order_items') }}
  group by order_id
),

src as (
  select
    o.order_id,
    o.customer_id,
    o.order_status,
    to_timestamp(o.order_purchase_timestamp) as purchase_ts
  from {{ ref('stg_orders') }} o
),

final as (
  select
    s.order_id,
    c.customer_pk,
    d.date_pk as order_date_pk,
    os.order_status_pk,
    coalesce(rev.total_revenue, 0) as total_revenue,
    coalesce(pay.total_paid, 0) as total_paid,
    coalesce(pay.payment_count, 0) as payment_count,
    t.time_pk as order_time_pk
  from src s
  left join payments_rollup pay on s.order_id = pay.order_id
  left join revenue_rollup rev on s.order_id = rev.order_id
  left join {{ ref('dim_customer') }} c on s.customer_id = c.customer_id
    and s.purchase_ts between c.valid_from and coalesce(c.valid_to, '2999-12-31')
  left join {{ ref('dim_date') }} d on cast(s.purchase_ts as date) = d.date_key
  left join {{ ref('dim_order_status') }} os on s.order_status = os.order_status
  left join {{ ref('dim_time') }} t
    on to_time(s.purchase_ts) = t.time_key
)
select * from final
