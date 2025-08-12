{{ config(
    materialized='incremental',
    on_schema_change='fail'
) }}

with base as (
  select
    oi.order_id,
    oi.order_item_id,
    upper(trim(oi.product_id)) as product_id,
    upper(trim(oi.seller_id))  as seller_id,
    to_timestamp(o.order_purchase_timestamp) as purchase_ts,
    o.customer_id,
    coalesce(oi.price, 0)        as price,
    coalesce(oi.freight_value, 0) as freight_value
  from {{ ref('stg_order_items') }} oi
  join {{ ref('stg_orders') }} o
    on oi.order_id = o.order_id
),

dimmed as (
  select
    b.*,
    c.customer_pk,
    p.product_pk,
    s.seller_pk,
    d.date_pk as order_date_pk
  from base b
  left join {{ ref('dim_customer') }} c
    on upper(trim(b.customer_id)) = upper(trim(c.customer_id))
   and b.purchase_ts between c.valid_from
                         and coalesce(c.valid_to, to_timestamp('2999-12-31'))
  left join {{ ref('dim_product') }} p
    on upper(trim(b.product_id)) = upper(trim(p.product_id))
   and b.purchase_ts between p.valid_from
                         and coalesce(p.valid_to, to_timestamp('2999-12-31'))
  left join {{ ref('dim_seller') }} s
    on upper(trim(b.seller_id)) = upper(trim(s.seller_id))
   and b.purchase_ts between s.valid_from
                         and coalesce(s.valid_to, to_timestamp('2999-12-31'))
  left join {{ ref('dim_date') }} d
    on cast(b.purchase_ts as date) = d.date_key
)

select
  order_id,
  order_item_id,
  customer_pk,
  product_pk,
  seller_pk,
  order_date_pk,
  price  as line_price,
  freight_value as line_freight,
  md5(order_id || '|' || order_item_id::string) as order_item_pk
from dimmed
where order_id is not null and order_item_id is not null