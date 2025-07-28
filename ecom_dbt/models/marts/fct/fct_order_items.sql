{{ config(
    materialized='incremental',
    on_schema_change='fail'
) }}

with src as (
  select
    oi.order_id,
    oi.order_item_id,
    oi.product_id,
    oi.seller_id,
    to_timestamp(oi.shipping_limit_date) as shipping_limit_ts,
    coalesce(oi.price, 0) as price,
    coalesce(oi.freight_value, 0) as freight_value
  from {{ ref('stg_order_items') }} as oi

),

enriched as (
  select
    src.*,
    c.customer_pk,
    p.product_pk,
    s.seller_pk,
    d.date_pk as order_date_pk,
    md5(concat(src.order_id, '|', src.order_item_id)) as order_item_unique_key
  from src
  join {{ ref('stg_orders') }} o on src.order_id = o.order_id
  left join {{ ref('dim_customer') }} c
    on o.customer_id = c.customer_id
      and o.order_purchase_timestamp between c.valid_from and coalesce(c.valid_to, '2999-12-31')
  left join {{ ref('dim_product') }} p
    on src.product_id = p.product_id
      and o.order_purchase_timestamp between p.valid_from and coalesce(p.valid_to, '2999-12-31')
  left join {{ ref('dim_seller') }} s
    on src.seller_id = s.seller_id
      and o.order_purchase_timestamp between s.valid_from and coalesce(s.valid_to, '2999-12-31')
  left join {{ ref('dim_date') }} d on cast(o.order_purchase_timestamp as date) = d.date_key
)

select
  order_id,
  order_item_id,
  customer_pk,
  product_pk,
  seller_pk,
  order_date_pk,
  price as line_price,
  freight_value as line_freight,
  order_item_unique_key
from enriched