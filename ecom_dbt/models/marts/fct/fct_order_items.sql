{{ config(materialized='incremental', on_schema_change='fail') }}

with base as (
  select
    oi.order_id,
    oi.order_item_id,
    upper(trim(oi.product_id)) as product_id_norm,
    upper(trim(oi.seller_id))  as seller_id_norm,
    o.customer_id,
    upper(trim(o.customer_id)) as customer_id_norm,
    to_timestamp(o.order_purchase_timestamp) as purchase_ts,
    coalesce(oi.price, 0)        as price,
    coalesce(oi.freight_value, 0) as freight_value
  from {{ ref('stg_order_items') }} oi
  join {{ ref('stg_orders') }} o
    on oi.order_id = o.order_id
)

, dimmed as (
  select
    b.*,
    c.customer_pk,
    p.product_pk,
    s.seller_pk,
    d.date_pk as order_date_pk
  from base b
  left join {{ ref('dim_customer') }} c
    on upper(trim(b.customer_id)) = c.customer_id_norm

  left join {{ ref('dim_product') }} p
    on upper(trim(src.product_id)) = p.product_id_norm

  left join {{ ref('dim_seller') }} s
    on upper(trim(src.seller_id)) = s.seller_id_norm
  
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
  price        as line_price,
  freight_value as line_freight,
  md5(coalesce(order_id,'') || '|' || coalesce(order_item_id::string,'')) as order_item_pk
from dimmed
