{{
    config(
        materialized = 'incremental',
        unique_key = 'review_id',
        on_schema_change = 'fail'
    )
}}

with src as (
  select
    r.review_id,
    r.order_id,
    o.customer_id,
    coalesce(review_score, 0) as review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.review_creation_date,
    r.review_answer_timestamp,
    row_number() over (partition by r.review_id order by r.review_creation_date desc) as rn
  from {{ ref('stg_order_reviews') }} r
  left join {{ ref('stg_orders') }} o on r.order_id = o.order_id
  where r.review_comment_message is not null
),

deduplicated_src as (
  select * from src where rn = 1
),

final as (
  select
    s.review_id,
    s.order_id,
    c.customer_pk,
    d1.date_pk as creation_date_pk,
    d2.date_pk as answer_date_pk,
    t1.time_pk as review_time_pk,
    t2.time_pk as answer_time_pk,
    s.review_score,
    s.review_comment_title,
    s.review_comment_message,
    concat(year(cast(s.review_creation_date as timestamp)), '-', lpad(month(cast(s.review_creation_date as timestamp)), 2, '0')) as review_month
  from deduplicated_src s
  left join {{ ref('dim_customer') }} c on s.customer_id = c.customer_id
  left join {{ ref('dim_date') }} d1 on cast(s.review_creation_date as date) = d1.date_key
  left join {{ ref('dim_date') }} d2 on cast(s.review_answer_timestamp as date) = d2.date_key
  left join {{ ref('dim_time') }} t1 on to_time(to_char(cast(s.review_creation_date as timestamp), 'HH24:MI:SS')) = t1.time_key
  left join {{ ref('dim_time') }} t2 on to_time(to_char(cast(s.review_answer_timestamp as timestamp), 'HH24:MI:SS')) = t2.time_key
)

select * from final