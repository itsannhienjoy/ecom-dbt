{{ config(materialized='table') }}

with times as (
    select distinct to_time(to_char(cast(order_purchase_timestamp as timestamp), 'HH24:MI:SS')) as tm from {{ ref('stg_orders') }}
    union
    select distinct to_time(to_char(cast(order_approved_at as timestamp), 'HH24:MI:SS')) from {{ ref('stg_orders') }}
    union
    select distinct to_time(to_char(cast(review_creation_date as timestamp), 'HH24:MI:SS')) from {{ ref('stg_order_reviews') }}
    union
    select distinct to_time(to_char(cast(review_answer_timestamp as timestamp), 'HH24:MI:SS')) from {{ ref('stg_order_reviews') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['tm']) }} as time_pk,
    tm as time_key,
    date_part('hour', tm) as hour,
    date_part('minute', tm) as minute,
    date_part('second', tm) as second
from times
