{{ config(materialized='table') }}

with source as (
    select * from {{ source('lnd', 'order_reviews') }}
)
select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date_clean as review_creation_date,
    review_answer_timestamp_clean as review_answer_timestamp
from source
where review_id is not null and order_id is not null