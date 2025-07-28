{{ config(materialized='table') }}

with source as (
    select * from {{ source('lnd', 'sellers') }}
)

select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
from source
where seller_id is not null