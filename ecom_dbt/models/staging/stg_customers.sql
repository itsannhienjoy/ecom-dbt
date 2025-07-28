{{ config(materialized='table') }}

with source as (
    select * from {{ source('lnd', 'customers') }}
),
customers_cleaned as (
    select
    trim(customer_id) as customer_id,
    trim(customer_unique_id) as customer_unique_id,
    try_cast(customer_zip_code_prefix as integer) as customer_zip_code_prefix,
    nullif(trim(customer_city), '') as customer_city,
    nullif(trim(customer_state), '') as customer_state
    from source
    where customer_id is not null
)
select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
from customers_cleaned

