{% snapshot customers_snapshot %}

{{
  config(
    target_schema='DEV',
    unique_key='customer_id',
    strategy='check',
    check_cols=['customer_unique_id', 'customer_zip_code_prefix', 'customer_city', 'customer_state'
    ]
  )
}}

select
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
from {{ ref('stg_customers') }}

{% endsnapshot %}
