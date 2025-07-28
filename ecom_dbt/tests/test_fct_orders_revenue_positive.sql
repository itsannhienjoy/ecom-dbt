-- Test that all order revenue values are positive
select *
from {{ ref('fct_orders') }}
where total_revenue < 0 