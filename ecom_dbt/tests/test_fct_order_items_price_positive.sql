-- Test that all order item prices are positive
select *
from {{ ref('fct_order_items') }}
where line_price < 0 