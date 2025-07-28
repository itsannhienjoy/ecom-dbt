-- Test that order dates are not in the future
select 
    order_id,
    order_date_pk
from {{ ref('fct_orders') }} f
join {{ ref('dim_date') }} d on f.order_date_pk = d.date_pk
where d.date_key > current_date() 