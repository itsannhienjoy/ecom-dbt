select
    p.payment_type,
    avg(p.payment_value) as avg_payment
from {{ ref('fct_orders') }} f
join {{ ref('dim_payment') }} p on f.order_id = p.order_id
group by p.payment_type
order by avg_payment desc