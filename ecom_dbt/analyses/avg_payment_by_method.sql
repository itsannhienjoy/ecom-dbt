select
    pt.payment_type,
    avg(f.total_paid) as avg_payment
from {{ ref('fct_orders') }} f
join {{ ref('dim_payment_type') }} pt on f.payment_type_pk = pt.payment_type_pk
group by pt.payment_type
order by avg_payment desc