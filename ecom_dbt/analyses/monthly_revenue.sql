select
    d.year,
    d.month,
    sum(f.total_revenue) as monthly_revenue
from {{ ref('dim_date') }} d
join {{ ref('fct_orders') }} f on d.date_pk = f.order_date_pk
group by d.year, d.month
order by d.year, d.month