select
    p.product_id,
    p.product_category_name,
    sum(oi.line_price) as total_sales
from {{ ref('fct_order_items') }} oi
join {{ ref('dim_product') }} p on oi.product_pk = p.product_pk
group by p.product_id, p.product_category_name
order by total_sales desc
limit 20