select
    p.product_id,
    p.product_category_name,
    avg(r.review_score) as avg_review_score
from {{ ref('fct_order_reviews') }} r
join {{ ref('dim_product') }} p on r.product_pk = p.product_pk
group by p.product_id, p.product_category_name
order by avg_review_score desc