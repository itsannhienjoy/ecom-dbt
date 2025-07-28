-- Test that review scores are within valid range (1-5)
select *
from {{ ref('fct_order_reviews') }}
where review_score < 1 or review_score > 5 