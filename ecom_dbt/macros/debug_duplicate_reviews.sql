{% macro debug_duplicate_reviews(limit=10) %}
  select 
    review_id,
    count(*) as count
  from {{ ref('stg_order_reviews') }}
  group by review_id
  having count(*) > 1
  order by count desc
  limit {{ limit }}
{% endmacro %} 