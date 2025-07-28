{% macro debug_scd2_joins() %}
    {% set query %}
        select 
            count(*) as total_order_items,
            count(c.customer_pk) as customer_matches,
            count(p.product_pk) as product_matches,
            count(s.seller_pk) as seller_matches,
            count(c.customer_pk) + count(p.product_pk) + count(s.seller_pk) as total_matches
        from {{ ref('stg_order_items') }} oi
        join {{ ref('stg_orders') }} o on oi.order_id = o.order_id
        left join {{ ref('dim_customer') }} c 
            on o.customer_id = c.customer_id 
            and o.order_purchase_timestamp between c.valid_from and coalesce(c.valid_to, '2999-12-31')
        left join {{ ref('dim_product') }} p 
            on oi.product_id = p.product_id 
            and o.order_purchase_timestamp between p.valid_from and coalesce(p.valid_to, '2999-12-31')
        left join {{ ref('dim_seller') }} s 
            on oi.seller_id = s.seller_id 
            and o.order_purchase_timestamp between s.valid_from and coalesce(s.valid_to, '2999-12-31')
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% if execute %}
        {% for row in results %}
            {{ log("Total order items: " ~ row[0], info=true) }}
            {{ log("Customer matches: " ~ row[1], info=true) }}
            {{ log("Product matches: " ~ row[2], info=true) }}
            {{ log("Seller matches: " ~ row[3], info=true) }}
            {{ log("Total matches: " ~ row[4], info=true) }}
        {% endfor %}
    {% endif %}
{% endmacro %} 