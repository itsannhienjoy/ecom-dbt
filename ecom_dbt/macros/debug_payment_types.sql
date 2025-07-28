{% macro debug_payment_types() %}
    {% set query %}
        select distinct payment_type from {{ ref('stg_order_payments') }} order by payment_type
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% if execute %}
        {% for row in results %}
            {{ log(row[0], info=true) }}
        {% endfor %}
    {% endif %}
{% endmacro %} 