{% macro debug_dimension_data() %}
    {% set query %}
        select 
            'customer' as table_name,
            count(*) as total_rows,
            count(case when valid_to is null then 1 end) as current_records,
            min(valid_from) as min_valid_from,
            max(valid_from) as max_valid_from,
            min(valid_to) as min_valid_to,
            max(valid_to) as max_valid_to
        from {{ ref('dim_customer') }}
        union all
        select 
            'product' as table_name,
            count(*) as total_rows,
            count(case when valid_to is null then 1 end) as current_records,
            min(valid_from) as min_valid_from,
            max(valid_from) as max_valid_from,
            min(valid_to) as min_valid_to,
            max(valid_to) as max_valid_to
        from {{ ref('dim_product') }}
        union all
        select 
            'seller' as table_name,
            count(*) as total_rows,
            count(case when valid_to is null then 1 end) as current_records,
            min(valid_from) as min_valid_from,
            max(valid_from) as max_valid_from,
            min(valid_to) as min_valid_to,
            max(valid_to) as max_valid_to
        from {{ ref('dim_seller') }}
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% if execute %}
        {% for row in results %}
            {{ log("Table: " ~ row[0] ~ " | Total: " ~ row[1] ~ " | Current: " ~ row[2] ~ " | Valid_from range: " ~ row[3] ~ " to " ~ row[4] ~ " | Valid_to range: " ~ row[5] ~ " to " ~ row[6], info=true) }}
        {% endfor %}
    {% endif %}
{% endmacro %} 