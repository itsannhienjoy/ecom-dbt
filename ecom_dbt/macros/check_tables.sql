{% macro check_tables() %}
    {% set query %}
        select table_name from information_schema.tables 
        where table_schema = 'LND' and table_catalog = 'ECOM'
        order by table_name
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% if execute %}
        {% for row in results %}
            {{ log(row[0], info=true) }}
        {% endfor %}
    {% endif %}
{% endmacro %} 