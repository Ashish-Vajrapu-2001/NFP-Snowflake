{% macro resume_warehouse(warehouse) %}
    ALTER WAREHOUSE {{ warehouse }} RESUME IF SUSPENDED
{% endmacro %}

{% macro suspend_warehouse(warehouse) %}
    ALTER WAREHOUSE {{ warehouse }} SUSPEND
{% endmacro %}