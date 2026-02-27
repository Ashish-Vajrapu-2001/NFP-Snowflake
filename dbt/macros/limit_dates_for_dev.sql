{% macro limit_dates_for_dev(date_col) -%}
{% if target.name != 'prod' -%}
    AND {{ date_col }} >= DATEADD(
        'day',
        -{{ var('dev_num_days_to_include', 90) }},
        CURRENT_DATE
    )
{%- endif %}
{%- endmacro %}