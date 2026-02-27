{% test expected_column_type(model, column_name, expected_type) %}
with type_check as (
    select
        '{{ column_name }}' as col,
        '{{ expected_type }}' as expected,
        typeof({{ column_name }}) as actual
    from {{ model }}
    limit 1
)
select * from type_check where upper(actual) <> upper(expected)
{% endtest %}