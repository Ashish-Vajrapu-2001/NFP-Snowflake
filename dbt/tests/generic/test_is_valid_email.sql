{% test is_valid_email(model, column_name) %}
with validation_errors as (
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and not REGEXP_LIKE(
          {{ column_name }},
          '^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$'
      )
)
select * from validation_errors
{% endtest %}