{% macro email_structure_validation(email_col) -%}
    CASE
        WHEN {{ email_col }} RLIKE '^[a-zA-Z0-9._%+''-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        THEN TRUE
        ELSE FALSE
    END
{%- endmacro %}