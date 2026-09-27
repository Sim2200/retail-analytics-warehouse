{% macro money(expression) -%}
    cast(round({{ expression }}, 2) as numeric(12, 2))
{%- endmacro %}
