{% macro money(expression) -%}
    cast(round({{ expression }}, 2) as numeric)
{%- endmacro %}
