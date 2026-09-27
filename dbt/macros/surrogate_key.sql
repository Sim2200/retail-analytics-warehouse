{#- A stable hash of one or more columns, used for dimension surrogate keys. -#}
{% macro surrogate_key(columns) -%}
    {{ md5_hex(surrogate_key_expr(columns)) }}
{%- endmacro %}
{% macro surrogate_key_expr(columns) -%}
    {% for c in columns %}coalesce(cast({{ c }} as string), ''){% if not loop.last %} || '|' || {% endif %}{% endfor %}
{%- endmacro %}
