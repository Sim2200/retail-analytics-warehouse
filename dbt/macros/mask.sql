{#- PII masking used by the analyst views. Keeps the domain / area code so the
    data is still useful for analysis, without identifying anyone. -#}
{% macro mask_email(column) -%}
    concat(left({{ column }}, 1), '***@', split_part({{ column }}, '@', 2))
{%- endmacro %}

{% macro mask_phone(column) -%}
    concat(left({{ column }}, 7), '***-****')
{%- endmacro %}

{% macro hash_pii(column) -%}
    md5(cast({{ column }} as varchar))
{%- endmacro %}
