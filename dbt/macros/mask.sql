{#- PII masking used by the analyst views. Keeps the domain / area code so the
    data is still useful for analysis, without identifying anyone. -#}
{% macro mask_email(column) -%}
    concat(left({{ column }}, 1), '***@', {{ email_domain(column) }})
{%- endmacro %}

{% macro mask_phone(column) -%}
    concat(left({{ column }}, 7), '***-****')
{%- endmacro %}

{% macro hash_pii(column) -%}
    {{ md5_hex('cast(' ~ column ~ ' as string)') }}
{%- endmacro %}
