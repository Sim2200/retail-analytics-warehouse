{#- Cross-database helpers so every model runs on DuckDB and BigQuery unchanged.
    Each macro dispatches on the adapter; the default branch is DuckDB. -#}

{% macro date_key(ts) -%}
    {{ return(adapter.dispatch('date_key', 'retail_warehouse')(ts)) }}
{%- endmacro %}
{% macro default__date_key(ts) -%}
    cast(strftime({{ ts }}, '%Y%m%d') as int64)
{%- endmacro %}
{% macro bigquery__date_key(ts) -%}
    cast(format_date('%Y%m%d', date({{ ts }})) as int64)
{%- endmacro %}

{% macro date_name(d, fmt) -%}
    {{ return(adapter.dispatch('date_name', 'retail_warehouse')(d, fmt)) }}
{%- endmacro %}
{% macro default__date_name(d, fmt) -%}
    strftime({{ d }}, '{{ fmt }}')
{%- endmacro %}
{% macro bigquery__date_name(d, fmt) -%}
    format_date('{{ fmt }}', {{ d }})
{%- endmacro %}

{#- part: year | quarter | month | day | isodow | isoweek -#}
{% macro date_part(part, d) -%}
    {{ return(adapter.dispatch('date_part', 'retail_warehouse')(part, d)) }}
{%- endmacro %}
{% macro default__date_part(part, d) -%}
    {%- set p = {'isoweek': 'week'}.get(part, part) -%}
    cast(extract({{ p }} from {{ d }}) as int64)
{%- endmacro %}
{% macro bigquery__date_part(part, d) -%}
    {%- if part == 'isodow' -%}
        mod(extract(dayofweek from {{ d }}) + 5, 7) + 1
    {%- else -%}
        extract({{ part }} from {{ d }})
    {%- endif -%}
{%- endmacro %}

{#- one row per day from start to end inclusive, column date_day -#}
{% macro day_spine(start_date, end_date) -%}
    {{ return(adapter.dispatch('day_spine', 'retail_warehouse')(start_date, end_date)) }}
{%- endmacro %}
{% macro default__day_spine(start_date, end_date) -%}
    select cast(range as date) as date_day
    from range(cast('{{ start_date }}' as date), cast('{{ end_date }}' as date) + interval 1 day, interval 1 day)
{%- endmacro %}
{% macro bigquery__day_spine(start_date, end_date) -%}
    select date_day
    from unnest(generate_date_array(date '{{ start_date }}', date '{{ end_date }}', interval 1 day)) as date_day
{%- endmacro %}

{% macro md5_hex(expr) -%}
    {{ return(adapter.dispatch('md5_hex', 'retail_warehouse')(expr)) }}
{%- endmacro %}
{% macro default__md5_hex(expr) -%}
    md5({{ expr }})
{%- endmacro %}
{% macro bigquery__md5_hex(expr) -%}
    to_hex(md5({{ expr }}))
{%- endmacro %}

{% macro email_domain(col) -%}
    {{ return(adapter.dispatch('email_domain', 'retail_warehouse')(col)) }}
{%- endmacro %}
{% macro default__email_domain(col) -%}
    split_part({{ col }}, '@', 2)
{%- endmacro %}
{% macro bigquery__email_domain(col) -%}
    split({{ col }}, '@')[safe_offset(1)]
{%- endmacro %}

{#- BigQuery has no delete+insert; merge on the unique key does the same job. -#}
{% macro incremental_strategy() -%}
    {{ return('merge' if target.type == 'bigquery' else 'delete+insert') }}
{%- endmacro %}
