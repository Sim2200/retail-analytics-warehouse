{#- Generated from the dbt graph at compile time: every column tagged
    meta.pii: true across all models, so the inventory can never drift
    from the YAML. Written as a UNION ALL of literals because BigQuery has
    no VALUES clause in FROM. -#}
{%- set rows = [] -%}
{%- if execute -%}
    {%- for node in graph.nodes.values() if node.resource_type == 'model' -%}
        {%- for col in node.columns.values() if col.meta.get('pii') -%}
            {%- do rows.append((node.schema, node.name, col.name)) -%}
        {%- endfor -%}
    {%- endfor -%}
{%- endif -%}

{%- for r in rows %}
select
    '{{ r[0] }}' as schema_name,
    '{{ r[1] }}' as model_name,
    '{{ r[2] }}' as column_name
{%- if not loop.last %}
union all
{%- endif %}
{%- endfor %}
