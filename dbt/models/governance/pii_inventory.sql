{#- Generated from the dbt graph at compile time: every column tagged
    meta.pii: true across all models, so the inventory can never drift
    from the YAML. -#}
{%- set rows = [] -%}
{%- if execute -%}
    {%- for node in graph.nodes.values() if node.resource_type == 'model' -%}
        {%- for col in node.columns.values() if col.meta.get('pii') -%}
            {%- do rows.append((node.schema, node.name, col.name)) -%}
        {%- endfor -%}
    {%- endfor -%}
{%- endif -%}

select * from (
    values
    {%- for r in rows %}
        ('{{ r[0] }}', '{{ r[1] }}', '{{ r[2] }}'){{ "," if not loop.last }}
    {%- endfor %}
)
