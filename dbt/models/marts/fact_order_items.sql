{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail',
    )
}}

-- Grain: one row per product line in an order.
with items as (
    select
        i.*,
        o.ordered_at,
        o.customer_id
    from {{ ref('stg_order_items') }} as i
    inner join {{ ref('stg_orders') }} as o on i.order_id = o.order_id
    {% if is_incremental() %}
    where o.ordered_at > (select coalesce(max(ordered_at), '1900-01-01') from {{ this }})
    {% endif %}
),

customers as (
    select
        customer_sk,
        customer_id,
        valid_from,
        coalesce(valid_to, cast('9999-12-31' as timestamp)) as valid_to
    from {{ ref('dim_customer') }}
),

products as (
    select
        product_sk,
        product_id
    from {{ ref('dim_product') }}
)

select
    i.order_item_id,
    i.order_id,
    i.line_number,
    c.customer_sk,
    p.product_sk,
    i.product_id,
    cast(strftime(i.ordered_at, '%Y%m%d') as integer) as date_key,
    i.ordered_at,
    i.quantity,
    i.unit_price,
    i.discount,
    i.line_total
from items as i
left join customers as c
    on
        i.customer_id = c.customer_id
        and i.ordered_at >= c.valid_from
        and i.ordered_at < c.valid_to
left join products as p on i.product_id = p.product_id
