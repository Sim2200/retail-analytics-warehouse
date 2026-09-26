{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail',
    )
}}

-- One row per order. Incremental: only orders newer than the latest loaded one
-- are (re)processed; `dbt build --full-refresh` rebuilds everything.
with orders as (
    select * from {{ ref('stg_orders') }}
    {% if is_incremental() %}
    where ordered_at > (select coalesce(max(ordered_at), '1900-01-01') from {{ this }})
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

totals as (
    select * from {{ ref('int_order_totals') }}
)

select
    o.order_id,
    c.customer_sk,
    o.customer_id,
    cast(strftime(o.ordered_at, '%Y%m%d') as integer) as date_key,
    o.ordered_at,
    o.status,
    o.channel,
    o.promo_code,
    cast(coalesce(t.item_count, 0) as integer) as item_count,
    cast(coalesce(t.unit_count, 0) as integer) as unit_count,
    {{ money('coalesce(t.gross_amount, 0)') }} as gross_amount,
    {{ money('coalesce(t.discount_amount, 0)') }} as discount_amount,
    {{ money('coalesce(t.order_total, 0)') }} as order_total
from orders as o
left join customers as c
    on
        o.customer_id = c.customer_id
        and o.ordered_at >= c.valid_from
        and o.ordered_at < c.valid_to
left join totals as t on o.order_id = t.order_id
