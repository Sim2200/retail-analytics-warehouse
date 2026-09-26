-- Order-level totals derived from the lines, so fact_orders never disagrees
-- with fact_order_items (a singular test checks this).
select
    order_id,
    count(*) as item_count,
    sum(quantity) as unit_count,
    {{ money('sum(quantity * unit_price)') }} as gross_amount,
    {{ money('sum(discount)') }} as discount_amount,
    {{ money('sum(line_total)') }} as order_total
from {{ ref('stg_order_items') }}
group by order_id
