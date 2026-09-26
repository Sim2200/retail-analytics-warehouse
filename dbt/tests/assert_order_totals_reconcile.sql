-- fact_orders.order_total must equal the sum of its lines in fact_order_items,
-- to the cent. Any mismatch means the two facts drifted apart.
with lines as (
    select order_id, sum(line_total) as line_sum
    from {{ ref('fact_order_items') }}
    group by order_id
)

select o.order_id, o.order_total, l.line_sum
from {{ ref('fact_orders') }} as o
inner join lines as l on o.order_id = l.order_id
where abs(o.order_total - l.line_sum) > 0.005
