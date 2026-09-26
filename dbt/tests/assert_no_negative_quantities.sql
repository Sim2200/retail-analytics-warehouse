-- Every order line must have a positive quantity and a non-negative price.
select *
from {{ ref('fact_order_items') }}
where quantity <= 0 or unit_price < 0 or line_total < 0
