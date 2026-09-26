select
    cast(order_item_id as bigint) as order_item_id,
    cast(order_id as bigint) as order_id,
    cast(line_number as integer) as line_number,
    cast(product_id as bigint) as product_id,
    cast(quantity as integer) as quantity,
    cast(unit_price as decimal(10, 2)) as unit_price,
    cast(discount as decimal(10, 2)) as discount,
    {{ money('quantity * unit_price - discount') }} as line_total
from {{ source('raw', 'order_items') }}
