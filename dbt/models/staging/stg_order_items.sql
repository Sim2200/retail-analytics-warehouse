select
    cast(order_item_id as int64) as order_item_id,
    cast(order_id as int64) as order_id,
    cast(line_number as int64) as line_number,
    cast(product_id as int64) as product_id,
    cast(quantity as int64) as quantity,
    cast(unit_price as numeric(10, 2)) as unit_price,
    cast(discount as numeric(10, 2)) as discount,
    {{ money('quantity * unit_price - discount') }} as line_total
from {{ source('raw', 'order_items') }}
