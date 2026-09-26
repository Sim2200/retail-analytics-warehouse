select
    cast(product_id as bigint) as product_id,
    cast(product_name as varchar) as product_name,
    cast(department as varchar) as department,
    cast(aisle_id as integer) as aisle_id,
    cast(unit_price as decimal(10, 2)) as list_price,
    cast(is_active as boolean) as is_active
from {{ source('raw', 'products') }}
