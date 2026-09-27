select
    cast(product_id as int64) as product_id,
    cast(product_name as string) as product_name,
    cast(department as string) as department,
    cast(aisle_id as int64) as aisle_id,
    cast(unit_price as numeric(10, 2)) as list_price,
    cast(is_active as bool) as is_active
from {{ source('raw', 'products') }}
