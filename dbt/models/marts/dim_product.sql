select
    {{ surrogate_key(['product_id']) }} as product_sk,
    product_id,
    product_name,
    department,
    aisle_id,
    list_price,
    is_active
from {{ ref('stg_products') }}
