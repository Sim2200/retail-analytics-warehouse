select
    cast(order_id as int64) as order_id,
    cast(customer_id as int64) as customer_id,
    cast(ordered_at as timestamp) as ordered_at,
    cast(status as string) as status,
    cast(channel as string) as channel,
    cast(promo_code as string) as promo_code
from {{ source('raw', 'orders') }}
