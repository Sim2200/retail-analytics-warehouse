select
    cast(order_id as bigint) as order_id,
    cast(customer_id as bigint) as customer_id,
    cast(ordered_at as timestamp) as ordered_at,
    cast(status as varchar) as status,
    cast(channel as varchar) as channel,
    cast(promo_code as varchar) as promo_code
from {{ source('raw', 'orders') }}
