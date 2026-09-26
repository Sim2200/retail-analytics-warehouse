-- Customer state used by the snapshot: the initial load, overridden by the
-- update batch when the var include_customer_updates is true. Types are cast
-- here once so every downstream model can rely on them.
with initial as (
    select * from {{ source('raw', 'customers') }}
),

updates as (
    select * from {{ source('raw', 'customer_updates') }}
    where {{ var('include_customer_updates') }}
),

unioned as (
    select * from initial
    union all
    select * from updates
),

latest as (
    select
        *,
        row_number() over (partition by customer_id order by updated_at desc) as rn
    from unioned
)

select
    cast(customer_id as bigint) as customer_id,
    cast(first_name as varchar) as first_name,
    cast(last_name as varchar) as last_name,
    lower(cast(email as varchar)) as email,
    cast(phone as varchar) as phone,
    cast(street_address as varchar) as street_address,
    cast(city as varchar) as city,
    cast(state as varchar) as state,
    cast(segment as varchar) as segment,
    cast(signup_date as date) as signup_date,
    cast(updated_at as timestamp) as updated_at
from latest
where rn = 1
