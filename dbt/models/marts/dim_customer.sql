-- SCD Type 2 customer dimension built from the snapshot. One row per customer
-- version; facts join on customer_id and the validity range to pick the version
-- that was current when the order happened.
select
    {{ surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_sk,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    street_address,
    city,
    state,
    segment,
    signup_date,
    cast(dbt_valid_from as timestamp) as valid_from,
    cast(dbt_valid_to as timestamp) as valid_to,
    cast(dbt_valid_to is null as boolean) as is_current
from {{ ref('customers_snapshot') }}
