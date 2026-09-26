-- For each customer, versions must form a clean timeline: exactly one current
-- row, and each version's valid_to equals the next version's valid_from.
with ordered as (
    select
        customer_id,
        valid_from,
        valid_to,
        is_current,
        lead(valid_from) over (partition by customer_id order by valid_from) as next_from
    from {{ ref('dim_customer') }}
)

select *
from ordered
where (next_from is not null and valid_to <> next_from)
   or (next_from is null and not is_current)
