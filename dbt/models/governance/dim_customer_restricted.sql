-- The analyst-facing customer dimension: same rows as marts.dim_customer, PII
-- masked. Names are dropped, email keeps its domain, phone keeps its area code,
-- and a hashed id lets analysts still count distinct people.
select
    customer_sk,
    customer_id,
    {{ hash_pii('email') }} as email_hash,
    {{ mask_email('email') }} as email_masked,
    {{ mask_phone('phone') }} as phone_masked,
    city,
    state,
    segment,
    signup_date,
    valid_from,
    valid_to,
    is_current
from {{ ref('dim_customer') }}
