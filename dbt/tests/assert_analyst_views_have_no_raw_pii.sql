-- The analyst schema must never expose a full email address or phone number.
select customer_sk, email_masked, phone_masked
from {{ ref('dim_customer_restricted') }}
where email_masked not like '_***@%'
   or phone_masked not like '+1-___-***-****'
