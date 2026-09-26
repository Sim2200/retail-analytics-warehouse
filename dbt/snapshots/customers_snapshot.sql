{% snapshot customers_snapshot %}

{{
    config(
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
    )
}}

-- SCD Type 2: every time a customer's updated_at moves, the old row is closed
-- (dbt_valid_to set) and a new row opened. dim_customer is built from this.
    select * from {{ ref('stg_customers') }}

{% endsnapshot %}
