-- A calendar table generated in SQL (no seed file to maintain). DuckDB's
-- range() table function returns one column named "range".
with days as (
    select cast(range as date) as date_day
    from range(
        cast('{{ var("date_start") }}' as date),
        cast('{{ var("date_end") }}' as date) + interval 1 day,
        interval 1 day
    )
)

select
    cast(strftime(date_day, '%Y%m%d') as integer) as date_key,
    date_day,
    cast(year(date_day) as integer) as year,
    cast(quarter(date_day) as integer) as quarter,
    cast(month(date_day) as integer) as month,
    cast(strftime(date_day, '%B') as varchar) as month_name,
    cast(week(date_day) as integer) as week_of_year,
    cast(day(date_day) as integer) as day_of_month,
    cast(isodow(date_day) as integer) as day_of_week,
    cast(strftime(date_day, '%A') as varchar) as day_name,
    cast(isodow(date_day) >= 6 as boolean) as is_weekend
from days
