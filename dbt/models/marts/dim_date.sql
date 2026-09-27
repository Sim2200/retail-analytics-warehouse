-- A calendar table generated in SQL (no seed file to maintain), on either engine.
with days as (
    {{ day_spine(var("date_start"), var("date_end")) }}
)

select
    {{ date_key('date_day') }} as date_key,
    date_day,
    {{ date_part('year', 'date_day') }} as year,
    {{ date_part('quarter', 'date_day') }} as quarter,
    {{ date_part('month', 'date_day') }} as month,
    cast({{ date_name('date_day', '%B') }} as string) as month_name,
    {{ date_part('isoweek', 'date_day') }} as week_of_year,
    {{ date_part('day', 'date_day') }} as day_of_month,
    {{ date_part('isodow', 'date_day') }} as day_of_week,
    cast({{ date_name('date_day', '%A') }} as string) as day_name,
    cast({{ date_part('isodow', 'date_day') }} >= 6 as bool) as is_weekend
from days
