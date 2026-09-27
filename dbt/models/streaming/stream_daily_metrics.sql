-- Streaming aggregates (one row per one-minute window from Flink) rolled up by
-- calendar day and joined to dim_date, with the end-to-end latency the loader
-- measured for each window.
select
    d.date_day,
    d.day_name,
    count(*) as windows,
    sum(w.order_count) as orders,
    {{ money('sum(w.revenue)') }} as revenue,
    {{ money('avg(w.avg_basket_size)') }} as avg_basket_size,
    round(avg(w.latency_ms), 1) as avg_latency_ms,
    max(w.latency_ms) as max_latency_ms
from {{ source('stream', 'window_metrics') }} as w
inner join {{ ref('dim_date') }} as d
    on {{ date_key('w.window_start') }} = d.date_key
group by d.date_day, d.day_name
