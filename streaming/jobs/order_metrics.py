"""PyFlink job: one-minute event-time windows over the order stream.

Reads JSON order events from Kafka topic `orders` (event_time set by the producer),
assigns watermarks with 2 s of allowed out-of-orderness, and writes one row per
tumbling one-minute window to topic `order_metrics`:

    window_start, window_end, order_count, revenue, avg_basket_size, emitted_at

`emitted_at` is the Flink processing time when the window result was produced;
streaming/loader.py compares it (and its own load time) with window_end to
measure latency.

Submitted with:  flink run -py /opt/flink/jobs/order_metrics.py -d
"""

from pyflink.table import EnvironmentSettings, TableEnvironment

BOOTSTRAP = "kafka:29092"

t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
t_env.get_config().set("pipeline.name", "order-metrics-1min")
# Emit a window as soon as the watermark passes its end; keep state small.
t_env.get_config().set("table.exec.source.idle-timeout", "5 s")

t_env.execute_sql(f"""
    CREATE TABLE orders (
        order_id BIGINT,
        customer_id BIGINT,
        order_total DOUBLE,
        item_count INT,
        status STRING,
        event_time TIMESTAMP_LTZ(3),
        produced_at TIMESTAMP_LTZ(3),
        WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'orders',
        'properties.bootstrap.servers' = '{BOOTSTRAP}',
        'properties.group.id' = 'order-metrics',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601',
        'json.ignore-parse-errors' = 'true'
    )
""")

t_env.execute_sql(f"""
    CREATE TABLE order_metrics (
        window_start TIMESTAMP(3),
        window_end TIMESTAMP(3),
        order_count BIGINT,
        revenue DOUBLE,
        avg_basket_size DOUBLE,
        emitted_at TIMESTAMP_LTZ(3)
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'order_metrics',
        'properties.bootstrap.servers' = '{BOOTSTRAP}',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601'
    )
""")

t_env.execute_sql("""
    INSERT INTO order_metrics
    SELECT
        window_start,
        window_end,
        COUNT(*) AS order_count,
        SUM(order_total) AS revenue,
        AVG(CAST(item_count AS DOUBLE)) AS avg_basket_size,
        CURRENT_TIMESTAMP AS emitted_at
    FROM TABLE(
        TUMBLE(TABLE orders, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
    )
    WHERE status = 'completed'
    GROUP BY window_start, window_end
""").wait()
