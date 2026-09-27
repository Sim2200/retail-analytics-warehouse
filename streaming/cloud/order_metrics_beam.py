"""Apache Beam job: the Flink job's one-minute event-time windows, on Dataflow.

Reads JSON order events from the Pub/Sub subscription, uses the `event_time`
message attribute as the element timestamp (so windows are event-time windows,
exactly like the Flink job), keeps completed orders, and writes one row per
tumbling one-minute window to BigQuery `stream.window_metrics`:

    window_start, window_end, order_count, revenue, avg_basket_size, emitted_at

`emitted_at` is the processing time when the window result was produced. The
runner (run_experiment.py) polls BigQuery and records when each window becomes
queryable, which gives the same end-to-end latency the local loader measures.

Local test:   python streaming/cloud/order_metrics_beam.py --runner DirectRunner ...
On Dataflow:  see run_experiment.py (it passes the pipeline options).
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import trigger, window

SCHEMA = ("window_start:TIMESTAMP,window_end:TIMESTAMP,order_count:INT64,revenue:FLOAT64,"
          "avg_basket_size:FLOAT64,emitted_at:TIMESTAMP")


def parse(msg) -> dict | None:
    try:
        v = json.loads(msg.data.decode())
    except (ValueError, UnicodeDecodeError):
        return None
    if v.get("status") != "completed":
        return None
    return {"order_total": float(v["order_total"]), "item_count": int(v["item_count"])}


class Aggregate(beam.CombineFn):
    """(count, revenue, items) per window."""

    def create_accumulator(self):
        return [0, 0.0, 0]

    def add_input(self, acc, o):
        acc[0] += 1
        acc[1] += o["order_total"]
        acc[2] += o["item_count"]
        return acc

    def merge_accumulators(self, accs):
        out = [0, 0.0, 0]
        for a in accs:
            out[0] += a[0]
            out[1] += a[1]
            out[2] += a[2]
        return out

    def extract_output(self, acc):
        return acc


class ToRow(beam.DoFn):
    def process(self, acc, w=beam.DoFn.WindowParam):
        count, revenue, items = acc
        if count == 0:
            return
        fmt = lambda ts: datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        yield {
            "window_start": fmt(w.start), "window_end": fmt(w.end),
            "order_count": count, "revenue": round(revenue, 2), "avg_basket_size": round(items / count, 4),
            "emitted_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }


def run(argv=None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--subscription", required=True, help="projects/<p>/subscriptions/<s>")
    ap.add_argument("--table", required=True, help="<project>:stream.window_metrics")
    ap.add_argument("--wait", action="store_true", help="block until the pipeline finishes (DirectRunner)")
    a, beam_args = ap.parse_known_args(argv)
    opts = PipelineOptions(beam_args, streaming=True, save_main_session=True)
    p = beam.Pipeline(options=opts)
    (p
     | "read" >> beam.io.ReadFromPubSub(subscription=a.subscription, with_attributes=True,
                                        timestamp_attribute="event_time")
     | "parse" >> beam.Map(parse)
     | "completed" >> beam.Filter(lambda o: o is not None)
     | "1min" >> beam.WindowInto(window.FixedWindows(60), trigger=trigger.AfterWatermark(),
                                 accumulation_mode=trigger.AccumulationMode.DISCARDING,
                                 allowed_lateness=0)
     | "aggregate" >> beam.CombineGlobally(Aggregate()).without_defaults()
     | "row" >> beam.ParDo(ToRow())
     | "bigquery" >> beam.io.WriteToBigQuery(a.table, schema=SCHEMA,
                                             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                             method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS))
    result = p.run()
    if a.wait:
        result.wait_until_finish()


if __name__ == "__main__":
    run()
