"""Register analyst.dim_customer_restricted as an authorized view on the marts dataset.

Analysts then get access to the `analyst` dataset only; the view can read
marts.dim_customer on their behalf without them having any permission on it.

    python scripts/bq_authorized_view.py --project <id>
"""

from __future__ import annotations

import argparse

from google.cloud import bigquery


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", required=True)
    ap.add_argument("--location", default="US")
    a = ap.parse_args()
    client = bigquery.Client(project=a.project, location=a.location)
    marts = client.get_dataset(f"{a.project}.marts")
    view = {"projectId": a.project, "datasetId": "analyst", "tableId": "dim_customer_restricted"}
    entries = list(marts.access_entries)
    if not any(e.entity_type == "view" and e.entity_id == view for e in entries):
        entries.append(bigquery.AccessEntry(None, "view", view))
        marts.access_entries = entries
        marts = client.update_dataset(marts, ["access_entries"])
    print("authorized views on marts:", [e.entity_id["tableId"] for e in marts.access_entries if e.entity_type == "view"])


if __name__ == "__main__":
    main()
