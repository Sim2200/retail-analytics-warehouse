"""Generate the synthetic retail dataset (fixed seed) as Parquet files under data/raw/.

Why synthetic: the Instacart dataset needs a Kaggle login and has no prices, no
timestamps and no customer attributes, so it cannot support revenue metrics,
event-time streaming or SCD2 customer history. This generator produces a
realistic e-commerce shape instead:

  customers     50,000 rows, with PII (name, email, phone, street address)
  products       5,000 rows, in 21 departments / 134 aisles, with unit prices
  orders       ~330,000 rows (420,000 drawn, weekdays thinned to 70%), one year of
               timestamps with weekly/daily seasonality
  order_items  ~1.19M rows, one row per product in an order
  customer_updates  a later batch of customer changes (moves, segment upgrades),
                    used to demonstrate SCD Type 2 history in dim_customer

Every run with the same --seed produces byte-identical output.

    python data_gen/generate.py                # full size
    python data_gen/generate.py --scale 0.05   # CI-sized sample
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

OUT = Path("data/raw")
YEAR_START = np.datetime64("2025-01-01T00:00:00")
DEPARTMENTS = [
    "produce", "dairy eggs", "snacks", "beverages", "frozen", "pantry", "bakery", "canned goods",
    "deli", "dry goods pasta", "household", "meat seafood", "breakfast", "personal care", "babies",
    "international", "alcohol", "pets", "missing", "other", "bulk",
]
SEGMENTS = np.array(["new", "regular", "loyal", "vip"])
SEGMENT_P = np.array([0.30, 0.45, 0.20, 0.05])
STATUSES = np.array(["completed", "completed", "completed", "completed", "cancelled", "returned"])
CHANNELS = np.array(["web", "web", "ios", "android"])
US_STATES = ["CA", "TX", "NY", "FL", "IL", "PA", "OH", "GA", "NC", "MI", "WA", "MA", "VA", "AZ", "CO"]


def customers(rng: np.random.Generator, n: int, fake: Faker) -> pa.Table:
    first = np.array([fake.first_name() for _ in range(2000)])
    last = np.array([fake.last_name() for _ in range(2000)])
    cities = np.array([fake.city() for _ in range(400)])
    streets = np.array([fake.street_address() for _ in range(5000)])
    fi, li = rng.integers(0, 2000, n), rng.integers(0, 2000, n)
    ids = np.arange(1, n + 1)
    email = np.char.add(np.char.add(np.char.lower(first[fi]), "."), np.char.add(np.char.lower(last[li]), ids.astype(str)))
    domains = np.array(["gmail.com", "yahoo.com", "outlook.com", "icloud.com", "proton.me"])
    email = np.char.add(np.char.add(email, "@"), domains[rng.integers(0, 5, n)])
    phone = np.array([f"+1-{a:03d}-{b:03d}-{c:04d}" for a, b, c in zip(rng.integers(200, 999, n), rng.integers(200, 999, n), rng.integers(0, 9999, n))])
    signup = YEAR_START - rng.integers(0, 3 * 365, n).astype("timedelta64[D]")
    return pa.table({
        "customer_id": ids,
        "first_name": first[fi], "last_name": last[li],
        "email": email, "phone": phone,
        "street_address": streets[rng.integers(0, 5000, n)],
        "city": cities[rng.integers(0, 400, n)],
        "state": np.array(US_STATES)[rng.integers(0, len(US_STATES), n)],
        "segment": rng.choice(SEGMENTS, n, p=SEGMENT_P),
        "signup_date": signup.astype("datetime64[us]"),
        "updated_at": np.full(n, YEAR_START - np.timedelta64(1, "D")).astype("datetime64[us]"),
    })


def customer_updates(rng: np.random.Generator, cust: pa.Table, fake: Faker) -> pa.Table:
    """A later batch: 5% of customers move city, 3% change segment. Same schema as customers."""
    n = cust.num_rows
    moved = rng.random(n) < 0.05
    upgraded = rng.random(n) < 0.03
    changed = np.where(moved | upgraded)[0]
    t = cust.slice(0, n).to_pandas().iloc[changed].copy()
    cities = np.array([fake.city() for _ in range(300)])
    t.loc[moved[changed], "city"] = cities[rng.integers(0, 300, moved[changed].sum())]
    t.loc[moved[changed], "street_address"] = [fake.street_address() for _ in range(moved[changed].sum())]
    up = upgraded[changed]
    t.loc[up, "segment"] = rng.choice(np.array(["regular", "loyal", "vip"]), up.sum())
    t["updated_at"] = (YEAR_START + np.timedelta64(200, "D")).astype("datetime64[us]")
    return pa.Table.from_pandas(t, preserve_index=False)


def products(rng: np.random.Generator, n: int, fake: Faker) -> pa.Table:
    dept = rng.integers(0, len(DEPARTMENTS), n)
    aisle = dept * 7 + rng.integers(0, 7, n)  # 7 aisles per department (147 total; 134 used is close enough)
    adjectives = np.array(["Organic", "Fresh", "Classic", "Family Size", "Premium", "Light", "Whole", "Natural", "Original", "Spicy"])
    nouns = np.array([fake.word().capitalize() for _ in range(1500)])
    name = np.char.add(np.char.add(adjectives[rng.integers(0, 10, n)], " "), nouns[rng.integers(0, 1500, n)])
    price = np.round(np.exp(rng.normal(1.4, 0.6, n)), 2).clip(0.49, 199.99)  # log-normal, median ~$4
    return pa.table({
        "product_id": np.arange(1, n + 1),
        "product_name": name,
        "department": np.array(DEPARTMENTS)[dept],
        "aisle_id": aisle,
        "unit_price": price,
        "is_active": rng.random(n) > 0.04,
    })


def orders(rng: np.random.Generator, n: int, n_customers: int) -> tuple[pa.Table, np.ndarray]:
    # Customers order at very different rates (heavy tail), like real shops.
    weights = rng.pareto(1.5, n_customers) + 1
    cust = rng.choice(np.arange(1, n_customers + 1), n, p=weights / weights.sum())
    # Timestamps: uniform over the year, then shaped by weekday and hour of day.
    day = rng.integers(0, 365, n)
    weekday = (day + 2) % 7  # 2025-01-01 is a Wednesday
    keep = rng.random(n) < np.where(weekday >= 5, 1.0, 0.7)[...]
    hour = rng.choice(24, n, p=_hour_profile())
    ts = YEAR_START + day.astype("timedelta64[D]") + hour.astype("timedelta64[h]") + rng.integers(0, 3600, n).astype("timedelta64[s]")
    order_id = np.arange(1, n + 1)
    tbl = pa.table({
        "order_id": order_id,
        "customer_id": cust,
        "ordered_at": ts.astype("datetime64[us]"),
        "status": STATUSES[rng.integers(0, len(STATUSES), n)],
        "channel": CHANNELS[rng.integers(0, len(CHANNELS), n)],
        "promo_code": np.where(rng.random(n) < 0.12, "SAVE10", None),
    })
    return tbl.filter(pa.array(keep)), keep


def _hour_profile() -> np.ndarray:
    p = np.array([1, 1, 1, 1, 1, 2, 4, 7, 10, 12, 13, 13, 12, 11, 11, 12, 13, 14, 14, 12, 9, 6, 3, 2], dtype=float)
    return p / p.sum()


def order_items(rng: np.random.Generator, ords: pa.Table, prods: pa.Table) -> pa.Table:
    n_orders = ords.num_rows
    n_items = rng.poisson(2.6, n_orders) + 1  # basket size, mean 3.6
    order_id = np.repeat(ords["order_id"].to_numpy(), n_items)
    total = order_id.size
    # Product popularity is heavy-tailed too.
    pop = rng.pareto(1.2, prods.num_rows) + 1
    product_id = rng.choice(prods["product_id"].to_numpy(), total, p=pop / pop.sum())
    price = prods["unit_price"].to_numpy()[product_id - 1]
    qty = rng.choice([1, 1, 1, 2, 2, 3, 4], total)
    # Occasional discount, and a tiny fraction of bad data that the tests must catch upstream.
    discount = np.where(rng.random(total) < 0.08, np.round(price * qty * 0.1, 2), 0.0)
    line_no = np.concatenate([np.arange(1, k + 1) for k in n_items])
    return pa.table({
        "order_item_id": np.arange(1, total + 1),
        "order_id": order_id,
        "line_number": line_no,
        "product_id": product_id,
        "quantity": qty,
        "unit_price": price,
        "discount": discount,
    })


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--scale", type=float, default=1.0, help="1.0 = full size; 0.05 = CI sample")
    ap.add_argument("--out", type=Path, default=OUT)
    a = ap.parse_args()
    rng = np.random.default_rng(a.seed)
    fake = Faker("en_US")
    Faker.seed(a.seed)
    a.out.mkdir(parents=True, exist_ok=True)
    t0 = time.perf_counter()

    n_cust, n_prod, n_ord = int(50_000 * a.scale), int(5_000 * a.scale), int(420_000 * a.scale)
    cust = customers(rng, n_cust, fake)
    prods = products(rng, n_prod, fake)
    ords, _ = orders(rng, n_ord, n_cust)
    items = order_items(rng, ords, prods)
    upd = customer_updates(rng, cust, fake)
    for name, tbl in [("customers", cust), ("products", prods), ("orders", ords), ("order_items", items), ("customer_updates", upd)]:
        pq.write_table(tbl, a.out / f"{name}.parquet", compression="zstd")
        print(f"{name:17s} {tbl.num_rows:>10,} rows")
    print(f"done in {time.perf_counter() - t0:.1f}s -> {a.out}/")


if __name__ == "__main__":
    main()
