"""Synthetic, reproducible source data with known defects.

Inputs go to one directory. Expected answers go to a separate directory, so the
pipeline never reads them. The same seed always produces the same bytes.
"""
from __future__ import annotations

import csv
import json
import random
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from pipeline.schemas import ENTITIES

IST = timezone(timedelta(hours=5, minutes=30))
BASE = datetime(2026, 1, 1, tzinfo=timezone.utc)
FIRST = ["Asha", "Ravi", "Meera", "Kiran", "Priya", "Arjun", "Divya", "Sanjay"]
LAST = ["Rao", "Reddy", "Khan", "Iyer", "Sharma", "Nair", "Patel", "Das"]
COUNTRIES = ["IN", "US", "GB", "AE", "SG"]
CATEGORIES = ["books", "electronics", "home", "apparel", "toys"]
N_PRODUCTS = 40

# Each kind is injected k times. Its reject reason must contain the kind name.
REJECT_KINDS = [
    "conflicting_duplicate",
    "format:quantity",
    "missing:event_type",
    "range:quantity",
    "invalid_reference:customer_id",
    "naive_timestamp",
    "domain:event_type",
    "malformed:column_count",
]

EVENT_TYPE, CUSTOMER, QUANTITY, TIMESTAMP = 4, 2, 5, 7  # positions in order_events.csv


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        writer.writerows(rows)


def generate(out_dir: Path, labels_dir: Path, *, orders: int = 1000, seed: int = 42) -> dict:
    if orders < 1:
        raise ValueError("orders must be at least 1")
    rng = random.Random(seed)
    out_dir.mkdir(parents=True, exist_ok=True)
    labels_dir.mkdir(parents=True, exist_ok=True)

    # Customers: one exact duplicate row is appended (dropped as a duplicate).
    n_customers = max(10, orders // 5)
    customers = []
    for i in range(1, n_customers + 1):
        first, last = rng.choice(FIRST), rng.choice(LAST)
        email = f"{first}.{last}{i}@example.com".lower() if rng.random() < 0.9 else ""
        signup = date(2020, 1, 1) + timedelta(days=rng.randint(0, 2190))
        customers.append([f"C{i:05d}", f"{first} {last}", email, rng.choice(COUNTRIES), signup.isoformat()])
    customer_file = customers + [customers[0]]

    # Products: product 1 is a free sample, a boundary value that is valid.
    products = []
    for i in range(1, N_PRODUCTS + 1):
        cents = 0 if i == 1 else rng.randint(100, 50000)
        products.append([f"P{i:04d}", f"Product {i}", rng.choice(CATEGORIES), f"{cents // 100}.{cents % 100:02d}"])
    price = {p[0]: p[3] for p in products}

    # Clean order events: placed, then optionally paid, shipped or cancelled.
    clean: list[list[str]] = []
    placed_gross = Decimal("0")
    for order_no in range(1, orders + 1):
        order_id = f"O{order_no:07d}"
        customer_id = customers[rng.randrange(n_customers)][0]
        t0 = BASE + timedelta(minutes=rng.randint(0, 270 * 24 * 60))
        for line in range(1, rng.randint(1, 3) + 1):
            product_id = products[rng.randrange(N_PRODUCTS)][0]
            quantity = 1000 if (order_no == 1 and line == 1) else rng.randint(1, 10)  # max valid quantity
            stages = [("placed", t0)]
            if rng.random() < 0.8:
                stages.append(("paid", t0 + timedelta(minutes=5)))
                if rng.random() < 0.6:
                    stages.append(("shipped", t0 + timedelta(days=1)))
            elif rng.random() < 0.3:
                stages.append(("cancelled", t0 + timedelta(hours=2)))
            for etype, when in stages:
                shown = when.astimezone(IST) if rng.random() < 0.3 else when  # same instant, other offset
                clean.append([f"{order_id}-L{line}-{etype}", order_id, customer_id, product_id,
                              etype, str(quantity), price[product_id], shown.isoformat()])
                if etype == "placed":
                    placed_gross += Decimal(price[product_id]) * quantity
    rng.shuffle(clean)  # arrival order differs from event time (out-of-order data)
    clean_count = len(clean)

    # Injected defects are appended after the clean rows, so the originals arrive first.
    k = max(1, orders // 100)

    def corrupt(kind: str) -> list[str]:
        row = list(clean[rng.randrange(clean_count)])
        if kind == "conflicting_duplicate":
            row[QUANTITY] = "7" if row[QUANTITY] != "7" else "8"
        elif kind == "format:quantity":
            row[QUANTITY] = "ten"
        elif kind == "missing:event_type":
            row[EVENT_TYPE] = ""
        elif kind == "range:quantity":
            row[QUANTITY] = rng.choice(["0", "1001"])
        elif kind == "invalid_reference:customer_id":
            row[CUSTOMER] = "C99999"
        elif kind == "naive_timestamp":
            row[TIMESTAMP] = row[TIMESTAMP][:19]
        elif kind == "domain:event_type":
            row[EVENT_TYPE] = "refunded"
        elif kind == "malformed:column_count":
            row.append("unexpected")
        return row

    defects: list[list[str]] = []
    for kind in REJECT_KINDS:
        defects.extend(corrupt(kind) for _ in range(k))
    exact_duplicates = [list(clean[rng.randrange(clean_count)]) for _ in range(k)]
    events = clean + defects + exact_duplicates

    labels = {
        "seed": seed,
        "orders": orders,
        "customers": {"source_rows": len(customer_file), "valid": n_customers,
                      "duplicates_dropped": 1, "rejected": 0},
        "products": {"source_rows": N_PRODUCTS, "valid": N_PRODUCTS,
                     "duplicates_dropped": 0, "rejected": 0},
        "order_events": {
            "source_rows": len(events),
            "valid": clean_count,
            "duplicates_dropped": k,
            "rejected": k * len(REJECT_KINDS),
            "rejects_by_kind": {kind: k for kind in REJECT_KINDS},
        },
        "order_summary": {
            "orders": orders,
            "placed_gross_total": format(placed_gross.quantize(Decimal("0.01")), "f"),
        },
    }

    _write_csv(out_dir / ENTITIES["customers"].file_name, ENTITIES["customers"].header, customer_file)
    _write_csv(out_dir / ENTITIES["products"].file_name, ENTITIES["products"].header, products)
    _write_csv(out_dir / ENTITIES["order_events"].file_name, ENTITIES["order_events"].header, events)
    (labels_dir / "expected.json").write_text(json.dumps(labels, indent=2) + "\n", encoding="utf-8")
    return labels
