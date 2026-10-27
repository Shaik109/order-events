"""SQLite warehouse: schema, idempotent loads, quarantine, order summary, reconciliation.

Idempotency rules:
- customers and products are upserted by natural key. Each distinct version is kept
  in <entity>_history, keyed by (key, row_hash), so re-running a file changes nothing.
- order_events is append-only. An identical re-delivery counts as already loaded.
  A different payload for a loaded event_id is quarantined. The stored event is never overwritten.
- quarantine rows get deterministic keys, so re-running a file does not duplicate its rejects.
- order_summary is derived. It is rebuilt inside one transaction, so it always matches order_events.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from pipeline.schemas import BatchResult, EntitySpec

SCHEMA = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id   TEXT PRIMARY KEY NOT NULL,
    full_name     TEXT NOT NULL,
    email         TEXT,
    country       TEXT NOT NULL,
    signup_date   TEXT NOT NULL,
    row_hash      TEXT NOT NULL,
    loaded_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS customers_history (
    customer_id       TEXT NOT NULL,
    row_hash          TEXT NOT NULL,
    payload           TEXT NOT NULL,
    first_seen_run_id TEXT NOT NULL,
    PRIMARY KEY (customer_id, row_hash)
);
CREATE TABLE IF NOT EXISTS products (
    product_id    TEXT PRIMARY KEY NOT NULL,
    product_name  TEXT NOT NULL,
    category      TEXT NOT NULL,
    unit_price    TEXT NOT NULL,
    row_hash      TEXT NOT NULL,
    loaded_run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS products_history (
    product_id        TEXT NOT NULL,
    row_hash          TEXT NOT NULL,
    payload           TEXT NOT NULL,
    first_seen_run_id TEXT NOT NULL,
    PRIMARY KEY (product_id, row_hash)
);
CREATE TABLE IF NOT EXISTS order_events (
    event_id        TEXT PRIMARY KEY NOT NULL,
    order_id        TEXT NOT NULL,
    customer_id     TEXT NOT NULL,
    product_id      TEXT NOT NULL,
    event_type      TEXT NOT NULL CHECK (event_type IN ('placed', 'paid', 'shipped', 'cancelled')),
    quantity        INTEGER NOT NULL CHECK (quantity BETWEEN 1 AND 1000),
    unit_price      TEXT NOT NULL,
    event_timestamp TEXT NOT NULL,
    row_hash        TEXT NOT NULL,
    loaded_run_id   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_order_events_order ON order_events (order_id);
CREATE TABLE IF NOT EXISTS order_summary (
    order_id       TEXT PRIMARY KEY NOT NULL,
    customer_id    TEXT NOT NULL,
    current_status TEXT NOT NULL,
    placed_at      TEXT,
    event_count    INTEGER NOT NULL,
    gross_amount   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine (
    quarantine_key TEXT PRIMARY KEY NOT NULL,
    run_id         TEXT NOT NULL,
    entity         TEXT NOT NULL,
    source_file    TEXT NOT NULL,
    line_number    INTEGER,
    reasons        TEXT NOT NULL,
    raw            TEXT NOT NULL
);
"""

EVENT_COLUMNS = ["event_id", "order_id", "customer_id", "product_id",
                 "event_type", "quantity", "unit_price", "event_timestamp"]


@contextmanager
def open_warehouse(db_path: Path) -> Iterator[sqlite3.Connection]:
    """One transaction per block: commit on success, roll back on any exception."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        conn.executescript(SCHEMA)
        with conn:
            yield conn
    finally:
        conn.close()


def _qmarks(n: int) -> str:
    return ", ".join("?" * n)


def money(amount: Decimal) -> str:
    return format(amount.quantize(Decimal("0.01")), "f")


def existing_keys(conn: sqlite3.Connection, table: str, key: str) -> set[str]:
    return {row[0] for row in conn.execute(f"SELECT {key} FROM {table}")}


def upsert_dimension(conn: sqlite3.Connection, spec: EntitySpec, rows: list[dict], run_id: str) -> dict:
    """Insert new keys, update changed keys, and keep every version in history."""
    cols = spec.header
    current = {key: digest for key, digest in conn.execute(f"SELECT {spec.key}, row_hash FROM {spec.name}")}
    counts = {"inserted": 0, "updated": 0, "unchanged": 0}
    history, changed = [], []
    for row in rows:
        key, values = row[spec.key], [row[c] for c in cols]
        history.append((key, row["row_hash"], json.dumps(dict(zip(cols, values)), sort_keys=True), run_id))
        if key not in current:
            counts["inserted"] += 1
        elif current[key] == row["row_hash"]:
            counts["unchanged"] += 1
            continue
        else:
            counts["updated"] += 1
        changed.append(values + [row["row_hash"], run_id])
    conn.executemany(
        f"INSERT OR IGNORE INTO {spec.name}_history ({spec.key}, row_hash, payload, first_seen_run_id) "
        "VALUES (?, ?, ?, ?)",
        history,
    )
    update = ", ".join(f"{c} = excluded.{c}" for c in cols if c != spec.key)
    conn.executemany(
        f"INSERT INTO {spec.name} ({', '.join(cols)}, row_hash, loaded_run_id) "
        f"VALUES ({_qmarks(len(cols) + 2)}) "
        f"ON CONFLICT ({spec.key}) DO UPDATE SET {update}, "
        "row_hash = excluded.row_hash, loaded_run_id = excluded.loaded_run_id",
        changed,
    )
    return counts


def insert_events(conn: sqlite3.Connection, batch: BatchResult, run_id: str) -> tuple[dict, list[dict]]:
    """Append new events. Returns counts plus the rows that conflict with already-loaded events."""
    loaded = {event_id: digest for event_id, digest in conn.execute("SELECT event_id, row_hash FROM order_events")}
    new_rows: list[list] = []
    conflicts: list[dict] = []
    counts = {"inserted": 0, "already_loaded": 0, "conflicts": 0}
    for row in batch.valid:
        prior = loaded.get(row["event_id"])
        if prior is None:
            new_rows.append([row[c] for c in EVENT_COLUMNS] + [row["row_hash"], run_id])
            counts["inserted"] += 1
        elif prior == row["row_hash"]:
            counts["already_loaded"] += 1
        else:
            counts["conflicts"] += 1
            conflicts.append({
                "source_file": batch.source_file,
                "line_number": row["line_number"],
                "reasons": "conflicts_with_loaded_event",
                "raw": json.dumps({c: row[c] for c in EVENT_COLUMNS}, sort_keys=True, default=str),
            })
    conn.executemany(
        f"INSERT INTO order_events ({', '.join(EVENT_COLUMNS)}, row_hash, loaded_run_id) "
        f"VALUES ({_qmarks(len(EVENT_COLUMNS) + 2)})",
        new_rows,
    )
    return counts, conflicts


def write_quarantine(conn: sqlite3.Connection, run_id: str, entity: str, rejects: list[dict]) -> int:
    """Store rejected rows under a deterministic key. Returns how many were new."""
    rows = []
    for r in rejects:
        basis = f"{entity}|{r['source_file']}|{r['line_number']}|{r['raw']}"
        key = hashlib.sha256(basis.encode("utf-8")).hexdigest()
        rows.append((key, run_id, entity, r["source_file"], r["line_number"], r["reasons"], r["raw"]))
    before = conn.total_changes
    conn.executemany(
        "INSERT OR IGNORE INTO quarantine (quarantine_key, run_id, entity, source_file, line_number, reasons, raw) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    return conn.total_changes - before


def rebuild_order_summary(conn: sqlite3.Connection) -> int:
    """Derive one row per order. Status comes from the latest event time, not arrival order."""
    rows = conn.execute(
        "SELECT order_id, customer_id, event_type, quantity, unit_price, event_timestamp, event_id "
        "FROM order_events"
    ).fetchall()
    orders: dict[str, dict] = {}
    for order_id, customer_id, event_type, quantity, unit_price, ts, event_id in rows:
        when = datetime.fromisoformat(ts)
        o = orders.get(order_id)
        if o is None:
            o = orders[order_id] = {"customer_id": customer_id, "latest": None,
                                    "placed_at": None, "events": 0, "gross": Decimal("0")}
        o["events"] += 1
        marker = (when, event_id)  # event_id breaks ties deterministically
        if o["latest"] is None or marker > o["latest"][0]:
            o["latest"] = (marker, event_type)
        if event_type == "placed":
            o["gross"] += Decimal(unit_price) * quantity
            if o["placed_at"] is None or when < o["placed_at"]:
                o["placed_at"] = when
    conn.execute("DELETE FROM order_summary")
    conn.executemany(
        "INSERT INTO order_summary (order_id, customer_id, current_status, placed_at, event_count, gross_amount) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [
            (oid, o["customer_id"], o["latest"][1],
             o["placed_at"].isoformat() if o["placed_at"] else None,
             o["events"], money(o["gross"]))
            for oid, o in orders.items()
        ],
    )
    return len(orders)


def reconcile_checks(conn: sqlite3.Connection) -> list[dict]:
    """Independent checks that the warehouse agrees with itself. Any failure blocks publishing."""
    checks: list[dict] = []

    def scalar(sql: str):
        return conn.execute(sql).fetchone()[0]

    def check(name: str, expected, actual) -> None:
        checks.append({"check": name, "expected": expected, "actual": actual, "passed": expected == actual})

    check("referential_integrity_orphan_events", 0, scalar("""
        SELECT COUNT(*) FROM order_events e
        WHERE NOT EXISTS (SELECT 1 FROM customers c WHERE c.customer_id = e.customer_id)
           OR NOT EXISTS (SELECT 1 FROM products p WHERE p.product_id = e.product_id)"""))
    check("order_customer_consistency", 0, scalar("""
        SELECT COUNT(*) FROM (SELECT order_id FROM order_events
                              GROUP BY order_id HAVING COUNT(DISTINCT customer_id) > 1)"""))
    check("event_count_matches_summary",
          scalar("SELECT COUNT(*) FROM order_events"),
          scalar("SELECT COALESCE(SUM(event_count), 0) FROM order_summary"))
    check("order_count_matches_summary",
          scalar("SELECT COUNT(DISTINCT order_id) FROM order_events"),
          scalar("SELECT COUNT(*) FROM order_summary"))
    expected = Decimal("0")
    for price, qty in conn.execute("SELECT unit_price, quantity FROM order_events WHERE event_type = 'placed'"):
        expected += Decimal(price) * qty
    actual = sum((Decimal(g) for (g,) in conn.execute("SELECT gross_amount FROM order_summary")), Decimal("0"))
    check("gross_amount_matches_events", money(expected), money(actual))
    return checks
