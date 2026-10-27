"""Pipeline tasks and the DAG that wires them together.

    check_sources
      ├── load_customers ──┐
      └── load_products ───┴── load_order_events ── build_order_summary ── reconcile ── publish

order_events validates references against the warehouse, so it must run after the
dimension loads. A failed critical task blocks everything below it, including publish.
"""
from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path

from pipeline import warehouse as wh
from pipeline.errors import ReconciliationError
from pipeline.orchestrator import Task, file_notifier, new_run_id, run_dag
from pipeline.schemas import ENTITIES, check_header, read_source, validate_batch

PUBLISHED_COLUMNS = ["order_id", "customer_id", "current_status", "placed_at", "event_count", "gross_amount"]


@dataclass
class RunContext:
    run_id: str
    input_dir: Path
    output_dir: Path
    db_path: Path


def _check_balance(source_rows: int, batch) -> None:
    if not batch.balanced(source_rows):
        raise ReconciliationError(f"{batch.source_file}: row accounting does not balance")


def _summary(source_rows: int, batch, *, rejected_total: int, quarantined_new: int, **counts) -> dict:
    return {
        "source_rows": source_rows,
        "valid_in_batch": len(batch.valid),
        "duplicates_dropped": batch.duplicates_dropped,
        "rejected_total": rejected_total,
        "quarantined_new": quarantined_new,
        **counts,
    }


def check_sources(ctx: RunContext) -> dict:
    """Fail fast: every file present, with the declared header, before the warehouse is touched."""
    for spec in ENTITIES.values():
        check_header(spec, ctx.input_dir / spec.file_name)
    return {"sources_checked": len(ENTITIES)}


def _load_dimension(ctx: RunContext, entity: str) -> dict:
    spec = ENTITIES[entity]
    rows = read_source(spec, ctx.input_dir / spec.file_name)
    batch = validate_batch(spec, spec.file_name, rows)
    _check_balance(len(rows), batch)
    with wh.open_warehouse(ctx.db_path) as conn:
        counts = wh.upsert_dimension(conn, spec, batch.valid, ctx.run_id)
        quarantined = wh.write_quarantine(conn, ctx.run_id, entity, batch.rejected)
    return _summary(len(rows), batch, rejected_total=len(batch.rejected),
                    quarantined_new=quarantined, **counts)


def load_customers(ctx: RunContext) -> dict:
    return _load_dimension(ctx, "customers")


def load_products(ctx: RunContext) -> dict:
    return _load_dimension(ctx, "products")


def load_order_events(ctx: RunContext) -> dict:
    spec = ENTITIES["order_events"]
    rows = read_source(spec, ctx.input_dir / spec.file_name)
    with wh.open_warehouse(ctx.db_path) as conn:
        known = {ref: wh.existing_keys(conn, ref, ENTITIES[ref].key) for ref in spec.references.values()}
        batch = validate_batch(spec, spec.file_name, rows, known_keys=known)
        _check_balance(len(rows), batch)
        counts, conflicts = wh.insert_events(conn, batch, ctx.run_id)
        quarantined = wh.write_quarantine(conn, ctx.run_id, spec.name, batch.rejected + conflicts)
    return _summary(len(rows), batch, rejected_total=len(batch.rejected) + len(conflicts),
                    quarantined_new=quarantined, **counts)


def build_order_summary(ctx: RunContext) -> dict:
    with wh.open_warehouse(ctx.db_path) as conn:
        return {"orders": wh.rebuild_order_summary(conn)}


def reconcile(ctx: RunContext) -> dict:
    with wh.open_warehouse(ctx.db_path) as conn:
        checks = wh.reconcile_checks(conn)
    failed = [c for c in checks if not c["passed"]]
    if failed:
        raise ReconciliationError(f"{len(failed)} reconciliation check(s) failed: {failed}")
    return {"checks": checks}


def publish(ctx: RunContext) -> dict:
    """Write the consumer extract. Write to a temp file, then rename, so readers never see a partial file."""
    ctx.output_dir.mkdir(parents=True, exist_ok=True)
    with wh.open_warehouse(ctx.db_path) as conn:
        rows = conn.execute(
            f"SELECT {', '.join(PUBLISHED_COLUMNS)} FROM order_summary ORDER BY order_id"
        ).fetchall()
    target = ctx.output_dir / "order_summary.csv"
    tmp = ctx.output_dir / "order_summary.csv.tmp"
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(PUBLISHED_COLUMNS)
        writer.writerows(rows)
    os.replace(tmp, target)
    return {"rows_published": len(rows), "file": target.name}


def build_tasks(retry_delay: float = 1.0) -> list[Task]:
    return [
        Task("check_sources", check_sources, timeout=60),
        Task("load_customers", load_customers, deps=("check_sources",),
             retries=2, retry_delay=retry_delay, timeout=300),
        Task("load_products", load_products, deps=("check_sources",),
             retries=2, retry_delay=retry_delay, timeout=300),
        Task("load_order_events", load_order_events, deps=("load_customers", "load_products"),
             retries=2, retry_delay=retry_delay, timeout=600),
        Task("build_order_summary", build_order_summary, deps=("load_order_events",), timeout=600),
        Task("reconcile", reconcile, deps=("build_order_summary",), timeout=300),
        Task("publish", publish, deps=("reconcile",), timeout=300),
    ]


def run_pipeline(input_dir, output_dir, db_path=None, *, run_id=None, tasks=None, notify=None) -> dict:
    input_dir, output_dir = Path(input_dir), Path(output_dir)
    db_path = Path(db_path) if db_path else output_dir / "warehouse.db"
    run_id = run_id or new_run_id()
    ctx = RunContext(run_id, input_dir, output_dir, db_path)
    notify = notify or file_notifier(output_dir / "notifications.jsonl")
    return run_dag(
        tasks or build_tasks(),
        ctx,
        run_id=run_id,
        notify=notify,
        report_path=output_dir / "run_report.json",
    )
