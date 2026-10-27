"""Source schemas, row-level validation and duplicate handling.

Every source file is checked against an explicit schema before any row is loaded.
Rejected rows are returned with their reasons and source line number. They are
never silently dropped, so they can be quarantined and traced back to the source.
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

from pipeline.errors import EmptyBatchError, MissingSourceError, SchemaChangeError

EVENT_TYPES = frozenset({"placed", "paid", "shipped", "cancelled"})
CUSTOMER_ID = re.compile(r"^C\d{4,}$")
PRODUCT_ID = re.compile(r"^P\d{4,}$")
MAX_QUANTITY = 1000
MAX_UNIT_PRICE = Decimal("100000")


@dataclass(frozen=True)
class Column:
    name: str
    required: bool = True  # False means the column may be empty (stored as NULL)


@dataclass(frozen=True)
class EntitySpec:
    name: str
    file_name: str
    key: str
    columns: tuple[Column, ...]
    references: dict[str, str] = field(default_factory=dict)  # local column -> referenced entity

    @property
    def header(self) -> list[str]:
        return [c.name for c in self.columns]


ENTITIES: dict[str, EntitySpec] = {
    "customers": EntitySpec(
        name="customers",
        file_name="customers.csv",
        key="customer_id",
        columns=(
            Column("customer_id"),
            Column("full_name"),
            Column("email", required=False),
            Column("country"),
            Column("signup_date"),
        ),
    ),
    "products": EntitySpec(
        name="products",
        file_name="products.csv",
        key="product_id",
        columns=(
            Column("product_id"),
            Column("product_name"),
            Column("category"),
            Column("unit_price"),
        ),
    ),
    "order_events": EntitySpec(
        name="order_events",
        file_name="order_events.csv",
        key="event_id",
        columns=(
            Column("event_id"),
            Column("order_id"),
            Column("customer_id"),
            Column("product_id"),
            Column("event_type"),
            Column("quantity"),
            Column("unit_price"),
            Column("event_timestamp"),
        ),
        references={"customer_id": "customers", "product_id": "products"},
    ),
}


def check_header(spec: EntitySpec, path: Path) -> None:
    """Fail before any row is read if the file is missing or its columns changed."""
    if not path.exists():
        raise MissingSourceError(f"{path.name} not found in {path.parent}")
    with path.open(newline="", encoding="utf-8") as fh:
        header = next(csv.reader(fh), [])
    if header != spec.header:
        raise SchemaChangeError(f"{path.name}: expected columns {spec.header}, found {header}")


def read_source(spec: EntitySpec, path: Path) -> list[tuple[int, dict]]:
    """Return (line_number, raw_row) pairs. Fails on schema change or an empty batch."""
    check_header(spec, path)
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = [(reader.line_num, raw) for raw in reader]
    if not rows:
        raise EmptyBatchError(f"{path.name} contains no data rows")
    return rows


def _normalize(spec: EntitySpec, raw: dict) -> tuple[dict, list[str]]:
    reasons: list[str] = []
    if None in raw:  # more values than header columns
        reasons.append("malformed:column_count")
    row: dict = {}
    for col in spec.columns:
        value = raw.get(col.name)
        value = value.strip() if isinstance(value, str) else ""
        if value:
            row[col.name] = value
        else:
            row[col.name] = None
            if col.required:
                reasons.append(f"missing:{col.name}")
    return row, reasons


def _parse_date(text: str) -> str | None:
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def _parse_money(text: str) -> Decimal | None:
    """Non-negative, finite amount rounded to cents. Returns None when invalid."""
    try:
        amount = Decimal(text).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None
    return amount if amount.is_finite() and amount >= 0 else None


def _check_timestamp(row: dict, reasons: list[str]) -> None:
    text = row["event_timestamp"]
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        reasons.append("format:event_timestamp")
        return
    if parsed.tzinfo is None:
        # An offset is required. Guessing one would silently shift the event in time.
        reasons.append("naive_timestamp")
        return
    row["event_timestamp"] = parsed.astimezone(timezone.utc).isoformat()


def _check_row(entity: str, row: dict, reasons: list[str]) -> None:
    """Type and business-rule checks. Runs only when every required field is present."""
    if entity == "customers":
        if not CUSTOMER_ID.match(row["customer_id"]):
            reasons.append("format:customer_id")
        if row["email"] is not None and "@" not in row["email"]:
            reasons.append("format:email")
        signup = _parse_date(row["signup_date"])
        if signup is None:
            reasons.append("format:signup_date")
        else:
            row["signup_date"] = signup
    elif entity == "products":
        if not PRODUCT_ID.match(row["product_id"]):
            reasons.append("format:product_id")
        price = _parse_money(row["unit_price"])
        if price is None:
            reasons.append("format:unit_price")
        else:
            row["unit_price"] = format(price, "f")
    elif entity == "order_events":
        if not CUSTOMER_ID.match(row["customer_id"]):
            reasons.append("format:customer_id")
        if not PRODUCT_ID.match(row["product_id"]):
            reasons.append("format:product_id")
        if row["event_type"] not in EVENT_TYPES:
            reasons.append("domain:event_type")
        try:
            quantity = int(row["quantity"])
        except ValueError:
            reasons.append("format:quantity")
        else:
            if not 1 <= quantity <= MAX_QUANTITY:
                reasons.append("range:quantity")
            row["quantity"] = quantity
        price = _parse_money(row["unit_price"])
        if price is None:
            reasons.append("format:unit_price")
        elif price > MAX_UNIT_PRICE:
            reasons.append("range:unit_price")
        else:
            row["unit_price"] = format(price, "f")
        _check_timestamp(row, reasons)


def fingerprint(row: dict, spec: EntitySpec) -> str:
    """Deterministic hash of the schema columns only (line numbers and bookkeeping excluded)."""
    payload = {c: row[c] for c in spec.header}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _reject(source_file: str, line_no: int, reasons: list[str], raw: dict) -> dict:
    return {
        "source_file": source_file,
        "line_number": line_no,
        "reasons": ";".join(reasons),
        "raw": json.dumps(raw, default=str),
    }


@dataclass
class BatchResult:
    entity: str
    source_file: str
    valid: list[dict] = field(default_factory=list)
    rejected: list[dict] = field(default_factory=list)
    duplicates_dropped: int = 0

    def balanced(self, source_rows: int) -> bool:
        """Every source row is either valid, rejected, or a dropped exact duplicate."""
        return len(self.valid) + len(self.rejected) + self.duplicates_dropped == source_rows


def validate_batch(
    spec: EntitySpec,
    source_file: str,
    rows: list[tuple[int, dict]],
    known_keys: dict[str, set[str]] | None = None,
) -> BatchResult:
    """Validate every row.

    Exact duplicates (same key, same content) are dropped and counted.
    Conflicting duplicates (same key, different content) are rejected.
    The first accepted occurrence is the one that is kept.
    """
    known = known_keys or {}
    result = BatchResult(entity=spec.name, source_file=source_file)
    accepted: dict[str, str] = {}  # natural key -> fingerprint of the accepted row
    for line_no, raw in rows:
        row, reasons = _normalize(spec, raw)
        if not reasons:
            _check_row(spec.name, row, reasons)
        if not reasons:
            for column, ref_entity in spec.references.items():
                if row[column] not in known.get(ref_entity, set()):
                    reasons.append(f"invalid_reference:{column}")
        if reasons:
            result.rejected.append(_reject(source_file, line_no, reasons, raw))
            continue
        row["line_number"] = line_no
        digest = fingerprint(row, spec)
        row["row_hash"] = digest
        key = row[spec.key]
        if key in accepted:
            if accepted[key] == digest:
                result.duplicates_dropped += 1
            else:
                result.rejected.append(
                    _reject(source_file, line_no, [f"conflicting_duplicate:{spec.key}"], raw)
                )
            continue
        accepted[key] = digest
        result.valid.append(row)
    return result
