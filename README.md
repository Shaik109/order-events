# Order Events Data Pipeline

An orchestrated, idempotent data pipeline built against the 72-hour data engineering challenge spec. It ingests customer, product and order-event files, validates and quarantines bad rows, loads a warehouse, reconciles the results, and publishes a consumer extract only when every critical check has passed.

The challenge did not name a domain, so this implementation uses an online shop's order events. The mechanics (dependent tasks, retries, timeouts, idempotent loads, quarantine, reconciliation) do not depend on the domain.

## 1. Quick start

Requires Python 3.10 or later. The runtime uses only the standard library. Tests use pytest.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

python -m pipeline generate                     # writes data/sample/ and data/expected/
python -m pipeline run --input data/sample --output out
pytest                                          # test suite
```

After a run, `out/` contains:

| File | Purpose |
|---|---|
| `warehouse.db` | SQLite warehouse: dimensions, event history, quarantine, order summary |
| `order_summary.csv` | Published consumer extract (written only if all checks pass) |
| `run_report.json` | Per-task status, attempts, duration, metrics and reconciliation results |
| `notifications.jsonl` | Retry, failure, timeout and blocked-task events |

The CLI exits with code 1 if the run does not succeed.

## 2. Data model

### Entities

| Entity | Source file | Grain | Natural key | Relationships |
|---|---|---|---|---|
| `customers` | `customers.csv` | one row per customer | `customer_id` | referenced by `order_events.customer_id` |
| `products` | `products.csv` | one row per product | `product_id` | referenced by `order_events.product_id` |
| `order_events` | `order_events.csv` | one row per lifecycle event of one order line | `event_id` | many events per `order_id`; FK to customers and products |
| `order_summary` | derived | one row per order | `order_id` | aggregates `order_events` |
| `quarantine` | derived | one row per rejected source line | `quarantine_key` | records `source_file` and `line_number` |
| `customers_history`, `products_history` | derived | one row per distinct version of a dimension row | (key, `row_hash`) | keeps change history |

### Data dictionary: `order_events`

| Column | Source type | Warehouse type | Nullable | Rule |
|---|---|---|---|---|
| `event_id` | text | TEXT PK | no | unique; a repeated identical row is dropped, a conflicting one is quarantined |
| `order_id` | text | TEXT | no | groups events into an order |
| `customer_id` | text | TEXT | no | matches `C` followed by at least 4 digits; must exist in `customers` |
| `product_id` | text | TEXT | no | matches `P` followed by at least 4 digits; must exist in `products` |
| `event_type` | text | TEXT CHECK | no | one of `placed`, `paid`, `shipped`, `cancelled` |
| `quantity` | integer | INTEGER CHECK | no | 1 to 1000 inclusive |
| `unit_price` | decimal | TEXT | no | non-negative, at most 100000, rounded to cents |
| `event_timestamp` | ISO-8601 | TEXT | no | must include an offset; normalised to UTC; naive values are rejected |
| `row_hash` | derived | TEXT | no | SHA-256 of the canonical schema columns |
| `loaded_run_id` | derived | TEXT | no | run that first loaded the row (lineage) |

`customers` and `products` follow the same pattern. Their emails are nullable (the only nullable customer field). `signup_date` must be an ISO date. `unit_price` must be a non-negative decimal, and zero is allowed for free samples.

### Order summary (derived)

| Column | Definition |
|---|---|
| `current_status` | `event_type` of the event with the latest `event_timestamp`; `event_id` breaks ties |
| `placed_at` | earliest `placed` event timestamp |
| `event_count` | number of events for the order |
| `gross_amount` | sum of `quantity × unit_price` over `placed` events, in decimal arithmetic |

### Conventions

- Table and column names are snake_case. Timestamps are ISO-8601 in UTC with an explicit offset.
- Natural keys are the IDs. No random surrogate keys are used, so IDs stay stable across reruns.
- `row_hash` is a SHA-256 over the schema columns only, which gives deterministic change detection.
- `quarantine_key` is a SHA-256 over (entity, file, line, raw content), so re-running a file does not duplicate its rejects.
- Every loaded row carries `loaded_run_id`, and every run writes its own `run_report.json`.

## 3. Architecture

```
check_sources
  ├── load_customers ──┐
  └── load_products ───┴── load_order_events ── build_order_summary ── reconcile ── publish
```

Each task runs in its own worker thread with its own warehouse transaction. The orchestrator (`pipeline/orchestrator.py`) handles ordering, retries, timeouts, blocking, status and notifications. The task definitions and DAG are in `pipeline/tasks.py`. Validation and schemas are in `pipeline/schemas.py`, and storage and reconciliation are in `pipeline/warehouse.py`.

## 4. Edge cases and how they are handled

| Case | Handling |
|---|---|
| Malformed input (wrong number of fields) | Row quarantined with `malformed:column_count` |
| Missing required field | Row quarantined with `missing:<column>` |
| Duplicate key, identical content | Dropped and counted in `duplicates_dropped` |
| Duplicate key, different content | Later copy quarantined as `conflicting_duplicate`; the first copy is kept |
| Unexpected schema change | Run fails before any load (`SchemaChangeError`, not retried) |
| Empty batch | Run fails (`EmptyBatchError`, not retried); publication is blocked |
| Partial failure and retry | Transient errors are retried with backoff. Each task is one transaction, so a failed attempt leaves no partial data |
| Late or out-of-order events | Accepted. Status is decided by event time, not arrival order |
| Late reference data (event arrives before its customer or product) | Quarantined as `invalid_reference:<column>`. Rerunning the file after the dimension has loaded accepts it |
| Invalid reference key | Quarantined as `invalid_reference:<column>` |
| Rerun of the same input | Nothing new is inserted or quarantined (verified by test) |
| Quantity boundaries | 1 and 1000 accepted; 0 and 1001 rejected |
| Free item (price 0.00) | Accepted; negative price rejected |
| Timezone offsets | `+05:30` and UTC instants for the same moment normalise identically |
| Naive timestamp (no offset) | Rejected, because guessing an offset would silently shift the event |
| Payload changed for an already-loaded event | Quarantined as `conflicts_with_loaded_event`. The stored event is never overwritten |

## 5. Orchestration

- **Dependencies.** Tasks run in topological order. Unknown dependencies and cycles are configuration errors.
- **Blocking.** A task whose critical dependency did not succeed is marked `UPSTREAM_FAILED` and never runs. This is what prevents `publish` after a failed `reconcile`.
- **Retries.** Transient errors are retried with exponential backoff. Errors that subclass `NonRetryableError` (schema change, missing file, empty batch, reconciliation failure) are not retried, because retrying cannot fix them.
- **Timeouts.** Each attempt has a deadline. Python cannot kill a thread, so a timed-out task keeps running in the background. It is therefore not retried, since a retry could run two copies at once, and the run fails.
- **Statuses.** `SUCCESS`, `FAILED`, `TIMED_OUT`, `UPSTREAM_FAILED`. Retries are reported as `RETRYING` events.
- **Notifications.** Retries, failures, timeouts and blocked tasks are logged and appended to `notifications.jsonl`. Replacing `file_notifier` with email or Slack is a one-function change.
- **Atomic publication.** The extract is written to a temporary file and renamed into place, so readers never see a partial file.

## 6. Idempotency, restart and backfill

- **Restart.** Rerun the whole pipeline. Every stage is idempotent: dimensions upsert by key, events insert only when new, quarantine keys are deterministic, and the summary is rebuilt inside one transaction.
- **Backfill.** Point `--input` at historical files. Events already in the warehouse are recognised by `event_id` and counted as `already_loaded`. The summary is rebuilt from the full event history, so it stays correct when older events arrive late.
- **History.** Events are append-only. Dimension changes are upserted, and every distinct version is kept in `*_history`, so nothing is overwritten silently.

## 7. Validation, reconciliation and observability

- **Row accounting.** For each source file, `source_rows = valid + rejected + duplicates_dropped`. If this does not balance, the run fails.
- **Reconciliation** (must all pass before publishing):
  - no orphan events (every customer and product reference exists)
  - each order has a single customer
  - total events equal the sum of `event_count` in the summary
  - distinct orders equal summary rows
  - gross amount recomputed from raw events equals the summary total
- **Metrics per task** in `run_report.json`: `source_rows`, `inserted`, `already_loaded`, `conflicts`, `duplicates_dropped`, `rejected_total`, `quarantined_new`, durations and attempt counts.

## 8. Tests

The suite has 28 tests in three files.

- `tests/test_schemas.py`: normalisation, numeric and timestamp boundaries, malformed and missing fields, duplicates (exact and conflicting), row accounting, and source-file checks.
- `tests/test_orchestrator.py`: topological order, cycle detection, retries, non-retryable errors, timeouts, blocking of publication after a failed check, and non-critical tasks.
- `tests/test_pipeline.py`: end-to-end runs. These check the pipeline's output against the generator's expected-answer labels, verify that a rerun changes nothing, verify that an empty batch and a failed reconciliation both block publication, check that status follows event time, and check that a changed payload is quarantined rather than overwritten.

## 9. Synthetic data

`python -m pipeline generate --orders N --seed S` produces reproducible inputs. Each defect kind is injected a known number of times, and the expected results are written to `data/expected/expected.json`, a separate directory the pipeline never reads.

The sample dataset (60 orders, seed 42) has 250 event rows. The pipeline accepts 241, drops 1 exact duplicate, and quarantines 8, one for each defect kind. That matches the labels exactly.

## 10. Measured performance

Measured on the sandbox container (single process, Python 3.12, SQLite on local disk) with 20,000 generated orders:

| Measure | Value |
|---|---|
| Event rows in source | 95,310 |
| Inserted | 93,510 |
| Quarantined | 1,600 |
| Duplicates dropped | 200 |
| Orders published | 20,000 |
| First run, wall clock | 5.0 s |
| Rerun on same input, wall clock | 4.25 s (0 inserted) |

Per-task durations on the first run: `load_order_events` 3.67 s, `build_order_summary` 0.63 s, `reconcile` 0.35 s, `load_customers` 0.12 s, `publish` 0.09 s.

Profiling `load_order_events` split its time roughly as follows: reading the CSV 0.41 s, validation 2.10 s, and loading existing event hashes 0.15 s. Within validation, fingerprinting (JSON encoding plus SHA-256 per row) took 0.77 s, normalisation 0.38 s, and rule checks 0.54 s. These were measured in separate passes, so they do not sum exactly to the total.

## 11. Optimization identified

The largest single cost is fingerprinting every row with `json.dumps` followed by SHA-256. It is about 20% of the event load. A cheaper canonical encoding, such as hashing a delimited tuple of the normalised values, would cut most of that cost while keeping deterministic, collision-resistant change detection. This is identified but not applied in the submitted version, so the stored hashes and the measurements above reflect the current code.

A second candidate is that `build_order_summary` rebuilds every order on each run (0.63 s here). Rebuilding only the orders touched by the batch would make it incremental. This would need a change-tracking step, which is why it is listed as the next step rather than implemented.

## 12. Design decisions and trade-offs

- **SQLite and the standard library.** The pipeline runs anywhere in seconds with no services to start. The SQL patterns (`ON CONFLICT` upserts, `INSERT OR IGNORE`, transactions per task) carry directly to PostgreSQL, which is what I would use in production.
- **A small custom DAG runner instead of Airflow or Prefect.** The whole orchestration is about 200 lines and easy to explain. What it lacks is a UI, a scheduler and distributed workers. Those belong in a real orchestrator.
- **Quarantine instead of dropping.** Every rejected row keeps its source file, line number and reasons, so it can be traced and reprocessed.
- **Events are immutable; dimensions are versioned.** Event history is the history that matters for orders, so events are never overwritten. Dimension changes are kept in history tables.
- **First accepted copy wins for conflicting duplicates.** This is deterministic, but it could keep the wrong copy. The conflicting copy is quarantined, so someone can review it.
- **Full summary rebuild.** It is simple and always consistent with the events. The cost is O(n) per run, which section 11 addresses.
- **Timeouts without killing threads.** Python offers no safe way to kill a thread. The trade-off is that a timed-out task may keep running, which is why timeouts fail the run rather than retry.

## 13. Limitations

- Single machine with SQLite, so only one run should write at a time. Production would add a lock or move to a shared database.
- A timed-out thread cannot be stopped.
- Notifications go to a file and the log. There is no paging.
- A late dimension means its events are quarantined until the file is rerun after the dimension arrives.
- Scheduling is outside this code. It would be driven by cron or CI.
- Business assumptions: `gross_amount` counts only `placed` events, and cancellations do not reverse amounts. Both are choices a real team would confirm with the business.
- This is not a cloud deployment. The challenge allowed a local run.

## 14. Layout

```
pipeline/
  __main__.py      CLI: run and generate
  tasks.py         task functions and the DAG
  orchestrator.py  DAG runner: retries, timeouts, blocking, status, notifications
  schemas.py       source schemas, row validation, duplicate handling
  warehouse.py     SQLite schema, idempotent loads, quarantine, summary, reconciliation
  generate.py      reproducible synthetic data with expected-answer labels
  errors.py        exception types (retryable vs non-retryable)
tests/             test_schemas.py, test_orchestrator.py, test_pipeline.py
data/sample/       sample inputs (generated)
data/expected/     expected answers, kept separate from inputs
```
