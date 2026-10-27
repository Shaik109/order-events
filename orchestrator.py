"""Dependency-aware task runner with retries, timeouts, run status and notifications.

- Tasks run in topological order. Unknown dependencies and cycles are configuration errors.
- Each attempt runs in a worker thread so a timeout can be detected. Python cannot kill
  a thread, so a timed-out task keeps running in the background. It is not retried,
  because a retry could run two copies at once. The run fails and dependents are blocked.
- Retries use exponential backoff. Errors that subclass NonRetryableError are never retried.
- A task is blocked (UPSTREAM_FAILED, never executed) when a critical dependency did not
  succeed. This is what stops a downstream publication after a failed check.
- Every status change goes to a notifier. The final run report is written as JSON.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from pipeline.errors import NonRetryableError

log = logging.getLogger("pipeline")

SUCCESS = "SUCCESS"
FAILED = "FAILED"
TIMED_OUT = "TIMED_OUT"
UPSTREAM_FAILED = "UPSTREAM_FAILED"
BLOCKING = frozenset({FAILED, TIMED_OUT, UPSTREAM_FAILED})

Notifier = Callable[[dict], None]


@dataclass
class Task:
    name: str
    fn: Callable[[Any], dict | None]
    deps: tuple[str, ...] = ()
    retries: int = 0
    retry_delay: float = 1.0      # seconds, doubled on each retry
    timeout: float | None = None  # seconds per attempt
    critical: bool = True         # a failed critical dependency blocks this task


@dataclass
class TaskResult:
    name: str
    status: str
    attempts: int = 0
    duration_s: float = 0.0
    error: str | None = None
    metrics: dict = field(default_factory=dict)


def topological_order(tasks: list[Task]) -> list[Task]:
    by_name: dict[str, Task] = {}
    for task in tasks:
        if task.name in by_name:
            raise ValueError(f"duplicate task name: {task.name}")
        by_name[task.name] = task
    for task in tasks:
        for dep in task.deps:
            if dep not in by_name:
                raise ValueError(f"{task.name} depends on unknown task {dep}")
    ordered: list[Task] = []
    state: dict[str, str] = {}

    def visit(name: str, trail: tuple[str, ...]) -> None:
        if state.get(name) == "done":
            return
        if state.get(name) == "visiting":
            raise ValueError("dependency cycle: " + " -> ".join(trail + (name,)))
        state[name] = "visiting"
        for dep in by_name[name].deps:
            visit(dep, trail + (name,))
        state[name] = "done"
        ordered.append(by_name[name])

    for task in tasks:
        visit(task.name, ())
    return ordered


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _event(run_id: str, task: str, status: str, attempts: int, error: str | None = None) -> dict:
    return {"run_id": run_id, "task": task, "status": status,
            "attempts": attempts, "error": error, "at": _now()}


def _execute(task: Task, ctx: Any, run_id: str, notify: Notifier) -> TaskResult:
    started = time.monotonic()
    attempts = 0
    status, error = FAILED, None
    while True:
        attempts += 1
        pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix=task.name)
        future = pool.submit(task.fn, ctx)
        try:
            metrics = future.result(timeout=task.timeout) or {}
        except FutureTimeout:
            pool.shutdown(wait=False, cancel_futures=True)
            status = TIMED_OUT
            error = f"timed out after {task.timeout}s; worker thread cannot be killed, so not retried"
            break
        except Exception as exc:  # every failure is recorded and reported, never swallowed
            pool.shutdown(wait=False)
            status, error = FAILED, f"{type(exc).__name__}: {exc}"
            if isinstance(exc, NonRetryableError) or attempts > task.retries:
                break
            notify(_event(run_id, task.name, "RETRYING", attempts, error))
            time.sleep(task.retry_delay * 2 ** (attempts - 1))
        else:
            pool.shutdown(wait=False)
            return TaskResult(task.name, SUCCESS, attempts,
                              round(time.monotonic() - started, 3), metrics=metrics)

    result = TaskResult(task.name, status, attempts, round(time.monotonic() - started, 3), error=error)
    notify(_event(run_id, task.name, status, attempts, error))
    return result


def run_dag(
    tasks: list[Task],
    ctx: Any,
    *,
    run_id: str,
    notify: Notifier,
    report_path: Path | None = None,
) -> dict:
    ordered = topological_order(tasks)
    by_name = {t.name: t for t in tasks}
    results: dict[str, TaskResult] = {}
    started_at = _now()
    for task in ordered:
        blocking = [d for d in task.deps if results[d].status in BLOCKING and by_name[d].critical]
        if blocking:
            error = "not run: upstream failed: " + ", ".join(blocking)
            results[task.name] = TaskResult(task.name, UPSTREAM_FAILED, error=error)
            notify(_event(run_id, task.name, UPSTREAM_FAILED, 0, error))
            continue
        results[task.name] = _execute(task, ctx, run_id, notify)
    status = SUCCESS if all(r.status == SUCCESS for r in results.values()) else FAILED
    report = {
        "run_id": run_id,
        "status": status,
        "started_at": started_at,
        "finished_at": _now(),
        "tasks": {name: asdict(res) for name, res in results.items()},
    }
    if report_path is not None:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    return report


def file_notifier(log_path: Path) -> Notifier:
    """Log every event and append it to a JSON-lines file. Swap in email or Slack here."""
    log_path.parent.mkdir(parents=True, exist_ok=True)

    def notify(event: dict) -> None:
        line = json.dumps(event, default=str)
        log.log(logging.ERROR if event["status"] in BLOCKING else logging.INFO, line)
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    return notify


def new_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"run-{stamp}-{uuid.uuid4().hex[:8]}"
