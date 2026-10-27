"""Command line.

    python -m pipeline generate --out data/sample --labels data/expected --orders 60
    python -m pipeline run --input data/sample --output out
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipeline.generate import generate
from pipeline.tasks import run_pipeline


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pipeline", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="run the pipeline end to end")
    run.add_argument("--input", type=Path, default=Path("data/sample"))
    run.add_argument("--output", type=Path, default=Path("out"))
    run.add_argument("--db", type=Path, default=None, help="SQLite file (default: <output>/warehouse.db)")
    run.add_argument("--run-id", default=None)

    gen = sub.add_parser("generate", help="create synthetic source files and expected-answer labels")
    gen.add_argument("--out", type=Path, default=Path("data/sample"))
    gen.add_argument("--labels", type=Path, default=Path("data/expected"))
    gen.add_argument("--orders", type=int, default=1000)
    gen.add_argument("--seed", type=int, default=42)

    args = parser.parse_args(argv)
    if args.command == "generate":
        labels = generate(args.out, args.labels, orders=args.orders, seed=args.seed)
        print(json.dumps({"orders": labels["orders"],
                          "order_events_source_rows": labels["order_events"]["source_rows"]}, indent=2))
        return 0

    report = run_pipeline(args.input, args.output, args.db, run_id=args.run_id)
    summary = {
        "run_id": report["run_id"],
        "status": report["status"],
        "tasks": {name: {"status": t["status"], "duration_s": t["duration_s"]}
                  for name, t in report["tasks"].items()},
    }
    print(json.dumps(summary, indent=2))
    return 0 if report["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
