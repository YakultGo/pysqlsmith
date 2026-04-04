"""PostgreSQL CLI entrypoint."""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass

if __package__ in (None, ""):
    # Allow `python pysqlsmith/postgres/main.py ...` from the repo root.
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
    from pysqlsmith.postgres.runtime import run
else:
    from .runtime import run


@dataclass(frozen=True)
class RunConfig:
    host: str = "127.0.0.1"
    port: int = 5432
    user: str = "postgres"
    password: str = ""
    dbname: str = "postgres"
    seed: int | None = None
    max_queries: int | None = None
    select_only: bool = False
    dry_run: bool = False
    dump_all_queries: bool = False
    exclude_catalog: bool = False
    verbose: bool = False


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="pysqlsmith PostgreSQL generator")
    parser.add_argument("--host", default="127.0.0.1", help="PostgreSQL host")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port")
    parser.add_argument("--user", default="postgres", help="PostgreSQL user")
    parser.add_argument("--password", default="", help="PostgreSQL password")
    parser.add_argument("--dbname", default="postgres", help="PostgreSQL database name")
    parser.add_argument("--seed", type=int, default=None, help="RNG seed (default: PID)")
    parser.add_argument("--max-queries", type=int, default=None, help="Stop after N queries")
    parser.add_argument("--select", action="store_true", help="Only generate SELECT statements")
    parser.add_argument("--dry-run", action="store_true", help="Print queries without executing")
    parser.add_argument("--dump-all-queries", action="store_true", help="Log all queries to file")
    parser.add_argument("--exclude-catalog", action="store_true", help="Exclude catalog relations during generation")
    parser.add_argument("--verbose", action="store_true", help="Show progress on stderr")
    return parser


def parse_args(argv: list[str] | None = None) -> RunConfig:
    args = build_arg_parser().parse_args(argv)
    return RunConfig(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname,
        seed=args.seed,
        max_queries=args.max_queries,
        select_only=args.select,
        dry_run=args.dry_run,
        dump_all_queries=args.dump_all_queries,
        exclude_catalog=args.exclude_catalog,
        verbose=args.verbose,
    )


def main(argv: list[str] | None = None):
    # Keep the entrypoint simple: parse once, then hand the frozen config to the runtime.
    config = parse_args(argv)
    run(config)


if __name__ == "__main__":
    main()
