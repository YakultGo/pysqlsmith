"""MySQL runtime orchestration."""

from __future__ import annotations

import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from . import random_utils
from .exceptions import DutBroken, DutFailure
from .logger import CerrLogger, ImpedanceFeedback, Logger, QueryDumper
from .relmodel import Scope

if TYPE_CHECKING:
    from .main import RunConfig

VERSION = "0.4.0"


_global_cerr_logger: CerrLogger | None = None


def _sigint_handler(signum, frame):
    if _global_cerr_logger:
        _global_cerr_logger.report()
    sys.exit(1)


@dataclass
class RuntimeContext:
    """Everything needed to keep one fuzzing session running."""
    config: RunConfig
    seed: int
    schema: object
    scope: Scope
    loggers: list[Logger]


def build_runtime(config: RunConfig) -> RuntimeContext:
    global _global_cerr_logger

    seed_val = config.seed if config.seed is not None else os.getpid()
    random_utils.seed(seed_val)

    print(f"pysqlsmith {VERSION} [mysql] (seed={seed_val})", file=sys.stderr)

    from .schema import SchemaMySQL

    # Schema loading is the expensive one-time setup step. After this, generation is in-memory.
    schema = SchemaMySQL(config, exclude_catalog=config.exclude_catalog)
    schema.summary()

    scope = Scope()
    schema.fill_scope(scope)

    loggers: list[Logger] = [ImpedanceFeedback()]

    if config.verbose:
        cerr_logger = CerrLogger()
        _global_cerr_logger = cerr_logger
        loggers.append(cerr_logger)
        signal.signal(signal.SIGINT, _sigint_handler)
    else:
        _global_cerr_logger = None

    if config.dump_all_queries:
        loggers.append(QueryDumper())

    return RuntimeContext(
        config=config,
        seed=seed_val,
        schema=schema,
        scope=scope,
        loggers=loggers,
    )


def _report_if_needed():
    if _global_cerr_logger:
        _global_cerr_logger.report()


def _generate_one(ctx: RuntimeContext):
    # The schema owns the statement factory so this runtime can stay small and backend-specific.
    gen = ctx.schema.statement_factory(ctx.scope, ctx.config.select_only)
    for logger in ctx.loggers:
        logger.generated(gen)
    return gen, gen.out()


def run_dry(ctx: RuntimeContext) -> None:
    queries_generated = 0
    while True:
        _, sql = _generate_one(ctx)
        print(sql + ";")
        queries_generated += 1
        if ctx.config.max_queries and queries_generated >= ctx.config.max_queries:
            _report_if_needed()
            return


def run_live(ctx: RuntimeContext) -> None:
    from .dut import DutMySQL

    # The DUT is intentionally separate from schema loading: one reads metadata, the other executes SQL.
    dut = DutMySQL(ctx.config, log=ctx.config.dump_all_queries)
    queries_generated = 0
    try:
        while True:
            try:
                while True:
                    if ctx.config.max_queries:
                        queries_generated += 1
                        if queries_generated > ctx.config.max_queries:
                            _report_if_needed()
                            return

                    gen, sql = _generate_one(ctx)

                    try:
                        dut.test(sql)
                        for logger in ctx.loggers:
                            logger.executed(gen)
                    except DutBroken as exc:
                        for logger in ctx.loggers:
                            try:
                                logger.error(gen, exc)
                            except RuntimeError:
                                pass
                        raise
                    except Exception as exc:
                        if isinstance(exc, DutFailure):
                            for logger in ctx.loggers:
                                try:
                                    logger.error(gen, exc)
                                except RuntimeError:
                                    pass
            except DutBroken:
                time.sleep(1.0)
    finally:
        if hasattr(dut, "close"):
            dut.close()


def run(config: RunConfig) -> None:
    ctx = build_runtime(config)
    if config.dry_run:
        run_dry(ctx)
    else:
        run_live(ctx)
