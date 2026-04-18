"""Loggers: stderr progress, query dumper, stats collection, impedance feedback."""

from __future__ import annotations
import sys
from collections import defaultdict
from typing import Optional, Dict, TextIO

from pgsmith.prod import Prod
from pgsmith.exceptions import DutFailure, DutBroken, DutTimeout, DutSyntax
from pgsmith import impedance


class StatsVisitor:
    """Walk the AST to count nodes, max depth, and retries."""

    def __init__(self):
        self.nodes: int = 0
        self.maxlevel: int = 0
        self.retries: int = 0
        self.production_stats: Dict[str, int] = defaultdict(int)

    def visit(self, p: Prod):
        self.nodes += 1
        if p.level > self.maxlevel:
            self.maxlevel = p.level
        self.production_stats[type(p).__name__] += 1
        self.retries += p.retries


class Logger:
    def generated(self, query: Prod):
        pass

    def executed(self, query: Prod):
        pass

    def error(self, query: Prod, e: DutFailure):
        pass


class StatsCollectingLogger(Logger):
    def __init__(self):
        self.queries: int = 0
        self.sum_nodes: float = 0
        self.sum_height: float = 0
        self.sum_retries: float = 0

    def generated(self, query: Prod):
        self.queries += 1
        v = StatsVisitor()
        query.accept(v)
        self.sum_nodes += v.nodes
        self.sum_height += v.maxlevel
        self.sum_retries += v.retries


class CerrLogger(StatsCollectingLogger):
    def __init__(self, columns: int = 80):
        super().__init__()
        self.columns = columns
        self.errors: Dict[str, int] = defaultdict(int)

    def report(self):
        print(f"\nqueries: {self.queries}", file=sys.stderr)
        if self.queries > 0:
            print(f"AST stats (avg): height = {self.sum_height / self.queries:.1f}"
                  f" nodes = {self.sum_nodes / self.queries:.1f}", file=sys.stderr)

        sorted_errors = sorted(self.errors.items(), key=lambda x: -x[1])
        err_count = 0
        for msg, cnt in sorted_errors:
            err_count += cnt
            print(f"{cnt}\t{msg[:80]}", file=sys.stderr)
        if self.queries > 0:
            print(f"error rate: {err_count / self.queries:.4f}", file=sys.stderr)
        impedance.report()

    def generated(self, query: Prod):
        super().generated(query)
        if (10 * self.columns - 1) == self.queries % (10 * self.columns):
            self.report()

    def executed(self, query: Prod):
        if self.columns - 1 == (self.queries % self.columns):
            print("", file=sys.stderr)
        print(".", end="", file=sys.stderr, flush=True)

    def error(self, query: Prod, e: DutFailure):
        if self.columns - 1 == (self.queries % self.columns):
            print("", file=sys.stderr)
        first_line = str(e).split("\n", 1)[0]
        self.errors[first_line] += 1
        if isinstance(e, DutTimeout):
            ch = "t"
        elif isinstance(e, DutSyntax):
            ch = "S"
        elif isinstance(e, DutBroken):
            ch = "C"
        else:
            ch = "e"
        print(ch, end="", file=sys.stderr, flush=True)


class ImpedanceFeedback(Logger):
    """Logger that feeds execution results back to the impedance system."""

    def executed(self, query: Prod):
        impedance.record_ok(query)

    def error(self, query: Prod, e: DutFailure):
        impedance.record_bad(query)


class QueryDumper(Logger):
    """Logger that writes all generated queries to a file."""

    def __init__(self, path: str = "queries.log"):
        self._file = open(path, "w")

    def generated(self, query: Prod):
        self._file.write(query.out())
        self._file.write(";\n")
        self._file.flush()

    def __del__(self):
        if hasattr(self, "_file") and self._file:
            self._file.close()
