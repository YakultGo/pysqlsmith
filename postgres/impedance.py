"""Impedance feedback: track error rates per production and blacklist bad ones."""

from __future__ import annotations
import sys
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, Set

if TYPE_CHECKING:
    from .prod import Prod

_ok_count: Dict[str, int] = defaultdict(int)
_bad_count: Dict[str, int] = defaultdict(int)
_retries: Dict[str, int] = defaultdict(int)
_limited: Dict[str, int] = defaultdict(int)
_failed: Dict[str, int] = defaultdict(int)


def matched(prod_or_name) -> bool:
    if not isinstance(prod_or_name, str):
        name = type(prod_or_name).__name__
    else:
        name = prod_or_name

    if _bad_count[name] < 100:
        return True
    total = _bad_count[name] + _ok_count[name]
    if total == 0:
        return True
    error_rate = _bad_count[name] / total
    return error_rate <= 0.99


def retry(prod_or_name):
    name = type(prod_or_name).__name__ if not isinstance(prod_or_name, str) else prod_or_name
    _retries[name] += 1


def limit(prod_or_name):
    name = type(prod_or_name).__name__ if not isinstance(prod_or_name, str) else prod_or_name
    _limited[name] += 1


def fail(prod_or_name):
    name = type(prod_or_name).__name__ if not isinstance(prod_or_name, str) else prod_or_name
    _failed[name] += 1


class ImpedanceVisitor:
    """Visitor that records which production types appear in an AST."""

    def __init__(self, target: Dict[str, int]):
        self._target = target
        self._found: Set[str] = set()

    def visit(self, p: "Prod"):
        self._found.add(type(p).__name__)

    def finalize(self):
        for name in self._found:
            self._target[name] += 1


def record_ok(query: "Prod"):
    v = ImpedanceVisitor(_ok_count)
    query.accept(v)
    v.finalize()


def record_bad(query: "Prod"):
    v = ImpedanceVisitor(_bad_count)
    query.accept(v)
    v.finalize()


def report(file=None):
    if file is None:
        file = sys.stderr
    print("impedance report:", file=file)
    for name in sorted(_bad_count.keys()):
        bad = _bad_count[name]
        ok = _ok_count.get(name, 0)
        line = f"  {name}: {bad}/{ok} (bad/ok)"
        if not matched(name):
            line += " -> BLACKLISTED"
        print(line, file=file)


def report_json() -> str:
    import json
    items = []
    for name in sorted(_bad_count.keys()):
        items.append({
            "prod": name,
            "bad": _bad_count[name],
            "ok": _ok_count.get(name, 0),
            "limited": _limited.get(name, 0),
            "failed": _failed.get(name, 0),
            "retries": _retries.get(name, 0),
        })
    return json.dumps({"impedance": items}, indent=2)
