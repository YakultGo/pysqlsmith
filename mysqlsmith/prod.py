"""Base class for grammar productions (AST nodes)."""

from __future__ import annotations
from io import StringIO
from typing import Optional, TYPE_CHECKING

from mysqlsmith import impedance

if TYPE_CHECKING:
    from mysqlsmith.relmodel import Scope


class Prod:
    def __init__(self, parent: Optional["Prod"] = None):
        self.pprod = parent
        if parent:
            self.level = parent.level + 1
            self.scope: Scope = parent.scope
        else:
            self.level = 0
            self.scope = None  # type: ignore
        self.retries: int = 0
        self.retry_limit: int = 100

    def indent(self, level: Optional[int] = None) -> str:
        lv = level if level is not None else self.level
        return "\n" + ("  " * lv)

    def out(self) -> str:
        raise NotImplementedError

    def __str__(self) -> str:
        return self.out()

    def match(self):
        if not impedance.matched(self):
            raise RuntimeError("impedance mismatch")

    def accept(self, visitor):
        visitor.visit(self)

    def fail(self, reason: str):
        impedance.fail(self)
        raise RuntimeError(reason)

    def retry(self):
        impedance.retry(self)
        if self.retries <= self.retry_limit:
            self.retries += 1
            return
        impedance.limit(self)
        raise RuntimeError(f"excessive retries in {type(self).__name__}")
