"""Schema base class: type/operator/routine/aggregate registration and index generation."""

from __future__ import annotations
from importlib import import_module
import sys
from collections import defaultdict
from typing import List, Optional, Dict

from .relmodel import SQLType, Table, Op, Routine, Scope, Column
from .random_utils import random_pick, random_pick_iter


class Schema:
    def __init__(self):
        # Keep the grammar module name configurable so schema can decide which AST to instantiate.
        self.grammar_module: str = "pysqlsmith.postgres.grammar"
        self.booltype: Optional[SQLType] = None
        self.inttype: Optional[SQLType] = None
        self.internaltype: Optional[SQLType] = None
        self.arraytype: Optional[SQLType] = None

        self.types: List[SQLType] = []
        self.tables: List[Table] = []
        self.operators: List[Op] = []
        self.routines: List[Routine] = []
        self.aggregates: List[Routine] = []

        self.index: Dict[tuple, List[Op]] = defaultdict(list)

        self.routines_returning_type: Dict[SQLType, List[Routine]] = defaultdict(list)
        self.aggregates_returning_type: Dict[SQLType, List[Routine]] = defaultdict(list)
        self.parameterless_routines_returning_type: Dict[SQLType, List[Routine]] = defaultdict(list)
        self.tables_with_columns_of_type: Dict[SQLType, List[Table]] = defaultdict(list)
        self.operators_returning_type: Dict[SQLType, List[Op]] = defaultdict(list)
        self.concrete_type: Dict[SQLType, List[SQLType]] = defaultdict(list)
        self.equality_operators: Dict[SQLType, List[Op]] = defaultdict(list)
        self.base_tables: List[Table] = []

        self.version: str = ""
        self.version_num: int = 0

        self.true_literal: str = "true"
        self.false_literal: str = "false"

    def quote_name(self, identifier: str) -> str:
        raise NotImplementedError

    def get_grammar_module(self):
        return import_module(self.grammar_module)

    def grammar_attr(self, name: str):
        return getattr(self.get_grammar_module(), name)

    def statement_factory(self, s: Scope, select_only: bool = False):
        return self.grammar_attr("statement_factory")(s, select_only)

    def summary(self):
        print(f"Found {len(self.tables)} user table(s) in information schema.", file=sys.stderr)

    def fill_scope(self, s: Scope):
        # Scope is the bridge between loaded metadata and the random AST generator.
        for t in self.tables:
            s.tables.append(t)
        s.schema = self

    def register_operator(self, o: Op):
        self.operators.append(o)
        key = (id(o.left), id(o.right), id(o.result))
        self.index[key].append(o)

    def register_routine(self, r: Routine):
        self.routines.append(r)

    def register_aggregate(self, r: Routine):
        self.aggregates.append(r)

    def find_operator(self, left: SQLType, right: SQLType, result: SQLType) -> Optional[Op]:
        key = (id(left), id(right), id(result))
        ops = self.index.get(key, [])
        if not ops:
            return None
        return random_pick(ops)

    def generate_indexes(self):
        print("Generating indexes...", end="", file=sys.stderr)

        if not self.types:
            self.types = list(SQLType._typemap.values())

        # Precompute the lookups used heavily during random generation so AST nodes can stay simple.
        for type_ in self.types:
            assert type_ is not None
            for r in self.aggregates:
                if type_.consistent(r.restype):
                    self.aggregates_returning_type[type_].append(r)

            for r in self.routines:
                if not type_.consistent(r.restype):
                    continue
                self.routines_returning_type[type_].append(r)
                if not r.argtypes:
                    self.parameterless_routines_returning_type[type_].append(r)

            for t in self.tables:
                for c in t.columns():
                    if type_.consistent(c.type):
                        self.tables_with_columns_of_type[type_].append(t)
                        break

            for concrete in self.types:
                if type_.consistent(concrete):
                    self.concrete_type[type_].append(concrete)

            for o in self.operators:
                if type_.consistent(o.result):
                    self.operators_returning_type[type_].append(o)

        for t in self.tables:
            if t.is_base_table:
                self.base_tables.append(t)

        if self.booltype is not None:
            for o in self.operators:
                if o.name == "=" and o.left is o.right and o.result is self.booltype:
                    self.equality_operators[o.left].append(o)

        print("done.", file=sys.stderr)

        assert self.booltype is not None
        assert self.inttype is not None
        assert self.internaltype is not None
        assert self.arraytype is not None

    def has_sql_equality(self, type_: SQLType) -> bool:
        return bool(self.equality_operators.get(type_))
