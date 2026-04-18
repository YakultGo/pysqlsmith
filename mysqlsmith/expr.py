"""MySQL value and boolean expression productions."""

from __future__ import annotations
from typing import Optional, List, TYPE_CHECKING

from mysqlsmith.prod import Prod
from mysqlsmith.relmodel import SQLType, Column
from mysqlsmith.random_utils import d6, d9, d12, d20, d42, d100, random_pick

if TYPE_CHECKING:
    from mysqlsmith.schema_base import Schema


def _type_cast_name(type_: SQLType) -> str:
    cast_names = {
        "INT": "SIGNED",
        "DECIMAL": "DECIMAL(20,10)",
        "TEXT": "CHAR",
        "DATETIME": "DATETIME",
        "BIT": "UNSIGNED",
        "BLOB": "BINARY",
        "ENUM": "CHAR",
        "SET": "CHAR",
    }
    return cast_names.get(type_.name, type_.name)


def _nearest_default_policy(p: Optional[Prod]) -> Optional[bool]:
    while p is not None:
        if hasattr(p, "_allow_default"):
            return bool(getattr(p, "_allow_default"))
        p = p.pprod
    return None


def _mutation_target_table(p: Optional[Prod]):
    from .grammar import ModifyingStmt

    while p is not None:
        if isinstance(p, ModifyingStmt):
            return getattr(p, "victim", None)
        p = p.pprod
    return None


def _quote_sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _int_literal() -> str:
    base = d100() - d100()
    if d6() == 1:
        return "0"
    return str(base)


def _decimal_literal() -> str:
    whole = d100()
    frac = d100() - 1
    sign = "-" if d12() == 1 else ""
    return f"{sign}{whole}.{frac:02d}"


def _text_literal() -> str:
    samples = [
        "",
        "a",
        "test",
        "hello",
        "mysql",
        "sqlsmith",
        "user_1",
        "2024-01-01",
        "alpha beta",
    ]
    if d6() == 1:
        samples.append(f"str_{d100()}")
    return _quote_sql_string(random_pick(samples))


def _datetime_literal() -> str:
    year = 2020 + (d9() - 1)
    month = min(d12(), 12)
    day = min(d20(), 28)
    hour = d20() % 24
    minute = d42() % 60
    second = d42() % 60
    return _quote_sql_string(
        f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
    )


def _blob_literal() -> str:
    samples = ["00", "01", "7f", "4142", "48656c6c6f", "deadbeef"]
    return f"x'{random_pick(samples)}'"


def _enum_or_set_literal(scope, type_name: str) -> Optional[str]:
    candidates: list[str] = []
    attr = "enum_values" if type_name == "ENUM" else "set_values"

    def collect_from_rel(rel) -> None:
        for col in rel.columns():
            if col.type is not None and col.type.name == type_name:
                values = getattr(col, attr, [])
                if type_name == "SET":
                    candidates.extend(values)
                    if len(values) >= 2:
                        candidates.append(",".join(values[:2]))
                else:
                    candidates.extend(values)

    for rel in getattr(scope, "refs", []):
        collect_from_rel(rel)

    schema = getattr(scope, "schema", None)
    if schema is not None:
        for table in getattr(schema, "tables", []):
            collect_from_rel(table)

    if not candidates:
        return None
    return _quote_sql_string(random_pick(candidates))


def _preferred_routine_names(type_name: str, agg: bool) -> set[str]:
    if agg:
        mapping = {
            "INT": {"count", "sum", "avg", "max"},
            "DECIMAL": {"sum", "avg", "max"},
            "TEXT": {"group_concat"},
        }
    else:
        mapping = {
            "INT": {"abs", "round", "char_length", "locate", "last_insert_id", "ifnull", "nullif"},
            "DECIMAL": {"round", "ifnull", "nullif"},
            "TEXT": {"concat", "replace", "lower", "upper", "trim", "ltrim", "rtrim", "substring", "quote", "hex", "ifnull", "nullif"},
            "DATETIME": {"current_timestamp", "date", "ifnull", "nullif"},
        }
    return mapping.get(type_name, set())


def _preferred_return_types(schema: "Schema", scope) -> list[SQLType]:
    seen: list[SQLType] = []
    for rel in getattr(scope, "refs", []):
        for col in rel.columns():
            if col.type not in seen and col.type in schema.types:
                seen.append(col.type)
    for type_name in ("INT", "DECIMAL", "TEXT", "DATETIME"):
        type_ = SQLType.get(type_name)
        if type_ not in seen and type_ in schema.types:
            seen.append(type_)
    return seen


class ValueExpr(Prod):
    type: Optional[SQLType] = None

    def __init__(self, parent: Optional[Prod] = None):
        super().__init__(parent)

    @staticmethod
    def factory(p: Prod, type_constraint: Optional[SQLType] = None) -> "ValueExpr":
        try:
            # Central weighted chooser for scalar expressions. Richer nodes go
            # first; column and constant fallbacks keep recursion bounded.
            if d20() == 1 and p.level < d6() and WindowFunction.allowed(p):
                return WindowFunction(p, type_constraint)
            elif d42() == 1 and p.level < d6():
                return Coalesce(p, type_constraint)
            elif d42() == 1 and p.level < d6():
                return Nullif(p, type_constraint)
            elif p.level < d6() and d6() == 1:
                return FunCall(p, type_constraint)
            elif d12() == 1:
                return AtomicSubselect(p, type_constraint)
            elif p.level < d6() and d9() == 1:
                return CaseExpr(p, type_constraint)
            elif p.scope.refs and d20() > 1:
                return ColumnReference(p, type_constraint)
            else:
                return ConstExpr(p, type_constraint)
        except RuntimeError:
            pass
        p.retry()
        return ValueExpr.factory(p, type_constraint)


class ConstExpr(ValueExpr):
    def __init__(self, parent: Prod, type_constraint: Optional[SQLType] = None):
        super().__init__(parent)
        schema: Schema = self.scope.schema
        self.type = type_constraint if type_constraint else schema.inttype

        if _is_insert_context(parent) and _nearest_default_policy(parent) is not False and d6() > 2:
            self.expr = "default"
        elif self.type is schema.booltype:
            self.expr = schema.true_literal if d6() > 2 else schema.false_literal
        elif self.type is schema.inttype or self.type.name == "INT":
            self.expr = _int_literal()
        elif self.type.name == "DECIMAL":
            self.expr = _decimal_literal()
        elif self.type.name == "ENUM":
            self.expr = _enum_or_set_literal(self.scope, "ENUM") or _text_literal()
        elif self.type.name == "SET":
            self.expr = _enum_or_set_literal(self.scope, "SET") or _text_literal()
        elif self.type.name == "TEXT":
            self.expr = _text_literal()
        elif self.type.name == "DATETIME":
            self.expr = _datetime_literal()
        elif self.type.name == "BIT":
            self.expr = "1" if d6() > 1 else "0"
        elif self.type.name == "BLOB":
            self.expr = _blob_literal()
        else:
            self.expr = f"cast(null as {_type_cast_name(self.type)})"

    def out(self) -> str:
        return self.expr


class ColumnReference(ValueExpr):
    def __init__(self, parent: Prod, type_constraint: Optional[SQLType] = None):
        super().__init__(parent)
        if type_constraint:
            pairs = self.scope.refs_of_type(type_constraint)
            picked = random_pick(pairs)
            self.reference = f"{picked[0].ident()}.{picked[1].name}"
            self.type = picked[1].type
            assert type_constraint.consistent(self.type)
        else:
            r = random_pick(self.scope.refs)
            cols = r.columns()
            c = random_pick(cols)
            self.type = c.type
            self.reference = f"{r.ident()}.{c.name}"

    def out(self) -> str:
        return self.reference


class FunCall(ValueExpr):
    def __init__(self, parent: Prod, type_constraint: Optional[SQLType] = None, agg: bool = False):
        super().__init__(parent)
        self.is_aggregate = agg
        self.proc = None
        self.parms: List[ValueExpr] = []
        schema: Schema = self.scope.schema

        if type_constraint is schema.internaltype:
            self.fail("cannot call functions involving internal type")

        if agg:
            idx = schema.aggregates_returning_type
        elif d6() > 2:
            idx = schema.routines_returning_type
        else:
            idx = schema.parameterless_routines_returning_type

        while True:
            if not type_constraint:
                preferred_types = _preferred_return_types(schema, self.scope)
                chosen_type = random_pick(preferred_types) if preferred_types and d6() > 1 else None
                items = idx.get(chosen_type, []) if chosen_type is not None else []
                preferred_names = _preferred_routine_names(chosen_type.name, agg) if chosen_type is not None else set()
                preferred_items = [proc for proc in items if proc.name in preferred_names]
                if preferred_items and d6() > 1:
                    self.proc = random_pick(preferred_items)
                elif items:
                    self.proc = random_pick(items)
                else:
                    all_items = []
                    for v in idx.values():
                        all_items.extend(v)
                    self.proc = random_pick(all_items)
            else:
                items = idx.get(type_constraint, [])
                preferred_names = _preferred_routine_names(type_constraint.name, agg)
                preferred_items = [proc for proc in items if proc.name in preferred_names]
                if preferred_items and d6() > 1:
                    self.proc = random_pick(preferred_items)
                else:
                    self.proc = random_pick(items)
                if self.proc and not type_constraint.consistent(self.proc.restype):
                    self.retry()
                    continue

            if not self.proc:
                self.retry()
                continue

            resolved_argtypes = list(self.proc.argtypes)
            self.type = type_constraint if type_constraint else self.proc.restype

            if self.type is schema.internaltype:
                self.retry()
                continue

            if any(argtype is schema.internaltype for argtype in resolved_argtypes):
                self.retry()
                continue

            break

        for argtype in resolved_argtypes:
            assert argtype is not None
            expr = ValueExpr.factory(self, argtype)
            self.parms.append(expr)

    def out(self) -> str:
        parts = []
        for expr in self.parms:
            parts.append(f"cast({expr.out()} as {_type_cast_name(expr.type)})")
        if self.is_aggregate and not self.parms:
            inner = "*"
        else:
            inner = ("," + self.indent()).join(parts)
        return f"{self.proc.ident()}({inner})"

    def accept(self, visitor):
        visitor.visit(self)
        for p in self.parms:
            p.accept(visitor)


class AtomicSubselect(ValueExpr):
    def __init__(self, parent: Prod, type_constraint: Optional[SQLType] = None):
        super().__init__(parent)
        self.offset = d100() if d6() == 3 else d6()
        self.match()
        schema: Schema = self.scope.schema
        mutation_target = _mutation_target_table(parent)

        self.agg = None
        # Scalar subqueries are a useful fallback when the current scope lacks
        # a direct value of the requested type.
        if d6() < 2:
            if type_constraint:
                items = schema.aggregates_returning_type.get(type_constraint, [])
                self.agg = random_pick(items)
            else:
                self.agg = random_pick(schema.aggregates)
            if len(self.agg.argtypes) != 1:
                self.agg = None
            else:
                type_constraint = self.agg.argtypes[0]

        if type_constraint:
            items = [
                t for t in schema.tables_with_columns_of_type.get(type_constraint, [])
                if t is not mutation_target
            ]
            self.tab = random_pick(items)
            self.col = None
            for cand in self.tab.columns():
                if type_constraint.consistent(cand.type):
                    self.col = cand
                    break
            assert self.col is not None
        else:
            items = [t for t in schema.tables if t is not mutation_target]
            self.tab = random_pick(items)
            self.col = random_pick(self.tab.columns())

        self.type = self.agg.restype if self.agg else self.col.type

    def out(self) -> str:
        parts = ["(select "]
        if self.agg:
            parts.append(f"{self.agg.ident()}({self.col.name})")
        else:
            parts.append(self.col.name)
        parts.append(f" from {self.tab.ident()}")
        if not self.agg:
            parts.append(f" limit 1 offset {self.offset}")
        parts.append(")")
        parts.append(self.indent())
        return "".join(parts)


class ForeignKeySubselect(ValueExpr):
    def __init__(self, parent: Prod, schema_name: str, table_name: str, column_name: str, type_constraint: SQLType):
        super().__init__(parent)
        self.schema_name = schema_name
        self.table_name = table_name
        self.column_name = column_name
        self.offset = d6()
        self.type = type_constraint

    def out(self) -> str:
        return (
            f"(select {self.column_name} from {self.schema_name}.{self.table_name}"
            f" limit 1 offset {self.offset})"
        )


class CaseExpr(ValueExpr):
    def __init__(self, parent: Prod, type_constraint: Optional[SQLType] = None):
        super().__init__(parent)
        self.condition = BoolExpr.factory(self)
        self.true_expr = ValueExpr.factory(self, type_constraint)
        self.false_expr = ValueExpr.factory(self, self.true_expr.type)

        if self.false_expr.type is not self.true_expr.type:
            if self.true_expr.type.consistent(self.false_expr.type):
                self.true_expr = ValueExpr.factory(self, self.false_expr.type)
            else:
                self.false_expr = ValueExpr.factory(self, self.true_expr.type)
        self.type = self.true_expr.type

    def out(self) -> str:
        return (f"case when {self.condition.out()}"
                f" then {self.true_expr.out()}"
                f" else {self.true_expr.out()}"
                f" end{self.indent()}")

    def accept(self, visitor):
        visitor.visit(self)
        self.condition.accept(visitor)
        self.true_expr.accept(visitor)
        self.false_expr.accept(visitor)


class Coalesce(ValueExpr):
    def __init__(self, parent: Prod, type_constraint: Optional[SQLType] = None,
                 abbrev: str = "coalesce"):
        super().__init__(parent)
        self.abbrev_ = abbrev
        self.value_exprs: List[ValueExpr] = []

        first_expr = ValueExpr.factory(self, type_constraint)
        second_expr = ValueExpr.factory(self, first_expr.type)

        self.retry_limit = 20
        while first_expr.type is not second_expr.type:
            self.retry()
            if first_expr.type.consistent(second_expr.type):
                first_expr = ValueExpr.factory(self, second_expr.type)
            else:
                second_expr = ValueExpr.factory(self, first_expr.type)
        self.type = second_expr.type
        self.value_exprs.append(first_expr)
        self.value_exprs.append(second_expr)

    def out(self) -> str:
        inner = ("," + self.indent()).join(e.out() for e in self.value_exprs)
        return f"cast({self.abbrev_}({inner}) as {_type_cast_name(self.type)})"

    def accept(self, visitor):
        visitor.visit(self)
        for p in self.value_exprs:
            p.accept(visitor)


class Nullif(Coalesce):
    def __init__(self, parent: Prod, type_constraint: Optional[SQLType] = None):
        super().__init__(parent, type_constraint, "nullif")
        if not self.scope.schema.has_sql_equality(self.type):
            self.fail(f"nullif requires equality-compatible type, got {self.type.name}")


class BoolExpr(ValueExpr):
    def __init__(self, parent: Prod):
        super().__init__(parent)
        self.type = self.scope.schema.booltype

    @staticmethod
    def factory(p: Prod) -> "BoolExpr":
        try:
            # Boolean expressions use a separate chooser so predicate depth does
            # not overwhelm the rest of statement generation.
            if p.level > d100():
                return TruthValue(p)
            if d6() < 2:
                return ComparisonOp(p)
            elif d6() < 2:
                return BoolTerm(p)
            elif d6() < 2:
                return NullPredicate(p)
            elif d6() < 2:
                return TruthValue(p)
            else:
                return ExistsPredicate(p)
        except RuntimeError:
            pass
        p.retry()
        return BoolExpr.factory(p)


class TruthValue(BoolExpr):
    def __init__(self, parent: Prod):
        super().__init__(parent)
        schema = self.scope.schema
        self.op = schema.true_literal if d6() < 4 else schema.false_literal

    def out(self) -> str:
        return self.op


class NullPredicate(BoolExpr):
    def __init__(self, parent: Prod):
        super().__init__(parent)
        self.negate = "not " if d6() < 4 else ""
        self.expr = ValueExpr.factory(self)

    def out(self) -> str:
        return f"{self.expr.out()} is {self.negate}NULL"

    def accept(self, visitor):
        visitor.visit(self)
        self.expr.accept(visitor)


class ExistsPredicate(BoolExpr):
    def __init__(self, parent: Prod):
        super().__init__(parent)
        from .grammar import QuerySpec
        self.subquery = QuerySpec(self, self.scope)

    def out(self) -> str:
        return f"EXISTS ({self.indent()}{self.subquery.out()})"

    def accept(self, visitor):
        visitor.visit(self)
        self.subquery.accept(visitor)


class BoolBinop(BoolExpr):
    def __init__(self, parent: Prod):
        super().__init__(parent)
        self.lhs: Optional[ValueExpr] = None
        self.rhs: Optional[ValueExpr] = None

    def accept(self, visitor):
        visitor.visit(self)
        self.lhs.accept(visitor)
        self.rhs.accept(visitor)


class BoolTerm(BoolBinop):
    def __init__(self, parent: Prod):
        super().__init__(parent)
        self.op = "or" if d6() < 4 else "and"
        self.lhs = BoolExpr.factory(self)
        self.rhs = BoolExpr.factory(self)

    def out(self) -> str:
        return (f"({self.lhs.out()}) "
                f"{self.indent()}{self.op} ({self.rhs.out()})")


class ComparisonOp(BoolBinop):
    def __init__(self, parent: Prod):
        super().__init__(parent)
        schema: Schema = self.scope.schema
        items = schema.operators_returning_type.get(schema.booltype, [])
        self.oper = random_pick(items)

        self.lhs = ValueExpr.factory(self, self.oper.left)
        self.rhs = ValueExpr.factory(self, self.oper.right)

        if (self.oper.left is self.oper.right
                and self.lhs.type is not self.rhs.type):
            if self.lhs.type.consistent(self.rhs.type):
                self.lhs = ValueExpr.factory(self, self.rhs.type)
            else:
                self.rhs = ValueExpr.factory(self, self.lhs.type)

    def out(self) -> str:
        return f"{self.lhs.out()} {self.oper.name} {self.rhs.out()}"


class WindowFunction(ValueExpr):
    def __init__(self, parent: Prod, type_constraint: Optional[SQLType] = None):
        super().__init__(parent)
        self.match()
        self.aggregate = FunCall(self, type_constraint, agg=True)
        if self.aggregate.proc.name == "group_concat":
            self.fail("group_concat is not usable as a window function on this MySQL")
        self.type = self.aggregate.type
        self.partition_by: List[ColumnReference] = [ColumnReference(self)]
        while d6() > 2:
            self.partition_by.append(ColumnReference(self))
        self.order_by: List[ColumnReference] = [ColumnReference(self)]
        while d6() > 2:
            self.order_by.append(ColumnReference(self))

    def out(self) -> str:
        agg = self.aggregate.out()
        part = ",".join(cr.out() for cr in self.partition_by)
        order = ",".join(cr.out() for cr in self.order_by)
        return f"{self.indent()}{agg} over (partition by {part} order by {order})"

    @staticmethod
    def allowed(p: Prod) -> bool:
        from .grammar import SelectList, QuerySpec
        if isinstance(p, SelectList):
            return isinstance(p.pprod, QuerySpec)
        if isinstance(p, WindowFunction):
            return False
        if isinstance(p, ValueExpr):
            return WindowFunction.allowed(p.pprod)
        return False

    def accept(self, visitor):
        visitor.visit(self)
        self.aggregate.accept(visitor)
        for cr in self.partition_by:
            cr.accept(visitor)
        for cr in self.order_by:
            cr.accept(visitor)


def _is_insert_context(p: Prod) -> bool:
    from .grammar import InsertStmt
    return isinstance(p, InsertStmt)
