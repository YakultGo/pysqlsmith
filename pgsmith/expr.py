"""PostgreSQL value and boolean expression productions."""

from __future__ import annotations
from typing import Optional, List, TYPE_CHECKING, Dict

from pgsmith.prod import Prod
from pgsmith.relmodel import SQLType, PGType, Column
from pgsmith.random_utils import d6, d9, d12, d20, d42, d100, random_pick, random_pick_iter

if TYPE_CHECKING:
    from pgsmith.schema_base import Schema


def _type_cast_name(type_: SQLType) -> str:
    if isinstance(type_, PGType):
        return type_.cast_name
    return type_.name


def _nearest_default_policy(p: Optional[Prod]) -> Optional[bool]:
    while p is not None:
        if hasattr(p, "_allow_default"):
            return bool(getattr(p, "_allow_default"))
        p = p.pprod
    return None


def _materialize_type(schema: "Schema", type_: SQLType) -> SQLType:
    # Pseudotypes are placeholders during generation, not renderable SQL types.
    # Before building an expression we collapse them to one concrete choice.
    if not isinstance(type_, PGType) or not type_.is_pseudotype:
        return type_

    candidates = [
        t for t in schema.concrete_type.get(type_, [])
        if isinstance(t, PGType) and not t.is_pseudotype
    ]
    if not candidates:
        raise RuntimeError(f"cannot materialize pseudotype {type_.name}")
    return random_pick(candidates)


def _pg_oid_type(schema: "Schema", oid: int) -> Optional[PGType]:
    oid_map = getattr(schema, "oid2type", None)
    if oid_map is None:
        return None
    return oid_map.get(int(oid))


def _pg_element_type(schema: "Schema", type_: SQLType) -> Optional[PGType]:
    if not isinstance(type_, PGType) or type_.typelem == PGType.INVALID_OID:
        return None
    return _pg_oid_type(schema, type_.typelem)


def _pg_array_type(schema: "Schema", type_: SQLType) -> Optional[PGType]:
    if not isinstance(type_, PGType) or type_.typarray == PGType.INVALID_OID:
        return None
    return _pg_oid_type(schema, type_.typarray)


def _pg_range_subtype(schema: "Schema", type_: SQLType) -> Optional[PGType]:
    if not isinstance(type_, PGType) or type_.rngsubtype == PGType.INVALID_OID:
        return None
    return _pg_oid_type(schema, type_.rngsubtype)


def _pg_multirange_type(schema: "Schema", type_: SQLType) -> Optional[PGType]:
    if not isinstance(type_, PGType) or type_.rngmultitypid == PGType.INVALID_OID:
        return None
    return _pg_oid_type(schema, type_.rngmultitypid)


def _pg_range_type(schema: "Schema", type_: SQLType) -> Optional[PGType]:
    if not isinstance(type_, PGType) or type_.rngrangetype == PGType.INVALID_OID:
        return None
    return _pg_oid_type(schema, type_.rngrangetype)


def _pseudo_family(type_: SQLType) -> Optional[str]:
    if not isinstance(type_, PGType) or not type_.is_pseudotype:
        return None

    name = type_.name
    if name in ("anyelement", "anynonarray", "anyenum"):
        return "element"
    if name == "anyarray":
        return "array"
    if name == "anyrange":
        return "range"
    if name == "anymultirange":
        return "multirange"
    if name in ("anycompatible", "anycompatiblenonarray"):
        return "compatible_element"
    if name == "anycompatiblearray":
        return "compatible_array"
    if name == "anycompatiblerange":
        return "compatible_range"
    if name == "anycompatiblemultirange":
        return "compatible_multirange"
    return None


def _bind_pseudotype(schema: "Schema", pseudo: SQLType, concrete: SQLType,
                     bindings: Dict[str, SQLType]) -> None:
    # One polymorphic decision often implies related family members such as
    # element/array or range/subtype. Record those relationships once here.
    family = _pseudo_family(pseudo)
    if family is None:
        return

    bindings[family] = concrete

    if family == "array":
        elem = _pg_element_type(schema, concrete)
        if elem is not None:
            bindings.setdefault("element", elem)
    elif family == "element":
        arr = _pg_array_type(schema, concrete)
        if arr is not None:
            bindings.setdefault("array", arr)
    elif family == "compatible_array":
        elem = _pg_element_type(schema, concrete)
        if elem is not None:
            bindings.setdefault("compatible_element", elem)
    elif family == "compatible_element":
        arr = _pg_array_type(schema, concrete)
        if arr is not None:
            bindings.setdefault("compatible_array", arr)
        for candidate in schema.concrete_type.get(SQLType.get("anycompatiblerange"), []):
            if isinstance(candidate, PGType) and not candidate.is_pseudotype and candidate.rngsubtype == getattr(concrete, "oid", 0):
                bindings.setdefault("compatible_range", candidate)
                mr = _pg_multirange_type(schema, candidate)
                if mr is not None:
                    bindings.setdefault("compatible_multirange", mr)
                break
    elif family == "range":
        subtype = _pg_range_subtype(schema, concrete)
        if subtype is not None:
            bindings.setdefault("element", subtype)
        mr = _pg_multirange_type(schema, concrete)
        if mr is not None:
            bindings.setdefault("multirange", mr)
    elif family == "multirange":
        rng = _pg_range_type(schema, concrete)
        if rng is not None:
            bindings.setdefault("range", rng)
            subtype = _pg_range_subtype(schema, rng)
            if subtype is not None:
                bindings.setdefault("element", subtype)
    elif family == "compatible_range":
        subtype = _pg_range_subtype(schema, concrete)
        if subtype is not None:
            bindings.setdefault("compatible_element", subtype)
        mr = _pg_multirange_type(schema, concrete)
        if mr is not None:
            bindings.setdefault("compatible_multirange", mr)
    elif family == "compatible_multirange":
        rng = _pg_range_type(schema, concrete)
        if rng is not None:
            bindings.setdefault("compatible_range", rng)
            subtype = _pg_range_subtype(schema, rng)
            if subtype is not None:
                bindings.setdefault("compatible_element", subtype)


def _resolve_pseudotype(schema: "Schema", pseudo: SQLType,
                        bindings: Dict[str, SQLType]) -> SQLType:
    # Reuse previous bindings whenever possible so a single function call keeps
    # all polymorphic arguments internally consistent.
    assert isinstance(pseudo, PGType) and pseudo.is_pseudotype

    family = _pseudo_family(pseudo)
    if family is not None:
        bound = bindings.get(family)
        if bound is not None and pseudo.consistent(bound):
            return bound

        if family == "array":
            elem = bindings.get("element")
            arr = _pg_array_type(schema, elem) if elem is not None else None
            if arr is not None and pseudo.consistent(arr):
                bindings[family] = arr
                return arr
        elif family == "element":
            arr = bindings.get("array")
            elem = _pg_element_type(schema, arr) if arr is not None else None
            if elem is not None and pseudo.consistent(elem):
                bindings[family] = elem
                return elem
        elif family == "compatible_array":
            elem = bindings.get("compatible_element")
            arr = _pg_array_type(schema, elem) if elem is not None else None
            if arr is not None and pseudo.consistent(arr):
                bindings[family] = arr
                return arr
        elif family == "compatible_element":
            arr = bindings.get("compatible_array")
            elem = _pg_element_type(schema, arr) if arr is not None else None
            if elem is not None and pseudo.consistent(elem):
                bindings[family] = elem
                return elem
            rng = bindings.get("compatible_range")
            elem = _pg_range_subtype(schema, rng) if rng is not None else None
            if elem is not None and pseudo.consistent(elem):
                bindings[family] = elem
                return elem
        elif family == "range":
            elem = bindings.get("element")
            if elem is not None:
                candidates = [
                    t for t in schema.concrete_type.get(pseudo, [])
                    if isinstance(t, PGType) and not t.is_pseudotype and t.rngsubtype == getattr(elem, "oid", 0)
                ]
                if candidates:
                    concrete = random_pick(candidates)
                    bindings[family] = concrete
                    return concrete
            mr = bindings.get("multirange")
            rng = _pg_range_type(schema, mr) if mr is not None else None
            if rng is not None and pseudo.consistent(rng):
                bindings[family] = rng
                return rng
        elif family == "multirange":
            rng = bindings.get("range")
            mr = _pg_multirange_type(schema, rng) if rng is not None else None
            if mr is not None and pseudo.consistent(mr):
                bindings[family] = mr
                return mr
        elif family == "compatible_range":
            elem = bindings.get("compatible_element")
            if elem is not None:
                candidates = [
                    t for t in schema.concrete_type.get(pseudo, [])
                    if isinstance(t, PGType) and not t.is_pseudotype and t.rngsubtype == getattr(elem, "oid", 0)
                ]
                if candidates:
                    concrete = random_pick(candidates)
                    bindings[family] = concrete
                    return concrete
            mr = bindings.get("compatible_multirange")
            rng = _pg_range_type(schema, mr) if mr is not None else None
            if rng is not None and pseudo.consistent(rng):
                bindings[family] = rng
                return rng
        elif family == "compatible_multirange":
            rng = bindings.get("compatible_range")
            mr = _pg_multirange_type(schema, rng) if rng is not None else None
            if mr is not None and pseudo.consistent(mr):
                bindings[family] = mr
                return mr

    concrete = _materialize_type(schema, pseudo)
    _bind_pseudotype(schema, pseudo, concrete, bindings)
    return concrete


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

        if isinstance(self.type, PGType) and self.type.is_pseudotype:
            self.type = _materialize_type(schema, self.type)

        if self.type is schema.inttype:
            self.expr = str(d100())
        elif self.type is schema.booltype:
            self.expr = schema.true_literal if d6() > 2 else schema.false_literal
        elif _is_insert_context(parent) and _nearest_default_policy(parent) is not False and d6() > 2:
            self.expr = "default"
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
        self._bindings: Dict[str, SQLType] = {}
        schema: Schema = self.scope.schema

        if type_constraint is schema.internaltype:
            self.fail("cannot call functions involving internal type")

        if agg:
            idx = schema.aggregates_returning_type
        elif d6() > 2:
            idx = schema.routines_returning_type
        else:
            idx = schema.parameterless_routines_returning_type

        # Choose a routine by return type first, then resolve its argument
        # types so polymorphic signatures remain self-consistent.
        while True:
            if not type_constraint:
                all_items = []
                for v in idx.values():
                    all_items.extend(v)
                self.proc = random_pick(all_items)
            else:
                items = idx.get(type_constraint, [])
                self.proc = random_pick(items)
                if self.proc and not type_constraint.consistent(self.proc.restype):
                    self.retry()
                    continue

            if not self.proc:
                self.retry()
                continue

            resolved_argtypes: List[SQLType] = []
            self._bindings = {}

            try:
                if type_constraint:
                    self.type = _materialize_type(schema, type_constraint)
                    if isinstance(self.proc.restype, PGType) and self.proc.restype.is_pseudotype:
                        _bind_pseudotype(schema, self.proc.restype, self.type, self._bindings)
                elif isinstance(self.proc.restype, PGType) and self.proc.restype.is_pseudotype:
                    self.type = None
                else:
                    self.type = self.proc.restype

                for at in self.proc.argtypes:
                    if at is schema.internaltype:
                        raise RuntimeError("internal arg type")
                    if isinstance(at, PGType) and at.is_pseudotype:
                        resolved = _resolve_pseudotype(schema, at, self._bindings)
                    else:
                        resolved = at
                    if resolved is schema.internaltype:
                        raise RuntimeError("internal resolved type")
                    resolved_argtypes.append(resolved)

                if self.type is None:
                    if isinstance(self.proc.restype, PGType) and self.proc.restype.is_pseudotype:
                        self.type = _resolve_pseudotype(schema, self.proc.restype, self._bindings)
                    else:
                        self.type = self.proc.restype
            except RuntimeError:
                self.retry()
                continue

            if self.type is schema.internaltype:
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
            items = schema.tables_with_columns_of_type.get(type_constraint, [])
            self.tab = random_pick(items)
            self.col = None
            for cand in self.tab.columns():
                if type_constraint.consistent(cand.type):
                    self.col = cand
                    break
            assert self.col is not None
        else:
            self.tab = random_pick(schema.tables)
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
