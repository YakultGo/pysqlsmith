"""MySQL statement-level grammar productions."""

from __future__ import annotations
from typing import Optional, List

from .prod import Prod
from .relmodel import (
    SQLType, Column, Relation, NamedRelation, AliasedRelation, Table, Scope,
)
from .expr import ValueExpr, BoolExpr, ColumnReference, WindowFunction, ConstExpr, ForeignKeySubselect
from .random_utils import d6, d9, d12, d20, d42, d100, random_pick


# ---------------------------------------------------------------------------
# Table references
# ---------------------------------------------------------------------------

class TableRef(Prod):
    def __init__(self, parent: Prod):
        super().__init__(parent)
        self.refs: List[AliasedRelation] = []

    @staticmethod
    def factory(p: Prod) -> "TableRef":
        try:
            if p.level < d6():
                if d6() > 2 and p.level < d6():
                    return TableSubquery(p)
                if d6() > 2:
                    return JoinedTable(p)
            if d6() < 3:
                return TableOrQueryName(p)
            else:
                return TableSample(p)
        except RuntimeError:
            p.retry()
        return TableRef.factory(p)


class TableOrQueryName(TableRef):
    def __init__(self, parent: Prod):
        super().__init__(parent)
        self.t: NamedRelation = random_pick(self.scope.tables)
        self.refs.append(AliasedRelation(self.scope.stmt_uid("ref"), self.t))

    def out(self) -> str:
        return f"{self.t.ident()} as {self.refs[0].ident()}"


class TargetTable(TableRef):
    def __init__(self, parent: Prod, victim: Optional[Table] = None):
        super().__init__(parent)
        while (victim is None
               or not victim.is_base_table
               or not victim.columns()):
            pick = random_pick(self.scope.tables)
            victim = pick if isinstance(pick, Table) else None
            self.retry()
        self.victim_ = victim
        self.refs.append(AliasedRelation(self.scope.stmt_uid("target"), victim))

    def out(self) -> str:
        return f"{self.victim_.ident()} as {self.refs[0].ident()}"


class TableSample(TableRef):
    def __init__(self, parent: Prod):
        super().__init__(parent)
        self.match()
        self.retry_limit = 1000
        schema = self.scope.schema
        self.t = None
        while not self.t or not self.t.is_base_table:
            pick = random_pick(schema.base_tables)
            self.t = pick if isinstance(pick, Table) else None
            self.retry()
        self.refs.append(AliasedRelation(self.scope.stmt_uid("sample"), self.t))
        self.percent = 0.1 * d100()
        self.method = "system" if d6() > 2 else "bernoulli"

    def out(self) -> str:
        return f"{self.t.ident()} as {self.refs[0].ident()}"


class TableSubquery(TableRef):
    def __init__(self, parent: Prod, lateral: bool = False):
        super().__init__(parent)
        self.is_lateral = lateral
        self.query = QuerySpec(self, self.scope, lateral)
        alias = self.scope.stmt_uid("subq")
        aliased_rel = AliasedRelation(alias, self.query.select_list.derived_table)
        self.refs.append(aliased_rel)

    def out(self) -> str:
        prefix = "lateral " if self.is_lateral else ""
        return f"{prefix}({self.query.out()}) as {self.refs[0].ident()}"

    def accept(self, visitor):
        self.query.accept(visitor)
        visitor.visit(self)


class LateralSubquery(TableSubquery):
    def __init__(self, parent: Prod):
        super().__init__(parent, lateral=True)


# ---------------------------------------------------------------------------
# Join conditions
# ---------------------------------------------------------------------------

class JoinCond(Prod):
    def __init__(self, parent: Prod, lhs: TableRef, rhs: TableRef):
        super().__init__(parent)

    @staticmethod
    def factory(p: Prod, lhs: TableRef, rhs: TableRef) -> "JoinCond":
        try:
            if d6() < 2:
                return ExprJoinCond(p, lhs, rhs)
            else:
                return SimpleJoinCond(p, lhs, rhs)
        except RuntimeError:
            p.retry()
        return JoinCond.factory(p, lhs, rhs)


class SimpleJoinCond(JoinCond):
    def __init__(self, parent: Prod, lhs: TableRef, rhs: TableRef):
        super().__init__(parent, lhs, rhs)
        self.condition = ""

        while True:
            left_rel = random_pick(lhs.refs)
            if not left_rel.columns():
                self.retry()
                continue
            right_rel = random_pick(rhs.refs)
            c1 = random_pick(left_rel.columns())
            for c2 in right_rel.columns():
                if c1.type is c2.type:
                    self.condition = (f"{left_rel.ident()}.{c1.name}"
                                      f" = {right_rel.ident()}.{c2.name} ")
                    break
            if self.condition:
                break
            self.retry()

    def out(self) -> str:
        return self.condition


class ExprJoinCond(JoinCond):
    def __init__(self, parent: Prod, lhs: TableRef, rhs: TableRef):
        super().__init__(parent, lhs, rhs)
        self.joinscope = Scope(parent.scope)
        self.scope = self.joinscope
        for ref in lhs.refs:
            self.joinscope.refs.append(ref)
        for ref in rhs.refs:
            self.joinscope.refs.append(ref)
        self.search = BoolExpr.factory(self)

    def out(self) -> str:
        return self.search.out()

    def accept(self, visitor):
        self.search.accept(visitor)
        visitor.visit(self)


# ---------------------------------------------------------------------------
# Joined table
# ---------------------------------------------------------------------------

class JoinedTable(TableRef):
    def __init__(self, parent: Prod):
        super().__init__(parent)
        self.lhs = TableRef.factory(self)
        self.rhs = TableRef.factory(self)
        self.condition = JoinCond.factory(self, self.lhs, self.rhs)

        if d6() < 2:
            self.join_type = "inner"
        elif d6() < 2:
            self.join_type = "left"
        else:
            self.join_type = "right"

        for ref in self.lhs.refs:
            self.refs.append(ref)
        for ref in self.rhs.refs:
            self.refs.append(ref)

    def out(self) -> str:
        return (f"{self.lhs.out()}"
                f"{self.indent()}{self.join_type} join {self.rhs.out()}"
                f"{self.indent()}on ({self.condition.out()})")

    def accept(self, visitor):
        self.lhs.accept(visitor)
        self.rhs.accept(visitor)
        self.condition.accept(visitor)
        visitor.visit(self)


# ---------------------------------------------------------------------------
# FROM clause
# ---------------------------------------------------------------------------

class FromClause(Prod):
    def __init__(self, parent: Prod):
        super().__init__(parent)
        self.reflist: List[TableRef] = []
        ref = TableRef.factory(self)
        self.reflist.append(ref)
        for r in ref.refs:
            self.scope.refs.append(r)

        while self.scope.refs and d6() == 1:
            lateral = LateralSubquery(self)
            self.reflist.append(lateral)
            for r in lateral.refs:
                self.scope.refs.append(r)

    def out(self) -> str:
        if not self.reflist:
            return ""
        parts = ["from "]
        for i, r in enumerate(self.reflist):
            parts.append(self.indent())
            parts.append(r.out())
            if i + 1 != len(self.reflist):
                parts.append(",")
        return "".join(parts)

    def accept(self, visitor):
        visitor.visit(self)
        for r in self.reflist:
            r.accept(visitor)


# ---------------------------------------------------------------------------
# SELECT list
# ---------------------------------------------------------------------------

class SelectList(Prod):
    def __init__(self, parent: Prod):
        super().__init__(parent)
        self.value_exprs: List[ValueExpr] = []
        # Each SELECT also materializes a synthetic relation so outer queries can refer to subquery columns.
        self.derived_table = Relation()
        col_count = 0

        while True:
            e = ValueExpr.factory(self)
            self.value_exprs.append(e)
            name = f"c{col_count}"
            col_count += 1
            t = e.type
            assert t is not None
            self.derived_table.columns().append(Column(name, t))
            if d6() <= 2:
                break

    def out(self) -> str:
        parts = []
        cols = self.derived_table.columns()
        for i, expr in enumerate(self.value_exprs):
            parts.append(f"{self.indent()}{expr.out()} as {cols[i].name}")
        return ", ".join(parts)

    def accept(self, visitor):
        visitor.visit(self)
        for e in self.value_exprs:
            e.accept(visitor)


# ---------------------------------------------------------------------------
# Query spec (SELECT)
# ---------------------------------------------------------------------------

class QuerySpec(Prod):
    def __init__(self, parent: Optional[Prod], s: Scope, lateral: bool = False):
        super().__init__(parent)
        # Query blocks work on a child scope so refs introduced here do not leak outward.
        self.myscope = Scope(s)
        self.scope = self.myscope
        self.myscope.tables = list(s.tables)

        if lateral:
            self.myscope.refs = list(s.refs)

        self.from_clause = FromClause(self)
        self.select_list = SelectList(self)
        self.set_quantifier = "distinct" if d100() == 1 else ""
        if self.set_quantifier:
            if any(not self.scope.schema.has_sql_equality(expr.type)
                   for expr in self.select_list.value_exprs):
                self.set_quantifier = ""
        self.search = BoolExpr.factory(self)

        self.limit_clause = ""
        if d6() > 2:
            self.limit_clause = f"limit {d100() + d100()}"

    def out(self) -> str:
        parts = [f"select {self.set_quantifier} {self.select_list.out()}"]
        parts.append(self.indent())
        parts.append(self.from_clause.out())
        parts.append(self.indent())
        parts.append(f"where {self.search.out()}")
        if self.limit_clause:
            parts.append(self.indent())
            parts.append(self.limit_clause)
        return "".join(parts)

    def accept(self, visitor):
        visitor.visit(self)
        self.select_list.accept(visitor)
        self.from_clause.accept(visitor)
        self.search.accept(visitor)


# ---------------------------------------------------------------------------
# SELECT FOR UPDATE
# ---------------------------------------------------------------------------

class _ForUpdateVerify:
    """Visitor that checks if FOR UPDATE is safe."""
    def __init__(self):
        self.ok = True

    def visit(self, p):
        if isinstance(p, WindowFunction):
            self.ok = False
        if isinstance(p, JoinedTable) and p.join_type != "inner":
            self.ok = False
        if isinstance(p, QuerySpec):
            p.set_quantifier = ""
        if isinstance(p, TableOrQueryName):
            actual_table = p.t if isinstance(p.t, Table) else None
            if actual_table and not actual_table.is_insertable:
                self.ok = False
        if isinstance(p, TableSample):
            actual_table = p.t if isinstance(p.t, Table) else None
            if actual_table and not actual_table.is_insertable:
                self.ok = False


class SelectForUpdate(QuerySpec):
    MODES = ["update", "share"]

    def __init__(self, parent: Optional[Prod], s: Scope, lateral: bool = False):
        super().__init__(parent, s, lateral)
        self.lockmode: Optional[str] = None

        v = _ForUpdateVerify()
        self.accept(v)
        if not v.ok:
            return

        self.lockmode = self.MODES[d6() % len(self.MODES)]
        self.set_quantifier = ""

    def out(self) -> str:
        base = super().out()
        if self.lockmode:
            base += f"{self.indent()} for {self.lockmode}"
        return base


class PrepareStmt(Prod):
    seq = 0

    def __init__(self, parent: Optional[Prod], s: Scope):
        super().__init__(parent)
        self.myscope = Scope(s)
        self.scope = self.myscope
        self.id = PrepareStmt.seq
        PrepareStmt.seq += 1
        self.query = QuerySpec(self, self.scope)

    def out(self) -> str:
        return f"prepare prep{self.id} as {self.query.out()}"

    def accept(self, visitor):
        visitor.visit(self)
        self.query.accept(visitor)


# ---------------------------------------------------------------------------
# DML: INSERT / UPDATE
# ---------------------------------------------------------------------------


def _safe_dml_value(parent: Prod, col: Column) -> ValueExpr:
    # DML uses a slightly safer value policy than plain expression generation so
    # obvious NOT NULL / FK failures are less common.
    if getattr(col, "is_foreign_key", False) and col.fk_ref_schema and col.fk_ref_table and col.fk_ref_column:
        return ForeignKeySubselect(parent, col.fk_ref_schema, col.fk_ref_table, col.fk_ref_column, col.type)
    if (getattr(col, "not_null", False)
            and not getattr(col, "has_default", False)
            and col.type in (parent.scope.schema.inttype, parent.scope.schema.booltype)):
        return ConstExpr(parent, col.type)
    return ValueExpr.factory(parent, col.type)

class ModifyingStmt(Prod):
    def __init__(self, parent: Optional[Prod], s: Scope, victim: Optional[Table] = None):
        super().__init__(parent)
        # DML picks one victim table and then generates around that fixed target.
        self.myscope = Scope(s)
        self.scope = self.myscope
        self.myscope.tables = list(s.tables)
        self.victim = victim
        if not self.victim:
            self._pick_victim()

    def _pick_victim(self):
        while True:
            pick = random_pick(self.scope.tables)
            if isinstance(pick, Table):
                self.victim = pick
            else:
                self.victim = None
            self.retry()
            if (self.victim
                    and self.victim.schema != "pg_catalog"
                    and self.victim.is_base_table
                    and self.victim.columns()):
                break


class DeleteStmt(ModifyingStmt):
    def __init__(self, parent: Optional[Prod], s: Scope, victim: Optional[Table] = None):
        if victim is None:
            candidates = [
                t for t in s.tables
                if isinstance(t, Table)
                and t.schema != "pg_catalog"
                and t.is_base_table
                and t.columns()
                and not t.is_referenced_by_fk
            ]
            if candidates:
                victim = random_pick(candidates)
        super().__init__(parent, s, victim)
        self.scope.refs.append(self.victim)
        self.search = BoolExpr.factory(self)

    def out(self) -> str:
        return f"delete from {self.victim.ident()} where {self.search.out()}"

    def accept(self, visitor):
        visitor.visit(self)
        self.search.accept(visitor)


class InsertStmt(ModifyingStmt):
    def __init__(self, parent: Optional[Prod], s: Scope, victim: Optional[Table] = None):
        super().__init__(parent, s, victim)
        self.match()
        self.value_exprs: List[ValueExpr] = []

        for col in self.victim.columns():
            self._allow_default = not (getattr(col, "not_null", False) and not getattr(col, "has_default", False))
            expr = _safe_dml_value(self, col)
            assert expr.type is col.type
            self.value_exprs.append(expr)
        self._allow_default = True

    def out(self) -> str:
        parts = [f"insert into {self.victim.ident()} "]
        if not self.value_exprs:
            parts.append("default values")
            return "".join(parts)
        parts.append("values (")
        for i, expr in enumerate(self.value_exprs):
            parts.append(self.indent())
            parts.append(expr.out())
            if i + 1 != len(self.value_exprs):
                parts.append(", ")
        parts.append(")")
        return "".join(parts)

    def accept(self, visitor):
        visitor.visit(self)
        for e in self.value_exprs:
            e.accept(visitor)


class SetList(Prod):
    def __init__(self, parent: Prod, target: Table):
        super().__init__(parent)
        self.value_exprs: List[ValueExpr] = []
        self.names: List[str] = []
        candidate_cols = [
            col for col in target.columns()
            if not getattr(col, "is_primary_key", False) and not getattr(col, "is_foreign_key", False)
        ]
        if not candidate_cols:
            candidate_cols = list(target.columns())

        while not self.names:
            for col in candidate_cols:
                if d6() < 2:
                    continue
                expr = _safe_dml_value(self, col)
                self.value_exprs.append(expr)
                self.names.append(col.name)

    def out(self) -> str:
        assert self.names
        parts = [" set "]
        for i in range(len(self.names)):
            parts.append(self.indent())
            parts.append(f"{self.names[i]} = {self.value_exprs[i].out()}")
            if i + 1 != len(self.names):
                parts.append(", ")
        return "".join(parts)

    def accept(self, visitor):
        visitor.visit(self)
        for e in self.value_exprs:
            e.accept(visitor)


class UpdateStmt(ModifyingStmt):
    def __init__(self, parent: Optional[Prod], s: Scope, victim: Optional[Table] = None):
        super().__init__(parent, s, victim)
        self.scope.refs.append(self.victim)
        self.search = BoolExpr.factory(self)
        self.set_list = SetList(self, self.victim)

    def out(self) -> str:
        return (
            f"update {self.victim.ident()}"
            f"{self.set_list.out()}"
            f"{self.indent()}where {self.search.out()}"
        )

    def accept(self, visitor):
        visitor.visit(self)
        self.search.accept(visitor)
        self.set_list.accept(visitor)


# ---------------------------------------------------------------------------
# RETURNING / UPSERT / MERGE / CTE
# ---------------------------------------------------------------------------

class DeleteReturning(DeleteStmt):
    def __init__(self, parent: Optional[Prod], s: Scope, victim: Optional[Table] = None):
        super().__init__(parent, s, victim)
        self.match()
        self.select_list = SelectList(self)

    def out(self) -> str:
        return f"{super().out()}{self.indent()}returning {self.select_list.out()}"

    def accept(self, visitor):
        visitor.visit(self)
        self.search.accept(visitor)
        self.select_list.accept(visitor)


class UpdateReturning(UpdateStmt):
    def __init__(self, parent: Optional[Prod], s: Scope, victim: Optional[Table] = None):
        super().__init__(parent, s, victim)
        self.match()
        self.select_list = SelectList(self)

    def out(self) -> str:
        return f"{super().out()}{self.indent()}returning {self.select_list.out()}"

    def accept(self, visitor):
        visitor.visit(self)
        self.search.accept(visitor)
        self.set_list.accept(visitor)
        self.select_list.accept(visitor)


class UpsertStmt(InsertStmt):
    def __init__(self, parent: Optional[Prod], s: Scope, victim: Optional[Table] = None):
        super().__init__(parent, s, victim)
        self.match()

        if not self.victim.constraints:
            self.fail("need table w/ constraint for upsert")

        self.set_list = SetList(self, self.victim)
        self.search = BoolExpr.factory(self)
        self.constraint = random_pick(self.victim.constraints)

    def out(self) -> str:
        return (
            f"{super().out()}"
            f"{self.indent()}on conflict on constraint {self.constraint} do update"
            f"{self.set_list.out()}"
            f"{self.indent()}where {self.search.out()}"
        )

    def accept(self, visitor):
        super().accept(visitor)
        self.set_list.accept(visitor)
        self.search.accept(visitor)


class WhenClause(Prod):
    def __init__(self, parent: "MergeStmt"):
        super().__init__(parent)
        self.condition = BoolExpr.factory(self)
        self.matched = d6() > 2

    @staticmethod
    def factory(parent: "MergeStmt") -> "WhenClause":
        try:
            roll = d6()
            if roll == 1:
                return WhenClauseInsert(parent)
            if roll == 2:
                return WhenClauseUpdate(parent)
            return WhenClause(parent)
        except RuntimeError:
            parent.retry()
        return WhenClause.factory(parent)

    def out(self) -> str:
        if self.matched:
            return f"when matched and {self.condition.out()} then delete"
        return f"when not matched and {self.condition.out()} then do nothing"

    def accept(self, visitor):
        visitor.visit(self)
        self.condition.accept(visitor)


class WhenClauseUpdate(WhenClause):
    def __init__(self, parent: "MergeStmt"):
        super().__init__(parent)
        self.myscope = Scope(parent.scope)
        self.scope = self.myscope
        self.scope.refs.append(parent.target_table.refs[0])
        self.set_list = SetList(self, parent.victim)

    def out(self) -> str:
        return f"when matched and {self.condition.out()} then update{self.set_list.out()}"

    def accept(self, visitor):
        visitor.visit(self)
        self.condition.accept(visitor)
        self.set_list.accept(visitor)


class WhenClauseInsert(WhenClause):
    def __init__(self, parent: "MergeStmt"):
        super().__init__(parent)
        self.exprs: List[ValueExpr] = []
        for col in parent.victim.columns():
            expr = _safe_dml_value(self, col)
            assert expr.type is col.type
            self.exprs.append(expr)

    def out(self) -> str:
        values = ", ".join(expr.out() for expr in self.exprs)
        return f"when not matched and {self.condition.out()} then insert values ({values})"

    def accept(self, visitor):
        visitor.visit(self)
        self.condition.accept(visitor)
        for expr in self.exprs:
            expr.accept(visitor)


class MergeStmt(ModifyingStmt):
    def __init__(self, parent: Optional[Prod], s: Scope, victim: Optional[Table] = None):
        super().__init__(parent, s, victim)
        self.match()
        self.target_table = TargetTable(self, self.victim)
        self.data_source = TableRef.factory(self)
        self.join_condition = SimpleJoinCond(self, self.target_table, self.data_source)

        self.clauselist: List[WhenClause] = [WhenClause.factory(self)]
        while d6() > 2:
            self.clauselist.append(WhenClause.factory(self))

    def out(self) -> str:
        parts = [f"merge into {self.target_table.out()}"]
        parts.append(self.indent())
        parts.append(f"using {self.data_source.out()}")
        parts.append(self.indent())
        parts.append(f"on {self.join_condition.out()}")
        for clause in self.clauselist:
            parts.append(self.indent())
            parts.append(clause.out())
        return "".join(parts)

    def accept(self, visitor):
        visitor.visit(self)
        self.target_table.accept(visitor)
        self.data_source.accept(visitor)
        self.join_condition.accept(visitor)
        for clause in self.clauselist:
            clause.accept(visitor)


class CommonTableExpression(Prod):
    def __init__(self, parent: Optional[Prod], s: Scope):
        super().__init__(parent)
        self.myscope = Scope(s)
        self.scope = self.myscope
        self.refs: List[AliasedRelation] = []
        self.with_queries: List[QuerySpec] = []

        while True:
            query = QuerySpec(self, s)
            self.with_queries.append(query)
            alias = self.scope.stmt_uid("cte")
            aliased_rel = AliasedRelation(alias, query.select_list.derived_table)
            self.refs.append(aliased_rel)
            self.scope.tables.append(aliased_rel)
            if d6() <= 2:
                break

        while d6() > 2:
            self.scope.tables.append(random_pick(s.tables))

        self.query = QuerySpec(self, self.scope)

    def out(self) -> str:
        with_parts = []
        for i, query in enumerate(self.with_queries):
            with_parts.append(f"{self.refs[i].ident()} as ({query.out()})")
        return f"with {', '.join(with_parts)}{self.indent()}{self.query.out()}"

    def accept(self, visitor):
        visitor.visit(self)
        for query in self.with_queries:
            query.accept(visitor)
        self.query.accept(visitor)


# ---------------------------------------------------------------------------
# Top-level statement factory
# ---------------------------------------------------------------------------

def statement_factory(s: Scope, select_only: bool = False) -> Prod:
    try:
        s.new_stmt()

        if d100() == 1:
            return PrepareStmt(None, s)
        if d6() > 2:
            return CommonTableExpression(None, s)
        if d42() == 1 and not select_only:
            return InsertStmt(None, s)
        if d42() == 1 and not select_only:
            return DeleteStmt(None, s)
        if d42() == 1 and not select_only:
            return UpdateStmt(None, s)
        if d6() > 2:
            return SelectForUpdate(None, s)
        return QuerySpec(None, s)
    except RuntimeError:
        return statement_factory(s, select_only)
