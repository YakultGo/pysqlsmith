"""Relational model: types, columns, tables, scopes, operators, routines."""

from __future__ import annotations
from typing import List, Tuple, Optional, Dict


class SQLType:
    """Minimal type object used by the generator.

    Types are interned by name so identity checks stay cheap throughout AST generation.
    """
    _typemap: Dict[str, "SQLType"] = {}

    def __init__(self, name: str):
        self.name = name

    @classmethod
    def get(cls, name: str) -> "SQLType":
        if name not in cls._typemap:
            cls._typemap[name] = cls(name)
        return cls._typemap[name]

    @classmethod
    def register(cls, type_: "SQLType") -> "SQLType":
        cls._typemap[type_.name] = type_
        return type_

    def consistent(self, rvalue: "SQLType") -> bool:
        return self is rvalue

    def __repr__(self):
        return f"SQLType({self.name!r})"


class Column:
    """Column metadata carried into generation.

    The generator only stores the fields that influence SQL construction.
    """
    def __init__(
        self,
        name: str,
        type_: Optional[SQLType] = None,
        *,
        not_null: bool = False,
        has_default: bool = False,
        is_primary_key: bool = False,
        is_foreign_key: bool = False,
        fk_ref_schema: str = "",
        fk_ref_table: str = "",
        fk_ref_column: str = "",
        enum_values: Optional[List[str]] = None,
        set_values: Optional[List[str]] = None,
    ):
        self.name = name
        self.type = type_
        self.not_null = not_null
        self.has_default = has_default
        self.is_primary_key = is_primary_key
        self.is_foreign_key = is_foreign_key
        self.fk_ref_schema = fk_ref_schema
        self.fk_ref_table = fk_ref_table
        self.fk_ref_column = fk_ref_column
        self.enum_values = list(enum_values or [])
        self.set_values = list(set_values or [])
        if type_ is not None:
            assert type_ is not None


class Relation:
    def __init__(self):
        self.cols: List[Column] = []

    def columns(self) -> List[Column]:
        return self.cols


class NamedRelation(Relation):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    def ident(self) -> str:
        return self.name


class AliasedRelation(NamedRelation):
    def __init__(self, name: str, rel: Relation):
        super().__init__(name)
        self.rel = rel

    def columns(self) -> List[Column]:
        return self.rel.columns()


class Table(NamedRelation):
    """Concrete relation loaded from the target database."""
    def __init__(self, name: str, schema: str, is_insertable: bool, is_base_table: bool):
        super().__init__(name)
        self.schema = schema
        self.is_insertable = is_insertable
        self.is_base_table = is_base_table
        self.constraints: List[str] = []
        self.is_referenced_by_fk: bool = False

    def ident(self) -> str:
        return f"{self.schema}.{self.name}"


class Op:
    def __init__(self, name: str = "", left: Optional[SQLType] = None,
                 right: Optional[SQLType] = None, result: Optional[SQLType] = None):
        self.name = name
        self.left = left
        self.right = right
        self.result = result


class Routine:
    def __init__(
        self,
        schema: str,
        specific_name: str,
        restype: SQLType,
        name: str,
        variadic: bool = False,
    ):
        self.specific_name = specific_name
        self.schema = schema
        self.argtypes: List[SQLType] = []
        self.restype = restype
        self.name = name
        self.variadic = variadic
        assert restype is not None

    def ident(self) -> str:
        if self.schema:
            return f"{self.schema}.{self.name}"
        return self.name


class Scope:
    """Per-statement view of the world.

    `tables` contains schema-level relations that can be picked.
    `refs` contains relations already visible in the current query block.
    """
    def __init__(self, parent: Optional["Scope"] = None):
        self.parent = parent
        self.tables: List[NamedRelation] = []
        self.refs: List[NamedRelation] = []
        self.schema = None  # will be set by Schema.fill_scope
        self._stmt_seq: Dict[str, int] = {}

        if parent:
            self.schema = parent.schema
            self.tables = list(parent.tables)
            self.refs = list(parent.refs)
            self._stmt_seq = dict(parent._stmt_seq)

    def refs_of_type(self, t: SQLType) -> List[Tuple[NamedRelation, Column]]:
        result = []
        for r in self.refs:
            for c in r.columns():
                if t.consistent(c.type):
                    result.append((r, c))
        return result

    def stmt_uid(self, prefix: str) -> str:
        key = prefix + "_"
        count = self._stmt_seq.get(key, 0)
        self._stmt_seq[key] = count + 1
        return f"{prefix}_{count}"

    def new_stmt(self):
        self._stmt_seq = {}
