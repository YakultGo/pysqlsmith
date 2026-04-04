"""MySQL schema loader: read table metadata and register operators/functions."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pymysql

from .relmodel import SQLType, Column, Table, Op, Routine
from .schema_base import Schema

if TYPE_CHECKING:
    from .main import RunConfig


def _parse_column_type(column_type: str) -> str:
    """Normalize a MySQL data type name to a canonical type."""
    ct = column_type.upper()

    if ct in ("TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT"):
        return "INTEGER"
    if ct in ("DOUBLE", "FLOAT", "NUMERIC", "DECIMAL"):
        return "DOUBLE"
    if ct in ("VARCHAR", "CHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT"):
        return "VARCHAR"
    if ct in ("DATE", "TIME", "DATETIME", "TIMESTAMP", "YEAR"):
        return "TIMESTAMP"
    if ct == "BIT":
        return "BIT"
    if ct in ("BINARY", "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "VARBINARY"):
        return "BINARY"
    if ct == "ENUM":
        return "ENUM"
    if ct == "SET":
        return "SET"
    if ct == "JSON":
        return "VARCHAR"

    raise RuntimeError(f"Unhandled data type: {column_type}")


class SchemaMySQL(Schema):
    def __init__(self, config: "RunConfig", exclude_catalog: bool = False):
        super().__init__()
        self.grammar_module = "pysqlsmith.mysql.grammar"
        # MySQL setup is mostly "load user schema" plus a curated builtin function/operator set.
        conn = pymysql.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            database=config.dbname,
        )
        dbname = config.dbname

        try:
            self._load_tables(conn, dbname)
            self._load_columns(conn, dbname)
        finally:
            conn.close()

        self._register_operators()
        self._register_functions()
        self._register_aggregates()

        self.booltype = SQLType.get("INTEGER")
        self.inttype = SQLType.get("INTEGER")
        self.internaltype = SQLType.get("internal")
        self.arraytype = SQLType.get("ARRAY")

        self.true_literal = "1"
        self.false_literal = "0"

        self.types = [
            SQLType.get("INTEGER"),
            SQLType.get("DOUBLE"),
            SQLType.get("VARCHAR"),
            SQLType.get("TIMESTAMP"),
            SQLType.get("BIT"),
            SQLType.get("BINARY"),
            SQLType.get("ENUM"),
            SQLType.get("SET"),
            SQLType.get("internal"),
            SQLType.get("ARRAY"),
        ]

        self.generate_indexes()

    def _load_tables(self, conn, dbname: str):
        print("Loading tables...", end="", file=sys.stderr)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT TABLE_NAME, TABLE_SCHEMA, TABLE_TYPE "
                "FROM information_schema.tables WHERE TABLE_SCHEMA = %s",
                (dbname,),
            )
            for row in cur.fetchall():
                tname, tschema, ttype = row[0], row[1], row[2]
                if ttype == "BASE TABLE":
                    insertable, base_table = True, True
                elif ttype == "VIEW":
                    insertable, base_table = False, False
                else:
                    continue
                self.tables.append(Table(tname, tschema, insertable, base_table))
        print("done.", file=sys.stderr)

    def _load_columns(self, conn, dbname: str):
        print("Loading columns and constraints...", end="", file=sys.stderr)
        for t in self.tables:
            with conn.cursor() as cur:
                # For MySQL we currently keep column metadata intentionally lightweight.
                cur.execute(
                    "SELECT COLUMN_NAME, UPPER(DATA_TYPE) "
                    "FROM information_schema.columns "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                    (t.schema, t.name),
                )
                for row in cur.fetchall():
                    try:
                        col_type = _parse_column_type(row[1])
                    except RuntimeError:
                        continue
                    t.columns().append(Column(row[0], SQLType.get(col_type)))
        print("done.", file=sys.stderr)

    def _register_operators(self):
        def binop(name, typename):
            t = SQLType.get(typename)
            self.register_operator(Op(name, t, t, t))

        binop("*", "INTEGER")
        binop("/", "INTEGER")
        binop("+", "INTEGER")
        binop("-", "INTEGER")
        binop(">>", "INTEGER")
        binop("<<", "INTEGER")
        binop("&", "INTEGER")
        binop("|", "INTEGER")
        binop("<", "INTEGER")
        binop("<=", "INTEGER")
        binop(">", "INTEGER")
        binop(">=", "INTEGER")
        binop("=", "INTEGER")
        binop("<>", "INTEGER")
        binop("IS", "INTEGER")
        binop("IS NOT", "INTEGER")
        binop("AND", "INTEGER")
        binop("OR", "INTEGER")

    def _register_functions(self):
        def func(name, restype):
            self.register_routine(Routine("", "", SQLType.get(restype), name))

        def func1(name, restype, a):
            r = Routine("", "", SQLType.get(restype), name)
            r.argtypes.append(SQLType.get(a))
            self.register_routine(r)

        def func2(name, restype, a, b):
            r = Routine("", "", SQLType.get(restype), name)
            r.argtypes.append(SQLType.get(a))
            r.argtypes.append(SQLType.get(b))
            self.register_routine(r)

        def func3(name, restype, a, b, c):
            r = Routine("", "", SQLType.get(restype), name)
            r.argtypes.append(SQLType.get(a))
            r.argtypes.append(SQLType.get(b))
            r.argtypes.append(SQLType.get(c))
            self.register_routine(r)

        func("last_insert_rowid", "INTEGER")
        func1("abs", "INTEGER", "INTEGER")
        func1("hex", "VARCHAR", "VARCHAR")
        func1("length", "INTEGER", "VARCHAR")
        func1("lower", "VARCHAR", "VARCHAR")
        func1("ltrim", "VARCHAR", "VARCHAR")
        func1("rtrim", "VARCHAR", "VARCHAR")
        func1("trim", "VARCHAR", "VARCHAR")
        func1("quote", "VARCHAR", "VARCHAR")
        func1("round", "INTEGER", "DOUBLE")
        func1("rtrim", "VARCHAR", "VARCHAR")
        func1("trim", "VARCHAR", "VARCHAR")
        func1("upper", "VARCHAR", "VARCHAR")
        func2("instr", "INTEGER", "VARCHAR", "VARCHAR")
        func2("substr", "VARCHAR", "VARCHAR", "INTEGER")
        func3("substr", "VARCHAR", "VARCHAR", "INTEGER", "INTEGER")
        func3("replace", "VARCHAR", "VARCHAR", "VARCHAR", "VARCHAR")

    def _register_aggregates(self):
        def agg(name, restype, argtype):
            r = Routine("", "", SQLType.get(restype), name)
            r.argtypes.append(SQLType.get(argtype))
            self.register_aggregate(r)

        agg("avg", "INTEGER", "INTEGER")
        agg("avg", "DOUBLE", "DOUBLE")
        agg("count", "INTEGER", "INTEGER")
        agg("group_concat", "VARCHAR", "VARCHAR")
        agg("max", "DOUBLE", "DOUBLE")
        agg("max", "INTEGER", "INTEGER")
        agg("sum", "DOUBLE", "DOUBLE")
        agg("sum", "INTEGER", "INTEGER")

    def quote_name(self, identifier: str) -> str:
        return f"`{identifier}`"
