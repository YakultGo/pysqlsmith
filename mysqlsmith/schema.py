"""MySQL schema loader: read table metadata and register operators/functions."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import pymysql

from mysqlsmith.relmodel import SQLType, Column, Table, Op, Routine
from mysqlsmith.schema_base import Schema

if TYPE_CHECKING:
    from mysqlsmith.main import RunConfig


def _parse_mysql_quoted_list(column_type: str) -> list[str]:
    values: list[str] = []
    current: list[str] = []
    in_quote = False
    escape = False

    for ch in column_type:
        if not in_quote:
            if ch == "'":
                in_quote = True
                current = []
            continue

        if escape:
            current.append(ch)
            escape = False
            continue

        if ch == "\\":
            escape = True
            continue

        if ch == "'":
            values.append("".join(current))
            in_quote = False
            continue

        current.append(ch)

    return values


def _parse_column_type(column_type: str) -> str:
    """Normalize a MySQL data type name to a canonical type."""
    ct = column_type.upper()

    if ct in ("TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT"):
        return "INT"
    if ct in ("DOUBLE", "FLOAT", "NUMERIC", "DECIMAL"):
        return "DECIMAL"
    if ct in ("VARCHAR", "CHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT"):
        return "TEXT"
    if ct in ("DATE", "TIME", "DATETIME", "TIMESTAMP", "YEAR"):
        return "DATETIME"
    if ct == "BIT":
        return "BIT"
    if ct in ("BINARY", "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "VARBINARY"):
        return "BLOB"
    if ct == "ENUM":
        return "ENUM"
    if ct == "SET":
        return "SET"
    if ct == "JSON":
        return "TEXT"

    raise RuntimeError(f"Unhandled data type: {column_type}")


class SchemaMySQL(Schema):
    def __init__(self, config: "RunConfig", exclude_catalog: bool = False):
        super().__init__()
        self.grammar_module = f"{__package__}.grammar"
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
            self._load_foreign_keys(conn, dbname)
        finally:
            conn.close()

        self.booltype = SQLType.get("BOOL")
        self.inttype = SQLType.get("INT")
        self.internaltype = SQLType.get("internal")
        self.arraytype = SQLType.get("ARRAY")

        self._register_operators()
        self._register_functions()
        self._register_aggregates()

        self.true_literal = "1"
        self.false_literal = "0"

        self.types = [
            SQLType.get("BOOL"),
            SQLType.get("INT"),
            SQLType.get("DECIMAL"),
            SQLType.get("TEXT"),
            SQLType.get("DATETIME"),
            SQLType.get("BIT"),
            SQLType.get("BLOB"),
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
                cur.execute(
                    "SELECT COLUMN_NAME, UPPER(DATA_TYPE), IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY, COLUMN_TYPE "
                    "FROM information_schema.columns "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                    (t.schema, t.name),
                )
                for row in cur.fetchall():
                    try:
                        col_type = _parse_column_type(row[1])
                    except RuntimeError:
                        continue
                    t.columns().append(
                        Column(
                            row[0],
                            SQLType.get(col_type),
                            not_null=(row[2] == "NO"),
                            has_default=(row[3] is not None),
                            is_primary_key=(row[4] == "PRI"),
                            enum_values=_parse_mysql_quoted_list(row[5]) if col_type == "ENUM" else [],
                            set_values=_parse_mysql_quoted_list(row[5]) if col_type == "SET" else [],
                        )
                    )
        print("done.", file=sys.stderr)

    def _load_foreign_keys(self, conn, dbname: str):
        table_map = {(t.schema, t.name): t for t in self.tables}
        with conn.cursor() as cur:
            cur.execute(
                "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_SCHEMA, "
                "REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME "
                "FROM information_schema.KEY_COLUMN_USAGE "
                "WHERE TABLE_SCHEMA = %s AND REFERENCED_TABLE_SCHEMA IS NOT NULL",
                (dbname,),
            )
            for tschema, tname, cname, ref_schema, ref_table, ref_column in cur.fetchall():
                table = table_map.get((tschema, tname))
                ref = table_map.get((ref_schema, ref_table))
                if table is None:
                    continue
                column = next((c for c in table.columns() if c.name == cname), None)
                if column is None:
                    continue
                column.is_foreign_key = True
                column.fk_ref_schema = ref_schema
                column.fk_ref_table = ref_table
                column.fk_ref_column = ref_column
                if ref is not None:
                    ref.is_referenced_by_fk = True

    def _register_operators(self):
        def same_type_op(name, typename):
            t = SQLType.get(typename)
            self.register_operator(Op(name, t, t, t))

        def predicate_op(name, typename):
            t = SQLType.get(typename)
            self.register_operator(Op(name, t, t, self.booltype))

        same_type_op("*", "INT")
        same_type_op("/", "INT")
        same_type_op("+", "INT")
        same_type_op("-", "INT")
        same_type_op(">>", "INT")
        same_type_op("<<", "INT")
        same_type_op("&", "INT")
        same_type_op("|", "INT")

        same_type_op("+", "DECIMAL")
        same_type_op("-", "DECIMAL")

        for typename in ("INT", "DECIMAL", "TEXT", "DATETIME"):
            predicate_op("=", typename)
            predicate_op("<>", typename)

        for typename in ("INT", "DECIMAL", "DATETIME"):
            predicate_op("<", typename)
            predicate_op("<=", typename)
            predicate_op(">", typename)
            predicate_op(">=", typename)

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

        func("last_insert_id", "INT")
        func("current_timestamp", "DATETIME")
        func1("abs", "INT", "INT")
        func1("round", "INT", "DECIMAL")
        func1("hex", "TEXT", "TEXT")
        func1("char_length", "INT", "TEXT")
        func1("lower", "TEXT", "TEXT")
        func1("upper", "TEXT", "TEXT")
        func1("ltrim", "TEXT", "TEXT")
        func1("rtrim", "TEXT", "TEXT")
        func1("trim", "TEXT", "TEXT")
        func1("quote", "TEXT", "TEXT")
        func1("date", "DATETIME", "DATETIME")
        func2("concat", "TEXT", "TEXT", "TEXT")
        func2("locate", "INT", "TEXT", "TEXT")
        func2("substring", "TEXT", "TEXT", "INT")
        func2("ifnull", "INT", "INT", "INT")
        func2("ifnull", "DECIMAL", "DECIMAL", "DECIMAL")
        func2("ifnull", "TEXT", "TEXT", "TEXT")
        func2("ifnull", "DATETIME", "DATETIME", "DATETIME")
        func2("nullif", "INT", "INT", "INT")
        func2("nullif", "DECIMAL", "DECIMAL", "DECIMAL")
        func2("nullif", "TEXT", "TEXT", "TEXT")
        func2("nullif", "DATETIME", "DATETIME", "DATETIME")
        func3("substring", "TEXT", "TEXT", "INT", "INT")
        func3("replace", "TEXT", "TEXT", "TEXT", "TEXT")

    def _register_aggregates(self):
        def agg(name, restype, argtype):
            r = Routine("", "", SQLType.get(restype), name)
            r.argtypes.append(SQLType.get(argtype))
            self.register_aggregate(r)

        agg("avg", "INT", "INT")
        agg("avg", "DECIMAL", "DECIMAL")
        agg("count", "INT", "INT")
        agg("group_concat", "TEXT", "TEXT")
        agg("max", "DECIMAL", "DECIMAL")
        agg("max", "INT", "INT")
        agg("sum", "DECIMAL", "DECIMAL")
        agg("sum", "INT", "INT")

    def quote_name(self, identifier: str) -> str:
        return f"`{identifier}`"
