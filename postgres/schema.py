"""PostgreSQL schema loader ported from the C++ sqlsmith implementation."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from .relmodel import SQLType, PGType, Column, Table, Op, Routine
from .schema_base import Schema

if TYPE_CHECKING:
    from .main import RunConfig


def _import_psycopg():
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL support requires psycopg. Install it with "
            "`pip install 'psycopg[binary]'`."
        ) from exc
    return psycopg

class SchemaPostgres(Schema):
    _UNSAFE_ROUTINE_NAMES = {
        "currval",
        "inet_client_addr",
        "inet_client_port",
        "lastval",
        "loread",
        "lowrite",
        "nextval",
        "setval",
        "inet_server_port",
        "inet_server_addr",
        "pg_create_restore_point",
        "pg_get_wal_replay_pause_state",
        "pg_is_in_recovery",
        "pg_is_wal_replay_paused",
        "pg_last_wal_receive_lsn",
        "pg_last_wal_replay_lsn",
        "pg_last_xact_replay_timestamp",
        "pg_trigger_depth",
        "txid_current",
        "txid_current_if_assigned",
    }

    _UNSAFE_ROUTINE_PREFIXES = (
        "has_",
        "lo_",
        "pg_backup_",
        "pg_current_wal_",
        "pg_last_wal_",
        "pg_get_wal_",
        "pg_is_wal_",
        "pg_wal_",
    )

    def __init__(self, config: "RunConfig", exclude_catalog: bool = False):
        super().__init__()
        self.grammar_module = "pysqlsmith.postgres.grammar"
        psycopg = _import_psycopg()
        self.exclude_catalog = exclude_catalog
        self.oid2type: dict[int, PGType] = {}
        self.name2type: dict[str, PGType] = {}
        # PostgreSQL has a much richer type system than MySQL, so setup first builds a type catalog,
        # then loads relations, then filters routines/operators down to the subset we want to generate.
        conn = psycopg.connect(
            host=config.host,
            port=config.port,
            user=config.user,
            password=config.password,
            dbname=config.dbname,
        )

        try:
            with conn.cursor() as cur:
                cur.execute("select version()")
                self.version = cur.fetchone()[0]
                cur.execute("show server_version_num")
                self.version_num = int(cur.fetchone()[0])

            self._load_types(conn)
            self._load_tables(conn, exclude_catalog)
            self._load_columns_and_constraints(conn)
            self._load_operators(conn)
            self._load_routines(conn)
            self._load_aggregates(conn)
            self._prune_generation_objects()
        finally:
            conn.close()

        self.booltype = self.name2type["bool"]
        self.inttype = self.name2type["int4"]
        self.internaltype = self.name2type["internal"]
        self.arraytype = self.name2type["anyarray"]

        self.true_literal = "true"
        self.false_literal = "false"

        self.generate_indexes()

    def _load_types(self, conn):
        print("Loading types...", end="", file=sys.stderr)
        with conn.cursor() as cur:
            # Types drive almost every generator decision in the PostgreSQL backend.
            cur.execute(
                "select quote_ident(t.typname), n.nspname, t.oid, t.typdelim, t.typrelid, "
                "t.typelem, t.typarray, t.typtype "
                "from pg_type t "
                "join pg_namespace n on n.oid = t.typnamespace"
            )
            for row in cur.fetchall():
                name, schema_name, oid, typdelim, typrelid, typelem, typarray, typtype = row
                pg_type = PGType(
                    name=name,
                    schema=schema_name,
                    oid=int(oid),
                    typdelim=typdelim,
                    typrelid=int(typrelid),
                    typelem=int(typelem),
                    typarray=int(typarray),
                    typtype=typtype,
                )
                SQLType.register(pg_type)
                self.oid2type[pg_type.oid] = pg_type
                self.name2type[pg_type.name] = pg_type
                self.types.append(pg_type)

            cur.execute("select rngtypid, rngsubtype, rngmultitypid from pg_range")
            for rngtypid, rngsubtype, rngmultitypid in cur.fetchall():
                range_type = self.oid2type.get(int(rngtypid))
                multirange_type = self.oid2type.get(int(rngmultitypid))
                if range_type is not None:
                    range_type.rngsubtype = int(rngsubtype)
                    range_type.rngmultitypid = int(rngmultitypid)
                if multirange_type is not None:
                    multirange_type.rngrangetype = int(rngtypid)
                    multirange_type.rngsubtype = int(rngsubtype)
        print("done.", file=sys.stderr)

    def _user_type_names(self) -> set[str]:
        user_types = set()
        for table in self.tables:
            for column in table.columns():
                user_types.add(column.type.name)
        return user_types

    def _has_nonpseudo_concrete(self, type_: PGType) -> bool:
        return any(
            isinstance(candidate, PGType) and not candidate.is_pseudotype
            for candidate in self.concrete_type.get(type_, [])
        )

    def _type_allowed_for_generation(self, type_: PGType) -> bool:
        if type_.name == "cstring":
            return False
        if self.exclude_catalog and type_.schema == "information_schema":
            return False
        if type_.is_pseudotype:
            return self._has_nonpseudo_concrete(type_)
        return True

    def _routine_allowed_for_generation(self, routine: Routine) -> bool:
        if routine.variadic:
            return False
        if self._is_unsafe_system_routine(routine):
            return False
        if isinstance(routine.restype, PGType) and not self._type_allowed_for_generation(routine.restype):
            return False
        for argtype in routine.argtypes:
            if isinstance(argtype, PGType) and not self._type_allowed_for_generation(argtype):
                return False
        return True

    def _is_unsafe_system_routine(self, routine: Routine) -> bool:
        if routine.schema != "pg_catalog":
            return False

        name = routine.name
        if name in self._UNSAFE_ROUTINE_NAMES:
            return True
        if name.startswith("has_") and name.endswith("_privilege"):
            return True
        return any(name.startswith(prefix) for prefix in self._UNSAFE_ROUTINE_PREFIXES)

    def _operator_allowed_for_generation(self, oper: Op) -> bool:
        for type_ in (oper.left, oper.right, oper.result):
            if isinstance(type_, PGType) and not self._type_allowed_for_generation(type_):
                return False
        return True

    def _prune_generation_objects(self):
        self.routines = [r for r in self.routines if self._routine_allowed_for_generation(r)]
        self.aggregates = [r for r in self.aggregates if self._routine_allowed_for_generation(r)]
        self.operators = [o for o in self.operators if self._operator_allowed_for_generation(o)]

    def _load_tables(self, conn, exclude_catalog: bool):
        print("Loading tables...", end="", file=sys.stderr)
        with conn.cursor() as cur:
            cur.execute(
                "select table_name, table_schema, is_insertable_into, table_type "
                "from information_schema.tables"
            )
            for tname, tschema, insertable, ttype in cur.fetchall():
                if exclude_catalog and tschema in ("pg_catalog", "information_schema"):
                    continue
                self.tables.append(
                    Table(
                        tname,
                        tschema,
                        insertable == "YES",
                        ttype == "BASE TABLE",
                    )
                )
        print("done.", file=sys.stderr)

    def _load_columns_and_constraints(self, conn):
        print("Loading columns and constraints...", end="", file=sys.stderr)
        with conn.cursor() as cur:
            for table in self.tables:
                cur.execute(
                    "select attnum, attname, atttypid, attnotnull, atthasdef "
                    "from pg_attribute "
                    "join pg_class c on (c.oid = attrelid) "
                    "join pg_namespace n on n.oid = relnamespace "
                    "where not attisdropped "
                    "and attname not in ('xmin', 'xmax', 'ctid', 'cmin', 'cmax', 'tableoid', 'oid') "
                    "and relname = %s "
                    "and nspname = %s",
                    (table.name, table.schema),
                )
                attnum_to_column = {}
                for attnum, cname, atttypid, attnotnull, atthasdef in cur.fetchall():
                    pg_type = self.oid2type.get(int(atttypid))
                    if pg_type is None:
                        continue
                    column = Column(
                        cname,
                        pg_type,
                        not_null=bool(attnotnull),
                        has_default=bool(atthasdef),
                    )
                    table.columns().append(column)
                    attnum_to_column[int(attnum)] = column

                cur.execute(
                    "select conname, contype, conkey, confrelid, confkey "
                    "from pg_class t "
                    "join pg_constraint c on (t.oid = c.conrelid) "
                    "where contype in ('f', 'u', 'p') "
                    "and relnamespace = (select oid from pg_namespace where nspname = %s) "
                    "and relname = %s",
                    (table.schema, table.name),
                )
                # PostgreSQL stores constrained columns by attnum, so we first
                # map attnum -> Column above and then translate constraint
                # arrays back into the in-memory table/column objects here.
                for conname, contype, conkey, confrelid, confkey in cur.fetchall():
                    table.constraints.append(conname)
                    if contype == "p" and conkey:
                        for attnum in conkey:
                            column = attnum_to_column.get(int(attnum))
                            if column is not None:
                                column.is_primary_key = True
                    if contype == "f" and conkey and confrelid and confkey:
                        cur.execute(
                            "select n.nspname, c.relname "
                            "from pg_class c "
                            "join pg_namespace n on n.oid = c.relnamespace "
                            "where c.oid = %s",
                            (int(confrelid),),
                        )
                        ref_row = cur.fetchone()
                        if ref_row is None:
                            continue
                        ref_schema, ref_table = ref_row
                        ref_attnums = [int(v) for v in confkey]
                        cur.execute(
                            "select attnum, attname "
                            "from pg_attribute "
                            "where attrelid = %s and attnum = any(%s)",
                            (int(confrelid), ref_attnums),
                        )
                        ref_names = {int(attnum): attname for attnum, attname in cur.fetchall()}
                        for src_attnum, dst_attnum in zip(conkey, confkey):
                            column = attnum_to_column.get(int(src_attnum))
                            if column is None:
                                continue
                            column.is_foreign_key = True
                            column.fk_ref_schema = ref_schema
                            column.fk_ref_table = ref_table
                            column.fk_ref_column = ref_names.get(int(dst_attnum), "")

            cur.execute(
                "select c.confrelid, n.nspname, t.relname "
                "from pg_constraint c "
                "join pg_class t on t.oid = c.confrelid "
                "join pg_namespace n on n.oid = t.relnamespace "
                "where c.contype = 'f'"
            )
            referenced = {(schema_name, relname) for _, schema_name, relname in cur.fetchall()}
            for table in self.tables:
                if (table.schema, table.name) in referenced:
                    table.is_referenced_by_fk = True
        print("done.", file=sys.stderr)

    def _load_operators(self, conn):
        print("Loading operators...", end="", file=sys.stderr)
        with conn.cursor() as cur:
            cur.execute(
                "select oprname, oprleft, oprright, oprresult "
                "from pg_catalog.pg_operator "
                "where 0 not in (oprresult, oprright, oprleft)"
            )
            for oprname, oprleft, oprright, oprresult in cur.fetchall():
                left = self.oid2type.get(int(oprleft))
                right = self.oid2type.get(int(oprright))
                result = self.oid2type.get(int(oprresult))
                if left is None or right is None or result is None:
                    continue
                self.register_operator(Op(oprname, left, right, result))
        print("done.", file=sys.stderr)

    def _load_routines(self, conn):
        print("Loading routines...", end="", file=sys.stderr)
        procedure_is_aggregate = "proisagg" if self.version_num < 110000 else "prokind = 'a'"
        procedure_is_window = "proiswindow" if self.version_num < 110000 else "prokind = 'w'"

        with conn.cursor() as cur:
            cur.execute(
                "select (select nspname from pg_namespace where oid = pronamespace), "
                "oid, prorettype, proname, provariadic "
                "from pg_proc "
                "where prorettype::regtype::text not in ('event_trigger', 'trigger', 'opaque', 'internal') "
                "and proname <> 'pg_event_trigger_table_rewrite_reason' "
                "and proname <> 'pg_event_trigger_table_rewrite_oid' "
                "and proname !~ '^ri_fkey_' "
                f"and not (proretset or {procedure_is_aggregate} or {procedure_is_window})"
            )
            for schema_name, oid, prorettype, proname, provariadic in cur.fetchall():
                restype = self.oid2type.get(int(prorettype))
                if restype is None:
                    continue
                self.register_routine(
                    Routine(
                        schema_name,
                        str(oid),
                        restype,
                        proname,
                        variadic=int(provariadic) != 0,
                    )
                )

            print("done.", file=sys.stderr)
            print("Loading routine parameters...", end="", file=sys.stderr)
            # Argument types are expanded in a second pass because pg_proc keeps
            # return type and argtype arrays in different columns.
            for proc in self.routines:
                cur.execute("select unnest(proargtypes) from pg_proc where oid = %s", (proc.specific_name,))
                for (arg_oid,) in cur.fetchall():
                    argtype = self.oid2type.get(int(arg_oid))
                    if argtype is not None:
                        proc.argtypes.append(argtype)
        print("done.", file=sys.stderr)

    def _load_aggregates(self, conn):
        print("Loading aggregates...", end="", file=sys.stderr)
        procedure_is_aggregate = "proisagg" if self.version_num < 110000 else "prokind = 'a'"
        procedure_is_window = "proiswindow" if self.version_num < 110000 else "prokind = 'w'"

        with conn.cursor() as cur:
            cur.execute(
                "select (select nspname from pg_namespace where oid = pronamespace), "
                "oid, prorettype, proname, provariadic "
                "from pg_proc "
                "where prorettype::regtype::text not in ('event_trigger', 'trigger', 'opaque', 'internal') "
                "and proname not in ('pg_event_trigger_table_rewrite_reason') "
                "and proname not in ("
                "'percentile_cont', 'dense_rank', 'cume_dist', "
                "'rank', 'test_rank', 'percent_rank', 'percentile_disc', 'mode', 'test_percentile_disc'"
                ") "
                "and proname !~ '^ri_fkey_' "
                f"and not (proretset or {procedure_is_window}) "
                f"and {procedure_is_aggregate}"
            )
            for schema_name, oid, prorettype, proname, provariadic in cur.fetchall():
                restype = self.oid2type.get(int(prorettype))
                if restype is None:
                    continue
                self.register_aggregate(
                    Routine(
                        schema_name,
                        str(oid),
                        restype,
                        proname,
                        variadic=int(provariadic) != 0,
                    )
                )

            print("done.", file=sys.stderr)
            print("Loading aggregate parameters...", end="", file=sys.stderr)
            # Aggregates are loaded in the same two-phase shape as routines so
            # the generator can index them by both return type and arg types.
            for proc in self.aggregates:
                cur.execute("select unnest(proargtypes) from pg_proc where oid = %s", (proc.specific_name,))
                for (arg_oid,) in cur.fetchall():
                    argtype = self.oid2type.get(int(arg_oid))
                    if argtype is not None:
                        proc.argtypes.append(argtype)
        print("done.", file=sys.stderr)

    def quote_name(self, identifier: str) -> str:
        escaped = identifier.replace('"', '""')
        return f'"{escaped}"'
