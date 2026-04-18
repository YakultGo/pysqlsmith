"""PostgreSQL DUT (Device Under Test): execute generated SQL against a live instance."""

from __future__ import annotations
from typing import TYPE_CHECKING

from pgsmith.exceptions import DutBroken, DutFailure, DutSyntax, DutTimeout

if TYPE_CHECKING:
    from pgsmith.main import RunConfig


def _import_psycopg():
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL support requires psycopg. Install it with "
            "`pip install 'psycopg[binary]'`."
        ) from exc
    return psycopg


class DutPostgres:
    def __init__(self, config: "RunConfig", log: bool = False):
        self.config = config
        self.log = log
        self.queries: int = 0
        self.failed: int = 0
        self._psycopg = _import_psycopg()
        self._conn = None
        self._connect()

    def _connect(self):
        # PostgreSQL keeps one connection around so statement_timeout and session settings only need
        # to be configured once per reconnect.
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = self._psycopg.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            dbname=self.config.dbname,
        )
        self._conn.autocommit = False
        with self._conn.cursor() as cur:
            cur.execute("set statement_timeout to '1s'")
            cur.execute("set client_min_messages to 'ERROR'")
            cur.execute("set application_name to 'pysqlsmith::dut'")
        self._conn.commit()

    def _ensure_connection(self):
        if self._conn is None or self._conn.closed:
            self._connect()

    def _classify(self, exc: Exception) -> DutFailure:
        sqlstate = getattr(exc, "sqlstate", "") or ""
        msg = str(exc)

        if sqlstate.startswith("08"):
            return DutBroken(msg, sqlstate)
        if sqlstate == "57014":
            return DutTimeout(msg, sqlstate)
        if sqlstate == "42601":
            return DutSyntax(msg, sqlstate)
        if isinstance(exc, self._psycopg.OperationalError):
            return DutBroken(msg, sqlstate)
        return DutFailure(msg, sqlstate)

    def test(self, stmt: str):
        self.queries += 1
        try:
            self._ensure_connection()
            self._conn.rollback()
            with self._conn.cursor() as cur:
                cur.execute(stmt)
            self._conn.rollback()
        except Exception as exc:
            self.failed += 1
            classified = self._classify(exc)
            try:
                self._conn.rollback()
            except Exception:
                pass
            if isinstance(classified, DutBroken):
                try:
                    self._connect()
                except Exception:
                    pass
            raise classified

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
