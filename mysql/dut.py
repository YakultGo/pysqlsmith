"""MySQL DUT (Device Under Test): execute generated SQL against a live MySQL instance."""

from __future__ import annotations
from typing import TYPE_CHECKING

import pymysql

if TYPE_CHECKING:
    from .main import RunConfig


class DutMySQL:
    """Device Under Test: execute generated SQL against a live MySQL instance."""

    def __init__(self, config: "RunConfig", log: bool = False):
        self.config = config
        self.log = log
        self.queries: int = 0
        self.failed: int = 0
        self._err_file = None
        if log:
            self._err_file = open("queries.err", "a")

    def test(self, stmt: str):
        self._command(stmt)

    def _command(self, sql: str):
        # MySQL execution is intentionally stateless per statement: connect, run, commit, close.
        conn = pymysql.connect(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            database=self.config.dbname,
            connect_timeout=10,
            read_timeout=10,
        )
        self.queries += 1
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
        except Exception as e:
            self.failed += 1
            if self.log and self._err_file:
                self._err_file.write(sql + "\n")
                self._err_file.write(str(e) + "\n")
                self._err_file.flush()
        finally:
            conn.close()

        if self.queries % 1000 == 0 and self.log and self._err_file:
            self._err_file.write(f"Failed/Queries={self.failed}/{self.queries}\n")
            self._err_file.flush()
