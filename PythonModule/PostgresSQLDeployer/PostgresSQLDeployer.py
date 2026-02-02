#!/usr/bin/env python3
"""
PostgreSQL multi-DB SQL deployer.

Usage example:

    deployer = PostgresSqlDeployer(host="127.0.0.1", user="postgres", password="")
    deployer.deploy_file(
        sql_path="./audit_triggers.sql",
        include=None,                    # e.g. ["appdb_1", "appdb_2"]
        exclude=["postgres"],            # excluded by default anyway
        dry_run=False,
        continue_on_error=False,
        statement_timeout_ms=30000,      # optional
    )
"""

from __future__ import annotations
from pathlib import Path
from typing import Iterable, List, Optional, Union

import psycopg
from psycopg.rows import tuple_row


class PostgresSqlDeployer:
    """
    Deploy a SQL file/string to multiple PostgreSQL databases on one server.

    - Enumerates non-template DBs (excludes template0/template1; also excludes 'postgres' by default)
    - Executes within a transaction per database
    - Optional include/exclude filters
    - Optional dry-run and statement timeout
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "",
        sslmode: Optional[str] = None,
        default_exclude: Optional[Iterable[str]] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.sslmode = sslmode
        # Always exclude 'postgres' unless explicitly included
        self.default_exclude = set(default_exclude or []) | {"postgres"}

    # ---- Public API ---------------------------------------------------------

    def deploy_file(
        self,
        sql_path: str | Path,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        dry_run: bool = False,
        continue_on_error: bool = False,
        statement_timeout_ms: Optional[int] = None,
    ) -> None:
        """Read SQL from file, then deploy to target databases."""
        sql_text = self.read_sql_file(Path(sql_path))
        self.deploy_sql(
            sql_text=sql_text,
            include=include,
            exclude=exclude,
            dry_run=dry_run,
            continue_on_error=continue_on_error,
            statement_timeout_ms=statement_timeout_ms,
        )

    def deploy_sql(
        self,
        sql_text: str,
        include: Optional[Iterable[str]] = None,
        exclude: Optional[Iterable[str]] = None,
        dry_run: bool = False,
        continue_on_error: bool = False,
        statement_timeout_ms: Optional[int] = None,
    ) -> None:
        """Deploy raw SQL text to target databases."""
        all_dbs = self.list_databases()
        targets = self._filter_databases(all_dbs, include, exclude)

        if not targets:
            self._log("No target databases after filtering. Nothing to do.")
            return

        self._log(f"Target DBs: {', '.join(targets)}")
        for db in targets:
            try:
                self._run_sql_on_db(
                    dbname=db,
                    sql_text=sql_text,
                    statement_timeout_ms=statement_timeout_ms,
                    dry_run=dry_run,
                )
            except Exception as e:
                self._err(f"[{db}] ERROR: {e}")
                if not continue_on_error:
                    raise

    def list_databases(self) -> List[str]:
        """Return all non-template databases on the server."""
        with self._connect("postgres") as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT datname
                    FROM pg_database
                    WHERE datistemplate = false
                    ORDER BY datname;
                    """
                )
                return [r[0] for r in cur.fetchall()]

    # ---- Internals ----------------------------------------------------------

    def _connect(self, dbname: str):
        dsn_parts = [
            f"host={self.host}",
            f"port={self.port}",
            f"dbname={dbname}",
            f"user={self.user}",
        ]
        if self.password:
            dsn_parts.append(f"password={self.password}")
        if self.sslmode:
            dsn_parts.append(f"sslmode={self.sslmode}")
        dsn = " ".join(dsn_parts)
        return psycopg.connect(dsn, row_factory=tuple_row)

    def read_sql_file(self, path: Union[str, Path]) -> str:
        p = Path(path)  # normalize to Path
        if not p.exists():
            raise FileNotFoundError(f"SQL file not found: {p}")
        return p.read_text(encoding="utf-8")

    def _filter_databases(
        self,
        all_dbs: Iterable[str],
        include: Optional[Iterable[str]],
        exclude: Optional[Iterable[str]],
    ) -> List[str]:
        s = set(all_dbs)
        s -= self.default_exclude
        if exclude:
            s -= set(exclude)
        if include:
            s &= set(include)
        return sorted(s)

    def _run_sql_on_db(
        self,
        dbname: str,
        sql_text: str,
        statement_timeout_ms: Optional[int],
        dry_run: bool,
    ) -> None:
        self._log(f"==> [{dbname}] starting")
        if dry_run:
            self._log(f"    [dry-run] would execute SQL ({len(sql_text)} chars)")
            return

        with self._connect(dbname) as conn:
            # Transaction per database
            with conn.transaction():
                if statement_timeout_ms is not None:
                    with conn.cursor() as cur:
                        cur.execute(f"SET LOCAL statement_timeout = {int(statement_timeout_ms)};")
                        cur.execute("SET LOCAL lock_timeout = 1000;")  # 1s lock timeout guard
                with conn.cursor() as cur:
                    cur.execute(sql_text)

        self._log(f"==> [{dbname}] done")

    # ---- Logging hooks ------------------------------------------------------

    def _log(self, msg: str) -> None:
        print(msg, flush=True)

    def _err(self, msg: str) -> None:
        print(msg, flush=True)


# Optional: quick CLI shim (so you can also run the file directly)
if __name__ == "__main__":
    import argparse, os, sys

    def parse_args():
        p = argparse.ArgumentParser(description="Deploy a SQL file to each PostgreSQL database on a server.")
        p.add_argument("--host", default=os.getenv("PGHOST", "127.0.0.1"))
        p.add_argument("--port", type=int, default=int(os.getenv("PGPORT", "5432")))
        p.add_argument("--user", default=os.getenv("PGUSER", "postgres"))
        p.add_argument("--password", default=os.getenv("PGPASSWORD", ""))
        p.add_argument("--sslmode", default=os.getenv("PGSSLMODE", None))
        p.add_argument("--sql", required=True, help="Path to the SQL file to execute.")
        p.add_argument("--include", nargs="*", help="Only these DBs (space-separated).")
        p.add_argument("--exclude", nargs="*", help="DBs to exclude (space-separated). 'postgres' excluded by default.")
        p.add_argument("--continue-on-error", action="store_true", help="Continue to next DB if one fails.")
        p.add_argument("--statement-timeout-ms", type=int, default=None, help="Optional statement_timeout in ms.")
        p.add_argument("--dry-run", action="store_true", help="Don’t execute, only show what would happen.")
        return p.parse_args()

    args = parse_args()

    try:
        deployer = PostgresSqlDeployer(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            sslmode=args.sslmode,
        )
        deployer.deploy_file(
            sql_path=args.sql,
            include=args.include,
            exclude=args.exclude,
            dry_run=args.dry_run,
            continue_on_error=args.continue_on_error,
            statement_timeout_ms=args.statement_timeout_ms,
        )
    except Exception as e:
        print(f"Fatal: {e}", file=sys.stderr)
        sys.exit(1)
