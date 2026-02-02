"""
orchestrator.py

Reusable orchestration utilities for:
- Running SELECTs into pandas (same transaction)
- Streaming large results with a server-side cursor
- Conditional logic -> CALL stored procedures (SPs)
- Robust error handling + SQLSTATE-based retries
- Simple step pipeline you can extend

Requires: psycopg2-binary, pandas
"""

from __future__ import annotations
import os
import time
import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2
import psycopg2.extras
import pandas as pd

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("orchestrator")

# -----------------------------
# Config
# -----------------------------
@dataclass(frozen=True)
class DBConfig:
    dsn: str
    application_name: str = "etl_orchestrator"
    statement_timeout_ms: int = 30_000   # 30s per statement
    lock_timeout_ms: int = 5_000
    idle_in_tx_session_timeout_ms: int = 120_000
    isolation_level: str = "READ COMMITTED"  # or "REPEATABLE READ"
    autocommit: bool = False

    @staticmethod
    def from_env(prefix: str = "PG") -> "DBConfig":
        # Example DSN envs (or pass a full DSN via PG_DSN)
        dsn = (
            os.getenv(f"{prefix}_DSN")
            or f"dbname={os.getenv(f'{prefix}_DATABASE','postgres')} "
               f"user={os.getenv(f'{prefix}_USER','postgres')} "
               f"password={os.getenv(f'{prefix}_PASSWORD','postgres')} "
               f"host={os.getenv(f'{prefix}_HOST','localhost')} "
               f"port={os.getenv(f'{prefix}_PORT','5432')}"
        )
        return DBConfig(
            dsn=dsn,
            application_name=os.getenv("APP_NAME", "etl_orchestrator"),
            statement_timeout_ms=int(os.getenv("PG_STMT_TIMEOUT_MS", "30000")),
            lock_timeout_ms=int(os.getenv("PG_LOCK_TIMEOUT_MS", "5000")),
            idle_in_tx_session_timeout_ms=int(os.getenv("PG_IDLE_TX_TIMEOUT_MS", "120000")),
            isolation_level=os.getenv("PG_ISOLATION", "READ COMMITTED"),
            autocommit=False,
        )

# -----------------------------
# DB client
# -----------------------------
class PostgresClient:
    def __init__(self, cfg: DBConfig):
        self.cfg = cfg

    def connect(self):
        conn = psycopg2.connect(self.cfg.dsn)
        conn.autocommit = self.cfg.autocommit
        # session setup
        with conn.cursor() as cur:
            cur.execute("SET application_name = %s", (self.cfg.application_name,))
            cur.execute("SET statement_timeout = %s", (self.cfg.statement_timeout_ms,))
            cur.execute("SET lock_timeout = %s", (self.cfg.lock_timeout_ms,))
            cur.execute("SET idle_in_transaction_session_timeout = %s", (self.cfg.idle_in_tx_session_timeout_ms,))
            cur.execute(f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {self.cfg.isolation_level}")
        return conn

    # ----------- SELECT -> DataFrame (in current tx) -----------
    def select_df(self, conn, sql: str, params: Optional[Sequence[Any]] = None) -> pd.DataFrame:
        # Uses the same connection/transaction boundary as your SP calls
        return pd.read_sql_query(sql, conn, params=params)

    # ----------- Streaming SELECT (server-side cursor) ----------
    def stream_select_df(
        self,
        conn,
        sql: str,
        params: Optional[Sequence[Any]] = None,
        fetch_size: int = 5_000,
        cursor_name: str = "stream_cursor",
    ) -> Iterable[pd.DataFrame]:
        """
        Yields DataFrames in chunks using a server-side cursor.
        """
        with conn.cursor(name=cursor_name) as cur:
            cur.itersize = fetch_size
            cur.execute(sql, params or ())
            cols = [d.name for d in cur.description]
            while True:
                rows = cur.fetchmany(fetch_size)
                if not rows:
                    break
                yield pd.DataFrame(rows, columns=cols)

    # ----------- CALL stored procedure -----------
    def call_procedure(
        self,
        conn,
        qualified_name: str,
        in_params: Optional[Sequence[Any]] = None,
        expect_out_row: bool = True,
    ) -> Optional[Tuple[Any, ...]]:
        """
        CALL schema.proc(%s, %s, ...) optionally fetches the single OUT row.
        In Postgres, CALL with OUT params returns one row.
        """
        with conn.cursor() as cur:
            placeholders = ", ".join(["%s"] * len(in_params)) if in_params else ""
            sql = f"CALL {qualified_name}({placeholders})"
            cur.execute(sql, in_params or ())
            if expect_out_row:
                return cur.fetchone()  # tuple of OUT params
        return None

# -----------------------------
# Retry policy & helpers
# -----------------------------
TRANSIENT_SQLSTATES = {
    "40001",  # serialization_failure
    "40P01",  # deadlock_detected
    "55P03",  # lock_not_available
    "57014",  # query_canceled (can include statement_timeout)
    "53300",  # too_many_connections
    "08006", "08003", "08001",  # connection issues
}

def is_transient_sqlstate(pgcode: Optional[str]) -> bool:
    return pgcode in TRANSIENT_SQLSTATES

@dataclass
class RetryPolicy:
    max_attempts: int = 3
    initial_backoff_s: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff_s: float = 15.0

    def sleep(self, attempt: int):
        delay = min(self.initial_backoff_s * (self.backoff_multiplier ** (attempt - 1)), self.max_backoff_s)
        log.warning("Retrying in %.1fs ...", delay)
        time.sleep(delay)

# -----------------------------
# Orchestration base
# -----------------------------
class Orchestrator:
    """
    Extend this class to implement your own steps.
    Each step runs in its own transaction by default.
    """
    def __init__(self, db: PostgresClient, retry: RetryPolicy = RetryPolicy()):
        self.db = db
        self.retry = retry

    def run(self):
        """
        Override to define your pipeline (sequence of steps).
        """
        raise NotImplementedError

    # Utility to run a function inside a tx with retries on transient errors
    def _run_tx(self, fn: Callable[[Any], None], label: str):
        attempt = 0
        while True:
            attempt += 1
            try:
                conn = self.db.connect()
                try:
                    with conn:  # commit/rollback boundary
                        fn(conn)
                    log.info("Step '%s' committed.", label)
                    return
                finally:
                    conn.close()
            except psycopg2.Error as e:
                log.error("Step '%s' failed (attempt %d). sqlstate=%s detail=%s",
                          label, attempt, getattr(e, "pgcode", None), str(e))
                if attempt < self.retry.max_attempts and is_transient_sqlstate(getattr(e, "pgcode", None)):
                    self.retry.sleep(attempt)
                    continue
                raise  # re-raise non-transient or exhausted retries

# -----------------------------
# Example: Implement your pipeline
# -----------------------------
class ExampleETL(Orchestrator):
    """
    Example pipeline showing:
    1) DataFrame check
    2) Conditional SP calls
    3) Optional streaming read
    """

    def __init__(self, db: PostgresClient, since_ts: str, retry: RetryPolicy = RetryPolicy()):
        super().__init__(db, retry)
        self.since_ts = since_ts

    def run(self):
        self._run_tx(self._step_fetch_and_decide, "fetch_and_decide")
        self._run_tx(self._step_stream_and_summarize, "stream_and_summarize")

    # ---- Step 1: normal SELECT -> DataFrame, then CALL SPs conditionally
    def _step_fetch_and_decide(self, conn):
        sql = """
            SELECT job_id, status, updated_at, value
            FROM public.etl_inputs
            WHERE updated_at >= %s
        """
        df = self.db.select_df(conn, sql, params=(self.since_ts,))
        log.info("Fetched %d rows since %s", len(df), self.since_ts)

        if df.empty:
            log.info("No rows -> skipping SP calls.")
            return

        # Example conditions
        errors = (df["status"] == "error").sum()
        avg_value = float(df["value"].mean())

        log.info("errors=%s avg_value=%.4f", errors, avg_value)

        # Branch to SP calls
        if errors > 10:
            out = self.db.call_procedure(conn, "public.handle_failures", in_params=(self.since_ts,), expect_out_row=True)
            log.info("handle_failures OUT=%s", out)

        if avg_value > 0.85:
            out = self.db.call_procedure(conn, "public.promote_batch", in_params=(self.since_ts,), expect_out_row=True)
            log.info("promote_batch OUT=%s", out)

        # If you want to *soft-fail* (i.e., abort commit) on a condition:
        # if errors > 100:
        #     raise psycopg2.ProgrammingError("Aborting by policy: too many errors")

    # ---- Step 2: streaming example (server-side cursor)
    def _step_stream_and_summarize(self, conn):
        stream_sql = """
            SELECT job_id, status, updated_at, value
            FROM public.etl_inputs
            WHERE updated_at >= %s
            ORDER BY updated_at
        """
        total = 0
        err_rows = 0
        for chunk in self.db.stream_select_df(conn, stream_sql, params=(self.since_ts,), fetch_size=10_000):
            total += len(chunk)
            if not chunk.empty:
                err_rows += int((chunk["status"] == "error").sum())
        log.info("Stream summary: total=%d, errors=%d", total, err_rows)

        # Optional: write a summary row via SP (still in same tx)
        out = self.db.call_procedure(
            conn,
            "public.record_summary",
            in_params=(self.since_ts, total, err_rows),
            expect_out_row=True
        )
        log.info("record_summary OUT=%s", out)

# -----------------------------
# Entrypoint
# -----------------------------
def main():
    cfg = DBConfig.from_env("PG")
    client = PostgresClient(cfg)

    # Example: run pipeline for data since a timestamp
    since_ts = os.getenv("SINCE_TS", "2025-09-01 00:00:00")
    etl = ExampleETL(client, since_ts=since_ts)

    try:
        etl.run()
        log.info("Pipeline finished OK.")
    except Exception as e:
        log.exception("Pipeline failed: %s", e)
        raise

if __name__ == "__main__":
    main()
