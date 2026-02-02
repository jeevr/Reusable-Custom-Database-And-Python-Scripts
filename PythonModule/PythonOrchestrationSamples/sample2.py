"""
simple_orchestrator.py

pip install psycopg2-binary pandas

Env vars (or hard-code the DSN below):
  PG_DSN="postgresql://user:pass@host:5432/dbname"
"""

import os
import psycopg2
import pandas as pd

DSN = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")

# ========== DB helpers ==========

def get_conn():
    """
    Create a connection and set basic session settings.
    We do NOT enable autocommit so that 'with conn:' manages commit/rollback.
    """
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SET application_name = %s", ("simple_orchestrator",))
        cur.execute("SET statement_timeout = %s", (30_000,))  # 30s per statement
        cur.execute("SET lock_timeout = %s", (5_000,))        # 5s lock wait
    return conn

def select_df(conn, sql, params=None):
    """Read a SELECT into a pandas DataFrame within the current transaction."""
    return pd.read_sql_query(sql, conn, params=params)

def call_sp(conn, qualified_name, params=()):
    """
    CALL a stored procedure.
    If your SP has OUT params, PostgreSQL returns one row -> fetchone().
    If not, skip fetchone().
    """
    with conn.cursor() as cur:
        placeholders = ", ".join(["%s"] * len(params))
        sql = f"CALL {qualified_name}({placeholders})" if placeholders else f"CALL {qualified_name}()"
        cur.execute(sql, params)
        try:
            return cur.fetchone()   # (out1, out2, ...) if SP defines OUT params
        except psycopg2.ProgrammingError:
            # No OUT params (or nothing to fetch)
            conn.rollback()  # fetchone() error moves us out of sync; reset safely
            # Re-run nothing, just start a fresh tx boundary for the caller
            # Caller should be in `with conn:` which begins a new tx automatically
            raise

def stream_select_chunks(conn, sql, params=None, fetch_size=5000):
    """
    Server-side cursor (named cursor) to get large results in chunks.
    Yields pandas DataFrames of up to fetch_size rows.
    """
    with conn.cursor(name="stream_cur") as cur:
        cur.itersize = fetch_size
        cur.execute(sql, params or ())
        cols = [d.name for d in cur.description]
        while True:
            rows = cur.fetchmany(fetch_size)
            if not rows:
                break
            yield pd.DataFrame(rows, columns=cols)

# ========== Orchestration ==========

def orchestrate_since(since_ts: str):
    """
    1) SELECT some rows since a timestamp
    2) Check simple conditions in pandas
    3) Conditionally CALL stored procedures
    All inside a single transaction (commit on success, rollback on error).
    """
    conn = get_conn()
    try:
        with conn:  # <-- Transaction boundary (commit on block exit if no exception)
            # ---- Step 1: fetch recent records into a DataFrame
            df = select_df(conn, """
                SELECT job_id, status, updated_at, value
                FROM public.etl_inputs
                WHERE updated_at >= %s
            """, params=(since_ts,))

            print(f"Fetched {len(df)} rows since {since_ts}")

            if df.empty:
                print("No data; skipping stored procedures.")
                return

            # ---- Step 2: simple checks
            errors = int((df["status"] == "error").sum())
            avg_value = float(df["value"].mean())
            print(f"errors={errors} avg_value={avg_value:.4f}")

            # ---- Step 3: conditionally call SPs
            if errors > 10:
                out = call_sp(conn, "public.handle_failures", (since_ts,))
                print("handle_failures OUT:", out)

            if avg_value > 0.85:
                out = call_sp(conn, "public.promote_batch", (since_ts,))
                print("promote_batch OUT:", out)

            # If you need to abort on a condition, just raise an exception:
            # if errors > 100:
            #     raise RuntimeError("Too many errors; aborting this run.")

        # Exiting the `with conn:` block commits if no exceptions occurred.
        print("Done. Transaction committed.")
    except Exception as e:
        # Any error inside `with conn:` triggers an automatic rollback.
        print("Error encountered; transaction rolled back.")
        print("Detail:", e)
    finally:
        conn.close()

def summarize_streaming(since_ts: str):
    """
    Example for large data: stream results in chunks, compute a summary,
    then write a summary via an SP in the SAME transaction.
    """
    conn = get_conn()
    try:
        with conn:
            total = 0
            err_rows = 0
            for chunk in stream_select_chunks(conn, """
                SELECT job_id, status, updated_at, value
                FROM public.etl_inputs
                WHERE updated_at >= %s
                ORDER BY updated_at
            """, params=(since_ts,), fetch_size=10_000):
                total += len(chunk)
                err_rows += int((chunk["status"] == "error").sum())

            print(f"Stream summary: total={total} errors={err_rows}")

            out = call_sp(conn, "public.record_summary", (since_ts, total, err_rows))
            print("record_summary OUT:", out)

        print("Streaming step committed.")
    except Exception as e:
        print("Streaming step failed; rolled back.")
        print("Detail:", e)
    finally:
        conn.close()

# ========== Run it ==========

if __name__ == "__main__":
    # Change this timestamp as needed
    SINCE_TS = os.getenv("SINCE_TS", "2025-09-01 00:00:00")

    orchestrate_since(SINCE_TS)
    # summarize_streaming(SINCE_TS)
