"""
mixed_orchestrator.py

Pipeline:
  - Step 1: Read source rows since timestamp -> DataFrame
  - Step 2: Pandas transforms (clean/aggregate)
  - Step 3: CALL public.pre_ingest_check(since_ts)    -- SP A
  - Step 4: Read again (e.g., reference/lookup tables)
  - Step 5: Bulk dump processed DataFrame into DB:
            COPY -> temp table -> MERGE/UPSERT into target
  - Step 6: CALL public.post_ingest_finalize(since_ts) -- SP B

Requires:
  pip install psycopg2-binary pandas

Env:
  PG_DSN="postgresql://user:pass@host:5432/dbname"
  SINCE_TS="2025-09-01 00:00:00"
"""

import os
import io
import sys
import psycopg2
import pandas as pd


DSN = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")
SINCE_TS = os.getenv("SINCE_TS", "2025-09-01 00:00:00")


# ------------------------
# DB helpers
# ------------------------
def get_conn():
    conn = psycopg2.connect(DSN)
    conn.autocommit = False  # Python owns the transaction
    with conn.cursor() as cur:
        cur.execute("SET application_name = %s", ("mixed_orchestrator",))
        cur.execute("SET statement_timeout = %s", (60_000,))  # 60s/statement
        cur.execute("SET lock_timeout = %s", (10_000,))       # 10s lock wait
    return conn

def select_df(conn, sql, params=None):
    return pd.read_sql_query(sql, conn, params=params)

def call_sp(conn, qualified_name, params=()):
    with conn.cursor() as cur:
        placeholders = ", ".join(["%s"] * len(params))
        sql = f"CALL {qualified_name}({placeholders})" if placeholders else f"CALL {qualified_name}()"
        cur.execute(sql, params)
        # If your SP has OUT params, you can fetch them:
        try:
            return cur.fetchone()
        except psycopg2.ProgrammingError:
            # No OUT params to fetch; ignore
            conn.rollback()  # resync after failed fetchone attempt
            raise

def copy_dataframe_to_temp(conn, df: pd.DataFrame, temp_table: str):
    """
    Creates a TEMP TABLE with appropriate columns and COPYs the DataFrame into it.
    - Simple mapping: pandas dtype -> Postgres type (basic heuristic).
    - Adjust column types as needed for your schema.
    """
    if df.empty:
        raise ValueError("DataFrame is empty; nothing to COPY.")

    # Basic dtype mapping
    def pg_type(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return "bigint"
        if pd.api.types.is_float_dtype(dtype):
            return "double precision"
        if pd.api.types.is_bool_dtype(dtype):
            return "boolean"
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return "timestamp"
        return "text"

    cols = df.columns.tolist()
    col_defs = ", ".join(f'"{c}" {pg_type(df[c].dtype)}' for c in cols)

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        cur.execute(f"CREATE TEMP TABLE {temp_table} ({col_defs}) ON COMMIT DROP")

    # Use CSV via StringIO for COPY
    buf = io.StringIO()
    # Ensure datetime becomes ISO strings; keep NaNs as empty
    df.to_csv(buf, index=False, header=False, na_rep="")
    buf.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert(
            f'COPY {temp_table} ("' + '","'.join(cols) + '") FROM STDIN WITH (FORMAT CSV)',
            buf
        )

def upsert_from_temp(conn, temp_table: str, target_table: str, key_cols, data_cols=None):
    """
    Merge temp -> target using ON CONFLICT (key_cols) DO UPDATE.
    - key_cols: list of column names forming a unique or primary key in target
    - data_cols: list of columns to update; defaults to all non-key columns
    """
    if data_cols is None:
        with conn.cursor() as cur:
            cur.execute(f"SELECT column_name FROM information_schema.columns "
                        f"WHERE table_name = %s ORDER BY ordinal_position", (target_table.split('.')[-1],))
            target_cols = [r[0] for r in cur.fetchall()]
        data_cols = [c for c in target_cols if c not in key_cols]

    all_cols = key_cols + data_cols
    quoted_cols = ', '.join(f'"{c}"' for c in all_cols)
    excluded_assignments = ', '.join(f'"{c}" = EXCLUDED."{c}"' for c in data_cols)

    # Insert columns must exist both in temp and target with same names
    sql = f"""
        INSERT INTO {target_table} ({quoted_cols})
        SELECT {quoted_cols} FROM {temp_table}
        ON CONFLICT ({', '.join(f'"{c}"' for c in key_cols)})
        DO UPDATE SET
            {excluded_assignments};
    """
    with conn.cursor() as cur:
        cur.execute(sql)


# ------------------------
# Pandas processing
# ------------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Beginner-friendly example transforms:
      - Filter unwanted rows
      - Fill missing values
      - Compute a simple aggregate/signal
    """
    if df.empty:
        return df

    # Keep only finished/ready jobs
    df = df[df["status"].isin(["ready", "finished"])].copy()

    # Fill missing numeric values
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0.0)

    # Example derived metric
    if "value" in df.columns:
        df["value_norm"] = (df["value"] - df["value"].mean()) / (df["value"].std() or 1.0)

    # Ensure types commonly used in DB
    for col in df.columns:
        if pd.api.types.is_bool_dtype(df[col].dtype):
            df[col] = df[col].astype(bool)
        elif pd.api.types.is_integer_dtype(df[col].dtype):
            df[col] = df[col].astype("Int64")  # nullable int
        # timestamps and strings are fine

    return df


# ------------------------
# Pipeline steps
# ------------------------
def step_1_read_initial(conn, since_ts: str) -> pd.DataFrame:
    sql = """
        SELECT job_id, status, updated_at, value
        FROM public.etl_inputs
        WHERE updated_at >= %s
    """
    df = select_df(conn, sql, params=(since_ts,))
    print(f"[Step 1] fetched rows: {len(df)}")
    return df

def step_3_call_pre_ingest(conn, since_ts: str):
    print("[Step 3] CALL public.pre_ingest_check")
    with conn.cursor() as cur:
        cur.execute("CALL public.pre_ingest_check(%s)", (since_ts,))
        # If there are OUT params, use cur.fetchone()

def step_4_read_reference(conn) -> pd.DataFrame:
    sql = """
        SELECT code, multiplier
        FROM public.ref_multipliers
    """
    ref = select_df(conn, sql)
    print(f"[Step 4] reference rows: {len(ref)}")
    return ref

def step_5_dump_processed(conn, df_processed: pd.DataFrame):
    """
    COPY to temp then MERGE into target table.
    Adjust TARGET and keys to your schema.
    """
    if df_processed.empty:
        print("[Step 5] nothing to dump (empty DataFrame).")
        return

    temp_name = "temp_processed_input"
    target = "public.processed_inputs"
    key_cols = ["job_id"]  # must match a unique/PK on target

    print(f"[Step 5] dumping {len(df_processed)} rows via COPY -> MERGE")
    copy_dataframe_to_temp(conn, df_processed, temp_table=temp_name)
    # Decide which columns to update (all non-keys by default). You can pass data_cols=... if needed.
    upsert_from_temp(conn, temp_table=temp_name, target_table=target, key_cols=key_cols)

def step_6_call_post_ingest(conn, since_ts: str):
    print("[Step 6] CALL public.post_ingest_finalize")
    with conn.cursor() as cur:
        cur.execute("CALL public.post_ingest_finalize(%s)", (since_ts,))
        # If there are OUT params, use cur.fetchone()


# ------------------------
# Orchestrator (main)
# ------------------------
def run_pipeline(since_ts: str):
    """
    This version uses ONE transaction for the whole run so either
    all steps commit together or everything rolls back on any error.

    If you prefer isolation per step, wrap each step in its own `with conn:`.
    """
    conn = get_conn()
    try:
        with conn:  # single TX for the whole pipeline
            # Step 1: read
            df_raw = step_1_read_initial(conn, since_ts)

            # Step 2: pandas processing
            print("[Step 2] transforming in pandas")
            df_processed = transform(df_raw)

            # Step 3: SP A (pre-ingest checks)
            step_3_call_pre_ingest(conn, since_ts)

            # Step 4: read again (e.g., ref data), optionally join with processed
            df_ref = step_4_read_reference(conn)
            if not df_processed.empty and not df_ref.empty and "code" in df_ref.columns:
                # Example join if your processed df has a 'code' column
                if "code" in df_processed.columns:
                    df_processed = df_processed.merge(df_ref, on="code", how="left")
                    if "multiplier" in df_processed.columns and "value" in df_processed.columns:
                        df_processed["value_x_mult"] = df_processed["value"] * df_processed["multiplier"].fillna(1.0)

            # Step 5: dump DataFrame -> DB (COPY -> MERGE)
            step_5_dump_processed(conn, df_processed)

            # Step 6: SP B (finalize/housekeeping)
            step_6_call_post_ingest(conn, since_ts)

        print("Pipeline committed successfully.")
    except Exception as e:
        print("Pipeline failed; transaction rolled back.")
        print("Detail:", e)
        # You might want to sys.exit(1) for CI/cron jobs:
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    run_pipeline(SINCE_TS)


'''
Notes you can adapt

SPs should not COMMIT/ROLLBACK; use RAISE EXCEPTION to bubble up failures so Python can roll back the whole run.

The bulk load uses COPY into a TEMP table, then a single INSERT … ON CONFLICT DO UPDATE to upsert into the target.

If your target table needs specific types or defaults, tweak copy_dataframe_to_temp’s type mapping or cast in the MERGE SQL.

The script uses one transaction for the whole pipeline. If you prefer ** per-step commits**, wrap each step in its own with conn: block and reopen the connection between steps.

If you share your target table schema (PK/unique keys + columns) and SP names/signatures, I’ll tailor the key_cols, data_cols, and the two CALL statements so you can run this without edits.

'''