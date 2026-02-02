import os, sys
import json
from pathlib import Path
from urllib.parse import quote
from typing import Dict, Any, Optional, Iterable

import pandas as pd
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine, Connection


class DBUtils:
    """
    DB utilities that read connection settings from a JSON config file.

    db_config.json expected shape:
    {
      "environments": {
        "dev":    {"USER":"u", "PASS":"p", "HOST":"localhost", "PORT":5432, "NAME":"mydb"},
        "staging":{"USER":"u", "PASS":"p", "HOST":"stg-db",   "PORT":5432, "NAME":"mydb"},
        "prod":   {"USER":"u", "PASS":"p", "HOST":"prd-db",   "PORT":5432, "NAME":"mydb"}
      }
    }
    """

    def __init__(self, relative_config_dir: Optional[str] = None, db_config_file_name: str = "db_config.json"):
        # Resolve config file path
        base_dir = Path(os.getcwd())
        if relative_config_dir:
            # allow "a/b/c" or "./a/b" etc.
            cfg_dir = base_dir.joinpath(*[p for p in Path(relative_config_dir).parts if p not in ("", ".")])
        else:
            cfg_dir = base_dir
        self.config_file_path = cfg_dir / db_config_file_name

        # Load all environments
        self.db_environments: Dict[str, Dict[str, Any]] = self._load_environments()

        # Engine placeholder (set by build_engine)
        self.engine: Optional[Engine] = None

    # ---------- private helpers ----------
    def _load_environments(self) -> Dict[str, Dict[str, Any]]:
        if not self.config_file_path.exists():
            raise FileNotFoundError(f"DB config file not found: {self.config_file_path}")
        with open(self.config_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        envs = data.get("environments", {})
        if not isinstance(envs, dict) or not envs:
            raise ValueError(f"'environments' section missing/empty in {self.config_file_path}")
        return envs

    def _get_db_config(self, environment: str) -> Dict[str, Any]:
        if environment not in self.db_environments:
            raise KeyError(f"Unknown environment '{environment}'. Known: {list(self.db_environments.keys())}")
        return self.db_environments[environment]

    def _make_dsn(self, environment: str) -> str:
        cfg = self._get_db_config(environment)
        user = cfg["USER"]
        pwd = quote(str(cfg["PASS"]))  # URL-encode password
        host = cfg["HOST"]
        port = str(cfg["PORT"])
        name = cfg["NAME"]
        # Use SQLAlchemy dialect for psycopg2
        return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}"

    # ---------- public API ----------
    def build_engine(self, environment: str, application_name: str = "python_app",
                     statement_timeout_ms: int = 60000, lock_timeout_ms: int = 10000) -> Engine:
        """
        Create and store a SQLAlchemy Engine for the given environment.
        """
        dsn = self._make_dsn(environment)
        engine = create_engine(dsn, pool_pre_ping=True, future=True)

        @event.listens_for(engine, "connect")
        def _set_session(dbapi_conn, _):
            with dbapi_conn.cursor() as cur:
                cur.execute("SET application_name = %s", (application_name,))
                cur.execute("SET statement_timeout = %s", (statement_timeout_ms,))
                cur.execute("SET lock_timeout = %s", (lock_timeout_ms,))

        self.engine = engine
        return engine

    # Convenience: ensure we have an engine
    def _require_engine(self) -> Engine:
        if self.engine is None:
            raise RuntimeError("Engine is not initialized. Call build_engine(environment) first.")
        return self.engine

    # ----- pandas-friendly SELECT -----
    def select_df(self, conn: Connection, sql: str, params: Optional[dict] = None) -> pd.DataFrame:
        return pd.read_sql_query(text(sql), conn, params=params)

    # ----- CALL stored procedure -----
    def call_sp(self, conn: Connection, qualified_name: str, params: Optional[dict] = None, expect_row: bool = False):
        placeholders = ", ".join(f":{k}" for k in (params or {}).keys())
        stmt = text(f"CALL {qualified_name}({placeholders})") if placeholders else text(f"CALL {qualified_name}()")
        result = conn.execute(stmt, params or {})
        return result.fetchone() if expect_row else None

    # ----- COPY DataFrame -> TEMP table (fast bulk load) -----
    def copy_dataframe_to_temp(self, conn: Connection, df: pd.DataFrame, temp_table: str):
        if df.empty:
            raise ValueError("DataFrame is empty; nothing to COPY.")

        def pg_type(dtype):
            if pd.api.types.is_integer_dtype(dtype):           return "bigint"
            if pd.api.types.is_float_dtype(dtype):             return "double precision"
            if pd.api.types.is_bool_dtype(dtype):              return "boolean"
            if pd.api.types.is_datetime64_any_dtype(dtype):    return "timestamp"
            return "text"

        cols = list(df.columns)
        col_defs = ", ".join(f'"{c}" {pg_type(df[c].dtype)}' for c in cols)

        dbapi_conn = conn.connection  # underlying psycopg2 connection
        with dbapi_conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
            cur.execute(f"CREATE TEMP TABLE {temp_table} ({col_defs}) ON COMMIT DROP")

            import io
            buf = io.StringIO()
            df.to_csv(buf, index=False, header=False, na_rep="")
            buf.seek(0)
            cur.copy_expert(
                f'COPY {temp_table} ("' + '","'.join(cols) + '") FROM STDIN WITH (FORMAT CSV)',
                buf
            )

    # ----- UPSERT temp -> target -----
    def upsert_from_temp(self, conn: Connection, temp_table: str, target_table: str,
                         key_cols: list[str], data_cols: Optional[list[str]] = None):
        if data_cols is None:
            schema, _, table = target_table.partition(".")
            q = text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table
                  AND (:schema = '' OR table_schema = :schema)
                ORDER BY ordinal_position
            """)
            rows = conn.execute(q, {"table": table or target_table, "schema": schema}).fetchall()
            target_cols = [r[0] for r in rows]
            data_cols = [c for c in target_cols if c not in key_cols]

        all_cols = key_cols + data_cols
        quoted_cols = ", ".join(f'"{c}"' for c in all_cols)
        conflict_cols = ", ".join(f'"{c}"' for c in key_cols)
        updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in data_cols)

        stmt = text(f"""
            INSERT INTO {target_table} ({quoted_cols})
            SELECT {quoted_cols} FROM {temp_table}
            ON CONFLICT ({conflict_cols})
            DO UPDATE SET {updates};
        """)
        conn.execute(stmt)


    # ---- Optional: streaming SELECT in chunks (pandas) ----
    def select_chunks(
        self,
        conn: Connection,
        sql: str,
        params: Optional[dict] = None,
        chunksize: int = 10_000,
    ) -> Iterable[pd.DataFrame]:
        for chunk in pd.read_sql_query(text(sql), conn, params=params, chunksize=chunksize):
            yield chunk


# ---------- Optional pandas transform ----------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df[df["status"].isin(["ready", "finished"])].copy()
    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0.0)
        mean = df["value"].mean()
        std  = df["value"].std() or 1.0
        df["value_norm"] = (df["value"] - mean) / std
    return df

# ---------- Steps using DBUtils ----------
def step_1_read_initial(db: DBUtils, conn: Connection, since_ts: str) -> pd.DataFrame:
    sql = """
        SELECT job_id, status, updated_at, value
        FROM public.etl_inputs
        WHERE updated_at >= :since
    """
    df = db.select_df(conn, sql, params={"since": since_ts})
    print(f"[Step 1] fetched rows: {len(df)}")
    return df

def step_3_pre_ingest(db: DBUtils, conn: Connection, since_ts: str):
    print("[Step 3] CALL public.pre_ingest_check")
    db.call_sp(conn, "public.pre_ingest_check", {"since_ts": since_ts}, expect_row=False)

def step_4_read_reference(db: DBUtils, conn: Connection) -> pd.DataFrame:
    ref = db.select_df(conn, "SELECT code, multiplier FROM public.ref_multipliers")
    print(f"[Step 4] reference rows: {len(ref)}")
    return ref

def step_5_dump_processed(db: DBUtils, conn: Connection, df_processed: pd.DataFrame):
    if df_processed.empty:
        print("[Step 5] nothing to dump (empty DataFrame)")
        return
    temp   = "temp_processed_input"
    target = "public.processed_inputs"
    key_cols = ["job_id"]  # must be a UNIQUE/PK on target
    print(f"[Step 5] COPY {len(df_processed)} rows -> TEMP -> UPSERT {target}")
    db.copy_dataframe_to_temp(conn, df_processed, temp_table=temp)
    db.upsert_from_temp(conn, temp_table=temp, target_table=target, key_cols=key_cols)

def step_6_post_ingest(db: DBUtils, conn: Connection, since_ts: str):
    print("[Step 6] CALL public.post_ingest_finalize")
    db.call_sp(conn, "public.post_ingest_finalize", {"since_ts": since_ts}, expect_row=False)

# ---------- (Optional) verify session settings ----------
def verify_session(conn: Connection):
    for setting in ("application_name", "statement_timeout", "lock_timeout"):
        val = conn.execute(text(f"SHOW {setting}")).scalar_one()
        print(f"[Session] {setting} = {val}")

# ---------- Orchestrator ----------
def run_pipeline():
    # Build engine from config file (e.g., ./config/db_config.json)
    db = DBUtils(relative_config_dir='.configs')
    engine = db.build_engine(environment='drx_fpl_test',
                             application_name="etl_pipeline",
                             statement_timeout_ms=60_000,
                             lock_timeout_ms=10_000)

    try:
        # One transaction for the entire pipeline
        with engine.begin() as conn:
            # (optional) confirm session defaults were applied on connect
            verify_session(conn)

            # 1) read
            df_raw = step_1_read_initial(db, conn, SINCE_TS)

            # 2) transform
            print("[Step 2] pandas transforms")
            df_processed = transform(df_raw)

            # 3) SP A
            step_3_pre_ingest(db, conn, SINCE_TS)

            # 4) read again (e.g., lookup) and optionally join
            df_ref = step_4_read_reference(db, conn)
            if not df_processed.empty and not df_ref.empty and "code" in df_processed.columns:
                df_processed = df_processed.merge(df_ref, on="code", how="left")
                if "multiplier" in df_processed.columns and "value" in df_processed.columns:
                    df_processed["value_x_mult"] = df_processed["value"] * df_processed["multiplier"].fillna(1.0)

            # 5) upsert processed data
            step_5_dump_processed(db, conn, df_processed)

            # 6) SP B
            step_6_post_ingest(db, conn, SINCE_TS)

        print("✅ Pipeline committed successfully.")
    except Exception as e:
        print("❌ Pipeline failed; transaction rolled back.")
        print("Detail:", e)
        raise

if __name__ == "__main__":
    run_pipeline()