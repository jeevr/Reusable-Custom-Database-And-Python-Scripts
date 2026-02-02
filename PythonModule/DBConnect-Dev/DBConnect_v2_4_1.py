import os
import json
import pandas as pd
import geopandas as gpd
import fiona
from typing import List, Dict, Union, Optional
from urllib.parse import quote
from sqlalchemy import create_engine, exc, text, inspect
import psycopg2
from psycopg2 import sql


class DBConnect:
    """
    Main DBConnect container.

    Holds configuration path and provides factory methods to create helper objects:
    - Connector: Connect to PostgreSQL/PostGIS databases.
    - EnvironmentManager: Manage database environment configurations.
    - FileReader: Read supported file formats into Pandas/GeoPandas DataFrames.
    - DataDumper: Load data into PostgreSQL/PostGIS efficiently.
    - DatabaseExtractor: Extract/query data from PostgreSQL/PostGIS.
    - DatabaseStoredProcedureExecutor: Execute PostgreSQL stored procedures.
    """

    def __init__(self, config_file_path: str = None):
        """
        Initialize DBConnect and store configuration path.

        Args:
            config_file_path (str, optional):
                Path to db_config.json.
                Defaults to file in same directory as this script.
        """
        self._version = "v2.4.1"
        self.config_file_path = (
            os.path.join(os.path.dirname(__file__), "db_config.json")
            if config_file_path is None
            else config_file_path
        )

    @property
    def version(self) -> str:
        """Return module version string."""
        return self._version

    # ===== Factory Methods (This is to make it easy in calling the Sub-Class) =====

    def connector(self, environment: str):
        """Create and return a Connector instance for the given environment."""
        return self.Connector(self.config_file_path, environment)

    def environment_manager(self):
        """Create and return an EnvironmentManager instance."""
        return self.EnvironmentManager(self.config_file_path)

    def file_reader(self):
        """Create and return a FileReader instance."""
        return self.FileReader()

    def data_dumper(self, connection, engine):
        """Create and return a DataDumper instance."""
        return self.DataDumper(connection, engine)

    def database_extractor(self, connection, engine):
        """Create and return a DatabaseExtractor instance."""
        return self.DatabaseExtractor(connection, engine)

    def stored_procedure_executor(self, creds):
        """Create and return a DatabaseStoredProcedureExecutor instance."""
        return self.DatabaseStoredProcedureExecutor(creds)

    # ===== Sub-Classes =====

    class Connector:
        """
        Handles PostgreSQL/PostGIS database connections.

        Reads environment credentials from db_config.json and allows:
        - Connecting/disconnecting
        - Testing connectivity
        - Checking table existence
        """

        def __init__(self, config_file_path: str, environment: str):
            """
            Initialize Connector.

            Args:
                config_file_path (str): Path to db_config.json.
                environment (str): Environment key name in the config file.

            Raises:
                FileNotFoundError: If config file does not exist.
                KeyError: If environment is not found in the config.
            """
            if not os.path.exists(config_file_path):
                raise FileNotFoundError(f"Config file not found: {config_file_path}")

            with open(config_file_path, "r", encoding="utf-8") as f:
                self._environments = json.load(f)["environments"]

            if environment not in self._environments:
                raise KeyError(f"Environment '{environment}' not found.")

            self.environment = environment
            self.environment_creds = self._environments[environment]
            self.engine = None
            self.conn = None
            self._status = "Not Connected"

        def connect(self, echo: bool = False) -> bool:
            """
            Connect to the database.

            Args:
                echo (bool): If True, enable SQLAlchemy SQL echo.

            Returns:
                bool: True if connection successful, False otherwise.
            """
            try:
                creds = self.environment_creds
                self.engine = create_engine(
                    f"postgresql://{creds['USER']}:{quote(creds['PASS'])}"
                    f"@{creds['HOST']}:{creds['PORT']}/{creds['NAME']}",
                    echo=echo
                )
                self.conn = self.engine.connect()
                self._status = f"Connected to {self.environment} ({creds['NAME']})"
                print(f"[Connected] {self._status}")
                return True
            except exc.SQLAlchemyError as e:
                print(f"[Connection Error] {e}")
                return False

        def disconnect(self):
            """Close the current database connection."""
            if self.conn:
                self.conn.close()
                self.engine.dispose()
                self.conn = None
                self.engine = None
                self._status = "Not Connected"
                print("[Disconnected]")

        def test_connection(self) -> bool:
            """
            Test database connection without creating a persistent connection.

            Returns:
                bool: True if successful, False otherwise.
            """
            try:
                creds = self.environment_creds
                temp_engine = create_engine(
                    f"postgresql://{creds['USER']}:{quote(creds['PASS'])}"
                    f"@{creds['HOST']}:{creds['PORT']}/{creds['NAME']}"
                )
                with temp_engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                print("[Test Connection] Successful")
                return True
            except exc.SQLAlchemyError as e:
                print(f"[Test Connection Error] {e}")
                return False

        def table_exists(self, table_name: str, schema: str = "public") -> bool:
            """
            Check if a table exists in the database.

            Args:
                table_name (str): Name of the table.
                schema (str): Schema name.

            Returns:
                bool: True if table exists, False otherwise.
            """
            try:
                inspector = inspect(self.engine)
                exists = inspector.has_table(table_name, schema=schema)
                print(f"[Table Exists] {schema}.{table_name}: {exists}")
                return exists
            except exc.SQLAlchemyError as e:
                print(f"[Table Exists Error] {e}")
                return False

    class EnvironmentManager:
        """
        Manage database environments in db_config.json.

        Supports:
        - Adding environments
        - Deleting environments
        - Updating environment values
        - Listing environments
        """

        def __init__(self, config_file_path: str):
            self.config_file_path = config_file_path
            self._load_config()

        def _load_config(self):
            """Load or initialize the db_config.json."""
            if not os.path.exists(self.config_file_path):
                self.config_data = {"environments": {}}
                self._save_config()
            else:
                with open(self.config_file_path, "r", encoding="utf-8") as f:
                    self.config_data = json.load(f)
                if "environments" not in self.config_data:
                    self.config_data["environments"] = {}

        def _save_config(self):
            """Save the current configuration to db_config.json."""
            with open(self.config_file_path, "w", encoding="utf-8") as f:
                json.dump(self.config_data, f, indent=4)

        def list_environments(self) -> list:
            """Return list of available environments."""
            return list(self.config_data["environments"].keys())

        def add_environment(self, env_name: str, env_data: dict):
            """
            Add a new environment.

            Args:
                env_name (str): Name of the new environment.
                env_data (dict): Environment credentials.

            Raises:
                ValueError: If required keys are missing.
            """
            required = {"NAME", "HOST", "PORT", "USER", "PASS"}
            if not required.issubset(env_data.keys()):
                raise ValueError(f"Missing keys. Required: {required}")
            self.config_data["environments"][env_name] = env_data
            self._save_config()
            print(f"[EnvManager] Added: {env_name}")

        def delete_environment(self, env_name: str):
            """Delete an environment if it exists."""
            if env_name in self.config_data["environments"]:
                del self.config_data["environments"][env_name]
                self._save_config()
                print(f"[EnvManager] Deleted: {env_name}")

        def update_environment(self, env_name: str, key: str, value: str):
            """
            Update a specific key for an environment.

            Raises:
                KeyError: If environment or key is not found.
            """
            if env_name not in self.config_data["environments"]:
                raise KeyError(f"Environment '{env_name}' not found.")
            if key not in self.config_data["environments"][env_name]:
                raise KeyError(f"Key '{key}' not found in environment '{env_name}'.")
            self.config_data["environments"][env_name][key] = value
            self._save_config()
            print(f"[EnvManager] Updated '{key}' in '{env_name}' to '{value}'")

    class FileReader:
        """
        Read supported file formats into Pandas or GeoPandas DataFrames and
        provide utilities for File Geodatabases (.gdb, .gdb.zip).

        Supported formats:
        - .csv
        - .shp, .geojson
        - .xlsx, .xlsm, .xls, .xlsb (requires sheet name)
        - .gdb (directory) and .gdb.zip (zip containing a .gdb folder)
        """

        # ---------- Public API ----------

        def read_file(
            self,
            file_path: str,
            file_sheetname: Optional[str] = None,
            file_layer: Optional[str] = None,
        ) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
            """
            Read various geospatial and tabular file types into a DataFrame.

            Args:
                file_path: Full path to the file.
                file_sheetname: Sheet name for Excel files.
                file_layer: Layer name for GDB (optional; defaults to first layer).

            Returns:
                pd.DataFrame or gpd.GeoDataFrame

            Raises:
                ValueError: Unsupported type or missing required info.
            """
            file_path = os.path.abspath(file_path)
            file = os.path.basename(file_path)
            ext = os.path.splitext(file)[1].lower()

            # Geodatabase handling (directory .gdb or zipped .gdb.zip)
            if self._is_gdb_path(file_path):
                return self._read_gdb(file_path, file_layer)

            match ext:
                case ".shp" | ".geojson":
                    return gpd.read_file(file_path)
                case ".csv":
                    return pd.read_csv(file_path)
                case ".xlsx" | ".xlsm" | ".xls" | ".xlsb":
                    if not file_sheetname:
                        raise ValueError(f"Sheet name must be provided for Excel file: {file}")
                    return pd.read_excel(file_path, sheet_name=file_sheetname)
                case ".zip":
                    # Only supported zip flavor is .gdb.zip (caught above)
                    raise ValueError(f"Unsupported .zip content: {file}")
                case _:
                    raise ValueError(f"Unsupported file type: {ext}")

        def get_layers(self, gdb_path: str) -> List[str]:
            """
            List layers in a File Geodatabase (.gdb or .gdb.zip).

            Args:
                gdb_path: Path to .gdb directory or .gdb.zip file.

            Returns:
                List of layer names.

            Raises:
                ValueError: If not a valid GDB or no layers found.
            """
            vfs = self._resolve_vfs_path(gdb_path)
            layers = list(fiona.listlayers(vfs))
            if not layers:
                raise ValueError("No layers found in the geodatabase.")
            return layers

        def layer_statistics(self, gdb_path: str, layer_name: str) -> Dict[str, object]:
            """
            Compute simple stats for a geodatabase layer.

            Args:
                gdb_path: Path to .gdb directory or .gdb.zip.
                layer_name: Layer to inspect.

            Returns:
                Dict with basic stats.
            """
            gdf = self._read_gdb_layer(gdb_path, layer_name)
            return {
                "gdb_path": os.path.abspath(gdb_path),
                "layer": layer_name,
                "rows": int(len(gdf)),
                "columns": list(gdf.columns),
                "geometry_type": sorted({str(t) for t in gdf.geom_type.unique()}),
                "crs": str(gdf.crs),
                "bounds": tuple(map(float, gdf.total_bounds)) if not gdf.empty else None,  # (minx, miny, maxx, maxy)
                "null_counts": gdf.isna().sum().to_dict(),
            }
        
        def read_gdb_layer(self, gdb_path: str, layer_name: str) -> gpd.GeoDataFrame:
            """
            Public method to read a specific GDB layer into a GeoDataFrame.

            Args:
                gdb_path: Path to .gdb directory or .gdb.zip.
                layer_name: Exact layer name (see `get_layers`).

            Returns:
                GeoDataFrame for the requested layer.

            Raises:
                ValueError: If path is not a GDB or layer does not exist.
            """
            return self._read_gdb_layer(gdb_path, layer_name)

        # ---------- Internal helpers ----------

        @staticmethod
        def _is_gdb_path(path: str) -> bool:
            p = path.lower()
            return p.endswith(".gdb") or p.endswith(".gdb.zip")

        @staticmethod
        def _resolve_vfs_path(path: str) -> str:
            """
            Return path usable by fiona/geopandas:
            - '/abs/path/to/file.gdb' for directory geodatabases
            - '/vsizip/abs/path/to/file.gdb.zip/file.gdb' for zipped geodatabases
            """
            abs_path = os.path.abspath(path)
            lower = abs_path.lower()
            if lower.endswith(".gdb.zip"):
                gdb_name = os.path.basename(abs_path).replace(".zip", "")
                return f"/vsizip/{abs_path}/{gdb_name}"
            if lower.endswith(".gdb"):
                return abs_path
            raise ValueError("Not a geodatabase path (.gdb or .gdb.zip).")

        def _read_gdb(self, gdb_path: str, layer: Optional[str]) -> gpd.GeoDataFrame:
            vfs = self._resolve_vfs_path(gdb_path)
            layers = list(fiona.listlayers(vfs))
            if not layers:
                raise ValueError(f"No layers found in geodatabase: {gdb_path}")

            layer_to_read = layer or layers[0]
            if layer_to_read not in layers:
                raise ValueError(f"Layer '{layer_to_read}' not found. Available: {layers}")

            return gpd.read_file(vfs, layer=layer_to_read)

        def _read_gdb_layer(self, gdb_path: str, layer: str) -> gpd.GeoDataFrame:
            vfs = self._resolve_vfs_path(gdb_path)
            # Light validation before read
            if layer not in fiona.listlayers(vfs):
                raise ValueError(f"Layer '{layer}' not found in: {gdb_path}")
            return gpd.read_file(vfs, layer=layer)

    class DataDumper:
        """
        Import large datasets efficiently into PostgreSQL/PostGIS.
        """

        def __init__(self, connection, engine):
            if not (connection and engine):
                raise ValueError("Valid connection and engine required.")
            self.sql_conn = connection
            self.sql_engine = engine

        def data_import(self, df, table_name: str, schema: str, if_exists="replace", chunksize=10000, method="multi"):
            """
            Import Pandas DataFrame into PostgreSQL efficiently.

            Args:
                df (DataFrame): Data to import.
                table_name (str): Destination table.
                schema (str): Schema name.
                if_exists (str): replace, append, or fail.
                chunksize (int): Number of rows per batch.
                method (str): Pandas insert method (multi recommended).
            """
            try:
                df_copy = df.copy()
                df_copy.to_sql(
                    table_name, self.sql_engine, if_exists=if_exists, index=False,
                    schema=schema, chunksize=chunksize, method=method
                )
                print(f"[DataDumper] Imported {len(df_copy)} rows into {schema}.{table_name}")
            except Exception as e:
                print(f"[DataDumper Error] {e}")

        def geo_data_import(self, df, table_name: str, schema: str, if_exists="replace", chunksize=10000):
            """
            Import GeoDataFrame into PostGIS efficiently.

            Args:
                df (GeoDataFrame/DataFrame): Data to import.
                table_name (str): Destination table.
                schema (str): Schema name.
                if_exists (str): replace, append, or fail.
                chunksize (int): Number of rows per batch.
            """
            try:
                gdf = gpd.GeoDataFrame(df.copy(), geometry="geometry")
                gdf.to_postgis(
                    table_name, self.sql_engine, if_exists=if_exists, index=False,
                    schema=schema, chunksize=chunksize
                )
                print(f"[DataDumper] Imported {len(gdf)} geo rows into {schema}.{table_name}")
            except Exception as e:
                print(f"[DataDumper Error] {e}")

    class DatabaseExtractor:
        """
        Extract/query data from PostgreSQL/PostGIS databases.
        """

        def __init__(self, connection, engine):
            if not (connection and engine):
                raise ValueError("Valid connection and engine required.")
            self.sql_conn = connection
            self.sql_engine = engine

        def get_data(self, table_name: str, schema: str, columns="*", row_limit=0):
            """
            Get data from a table.

            Args:
                table_name (str): Table name.
                schema (str): Schema name.
                columns (str/list): Columns to select.
                row_limit (int): Limit number of rows.
            """
            cols = ",".join(columns) if isinstance(columns, (list, tuple)) else columns
            limit = f"LIMIT {row_limit}" if row_limit > 0 else ""
            query = f"SELECT {cols} FROM {schema}.{table_name} {limit};"
            result = self.sql_conn.execute(text(query))
            return pd.DataFrame(result, columns=result.keys())
        
        def get_data_with_custom_query(self, sql_query: str, safe_mode: bool = True):
            """
            Execute a custom SQL query and return the results as a Pandas DataFrame.

            This method can optionally validate that the query is a safe, read-only
            `SELECT` or CTE-based query to avoid destructive operations.

            Args:
                sql_query (str):
                    The SQL query string to execute.
                    Can include SELECT statements, JOINs, WHERE clauses, and CTEs.
                    Example:
                        "SELECT id, name FROM public.people WHERE active = TRUE;"
                        "WITH recent AS (SELECT * FROM logs ORDER BY ts DESC LIMIT 10)
                        SELECT * FROM recent;"
                safe_mode (bool, optional):
                    If True (default), only allows safe, read-only queries
                    (SELECT or WITH ... SELECT) and blocks destructive SQL.

            Returns:
                pd.DataFrame:
                    A DataFrame containing the query results.

            Raises:
                ValueError:
                    If safe_mode is enabled and the query is not allowed.
                sqlalchemy.exc.SQLAlchemyError:
                    If there is an error executing the SQL query.

            Notes:
                - Safe mode blocks destructive queries (`DELETE`, `DROP`, `UPDATE`, etc.).
                - Disable safe_mode if you need to run non-SELECT queries intentionally.
                - Be cautious when disabling safe_mode; it may modify database data.
            """
            if safe_mode:
                cleaned_query = sql_query.strip().lower()

                # Allow queries starting with SELECT or WITH (CTEs)
                if not (cleaned_query.startswith("select") or cleaned_query.startswith("with")):
                    raise ValueError("Only SELECT or WITH (CTE) queries are allowed in safe mode.")

                # Block potentially destructive keywords
                forbidden_keywords = ["insert", "update", "delete", "drop", "alter", "truncate"]
                for kw in forbidden_keywords:
                    if f" {kw} " in cleaned_query or cleaned_query.startswith(kw):
                        raise ValueError(f"Query contains forbidden keyword: {kw.upper()}")

            # Execute the query
            result = self.sql_conn.execute(text(sql_query))
            return pd.DataFrame(result, columns=result.keys())
        


    class DatabaseStoredProcedureExecutor:
        """
        Executes PostgreSQL stored procedures.
        - execute_sp() handles single SP (with or without parameters)
        - execute_multiple_sps() handles multiple SPs (with or without parameters in the same batch)
        """

        def __init__(self, environment_creds: dict):
            self.creds = environment_creds

        def _get_connection(self):
            """Create a new database connection."""
            return psycopg2.connect(
                dbname=self.creds["NAME"],
                user=self.creds["USER"],
                password=self.creds["PASS"],
                host=self.creds["HOST"],
                port=self.creds["PORT"]
            )

        def execute_sp(self, sp_template: str, params: list | None = None):
            """
            Execute a stored procedure.

            Args:
                sp_template (str): Procedure call template.
                                Example no params: "schema.my_proc()"
                                Example with params: "schema.my_proc(?,?,?)"
                params (list | None): Values for placeholders. None for no params.

            Returns:
                tuple | None: OUT parameters returned by the stored procedure.
            """
            conn = self._get_connection()
            cursor = conn.cursor()
            try:
                if params and "?" in sp_template:
                    # Replace ? placeholders with %s for psycopg2
                    pg_template = sp_template.replace("?", "%s")
                    query = f"CALL {pg_template};"
                    cursor.execute(query, params)
                else:
                    # No parameters — execute directly
                    query = f"CALL {sp_template};"
                    cursor.execute(query)

                result = cursor.fetchone()
                conn.commit()

                print(f"[SPExecutor] Executed: CALL {sp_template} with {params if params else 'no params'}")
                if result:
                    print(f"[SPExecutor] OUT parameters: {result}")
                else:
                    print("[SPExecutor] No OUT parameters returned.")
                return result

            except psycopg2.Error as e:
                conn.rollback()
                print(f"[SPExecutor Error] {e}")
                return None
            finally:
                cursor.close()
                conn.close()

        def execute_multiple_sps(self, sp_calls: list, stop_on_error: bool = True):
            """
            Execute multiple stored procedures in one transaction.

            Args:
                sp_calls (list): List of tuples:
                                - ("proc_name()", None) → no params
                                - ("proc_name(?,?,?)", [param1, param2, param3]) → with params
                stop_on_error (bool): If True, rollback and stop on first error.
                                    If False, rollback only failed SP and continue.

            Returns:
                dict: Mapping of (sp_template, params) → OUT parameters or None.
            """
            conn = self._get_connection()
            cursor = conn.cursor()
            results = {}
            try:
                for sp_template, params in sp_calls:
                    try:
                        if params and "?" in sp_template:
                            # Replace ? placeholders with %s for psycopg2
                            pg_template = sp_template.replace("?", "%s")
                            query = f"CALL {pg_template};"
                            cursor.execute(query, params)
                        else:
                            # No parameters — execute directly
                            query = f"CALL {sp_template};"
                            cursor.execute(query)

                        result = cursor.fetchone()
                        results[(sp_template, tuple(params) if params else None)] = result

                        print(f"[SPExecutor] Executed: CALL {sp_template} with {params if params else 'no params'}")
                        if result:
                            print(f"[SPExecutor] OUT parameters: {result}")
                        else:
                            print("[SPExecutor] No OUT parameters returned.")

                    except psycopg2.Error as e:
                        print(f"[SPExecutor Error] Failed on {sp_template}: {e}")
                        results[(sp_template, tuple(params) if params else None)] = None

                        if stop_on_error:
                            conn.rollback()
                            print("[SPExecutor] Transaction rolled back due to error.")
                            return results
                        else:
                            conn.rollback()

                conn.commit()
                print("[SPExecutor] Transaction committed successfully.")
                return results

            except Exception as e:
                conn.rollback()
                print(f"[SPExecutor Fatal Error] {e}")
                return results
            finally:
                cursor.close()
                conn.close()

                
                
        
