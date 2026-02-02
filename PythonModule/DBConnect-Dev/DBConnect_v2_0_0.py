import os
import json
import pandas as pd
import geopandas as gpd
from urllib.parse import quote
from sqlalchemy import create_engine, exc, text, inspect
from sqlalchemy.orm import sessionmaker
import psycopg2
from psycopg2 import sql


class DBConnect:
    """
    Main DBConnect container.
    Holds config file path so all sub-tools can reuse it.
    """

    def __init__(self, config_file_path: str = None):
        """
        Initialize DBConnect.

        Args:
            config_file_path (str, optional): Path to db_config.json file.
                                              Defaults to same folder as this file.
        """
        self._version = "v2.0.0"
        self.config_file_path = (
            os.path.join(os.path.dirname(__file__), "db_config.json")
            if config_file_path is None
            else config_file_path
        )

    @property
    def version(self):
        """Return module version."""
        return self._version

    # ============================================
    # Connector
    # ============================================
    class Connector:
        """
        Handles PostgreSQL database connections.
        """

        def __init__(self, parent, environment: str):
            """
            Args:
                parent (DBConnect): The main DBConnect instance.
                environment (str): Environment name in db_config.json.
            """
            self.config_file_path = parent.config_file_path
            try:
                if not os.path.exists(self.config_file_path):
                    raise FileNotFoundError(f"Config file not found: {self.config_file_path}")

                with open(self.config_file_path, "r", encoding="utf-8") as f:
                    self._environments = json.load(f)["environments"]

                if environment not in self._environments:
                    raise KeyError(f"Environment '{environment}' not found in configuration.")

                self.environment = environment
                self.engine = None
                self.conn = None
                self._status = "Not Connected"
                self.environment_creds = self._environments[environment]

            except (OSError, KeyError, json.JSONDecodeError) as e:
                raise RuntimeError(f"Error loading DB configuration: {e}")

        def get_available_environments(self):
            return [
                {"env_name": env, **cfg}
                for env, cfg in self._environments.items()
            ]

        def get_status(self):
            return self._status

        def connect(self, echo: bool = False) -> bool:
            try:
                creds = self.environment_creds
                self.engine = create_engine(
                    f"postgresql://{creds['USER']}:{quote(creds['PASS'])}"
                    f"@{creds['HOST']}:{creds['PORT']}/{creds['NAME']}",
                    echo=echo,
                )
                self.conn = self.engine.connect()
                self._status = f"Connected to {self.environment} ({creds['NAME']})"
                print(f"[Connected] {self._status}")
                return True
            except exc.SQLAlchemyError as e:
                print(f"[Error] Failed to connect: {e}")
                return False

        def disconnect(self):
            if self.conn:
                self.conn.close()
                self.engine.dispose()
                self.conn = None
                self.engine = None
                self._status = "Not Connected"
                print("[Disconnected]")
            else:
                print("[No Active Connection]")

        def test_connection(self) -> bool:
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
            try:
                inspector = inspect(self.engine) if self.engine else inspect(
                    create_engine(
                        f"postgresql://{self.environment_creds['USER']}:{quote(self.environment_creds['PASS'])}"
                        f"@{self.environment_creds['HOST']}:{self.environment_creds['PORT']}/{self.environment_creds['NAME']}"
                    )
                )
                exists = inspector.has_table(table_name, schema=schema)
                print(f"[Table Exists] {schema}.{table_name}: {exists}")
                return exists
            except exc.SQLAlchemyError as e:
                print(f"[Table Exists Error] {e}")
                return False

    # ============================================
    # Environment Manager
    # ============================================
    class EnvironmentManager:
        """CRUD operations for db_config.json environments."""

        def __init__(self, parent):
            self.config_file_path = parent.config_file_path
            self._load_config()

        def _load_config(self):
            if not os.path.exists(self.config_file_path):
                self.config_data = {"environments": {}}
                self._save_config()
            else:
                with open(self.config_file_path, "r", encoding="utf-8") as f:
                    self.config_data = json.load(f)
                if "environments" not in self.config_data:
                    self.config_data["environments"] = {}

        def _save_config(self):
            with open(self.config_file_path, "w", encoding="utf-8") as f:
                json.dump(self.config_data, f, indent=4)

        def list_environments(self):
            return list(self.config_data["environments"].keys())

        def add_environment(self, env_name: str, env_data: dict):
            required = {"NAME", "HOST", "PORT", "USER", "PASS"}
            if not required.issubset(env_data.keys()):
                raise ValueError(f"Missing keys. Required: {required}")
            self.config_data["environments"][env_name] = env_data
            self._save_config()
            print(f"[EnvManager] Added: {env_name}")

        def delete_environment(self, env_name: str):
            if env_name in self.config_data["environments"]:
                del self.config_data["environments"][env_name]
                self._save_config()
                print(f"[EnvManager] Deleted: {env_name}")
            else:
                print(f"[EnvManager] '{env_name}' does not exist.")

        def update_environment(self, env_name: str, key: str, value: str):
            if env_name not in self.config_data["environments"]:
                raise KeyError(f"Environment '{env_name}' not found.")
            if key not in self.config_data["environments"][env_name]:
                raise KeyError(f"Key '{key}' not found in environment '{env_name}'.")
            self.config_data["environments"][env_name][key] = value
            self._save_config()
            print(f"[EnvManager] Updated '{key}' in '{env_name}' to '{value}'")

    # ============================================
    # File Reader
    # ============================================
    class FileReader:
        """
        Reads supported file formats into Pandas/GeoPandas DataFrames.

        Supported formats:
            - .csv
            - .shp (shapefiles)
            - .xlsx, .xlsm, .xls, .xlsb (Excel files — requires sheet name)
        """

        def read_file(self, parent_folder: str, file_name: str, file_sheetname: str = None):
            """
            Read a file and return its contents as a Pandas or GeoPandas DataFrame.

            Args:
                parent_folder (str): Directory containing the file.
                file_name (str): Name of the file to read.
                file_sheetname (str, optional): Excel sheet name to load. Defaults to None.

            Returns:
                DataFrame or GeoDataFrame

            Raises:
                ValueError: If the file type is unsupported.
                ValueError: If reading an Excel file without specifying a sheet name.
            """
            path = os.path.join(parent_folder, file_name)
            ext = os.path.splitext(file_name)[1].lower()

            match ext:
                case ".shp":
                    return gpd.read_file(path)

                case ".csv":
                    return pd.read_csv(path)

                case ".xlsx" | ".xlsm" | ".xls" | ".xlsb":
                    if not file_sheetname:
                        raise ValueError(
                            f"Sheet name must be provided when reading Excel file: {file_name}"
                        )
                    return pd.read_excel(path, sheet_name=file_sheetname)

                case _:
                    raise ValueError(f"Unsupported file type: {ext}")

    # ============================================
    # Data Dumper
    # ============================================
    class DataDumper:
        """
        Optimized class for importing large datasets into PostgreSQL/PostGIS.
        """

        def __init__(self, connection, engine):
            if not (connection and engine):
                raise ValueError("Valid connection and engine required.")
            self.sql_conn = connection
            self.sql_engine = engine

        def data_import(
            self, 
            df, 
            table_name: str, 
            schema: str, 
            if_exists="replace", 
            chunksize: int = 10000, 
            method: str = "multi"
        ):
            """
            Import a DataFrame into a PostgreSQL table with optimizations for large data.

            Args:
                df (DataFrame): Pandas DataFrame to import.
                table_name (str): Destination table name.
                schema (str): Destination schema.
                if_exists (str): 'replace', 'append', or 'fail'.
                chunksize (int): Number of rows per insert batch.
                method (str): Pandas to_sql insert method ('multi' recommended for speed).
            """
            try:
                df_copy = df.copy()  # Avoid modifying the original DataFrame
                df_copy.to_sql(
                    table_name,
                    self.sql_engine,
                    if_exists=if_exists,
                    index=False,
                    schema=schema,
                    chunksize=chunksize,
                    method=method
                )
                print(f"[DataDumper] Successfully imported {len(df_copy)} rows into {schema}.{table_name}")
            except Exception as e:
                print(f"[DataDumper Error] Failed to import data into {schema}.{table_name}: {e}")

        def geo_data_import(
            self, 
            df, 
            table_name: str, 
            schema: str, 
            if_exists="replace", 
            chunksize: int = 10000
        ):
            """
            Import a GeoDataFrame into a PostGIS table with optimizations for large data.

            Args:
                df (GeoDataFrame/DataFrame): GeoPandas DataFrame to import.
                table_name (str): Destination table name.
                schema (str): Destination schema.
                if_exists (str): 'replace', 'append', or 'fail'.
                chunksize (int): Number of rows per insert batch.
            """
            try:
                gdf = gpd.GeoDataFrame(df.copy(), geometry="geometry")
                gdf.to_postgis(
                    table_name,
                    self.sql_engine,
                    if_exists=if_exists,
                    index=False,
                    schema=schema,
                    chunksize=chunksize
                )
                print(f"[DataDumper] Successfully imported {len(gdf)} geo records into {schema}.{table_name}")
            except Exception as e:
                print(f"[DataDumper Error] Failed to import geo data into {schema}.{table_name}: {e}")


    # ============================================
    # Database Extractor
    # ============================================
    class DatabaseExtractor:
        def __init__(self, connection, engine):
            if not (connection and engine):
                raise ValueError("Valid connection and engine required.")
            self.sql_conn = connection
            self.sql_engine = engine

        def get_data(self, table_name: str, schema: str, columns="*", row_limit=0):
            cols = ",".join(columns) if isinstance(columns, (list, tuple)) else columns
            limit = f"LIMIT {row_limit}" if row_limit > 0 else ""
            query = f"SELECT {cols} FROM {schema}.{table_name} {limit};"
            result = self.sql_conn.execute(text(query))
            return pd.DataFrame(result, columns=result.keys())

        def get_data_with_custom_query(self, sql_query: str):
            result = self.sql_conn.execute(text(sql_query))
            return pd.DataFrame(result, columns=result.keys())

    # ============================================
    # Stored Procedure Executor
    # ============================================
    class DatabaseStoredProcedureExecutor:
        def __init__(self, environment_creds: dict):
            self.creds = environment_creds

        def execute_sp(self, sp_name: str):
            conn = psycopg2.connect(
                dbname=self.creds["NAME"], user=self.creds["USER"],
                password=self.creds["PASS"], host=self.creds["HOST"], port=self.creds["PORT"]
            )
            cursor = conn.cursor()
            try:
                cursor.execute(sql.SQL(sp_name))
                conn.commit()
                print(f"[SPExecutor] Executed: {sp_name}")
            except psycopg2.Error as e:
                conn.rollback()
                print(f"[SPExecutor Error] {e}")
            finally:
                cursor.close()
                conn.close()
