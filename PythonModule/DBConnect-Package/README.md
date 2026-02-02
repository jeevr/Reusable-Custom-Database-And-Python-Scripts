# DBConnect Module

Version: **v2.5.2**  

---

## Overview
`DBConnect` is a lightweight ETL helper module for Python that simplifies:
- Connecting to PostgreSQL databases
- Reading various file formats into Pandas/GeoPandas DataFrames
- Dumping data to PostgreSQL/PostGIS
- Extracting data from PostgreSQL
- Executing stored procedures
- Managing database environment configurations (`db_config.json`)

---

## Features
1. **Connector** — Manage DB connections, test connectivity, check if tables exist.  
2. **EnvironmentManager** — Add, update, delete, and list database environment configurations.  
3. **FileReader** — Read `.csv`, `.shp`, `.xlsx` and more into DataFrames.  
4. **DataDumper** — Optimized loading of Pandas or GeoPandas data into PostgreSQL/PostGIS.  
5. **DatabaseExtractor** — Query data from PostgreSQL into DataFrames.  
6. **StoredProcedureExecutor** — Run PostgreSQL stored procedures.  

---

`
## Configuration
File: ``db_config.json``
```json
{
    "environments": {
        "dev": {
            "NAME": "my_db",
            "HOST": "localhost",
            "PORT": "5432",
            "USER": "username",
            "PASS": "password"
        }
    }
}
```


## Example Usage (v2.5.2 — New Recommended Usage)
In v2.5.0, helper classes are created through factory methods on the main DBConnect object.
You no longer need to pass the DBConnect instance into its own sub-classes.
### 1. Connect to a Database
```python
from dbconnect import DBConnect

db = DBConnect()

# Create connector for 'dev' environment
conn = db.connector("dev")

if conn.test_connection():
    conn.connect()

conn.disconnect()
```

### 2. Check if a Table Exists
```python
if conn.table_exists("my_table", schema="public"):
    print("Table exists!")
else:
    print("Table not found.")
```

### 3. Manage Environments (EnvironmentManager)
```python
env_mgr = db.environment_manager()

# List environments
print(env_mgr.list_environments())

# Add environment
env_mgr.add_environment("staging", {
    "NAME": "staging_db",
    "HOST": "localhost",
    "PORT": "5432",
    "USER": "stag_user",
    "PASS": "stag_pass"
})

# Update environment
env_mgr.update_environment("staging", "HOST", "192.168.1.10")

# Delete environment
env_mgr.delete_environment("staging")
```

### 4. Read Files into DataFrames
```python
reader = db.file_reader()
df_csv = reader.read_file("/data", "mydata.csv")
df_excel = reader.read_file("/data", "mydata.xlsx", file_sheetname="Sheet1")
gdf_shp = reader.read_file("/data", "myshapefile.shp")
```

### 5. Dump Data to PostgreSQL
```python
dumper = db.data_dumper(conn.conn, conn.engine)
dumper.data_import(df_csv, "my_table", schema="public")
```

### 6. Extract Data from PostgreSQL
#### Using method ``.get_data``
```python
extractor = db.database_extractor(conn.conn, conn.engine)
df_db = extractor.get_data("my_table", "public", columns=["id", "name"], row_limit=10)
```

#### Using method ``.get_data_with_custom_query``
Executes a **custom SQL query** and returns the results as a Pandas DataFrame.

By default (`safe_mode=True`), only **read-only queries** are allowed:
- Plain `SELECT ...` queries
- `WITH ... SELECT` queries (CTEs — Common Table Expressions)

Any query containing **destructive keywords** such as `DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, or `TRUNCATE` will be **blocked** in safe mode.

**Simple SELECT**
```python 
df = extractor.get_data_with_custom_query(
    "SELECT id, name FROM public.people LIMIT 5;"
)
```
**CTE Example**
```python
df = extractor.get_data_with_custom_query("""
WITH recent AS (
    SELECT * FROM logs ORDER BY ts DESC LIMIT 10
)
SELECT * FROM recent;
""")
```
**Disable Safe Mode (Dangerous)**
This is highly discouraged. Use unless needed.
```python 
df = extractor.get_data_with_custom_query(
    "DROP TABLE public.people;",
    safe_mode=False
)
```

### 7. Execute Stored Procedures

The `DatabaseStoredProcedureExecutor` now supports a **simplified, unified API** for running stored procedures:

- **`execute_sp()`** → Executes a **single stored procedure** (with or without parameters).
- **`execute_multiple_sps()`** → Executes **multiple stored procedures** in order, in a single transaction.
  - Supports **mixed** calls (some with parameters, some without).
  - Uses `?` placeholders for parameters for clarity and safety.

---

#### **7.1 Single Stored Procedure**

**No Parameters**
```python
sp_exec = db.stored_procedure_executor(conn.environment_creds)
```

**Run a procedure with no parameters**
```python
sp_exec.execute_sp("processing.refresh_summary()")
```

**With Parameters**
Call with parameters using ``?`` placeholders
```python
sp_exec.execute_sp("processing.insert_dummy_person(?,?,?)", ["Alice", "Admin", 1])
```
Internally, ``?`` is safely replaced with ``%s`` for psycopg2 binding, preventing SQL injection.



#### **7.2 Multiple Stored Procedures in Order**

**All Without Parameters**
```python
sp_exec.execute_multiple_sps([
    ("processing.refresh_summary()", None),
    ("processing.update_statistics()", None)
])
```

**All With Parameters**
```python
sp_exec.execute_multiple_sps([
    ("processing.insert_dummy_person(?,?,?)", ["Bob", "User", 2]),
    ("processing.insert_dummy_person(?)", ["Charlie"])
])
```

**Mixed Without and With Parameterss**
```python
sp_exec.execute_multiple_sps([
    ("processing.refresh_summary()", None),                         # no params
    ("processing.insert_dummy_person(?,?)", ["David", "Guest"]),    # with params
    ("processing.update_statistics()", None)                        # no params
])
```

#### **Notes**

- **Parameter Placeholders**:  
  Always use `?` in `sp_template` for parameters.  
  Example: `"schema.proc_name(?,?)"`, then pass the values as a list or tuple.

- **Transaction Handling**:  
  - By default (`stop_on_error=True`), if any stored procedure fails, **all previous SPs are rolled back** and execution stops.  
  - Set `stop_on_error=False` to rollback only the failing SP and continue executing the rest.

- **OUT Parameters**:  
  If the procedure returns OUT parameters, they will be printed and returned by the function.  
  If not, you will see `"No OUT parameters returned."`.


---

## Example Usage (v2.0.0 — Older Version for Reference)
Before v2.1.0, sub-classes required passing the ``DBConnect`` instance into their constructors.
### 1. Connect to a Database
```python
from dbconnect import DBConnect

# Create a connector
conn = DBConnect.Connector(environment="dev")

# Test connection before connecting
if conn.test_connection():
    conn.connect()

# Disconnect later
conn.disconnect()
```

### 2. Check if a Table Exists
```python
if conn.table_exists("my_table", schema="public"):
    print("Table exists!")
else:
    print("Table not found.")
```

### 3. Manage Environments (EnvironmentManager)
```python
db = DBConnect()
env_mgr = db.EnvironmentManager(db) #Pass the DBConnect as Parent Class

# List environments
print(env_mgr.list_environments())

# Add environment
env_mgr.add_environment("staging", {
    "NAME": "staging_db",
    "HOST": "localhost",
    "PORT": "5432",
    "USER": "stag_user",
    "PASS": "stag_pass"
})

# Update environment value
env_mgr.update_environment("staging", "HOST", "192.168.1.10")

# Delete environment
env_mgr.delete_environment("staging")
```

### 4. Read Files into DataFrames
```python
reader = DBConnect.FileReader()
df_csv = reader.read_file("/data", "mydata.csv")
df_excel = reader.read_file("/data", "mydata.xlsx", file_sheetname="Sheet1")
gdf_shp = reader.read_file("/data", "myshapefile.shp")
```

### 5. Dump Data to PostgreSQL
```python
dumper = DBConnect.DataDumper(conn.conn, conn.engine)
dumper.data_import(df_csv, "my_table", schema="public")
```

### 6. Extract Data from PostgreSQL
```python
extractor = DBConnect.DatabaseExtractor(conn.conn, conn.engine)
df_db = extractor.get_data("my_table", "public", columns=["id", "name"], row_limit=10)
```

### 7. Execute Stored Procedure
```python
sp_executor = DBConnect.DatabaseStoredProcedureExecutor(conn.environment_creds)
sp_executor.execute_sp("refresh_materialized_views")
```

--- 

## Versioning
```bash
v[MAJOR].[MINOR].[PATCH]
```
**MAJOR (0)** →`Increment when you make breaking changes that are not backward-compatible.`
- Example: Renaming methods, changing method arguments, restructuring the module.

**MINOR (1)** → `Increment when you add new features that are backward-compatible.`
- Example: Adding test_connection() and table_exists() without removing anything.

**PATCH (2)** → `Increment when you fix bugs, make small improvements, or do documentation-only updates.`
- Example: Cleaning up code style, fixing typos in docstrings, improving error - handling.

--- 

## Changelog
### **v2.5.2** — Latest Update
- **class FileReader — Simplified core reader**:
    - Now dedicated to reading only common tabular and vector formats:
        - `.csv`
        - `.shp`, `.geojson`
        - `.xlsx`, `.xlsm`, `.xls`, `.xlsb` (requires `file_sheetname`)
    - All geodatabase-related logic removed for cleaner separation of responsibilities.
    - Still accepts `file_layer` parameter for API compatibility, but it is ignored.
    - ----------------- Example usage -----------------
        ```python
        fr = FileReader()

        # Read CSV
        df_csv = fr.read_file("data/table.csv")

        # Read Shapefile
        gdf_shp = fr.read_file("data/boundary.shp")

        # Read Excel with sheet name
        df_xls = fr.read_file("data/data.xlsx", file_sheetname="Sheet1")
        ```

- **class GDBReader — Minimal Esri File Geodatabase loader**:
    - Supports both `.gdb` directories and `.gdb.zip` archives.
    - Reads entire layers **as-is** (no filtering, no reprojection).
    - Always prefers **pyogrio** for maximum speed, with automatic fallback to GeoPandas/Fiona if `pyogrio` is unavailable.
    - Includes quick metadata inspection without loading all features (`layer_statistics`).
    - ----------------- Example usage -----------------
        ```python
        gr = GDBReader()

        # List layers
        layers = gr.get_layers("data/city_roads.gdb.zip")
        print(layers)

        # Read the first layer
        gdf_first = gr.read_layer("data/city_roads.gdb.zip")

        # Read a specific layer
        gdf_roads = gr.read_layer("data/city_roads.gdb", layer_name="RoadCenterlines")

        # Get quick layer stats without loading full data
        stats = gr.layer_statistics("data/city_roads.gdb.zip", "RoadCenterlines")
        print(stats)
        ```

### **v2.4.1**
- **class FileReader — Added dedicated public method for reading a specific geodatabase layer**:
    - New method `read_gdb_layer(gdb_path, layer_name)` allows loading a `GeoDataFrame` from a specific `.gdb` or `.gdb.zip` layer without relying on internal `_read_gdb_layer()` calls.
    - Keeps `_read_gdb_layer()` as an internal helper while providing a clean public API for layer-level reads.
    - ----------------- Example usage -----------------
        ```python
        fr = FileReader()

        # Directly read a specific GDB layer
        gdf_layer = fr.read_gdb_layer("data/city_roads.gdb", "RoadCenterlines")
        print(gdf_layer.head())
        ```

### **v2.4.0**
- **class FileReader — Major refactor to unify geodatabase handling**:
    - Added support for both `.gdb` directories and zipped `.gdb.zip` files using GDAL VSI paths.
    - Introduced new helper methods:
        - `get_layers(gdb_path)` — List all layers in a geodatabase.
        - `layer_statistics(gdb_path, layer_name)` — Retrieve quick statistics for a specific layer.
    - `read_file()` now automatically detects `.gdb` and `.gdb.zip` and can default to reading the first layer if no layer name is provided.
    - Improved error handling for missing layers and unsupported formats.
    - ----------------- Example usage -----------------
        ```python
        fr = FileReader()

        # CSV example
        df_csv = fr.read_file("data/table.csv")

        # Read specific GDB layer via read_file()
        gdf = fr.read_file("data/city_roads.gdb.zip", file_layer="Roads")

        # List available layers
        layers = fr.get_layers("data/city_roads.gdb.zip")

        # Get quick stats for a layer
        stats = fr.layer_statistics("data/city_roads.gdb", "Parcels")

        print(layers)
        print(stats)
        ```

### **v2.1.0** 

- **Factory Pattern for Creating Helper Classes**:
    - `db.connector("dev")` instead of `DBConnect.Connector(db, "dev")`
    - `db.environment_manager()` instead of `DBConnect.EnvironmentManager(db)`
- **Optimized DataDumper**:
    - Added chunked inserts (`chunksize`) and `method="multi"` for faster large data loads.
- **DatabaseExtractor Enhancements**:
    - Added `get_data_with_custom_query()` method with **safe_mode** flag (enabled by default).
    - Safe mode allows **only** read-only queries (`SELECT` and CTEs: `WITH ... SELECT`).
    - Blocks destructive keywords like `DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, and `TRUNCATE` in safe mode.
    - Added ability to disable safe mode (`safe_mode=False`) for advanced use cases.
- **Stored Procedure Executor**:
  - Unified API for stored procedures into **two methods**:
    - `execute_sp()` → Single SP execution, handles both with and without parameters.
    - `execute_multiple_sps()` → Multiple SP execution, supports mixed calls with and without parameters in one transaction.
  - Supports `?` placeholders for parameters for easier reading and safe parameter binding.
  - Improved transaction handling with `stop_on_error` option.
- **Cleaner API**:
    - No more passing `DBConnect` instance into its own sub-classes.
    - Centralized config path management in the main `DBConnect` object.
- **Improved Maintainability**:
    - Standardized helper object creation.
    - Easier for testing and scripting.


--- 

### **v2.0.0**
- Integrated **EnvironmentManager** for CRUD operations on `db_config.json`.
- Centralized **config file path initialization** in `DBConnect` so all classes share the same path.
- Improved **Connector**:
  - Added `test_connection()` method.
  - Added `table_exists()` method.
- Applied **PEP8 cleanup** and improved docstrings for maintainability.
- Consolidated **config file handling** for all related classes to ensure consistency.

--- 

### **Planned Features**
- [ ] Package DBConnect Module to PyP making it available online.
- [ ] Add support for `.parquet` in `FileReader`.
- [ ] Add UPSERT support in `DataDumper`.
- [ ] Add JSON import/export helpers.
- [ ] Replace `print` statements with configurable logging.
- [ ] Add connection pooling for high-performance workloads.


