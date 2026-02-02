# Reusable Custom Database And Python Scripts

A comprehensive collection of **PostgreSQL functions**, **stored procedures**, and **Python utilities** for database development, deployment, and geospatial data workflows. Build faster with pre-built helpers for DB connectivity, GeoJSON generation, and SQL utilities.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Repository Structure](#repository-structure)
- [Quick Start](#quick-start)
- [Features](#features)
- [Usage Examples](#usage-examples)
- [Installation](#installation)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

---

## Overview

This repository is designed for **DBAs, developers, and data engineers** who need a lightweight toolkit for common database tasks. It includes:

- **PostgreSQL SQL functions** for metadata queries, conversions, safe operations, and data transformations
- **Python DB connectivity module** (`DBConnect`) for working with PostgreSQL from Python
- **GeoJSON generator** for exporting PostGIS tables to GeoJSON format
- **SQL deployer** for scripting database deployments
- **Sample orchestration scripts** showing how to use these tools together

---

## Repository Structure

### `PostgreSQL/` — SQL Functions & Helpers

Reusable PostgreSQL functions ready to deploy:

| Function | Purpose |
|----------|---------|
| `func_get_table_columns.sql` | Retrieve column metadata (name, type, nullable, defaults) |
| `func_get_table_columns_v2.sql` | Enhanced version of column retrieval |
| `func_get_duplicate_rows.sql` | Find and identify duplicate rows in a table |
| `func_get_table_dependents.sql` | Discover tables and views that depend on a table |
| `func_get_table_diff.sql` | Compare differences between two tables |
| `func_get_table_indexes.sql` | List all indexes on a table with details |
| `func_convert_coordinates.sql` | Convert JSON coordinates to different formats |
| `func_convert_to_base36.sql` | Encode numbers to Base36 format |
| `func_safe_division.sql` | Division with zero-division protection |
| `func_unpivot_table.sql` | Unpivot/transpose data from wide to long format |

**Sample Files:**
- `Sample Function that returns table.sql` — Template for table-returning functions
- `Sample SP that returns OUT variables.sql` — Template for stored procedures with output variables

**Events & Monitoring:**
- `Events/track_sp_and_function_changes.sql` — Audit trigger to track changes to stored procedures and functions

**Development:**
- `_dev/` — Scratch files and samples for experimentation

---

### `PythonModule/` — Python Utilities & Packages

#### **DBConnect** — Database Connection Helper
- **`DBConnect-Package/`** (Recommended) — Packaged version for production use
  - `pg_dbconnect/DBConnect.py` — Main module
  - `pyproject.toml` — Package metadata
  - Features: Query, insert, update, stored procedures, environment management
  
- **`DBConnect-Dev/`** — Historical versions (`v2.0.0` through `v2.5.2`)
  - Reference for version history and migration guidance

#### **GeojsonGenerator** — PostGIS to GeoJSON Exporter
- `GeojsonGenerator.py` — Export PostGIS tables to GeoJSON with filtering and reprojection
- `geojson_generator_from_jc.py` — Alternative implementation
- Features: Memory-safe streaming, RFC 7946 compliance, on-the-fly reprojection

#### **PostgresSQLDeployer** — SQL Deployment Tool
- `PostgresSQLDeployer.py` — Lightweight script for applying SQL files to databases
- Use case: Automate schema migrations and batch SQL execution

#### **PythonOrchestrationSamples** — Integration Examples
- `sample1.py`, `sample2.py`, `sample3.py`, `sample4.py` — Real-world examples combining DBConnect, GeoJSON, and deployer

#### **tests/** — Test & Example Scripts
- `test_dbconnect_v2_0_0.py`, `test_dbconnect_v2_1_0.py` — Unit tests for DBConnect versions
- `geojson_fpl.py`, `geojson_generator_from_jc.py` — GeoJSON export examples
- `sqldeployer_test1.py` — SQL deployer usage example
- `test.py` — General testing script
- `geo_data_read_via_pyogrio.ipynb` — Jupyter notebook for geospatial data workflows

#### **test_configs/** — Configuration Templates
- `db_config.json` — Sample database configuration file

---

## Quick Start

### 1. Using PostgreSQL Functions

**Apply a function to your database:**

```sql
-- Connect to your PostgreSQL database with psql, pgAdmin, or your preferred tool
-- Then execute the SQL file:
\i PostgreSQL/func_get_table_columns.sql

-- Use the function:
SELECT * FROM func_get_table_columns('public', 'my_table');
```

### 2. Using Python DBConnect Module

**Install from the packaged version:**

```bash
cd PythonModule/DBConnect-Package
pip install -e .
```

**Basic usage:**

```python
from pg_dbconnect import DBConnect

# Create connection
db = DBConnect()

# Read data into DataFrame
df = db.get_dataframe("SELECT * FROM my_table")

# Write data to database
db.dump_dataframe(df, "my_table", if_exists="replace")

# Execute stored procedure
db.execute_stored_procedure("my_procedure", {"param1": value1})
```

### 3. Exporting GeoJSON

**From PostGIS:**

```bash
cd PythonModule/GeojsonGenerator
python GeojsonGenerator.py
```

**In code:**

```python
from GeojsonGenerator import PostgresToGeoJsonExporter

exporter = PostgresToGeoJsonExporter(db_config="path/to/db_config.json")

exporter.export_table(
    table="my_spatial_table",
    output_path="output.geojson",
    columns=["id", "name", "geometry"],
    order_by="id ASC"
)
```

### 4. Deploying SQL Scripts

**Run a SQL file against your database:**

```python
from PostgresSQLDeployer import SQLDeployer

deployer = SQLDeployer(db_config="path/to/db_config.json")
deployer.deploy_sql_file("PostgreSQL/func_get_table_columns.sql")
```

---

## Features

### PostgreSQL Functions
✅ Metadata queries (columns, indexes, dependencies)  
✅ Data transformations (unpivot, conversions)  
✅ Safe operations (zero-division protection)  
✅ Duplicate detection & comparison  
✅ Coordinate & format conversions  

### DBConnect Module
✅ PostgreSQL connection management  
✅ Read/write DataFrames and GeoPandas GeoDataFrames  
✅ CSV, Excel, Shapefile import  
✅ Stored procedure execution  
✅ Environment configuration management  
✅ Table existence checks  

### GeoJSON Generator
✅ Memory-efficient streaming export  
✅ RFC 7946 compliant GeoJSON  
✅ WHERE clause filtering with parameters  
✅ On-the-fly coordinate reprojection (SRID conversion)  
✅ ORDER BY support  

### SQL Deployer
✅ Batch SQL file execution  
✅ Environment-aware deployments  
✅ Error handling and logging  

---

## Usage Examples

### Example 1: Get All Table Columns with Metadata

```sql
SELECT * 
FROM func_get_table_columns('public', 'customers')
ORDER BY ordinal_position;
```

### Example 2: Find Duplicate Rows

```sql
SELECT * 
FROM func_get_duplicate_rows('public', 'products', ARRAY['name', 'category']);
```

### Example 3: Convert Latitude/Longitude to Different Format

```sql
SELECT func_convert_coordinates(
    '{"coordinates": [{"longitude": -73.935242, "latitude": 40.730610}]}'::JSONB
);
```

### Example 4: Python Workflow — Read, Transform, Export

```python
from pg_dbconnect import DBConnect
from GeojsonGenerator import PostgresToGeoJsonExporter

# Connect to DB
db = DBConnect(environment="production")

# Read spatial data
gdf = db.get_geodataframe(
    "SELECT id, name, geom FROM cities WHERE status = 'active'"
)

# Process (e.g., reproject)
gdf = gdf.to_crs("EPSG:4326")

# Write back
db.dump_geodataframe(gdf, "cities_processed", if_exists="replace")

# Export to GeoJSON
exporter = PostgresToGeoJsonExporter(db_config=db.config)
exporter.export_table(
    table="cities_processed",
    output_path="cities.geojson",
    target_srid=4326
)
```

---

## Installation

### Prerequisites
- **PostgreSQL 12+** (with PostGIS for spatial functions)
- **Python 3.8+**
- **pip** (Python package manager)

### Setup Steps

1. **Clone the repository:**
   ```bash
   git clone https://github.com/jeevr/Reusable-Custom-Database-And-Python-Scripts.git
   cd Reusable-Custom-Database-And-Python-Scripts
   ```

2. **Install Python dependencies** (recommended):
   ```bash
   cd PythonModule/DBConnect-Package
   pip install -e .
   ```

3. **Configure database connection:**
   - Copy `PythonModule/test_configs/db_config.json` to your project
   - Update with your PostgreSQL credentials:
   ```json
   {
       "environments": {
           "dev": {
               "NAME": "my_database",
               "HOST": "localhost",
               "PORT": "5432",
               "USER": "postgres",
               "PASS": "your_password"
           }
       }
   }
   ```

4. **Deploy SQL functions** (optional):
   ```bash
   # Using psql
   psql -U postgres -d my_database -f PostgreSQL/func_get_table_columns.sql
   ```

---

## Testing

Run the test suite to validate your setup:

```bash
# Test DBConnect module
cd PythonModule/tests
python test_dbconnect_v2_5_2.py

# Test GeoJSON generation
python geojson_fpl.py

# Test SQL deployer
python sqldeployer_test1.py
```

**Jupyter Notebook Example:**
```bash
# Open the geospatial data workflow notebook
jupyter notebook geo_data_read_via_pyogrio.ipynb
```

---

## Contributing

We welcome contributions! Here's how:

1. **Report bugs** — Open an issue with clear descriptions and reproduction steps
2. **Submit improvements** — Fork, make changes, and submit a pull request
3. **Add examples** — Share new use cases in the `PythonOrchestrationSamples/` or `tests/` folder

**Style Guidelines:**
- Keep changes focused and minimal
- Follow existing naming conventions (e.g., `func_` prefix for SQL functions)
- Include comments for complex logic
- Test your changes before submitting

---

## License

This project is licensed under the **MIT License**. See [LICENSE](LICENSE) for details.

**Copyright © 2026 Jayver Lendio**

You are free to use, modify, and distribute this code in personal and commercial projects.

---

## Support & Resources

- **Documentation:** See individual README files in each module:
  - `PythonModule/DBConnect-Package/README.md` — DBConnect API reference
  - `PythonModule/GeojsonGenerator/README.md` — GeoJSON export examples
  
- **Examples:** Check `PythonModule/PythonOrchestrationSamples/` for real-world workflows

- **Questions?** Review the `tests/` folder for usage patterns

---

**Last updated:** February 3, 2026

