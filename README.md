# Reusable Custom Database And Python Scripts

This repository contains a collection of reusable PostgreSQL functions, stored-procedures, and Python utilities designed to speed up database development, deployment, and geospatial data workflows. The code is split between SQL helpers under `PostgreSQL/` and Python modules and tools under `PythonModule/`.

**Project Overview**
- **Purpose:** Reusable utilities for working with PostgreSQL and Python-based data workflows. Useful for DBAs, developers, and data engineers who want a lightweight set of tools and examples for common tasks (DB connect helpers, SQL utilities, GeoJSON generation, and simple deploy scripts).
- **Contents:** A focused set of SQL helper functions (conversions, metadata queries, safe operations) and Python modules for DB connectivity, GeoJSON generation, and deployment orchestration.

**Repository Structure**
- **`PostgreSQL/`**: Reusable SQL functions and helper scripts.
	- Key files: `func_convert_coordinates.sql`, `func_convert_to_base36.sql`, `func_get_duplicate_rows.sql`, `func_get_table_columns.sql`, `func_get_table_columns_v2.sql`, `func_get_table_dependents.sql`, `func_get_table_diff.sql`, `func_get_table_indexes.sql`, `func_safe_division.sql`, `func_unpivot_table.sql`.
	- Extras: `Sample Function that returns table.sql`, `Sample SP that returns OUT variables.sql` and an `_dev/` folder with development samples.
	- Events: `Events/track_sp_and_function_changes.sql` — example trigger/audit helper for tracking changes to SPs/functions.

- **`PythonModule/`**: Python utilities, packages, and sample scripts.
	- **`DBConnect-Dev/`**: Multiple historical versions of a DB connection helper (`DBConnect_v2_0_0.py` ... `DBConnect_v2_5_2.py`) plus `db_config.json` sample configs.
	- **`DBConnect-Package/`**: Packaged version of the DB connection helper — contains `pg_dbconnect/DBConnect.py`, `pyproject.toml`, and package metadata for reuse or installation.
	- **`GeojsonGenerator/`**: Scripts to generate GeoJSON output from various inputs (`GeojsonGenerator.py`, `geojson_generator_from_jc.py`). Useful for converting spatial data sources to GeoJSON.
	- **`PostgresSQLDeployer/`**: `PostgresSQLDeployer.py` — a lightweight deploy helper for applying SQL files to a target database (see tests for usage examples).
	- **`PythonOrchestrationSamples/`**: Small sample scripts demonstrating orchestration patterns and how to call the helpers.
	- **`test_configs/`**: Example `db_config.json` for testing.
	- **`tests/`**: Various example scripts and tests demonstrating usage of the above modules (including `test_dbconnect_*` files and GeoJSON test scripts).

**Quick Start**
- **Run a Python script (PowerShell):**
```
cd "z:\Projects\Reusable-Custom-Database-And-Python-Scripts\PythonModule\GeojsonGenerator"
python GeojsonGenerator.py
```
- **Use the packaged DBConnect:** install or reference `PythonModule/DBConnect-Package/` in your project, then import `pg_dbconnect.DBConnect` where needed.
- **Apply SQL helpers:** SQL files in `PostgreSQL/` can be executed directly against your Postgres instance with your preferred client (psql, pgAdmin, or a deploy script).

**Usage Examples**
- **Get table columns:** Use the SQL in `func_get_table_columns.sql` or `func_get_table_columns_v2.sql` to retrieve column metadata.
- **Convert coordinates:** `func_convert_coordinates.sql` contains helpers to reproject/convert coordinate values.
- **Generate GeoJSON:** The scripts in `GeojsonGenerator/` show examples of reading spatial inputs and writing GeoJSON.
- **DB connection helper:** `PythonModule/DBConnect-Dev/` contains multiple versions; `DBConnect-Package/pg_dbconnect/DBConnect.py` is the packaged utility to integrate into applications.

**Testing**
- The `PythonModule/tests/` folder includes small scripts and unit-test style examples. Run them with `python` to validate behavior in your environment.

**Contributing**
- **Bug reports & PRs:** Open issues or pull requests with clear descriptions and, where applicable, test scripts that demonstrate the problem or improvement.
- **Style:** Keep changes minimal and focused; follow existing naming and structural conventions.

**License**
- See the top-level `LICENSE` file for licensing details.

If you'd like, I can:
- Add example commands to run the DB deployer against a local Postgres instance.
- Create a small `requirements.txt` or `pyproject.toml` at the repo root for easier installs.

---
Last updated: 2026-02-03

