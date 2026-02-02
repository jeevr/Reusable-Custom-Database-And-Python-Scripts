# PostgresToGeoJsonExporter Documentation

A Python utility to export PostGIS tables into **GeoJSON FeatureCollections** efficiently.

---

## Features
- Stream export with **server-side cursors** (memory safe on large tables)
- Build valid RFC 7946 **GeoJSON Features**
- Choose **all columns** (`"*"` or omit `columns`) or a subset of columns
- Apply **WHERE filters** with parameters
- Support **ORDER BY**
- Support **on-the-fly reprojection** with `target_srid`
- Configurable **schema** and **geometry column**
- Multiple tables export in one go

---

## Usage Examples

### 1. Export **all columns**

#### a. Explicit `"*"`
```python
exporter.export_table(
    table="tblcircuits",
    output_path=r"D:\AERO\Geodata Assignment\tblcircuits.geojson",
    columns="*",                      # all non-geometry columns
    order_by="circuit_id ASC",
)
```

#### b. Omit columns entirely
```python
exporter.export_table(
    table="tblroads",
    output_path=r"D:\AERO\Geodata Assignment\tblroads.geojson",
    order_by="road_id ASC",
)
```
    Both include every non-geometry column in properties.

### 2. Export a subset of columns
```python
exporter.export_table(
    table="tblsites",
    output_path=r"D:\AERO\Geodata Assignment\tblsites_subset.geojson",
    columns=["site_id", "name", "status"],   # only these become 'properties'
    order_by="site_id ASC",
)
```

### 3. Apply a WHERE filter with parameters
```python
exporter.export_table(
    table="tblsites",
    output_path=r"D:\AERO\Geodata Assignment\tblsites_active.geojson",
    columns="*",
    where_sql="status = %s AND created_at >= %s",
    where_params=["ACTIVE", "2025-01-01"],
    order_by="created_at DESC",
)
```
    Use placeholders (%s) and where_params to avoid SQL injection.

### 4. Reproject on the fly (to WGS84 EPSG:4326)
```python
exporter.export_table(
    table="tblcircuits",
    output_path=r"D:\AERO\Geodata Assignment\tblcircuits_wgs84.geojson",
    columns="*",
    target_srid=4326,   # ST_Transform(t.geom, 4326) before ST_AsGeoJSON
)
```

### 5. Export multiple tables at once
```python
tables_and_outputs = [
    {
        "table": "tblcircuits",
        "output": r"D:\AERO\Geodata Assignment\tblcircuits.geojson",
        "columns": "*",
        "order_by": "circuit_id ASC",
    },
    {
        "table": "tblsites",
        "output": r"D:\AERO\Geodata Assignment\tblsites.geojson",
        "columns": ["site_id", "name"],
        "where_sql": "status = %s",
        "where_params": ["ACTIVE"],
        "order_by": "site_id ASC",
    },
]

exporter.export_many(tables_and_outputs)
```

### 6. Custom schema or geometry column
```python
exporter = PGToGeoJSONExporter(
    host="192.168.168.244",
    dbname="aero_demo",
    user="drx_demo_admin",
    password="***",
    port=5433,
    schema="aero",             # default: "public"
    geometry_column="shape",   # default: "geom"
)
```
