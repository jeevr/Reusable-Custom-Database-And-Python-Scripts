# pg_to_geojson.py
from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence

import psycopg2
from psycopg2 import sql


@dataclass
class GeojsonGenerator:
    """
    Stream PostGIS tables to GeoJSON FeatureCollections with per-table geometry column override.

    Features:
      - Server-side cursor streaming (memory-safe on huge tables)
      - RFC 7946 Feature construction in-database (ST_AsGeoJSON + jsonb)
      - 'columns': "*" or omit => all non-geometry columns; or pass a subset list
      - Per-table WHERE, params, ORDER BY
      - Optional on-the-fly reprojection via target_srid
      - Default schema + geometry column, both overridable per table

    Example:
      exporter = GeojsonGenerator(
          host="dbhost", dbname="mydb", user="me", password="secret",
          port=5432, schema="public", geometry_column="geom"
      )

      # Table A uses default 'geom'; Table B uses 'shape'
      exporter.export_many([
          {"table": "table_a", "output": "a.geojson", "columns": "*"},
          {"table": "table_b", "output": "b.geojson", "columns": ["id","name"], "geometry_column": "shape"},
      ])
    """

    # ---- Connection config ----
    host: str
    dbname: str
    user: str
    password: str
    port: int = 5432

    # ---- Defaults (overridable per call) ----
    schema: str = "public"
    geometry_column: str = "geom"  # global default; can be overridden per table
    batch_size: int = 10_000
    show_progress: bool = True

    # ---- Internal ----
    _conn: Optional[psycopg2.extensions.connection] = field(default=None, init=False, repr=False)

    @contextmanager
    def _connect(self):
        conn = psycopg2.connect(
            host=self.host, dbname=self.dbname, user=self.user,
            password=self.password, port=self.port,
        )
        try:
            yield conn
        finally:
            conn.close()

    # ---------- helpers ----------

    def _count_rows(
        self,
        conn,
        table: str,
        schema: str,
        where_sql: Optional[str],
        where_params: Optional[Sequence[object]],
    ) -> int:
        with conn.cursor() as cur:
            q = sql.SQL("SELECT COUNT(*) FROM {}.{} t").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
            if where_sql and where_sql.strip():
                q = sql.SQL("{} WHERE {}").format(q, sql.SQL(where_sql))
            cur.execute(q, where_params or [])
            return int(cur.fetchone()[0])

    def _geom_expr(self, geom_col: str, target_srid: Optional[int]) -> sql.Composed:
        """
        Build the geometry expression for a given geometry column name.
        """
        g = sql.Identifier(geom_col)
        if target_srid is None:
            return sql.SQL("ST_AsGeoJSON(t.{g})::jsonb").format(g=g)
        return sql.SQL("ST_AsGeoJSON(ST_Transform(t.{g}, {srid}))::jsonb").format(
            g=g, srid=sql.Literal(int(target_srid))
        )

    def _props_expr(self, columns: Optional[Sequence[str]], geom_col: str) -> sql.Composed:
        """
        Properties expression:
          - None/empty => all non-geometry columns: to_jsonb(t) - '<geom_col>'
          - Subset => to_jsonb((SELECT r FROM (SELECT t.c1, t.c2, ...) r))
        """
        if not columns:
            return sql.SQL("to_jsonb(t) - {}").format(sql.Literal(geom_col))
        cols = [sql.SQL("t.{}").format(sql.Identifier(c)) for c in columns]
        sub = sql.SQL("SELECT r FROM (SELECT {}) r").format(sql.SQL(", ").join(cols))
        return sql.SQL("to_jsonb(({}))").format(sub)

    def _build_query(
        self,
        *,
        schema: str,
        table: str,
        columns: Optional[Sequence[str]],
        geom_col: str,
        where_sql: Optional[str],
        order_by: Optional[str],
        target_srid: Optional[int],
    ) -> sql.Composed:
        sch = sql.Identifier(schema)
        tbl = sql.Identifier(table)

        geom_jsonb = self._geom_expr(geom_col, target_srid)
        props_jsonb = self._props_expr(columns, geom_col)

        feature_expr = sql.SQL(
            "jsonb_build_object('type','Feature','geometry', {g}, 'properties', COALESCE({p}, '{{}}'::jsonb))::text AS feature"
        ).format(g=geom_jsonb, p=props_jsonb)

        q = sql.SQL("SELECT {feature} FROM {sch}.{tbl} t WHERE t.{geom} IS NOT NULL").format(
            feature=feature_expr, sch=sch, tbl=tbl, geom=sql.Identifier(geom_col)
        )

        if where_sql and where_sql.strip():
            q = sql.SQL("{base} AND ({where})").format(base=q, where=sql.SQL(where_sql))

        if order_by and order_by.strip():
            q = sql.SQL("{base} ORDER BY {ob}").format(base=q, ob=sql.SQL(order_by))

        return q

    # ---------- public API ----------

    def export_table(
        self,
        table: str,
        output_path: str,
        *,
        columns: Optional[Sequence[str] | str] = None,
        where_sql: Optional[str] = None,
        where_params: Optional[Sequence[object]] = None,
        order_by: Optional[str] = None,
        target_srid: Optional[int] = None,
        # per-table overrides:
        schema: Optional[str] = None,
        geometry_column: Optional[str] = None,
    ) -> None:
        """
        Export one table into a .geojson file.

        Args:
          table:            table name
          output_path:      destination GeoJSON path
          columns:          list of properties to include; "*" or omit => all non-geometry
          where_sql:        SQL predicate without 'WHERE' (use %s placeholders)
          where_params:     parameters for where_sql
          order_by:         ORDER BY fragment
          target_srid:      reproject geometry to this SRID before ST_AsGeoJSON
          schema:           per-table schema override (default: self.schema)
          geometry_column:  per-table geometry column override (default: self.geometry_column)
        """
        # Normalize options
        if isinstance(columns, str) and columns.strip() == "*":
            columns = None
        eff_schema = schema or self.schema
        eff_geom = geometry_column or self.geometry_column

        with self._connect() as conn:
            total = None
            if self.show_progress:
                try:
                    total = self._count_rows(conn, table, eff_schema, where_sql, where_params)
                except Exception as e:
                    print(f"[warn] count(*) failed: {e}", file=sys.stderr)

            # Named cursor for streaming
            cur = conn.cursor(name=f"cur_{eff_schema}_{table}")
            cur.itersize = self.batch_size

            query = self._build_query(
                schema=eff_schema,
                table=table,
                columns=columns,
                geom_col=eff_geom,
                where_sql=where_sql,
                order_by=order_by,
                target_srid=target_srid,
            )
            cur.execute(query, where_params or [])

            # Stream write
            with open(output_path, "w", encoding="utf-8") as f:
                f.write('{"type":"FeatureCollection","features":[')
                first = True
                written = 0

                while True:
                    rows = cur.fetchmany(self.batch_size)
                    if not rows:
                        break

                    for (feature_text,) in rows:
                        if feature_text is None:
                            continue
                        if first:
                            first = False
                        else:
                            f.write(",")
                        f.write("\n")
                        f.write(feature_text)
                        written += 1

                        if total and self.show_progress and written % self.batch_size == 0:
                            pct = (written / max(total, 1)) * 100.0
                            print(f"[{eff_schema}.{table}] {written}/{total} ({pct:.1f}%)", file=sys.stderr)

                f.write("\n]}")

            if self.show_progress:
                if total is not None:
                    print(f"[{eff_schema}.{table}] done: {written}/{total} (100%)")
                else:
                    print(f"[{eff_schema}.{table}] done: {written} features")

            cur.close()

    def export_many(self, items: Iterable[Dict[str, object]]) -> None:
        """
        Export multiple tables. Each item supports:

          {
            "table": "tblcircuits",
            "output": r"D:\path\tblcircuits.geojson",
            "columns": "*",                       # or ["col1","col2"] or omit for all
            "where_sql": "status = %s",
            "where_params": ["ACTIVE"],
            "order_by": "circuit_id ASC",
            "target_srid": 4326,
            "schema": "aero",                     # per-table schema (optional)
            "geometry_column": "shape"            # per-table geometry column (optional)
          }
        """
        for item in items:
            self.export_table(
                table=item["table"],  # type: ignore[index]
                output_path=item["output"],  # type: ignore[index]
                columns=item.get("columns"),  # type: ignore[arg-type]
                where_sql=(item.get("where_sql") or item.get("where") or item.get("filter") or None),  # type: ignore[arg-type]
                where_params=item.get("where_params"),  # type: ignore[arg-type]
                order_by=item.get("order_by"),  # type: ignore[arg-type]
                target_srid=item.get("target_srid"),  # type: ignore[arg-type]
                schema=item.get("schema"),  # type: ignore[arg-type]
                geometry_column=item.get("geometry_column"),  # type: ignore[arg-type]
            )


# ----------------------------- CLI-style example -----------------------------
if __name__ == "__main__":
    # Prefer env vars for credentials/config
    exporter = GeojsonGenerator(
        host=os.getenv("PGHOST", "localhost"),
        dbname=os.getenv("PGDATABASE", "mydb"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
        port=int(os.getenv("PGPORT", "5432")),
        schema=os.getenv("PGSCHEMA", "public"),
        geometry_column=os.getenv("PGGEOM", "geom"),
        batch_size=int(os.getenv("PGBATCH", "10000")),
        show_progress=True,
    )

    tasks = [
        # Table with default geometry column "geom"
        {
            "table": "tblcircuits",
            "output": r"./tblcircuits.geojson",
            "columns": "*",
            "order_by": "circuit_id ASC",
            "target_srid": 4326,
        },
        # Table with geometry column named "shape"
        {
            "table": "tblsites",
            "output": r"./tblsites.geojson",
            "columns": ["site_id", "name", "status"],
            "geometry_column": "shape",   # per-table override
            "where_sql": "status = %s",
            "where_params": ["ACTIVE"],
            "order_by": "site_id ASC",
        },
        # Table with geometry column named "wkb_geometry" in a different schema
        {
            "table": "tblroads",
            "output": r"./tblroads.geojson",
            "schema": "aero",
            "geometry_column": "wkb_geometry",
            "order_by": "road_id ASC",
        },
    ]

    exporter.export_many(tasks)
    print("All exports finished.")
