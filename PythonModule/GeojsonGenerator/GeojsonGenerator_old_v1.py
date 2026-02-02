# geojson_from_dataframe.py
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import pandas as pd


GeometryLike = Dict[str, Any]  # GeoJSON-like dict


_POINT_WKT_RE = re.compile(
    r"^\s*POINT\s*\(\s*(?P<lon>[-+]?\d+(?:\.\d+)?)\s+(?P<lat>[-+]?\d+(?:\.\d+)?)\s*\)\s*$",
    re.IGNORECASE,
)


@dataclass
class DataFrameToGeoJSON:
    """
    Convert a pandas.DataFrame to a GeoJSON FeatureCollection.

    Notes:
    - RFC 7946 requires WGS84 lon/lat ordering.
    - 'crs' is intentionally omitted to comply with RFC 7946.
    """
    df: pd.DataFrame
    lat_col: Optional[str] = None
    lon_col: Optional[str] = None
    geometry_col: Optional[str] = None
    id_col: Optional[str] = None
    properties: Optional[Sequence[str]] = None  # which columns to include as properties
    dropna: bool = True                         # drop rows with missing coords/geometry
    precision: int = 6                          # rounding precision for coordinates
    _excluded_cols: set = field(default_factory=set, init=False)

    def __post_init__(self) -> None:
        self._validate_init()
        self._excluded_cols = {self.id_col, self.lat_col, self.lon_col, self.geometry_col}
        self._excluded_cols = {c for c in self._excluded_cols if c}

    def to_feature_collection(self) -> Dict[str, Any]:
        features: List[Dict[str, Any]] = []

        it = self.df.itertuples(index=False, name=None)
        cols = list(self.df.columns)

        for row in it:
            rec = dict(zip(cols, row))

            try:
                geom = self._build_geometry(rec)
            except ValueError as e:
                if self.dropna:
                    continue
                raise

            if geom is None:
                if self.dropna:
                    continue
                raise ValueError("Geometry could not be constructed for row: %r" % rec)

            feat: Dict[str, Any] = {
                "type": "Feature",
                "geometry": geom,
                "properties": self._build_properties(rec),
            }
            if self.id_col and self.id_col in rec and pd.notna(rec[self.id_col]):
                feat["id"] = rec[self.id_col]

            features.append(feat)

        return {"type": "FeatureCollection", "features": features}

    def to_file(self, path: str, indent: int = 2) -> None:
        fc = self.to_feature_collection()
        with open(path, "w", encoding="utf-8") as f:
            json.dump(fc, f, ensure_ascii=False, indent=indent)

    # -------------------- internals --------------------

    def _validate_init(self) -> None:
        if self.geometry_col is None and (self.lat_col is None or self.lon_col is None):
            raise ValueError(
                "Provide either (lat_col and lon_col) or geometry_col."
            )
        if self.properties is not None:
            unknown = [c for c in self.properties if c not in self.df.columns]
            if unknown:
                raise ValueError(f"properties contains unknown columns: {unknown}")

        # quick presence checks
        for c in (self.lat_col, self.lon_col, self.geometry_col, self.id_col):
            if c and c not in self.df.columns:
                raise ValueError(f"Column '{c}' not found in DataFrame.")

    def _build_geometry(self, rec: Dict[str, Any]) -> Optional[GeometryLike]:
        """
        Return a GeoJSON geometry dict or None.
        Accepted inputs:
          - lat/lon numeric columns -> Point
          - geometry_col as:
              * GeoJSON-like dict with 'type' and 'coordinates'
              * [lon, lat] or (lon, lat)
              * WKT 'POINT (lon lat)'
        """
        if self.geometry_col:
            raw = rec.get(self.geometry_col, None)
            if pd.isna(raw):
                return None
            # Case 1: already a GeoJSON-like dict
            if isinstance(raw, dict) and "type" in raw and "coordinates" in raw:
                return self._round_geometry(raw)

            # Case 2: pair-like [lon, lat]
            if isinstance(raw, (list, tuple)) and len(raw) == 2:
                lon, lat = raw[0], raw[1]
                lon, lat = self._to_float(lon), self._to_float(lat)
                if lon is None or lat is None:
                    return None
                return {"type": "Point", "coordinates": self._round_coord([lon, lat])}

            # Case 3: minimal WKT POINT
            if isinstance(raw, str):
                m = _POINT_WKT_RE.match(raw)
                if m:
                    lon = float(m.group("lon"))
                    lat = float(m.group("lat"))
                    return {"type": "Point", "coordinates": self._round_coord([lon, lat])}

            # Unknown geometry format
            raise ValueError(
                f"Unsupported geometry format in '{self.geometry_col}': {type(raw).__name__}"
            )

        # Build from lat/lon
        lat = self._to_float(rec.get(self.lat_col))
        lon = self._to_float(rec.get(self.lon_col))
        if lat is None or lon is None:
            return None
        return {"type": "Point", "coordinates": self._round_coord([lon, lat])}

    def _build_properties(self, rec: Dict[str, Any]) -> Dict[str, Any]:
        if self.properties is not None:
            prop_cols: Iterable[str] = self.properties
        else:
            prop_cols = [c for c in rec.keys() if c not in self._excluded_cols]

        props = {}
        for c in prop_cols:
            val = rec.get(c)
            # Convert pandas types to plain Python/JSON
            if pd.isna(val):
                continue
            if isinstance(val, (pd.Timestamp, pd.Timedelta)):
                val = str(val)
            elif hasattr(val, "item"):  # numpy scalar
                try:
                    val = val.item()
                except Exception:
                    val = str(val)
            props[c] = val
        return props

    def _to_float(self, v: Any) -> Optional[float]:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        try:
            return float(v)
        except Exception:
            return None

    def _round_coord(self, coord: Sequence[float]) -> List[float]:
        return [round(float(c), self.precision) for c in coord]

    def _round_geometry(self, geom: GeometryLike) -> GeometryLike:
        """
        Round coordinates recursively; supports Point/LineString/Polygon/Multi*.
        """
        def round_coords(obj: Any) -> Any:
            if isinstance(obj, (list, tuple)):
                if obj and isinstance(obj[0], (int, float)):  # a single coordinate pair
                    return self._round_coord(obj)  # type: ignore[arg-type]
                return [round_coords(x) for x in obj]
            return obj

        gtype = geom.get("type")
        coords = geom.get("coordinates")
        if gtype is None or coords is None:
            raise ValueError("Invalid GeoJSON geometry: missing 'type' or 'coordinates'")
        return {"type": gtype, "coordinates": round_coords(coords)}


# -------------------- example usage --------------------
if __name__ == "__main__":
    # Minimal demo: build points from lat/lon
    data = [
        {"id": 1, "name": "Site A", "lat": 14.5995, "lon": 120.9842},
        {"id": 2, "name": "Site B", "lat": 10.3157, "lon": 123.8854},
        {"id": 3, "name": "Bad Row", "lat": None, "lon": 100.0},  # dropped if dropna=True
    ]
    df = pd.DataFrame(data)

    g = DataFrameToGeoJSON(
        df=df,
        lat_col="lat",
        lon_col="lon",
        id_col="id",
        properties=["name"],   # optional; else it uses all non-geo columns
        precision=5,
    )
    fc = g.to_feature_collection()
    print(json.dumps(fc, indent=2, ensure_ascii=False))

    # To write a file:
    # g.to_file("sites.geojson")
