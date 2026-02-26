// src/types/geo.ts
// Minimal GeoJSON types used across the app (frontend only).

export type Position = [number, number]; // [lon, lat]

export type GeoJSONPoint = {
  type: 'Point';
  coordinates: Position;
};

export type GeoJSONLineString = {
  type: 'LineString';
  coordinates: Position[];
};

export type GeoJSONMultiLineString = {
  type: 'MultiLineString';
  coordinates: Position[][];
};

export type GeoJSONGeometry = GeoJSONPoint | GeoJSONLineString | GeoJSONMultiLineString;

type GeometryLike = { type?: unknown; coordinates?: unknown };

function isPosition(v: unknown): v is Position {
  return (
    Array.isArray(v) &&
    v.length >= 2 &&
    typeof v[0] === 'number' &&
    typeof v[1] === 'number' &&
    Number.isFinite(v[0]) &&
    Number.isFinite(v[1])
  );
}

export function isGeoJSONGeometry(v: unknown): v is GeoJSONGeometry {
  if (!v || typeof v !== 'object') return false;
  const o = v as GeometryLike;

  if (o.type === 'Point') {
    return isPosition(o.coordinates);
  }

  if (o.type === 'LineString') {
    return Array.isArray(o.coordinates) && (o.coordinates as unknown[]).every(isPosition);
  }

  if (o.type === 'MultiLineString') {
    return (
      Array.isArray(o.coordinates) &&
      (o.coordinates as unknown[]).every(
        (seg) => Array.isArray(seg) && (seg as unknown[]).every(isPosition)
      )
    );
  }

  return false;
}

export function isGeoJSONPoint(v: unknown): v is GeoJSONPoint {
  return isGeoJSONGeometry(v) && v.type === 'Point';
}
