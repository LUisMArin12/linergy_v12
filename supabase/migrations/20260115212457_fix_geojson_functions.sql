/*
  # Fix GeoJSON Helper Functions

  1. Changes
    - Update function return types to properly cast enum types to text
    - Ensure all functions return proper JSON geometries

  2. Purpose
    - Fix type mismatch errors when enum columns are returned
    - Ensure frontend receives data in expected format
*/

-- Drop existing functions
DROP FUNCTION IF EXISTS get_lineas_geojson();
DROP FUNCTION IF EXISTS get_estructuras_geojson();
DROP FUNCTION IF EXISTS get_subestaciones_geojson();
DROP FUNCTION IF EXISTS get_fallas_geojson();

-- Function to get lineas with GeoJSON geometries
CREATE OR REPLACE FUNCTION get_lineas_geojson()
RETURNS TABLE (
  id uuid,
  numero text,
  nombre text,
  km_inicio float,
  km_fin float,
  clasificacion text,
  prioridad int,
  geom jsonb,
  created_at timestamptz,
  updated_at timestamptz
) AS $$
BEGIN
  RETURN QUERY
  SELECT 
    l.id,
    l.numero,
    l.nombre,
    l.km_inicio,
    l.km_fin,
    l.clasificacion::text,
    l.prioridad,
    ST_AsGeoJSON(l.geom)::jsonb as geom,
    l.created_at,
    l.updated_at
  FROM lineas l;
END;
$$ LANGUAGE plpgsql STABLE;

-- Function to get estructuras with GeoJSON geometries
CREATE OR REPLACE FUNCTION get_estructuras_geojson()
RETURNS TABLE (
  id uuid,
  linea_id uuid,
  numero_estructura text,
  km float,
  geom jsonb,
  created_at timestamptz,
  updated_at timestamptz
) AS $$
BEGIN
  RETURN QUERY
  SELECT 
    e.id,
    e.linea_id,
    e.numero_estructura,
    e.km,
    ST_AsGeoJSON(e.geom)::jsonb as geom,
    e.created_at,
    e.updated_at
  FROM estructuras e;
END;
$$ LANGUAGE plpgsql STABLE;

-- Function to get subestaciones with GeoJSON geometries
CREATE OR REPLACE FUNCTION get_subestaciones_geojson()
RETURNS TABLE (
  id uuid,
  nombre text,
  linea_id uuid,
  geom jsonb,
  created_at timestamptz,
  updated_at timestamptz
) AS $$
BEGIN
  RETURN QUERY
  SELECT 
    s.id,
    s.nombre,
    s.linea_id,
    ST_AsGeoJSON(s.geom)::jsonb as geom,
    s.created_at,
    s.updated_at
  FROM subestaciones s;
END;
$$ LANGUAGE plpgsql STABLE;

-- Function to get fallas with GeoJSON geometries
CREATE OR REPLACE FUNCTION get_fallas_geojson()
RETURNS TABLE (
  id uuid,
  linea_id uuid,
  km float,
  tipo text,
  descripcion text,
  estado text,
  ocurrencia_ts timestamptz,
  geom jsonb,
  created_at timestamptz,
  updated_at timestamptz
) AS $$
BEGIN
  RETURN QUERY
  SELECT 
    f.id,
    f.linea_id,
    f.km,
    f.tipo,
    f.descripcion,
    f.estado::text,
    f.ocurrencia_ts,
    ST_AsGeoJSON(f.geom)::jsonb as geom,
    f.created_at,
    f.updated_at
  FROM fallas f;
END;
$$ LANGUAGE plpgsql STABLE;