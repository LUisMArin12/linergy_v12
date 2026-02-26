/*
  # Add function to insert falla with WKT geometry
  
  This migration creates a helper function to insert fallas with geometry
  provided as WKT (Well-Known Text) format.
  
  ## Changes
  
  1. New Function
     - `insert_falla_with_wkt` - Inserts a falla record converting WKT to PostGIS geometry
     - Parameters:
       - p_linea_id: UUID of the line
       - p_km: Kilometer position
       - p_tipo: Type of fault
       - p_descripcion: Description (optional)
       - p_ocurrencia_ts: Timestamp of occurrence
       - p_estado: Status of fault
       - p_geom_wkt: Geometry in WKT format (e.g., "POINT(lon lat)")
     - Returns: The inserted falla record with all fields
  
  2. Security
     - Grant execute permission to anon and authenticated users
*/

-- Create function to insert falla with WKT geometry
CREATE OR REPLACE FUNCTION insert_falla_with_wkt(
  p_linea_id uuid,
  p_km double precision,
  p_tipo text,
  p_descripcion text,
  p_ocurrencia_ts timestamptz,
  p_estado estado_falla,
  p_geom_wkt text
)
RETURNS TABLE (
  id uuid,
  linea_id uuid,
  km double precision,
  tipo text,
  descripcion text,
  estado estado_falla,
  ocurrencia_ts timestamptz,
  geom geometry,
  created_at timestamptz,
  updated_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN QUERY
  INSERT INTO fallas (linea_id, km, tipo, descripcion, ocurrencia_ts, estado, geom)
  VALUES (p_linea_id, p_km, p_tipo, p_descripcion, p_ocurrencia_ts, p_estado, ST_GeomFromText(p_geom_wkt, 4326))
  RETURNING 
    fallas.id,
    fallas.linea_id,
    fallas.km,
    fallas.tipo,
    fallas.descripcion,
    fallas.estado,
    fallas.ocurrencia_ts,
    fallas.geom,
    fallas.created_at,
    fallas.updated_at;
END;
$$;

-- Grant execute permission to anon and authenticated users
GRANT EXECUTE ON FUNCTION insert_falla_with_wkt(uuid, double precision, text, text, timestamptz, estado_falla, text) TO anon, authenticated, service_role;
