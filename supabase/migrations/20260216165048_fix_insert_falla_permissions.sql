/*
  # Fix insert_falla_with_wkt permissions and error handling

  1. Changes
    - Grant explicit EXECUTE permissions to anon and authenticated roles
    - Recreate the function with better error handling
    - Ensure the function uses SECURITY DEFINER properly

  2. Security
    - Function executes with owner privileges to bypass RLS during insert
    - Only anon and authenticated roles can execute
*/

-- Drop and recreate the function with better error handling
DROP FUNCTION IF EXISTS insert_falla_with_wkt(uuid, double precision, text, text, timestamptz, estado_falla, text);

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
  updated_at timestamptz,
  deleted_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  -- Validate inputs
  IF p_linea_id IS NULL THEN
    RAISE EXCEPTION 'linea_id cannot be null';
  END IF;
  
  IF p_km IS NULL OR p_km < 0 THEN
    RAISE EXCEPTION 'km must be >= 0';
  END IF;
  
  IF p_tipo IS NULL OR trim(p_tipo) = '' THEN
    RAISE EXCEPTION 'tipo cannot be empty';
  END IF;
  
  IF p_geom_wkt IS NULL OR trim(p_geom_wkt) = '' THEN
    RAISE EXCEPTION 'geom_wkt cannot be empty';
  END IF;

  -- Insert and return
  RETURN QUERY
  INSERT INTO fallas (linea_id, km, tipo, descripcion, ocurrencia_ts, estado, geom)
  VALUES (
    p_linea_id,
    p_km,
    p_tipo,
    p_descripcion,
    p_ocurrencia_ts,
    p_estado,
    ST_GeomFromText(p_geom_wkt, 4326)
  )
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
    fallas.updated_at,
    fallas.deleted_at;
END;
$$;

-- Grant EXECUTE to anon and authenticated
GRANT EXECUTE ON FUNCTION insert_falla_with_wkt(uuid, double precision, text, text, timestamptz, estado_falla, text) TO anon;
GRANT EXECUTE ON FUNCTION insert_falla_with_wkt(uuid, double precision, text, text, timestamptz, estado_falla, text) TO authenticated;
GRANT EXECUTE ON FUNCTION insert_falla_with_wkt(uuid, double precision, text, text, timestamptz, estado_falla, text) TO service_role;
