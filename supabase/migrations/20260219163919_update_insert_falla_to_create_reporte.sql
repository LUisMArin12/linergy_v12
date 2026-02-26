/*
  # Update insert_falla to create reporte automatically

  1. Changes
    - Modify insert_falla_with_wkt function to also create a reporte entry
    - When a falla is created, a reporte is automatically generated with the same data
    - The reporte references the falla via falla_id
  
  2. Behavior
    - Fallas and reportes are independent
    - Deleting a falla does NOT delete the reporte (FK is SET NULL)
    - Deleting a reporte does NOT affect the falla
*/

-- Drop and recreate the function
DROP FUNCTION IF EXISTS insert_falla_with_wkt(uuid, double precision, text, text, timestamptz, estado_falla, text);

CREATE OR REPLACE FUNCTION insert_falla_with_wkt(
  p_linea_id uuid,
  p_km double precision,
  p_tipo text,
  p_descripcion text DEFAULT NULL,
  p_ocurrencia_ts timestamptz DEFAULT now(),
  p_estado estado_falla DEFAULT 'ABIERTA',
  p_geom_wkt text DEFAULT NULL
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
DECLARE
  v_falla_id uuid;
  v_geom geometry;
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

  -- Convert WKT to geometry
  v_geom := ST_GeomFromText(p_geom_wkt, 4326);

  -- Insert falla and get the ID
  INSERT INTO fallas (linea_id, km, tipo, descripcion, ocurrencia_ts, estado, geom)
  VALUES (
    p_linea_id,
    p_km,
    p_tipo,
    p_descripcion,
    p_ocurrencia_ts,
    p_estado,
    v_geom
  )
  RETURNING fallas.id INTO v_falla_id;

  -- Insert corresponding reporte
  INSERT INTO reportes (falla_id, linea_id, km, tipo, descripcion, ocurrencia_ts, estado, geom)
  VALUES (
    v_falla_id,
    p_linea_id,
    p_km,
    p_tipo,
    p_descripcion,
    p_ocurrencia_ts,
    p_estado,
    v_geom
  );

  -- Return the falla data
  RETURN QUERY
  SELECT 
    f.id,
    f.linea_id,
    f.km,
    f.tipo,
    f.descripcion,
    f.estado,
    f.ocurrencia_ts,
    f.geom,
    f.created_at,
    f.updated_at,
    f.deleted_at
  FROM fallas f
  WHERE f.id = v_falla_id;
END;
$$;