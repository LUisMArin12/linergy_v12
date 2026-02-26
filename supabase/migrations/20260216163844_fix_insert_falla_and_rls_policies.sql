/*
  # Correcciones para Sistema de Fallas

  1. Cambios en Funciones
    - Actualizar `insert_falla_with_wkt` para incluir columna `deleted_at`
    
  2. Actualizar Políticas RLS
    - Modificar políticas SELECT para excluir fallas eliminadas (deleted_at IS NULL)
    - Mantener políticas INSERT, UPDATE, DELETE sin cambios
    
  3. Notas Importantes
    - Esta migración es segura y no afecta datos existentes
    - Las fallas eliminadas permanecen en la base de datos pero no aparecen en consultas normales
*/

-- Eliminar la función existente antes de recrearla
DROP FUNCTION IF EXISTS insert_falla_with_wkt(uuid, double precision, text, text, timestamptz, estado_falla, text);

-- Recrear la función insert_falla_with_wkt para incluir deleted_at
CREATE FUNCTION insert_falla_with_wkt(
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
    fallas.updated_at,
    fallas.deleted_at;
END;
$$;

-- Grant execute permission
GRANT EXECUTE ON FUNCTION insert_falla_with_wkt(uuid, double precision, text, text, timestamptz, estado_falla, text) TO anon, authenticated, service_role;

-- Actualizar política de lectura para excluir fallas eliminadas
DROP POLICY IF EXISTS "Public read access for fallas" ON fallas;
CREATE POLICY "Public read access for fallas"
  ON fallas FOR SELECT
  TO anon, authenticated
  USING (deleted_at IS NULL);
