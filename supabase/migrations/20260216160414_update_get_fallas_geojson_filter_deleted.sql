/*
  # Actualizar función get_fallas_geojson para Filtrar Fallas Eliminadas

  1. Cambios
    - Actualizar función `get_fallas_geojson` para filtrar fallas con `deleted_at IS NULL`
    - Solo retorna fallas activas (no eliminadas lógicamente)

  2. Notas
    - El histórico completo se mantiene en la base de datos
    - Para consultar fallas eliminadas, se debe usar una query directa con filtro específico
*/

-- Recrear función get_fallas_geojson con filtro de borrado lógico
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
  FROM fallas f
  WHERE f.deleted_at IS NULL;
END;
$$ LANGUAGE plpgsql STABLE;