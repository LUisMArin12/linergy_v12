/*
  # Actualizar función get_fallas_geojson para Filtrar Fallas Cerradas

  1. Cambios
    - Actualizar función `get_fallas_geojson` para filtrar fallas con estado 'CERRADA'
    - Solo retorna fallas con estado diferente a 'CERRADA'
    - Las fallas cerradas no aparecerán en el mapa

  2. Notas
    - Las fallas cerradas se mantienen en la base de datos para el historial
    - Para consultar todas las fallas (incluyendo cerradas), se debe usar una query directa
    - Esta función se usa para mostrar fallas activas en el mapa
*/

-- Recrear función get_fallas_geojson con filtro de fallas cerradas
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
  WHERE f.deleted_at IS NULL 
    AND f.estado != 'CERRADA';
END;
$$ LANGUAGE plpgsql STABLE;