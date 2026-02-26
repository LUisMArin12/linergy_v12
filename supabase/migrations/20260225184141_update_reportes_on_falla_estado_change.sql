/*
  # Actualizar reportes cuando el estado de una falla cambia

  1. Cambios
    - Crear trigger que actualiza el estado del reporte cuando cambia el estado de la falla
    - Cuando una falla se marca como CERRADA, el reporte asociado también se actualiza a CERRADA
    - Lo mismo aplica para estados ABIERTA y EN_ATENCION

  2. Comportamiento
    - El trigger se ejecuta automáticamente después de cada actualización de falla
    - Solo actualiza reportes que estén vinculados a la falla (falla_id no null)
    - Mantiene sincronizado el estado entre fallas y reportes

  3. Notas
    - Esto asegura que los reportes reflejen el estado actual de las fallas
    - Si un reporte no tiene falla_id, no se verá afectado
*/

-- Crear función para sincronizar estado de reporte con falla
CREATE OR REPLACE FUNCTION sync_reporte_estado_from_falla()
RETURNS TRIGGER AS $$
BEGIN
  -- Solo actualizar si el estado cambió
  IF OLD.estado IS DISTINCT FROM NEW.estado THEN
    -- Actualizar el reporte asociado a esta falla
    UPDATE reportes
    SET 
      estado = NEW.estado,
      updated_at = now()
    WHERE falla_id = NEW.id;
  END IF;
  
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Crear trigger que se ejecuta después de actualizar una falla
DROP TRIGGER IF EXISTS trigger_sync_reporte_estado ON fallas;

CREATE TRIGGER trigger_sync_reporte_estado
  AFTER UPDATE OF estado ON fallas
  FOR EACH ROW
  WHEN (OLD.estado IS DISTINCT FROM NEW.estado)
  EXECUTE FUNCTION sync_reporte_estado_from_falla();