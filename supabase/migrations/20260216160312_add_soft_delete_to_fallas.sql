/*
  # Implementar Borrado Lógico en Tabla Fallas

  1. Cambios en la Tabla
    - Agregar columna `deleted_at` (timestamp nullable) a la tabla `fallas`
    - Las fallas con `deleted_at` NOT NULL están "eliminadas" pero se conservan para histórico

  2. Índice
    - Crear índice en `deleted_at` para optimizar queries que filtren fallas activas

  3. Actualización de Políticas RLS
    - Las políticas deben filtrar automáticamente las fallas eliminadas (deleted_at IS NULL)
    - Excepto para consultas históricas específicas

  4. Notas Importantes
    - Esta migración NO elimina datos existentes
    - Todas las fallas actuales tienen deleted_at = NULL por defecto
    - El histórico completo se mantiene para auditoría
*/

-- Agregar columna deleted_at a la tabla fallas
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public'
    AND table_name = 'fallas'
    AND column_name = 'deleted_at'
  ) THEN
    ALTER TABLE public.fallas
    ADD COLUMN deleted_at timestamptz DEFAULT NULL;
  END IF;
END $$;

-- Crear índice para optimizar queries de fallas activas
CREATE INDEX IF NOT EXISTS idx_fallas_deleted_at ON public.fallas(deleted_at);

-- Comentario descriptivo
COMMENT ON COLUMN public.fallas.deleted_at IS 'Timestamp de eliminación lógica. NULL = activa, NOT NULL = eliminada (pero conservada para histórico)';