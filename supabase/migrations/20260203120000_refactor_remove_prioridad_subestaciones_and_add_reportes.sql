-- Refactorización: limpieza de entidades (sin CRUD de reportes)
-- 1) Eliminar Prioridad (columna en lineas) y Subestaciones (tabla + funciones)
-- 2) (eliminado) CRUD de reportes

-- =========================
-- Cleanup: Prioridad
-- =========================
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'lineas' AND column_name = 'prioridad'
  ) THEN
    ALTER TABLE public.lineas DROP COLUMN prioridad;
  END IF;
END $$;

-- =========================
-- Cleanup: Subestaciones
-- =========================
DROP FUNCTION IF EXISTS public.get_subestaciones_geojson();

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'subestaciones'
  ) THEN
    DROP TABLE public.subestaciones;
  END IF;
END $$;

-- =========================
