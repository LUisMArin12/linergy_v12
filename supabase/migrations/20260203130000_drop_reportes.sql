-- Revert: eliminar tabla "reportes" (si existiera) para volver al esquema anterior
-- Seguro: IF EXISTS evita fallos si nunca se creó.

begin;

drop table if exists public.reportes cascade;

commit;
