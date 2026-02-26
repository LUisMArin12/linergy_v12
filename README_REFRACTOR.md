# LINERGY – Refactorización (v4)

## Qué cambió

### 1) Limpieza de entidades (Cleanup)
- **Prioridades**: eliminadas del Frontend y del modelo de `lineas`.
- **Subestaciones**: eliminadas del Frontend y de los flujos de importación.
- **Configuración / Settings**: eliminado el módulo (ruta, menú y pantalla).

> En backend (Supabase) se incluye una migración nueva para **dropear** `lineas.prioridad`, **dropear** la tabla `subestaciones` y eliminar la función `get_subestaciones_geojson()`.

### 2) Reportes (modo clásico)
Se **revirtió** el CRUD independiente de reportes.

- La pantalla **Reportes** vuelve a ser un **inventario de fallas**.
- El “reporte” se **genera a partir de la Falla** (PDF / texto / modal) y **no** se persiste como entidad separada.

> Si en algún entorno ya existía la tabla `reportes`, se incluye una migración para eliminarla de forma segura.

### 3) UI/UX (Mobile‑first)
- Tabla (md+) con fallback en **tarjetas** para móvil.
- Formularios compactos y consistentes.

### 4) Calidad / Robustez
- Se corrigieron errores de parsing (ej. `src/types/geo.ts`).
- `eslint` fijado a **v8.57** para compatibilidad con `@typescript-eslint/*` y evitar el error de `allowShortCircuit`.

## Archivos clave tocados
- `src/pages/ReportsPage.tsx` (inventario de fallas + exportar/copy + modal)
- `src/lib/supabase.ts` (tipos: elimina prioridad/subestaciones; **sin** tipo `Reporte`)
- `src/lib/reportUtils.ts` (solo helpers para **Falla**)
- `src/pages/MapPage.tsx`, `src/components/map/LeafletMap.tsx`, `src/components/map/MapFilters.tsx` (remoción subestaciones/prioridad)
- `src/components/layout/Sidebar.tsx`, `src/App.tsx` (remoción Settings)
- `supabase/functions/import-kmz/index.ts` (subestaciones ya no se consideran)
- `supabase/migrations/20260203120000_refactor_remove_prioridad_subestaciones_and_add_reportes.sql`
- `supabase/migrations/20260203130000_drop_reportes.sql`

## Cómo ejecutar

### Frontend
```bash
npm install
npm run dev
```

### Lint
```bash
npm run lint
```

> Si ya habías instalado ESLint 9, borra dependencias e instala:
```bash
npm i -D eslint@^8.57.0 @eslint/js@^8.57.0
rmdir /s /q node_modules
del package-lock.json
npm install
npm run lint
```

### Supabase (migraciones)
Aplicar migraciones en tu proyecto Supabase (CLI / dashboard).

- `20260203120000_refactor_remove_prioridad_subestaciones_and_add_reportes.sql`: limpieza de entidades (prioridad/subestaciones).
- `20260203130000_drop_reportes.sql`: **revert** (elimina `public.reportes` si existe).

## Notas importantes
- Los archivos `supabase/functions/*` usan sintaxis Deno (`npm:` / `jsr:`). Si VS Code marca errores de tipos en esas carpetas, habilita Deno para `supabase/functions` o abre esa carpeta como workspace aparte.
