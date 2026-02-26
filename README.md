# Linergy

## Sistema de Gestión Geoespacial de Fallas en Líneas Eléctricas

---

## 1. Descripción del sistema

**Linergy** es una aplicación web para la **visualización, registro y gestión de fallas** en líneas eléctricas mediante un sistema geoespacial basado en mapas interactivos.

El sistema permite:

* Visualizar líneas eléctricas georreferenciadas.
* Importar infraestructura desde archivos KMZ/KML.
* Registrar fallas por kilómetro de línea.
* Calcular automáticamente la ubicación geográfica de la falla.
* Controlar accesos mediante un sistema de roles.

Está diseñado para operar como una herramienta centralizada para el seguimiento de incidencias en redes eléctricas.

---

## 2. Alcance funcional

### 2.1 Visualización geoespacial

* Mapa interactivo basado en Leaflet.
* Representación de:

  * Líneas eléctricas.
  * Estructuras.
  * Fallas.
* Filtros por:

  * Estado de falla.
  * Línea específica.

### 2.2 Importación de líneas

* Importación de archivos **KMZ/KML**.
* Extracción automática de:

  * Trazado de línea (LineString).
  * Estructuras (Point).
* Generación de geometría consolidada de la línea.
* Cálculo automático de kilómetros de estructuras.

### 2.3 Registro de fallas

* Selección de línea.
* Captura de kilómetro afectado.
* Cálculo automático de coordenadas (lat/lon).
* Inserción de la falla en base de datos geoespacial.

### 2.4 Control de accesos (RBAC)

Sistema de roles basado en perfiles:

| Rol           | Permisos                             |
| ------------- | ------------------------------------ |
| Anónimo       | Lectura del mapa                     |
| Usuario       | Lectura de información               |
| Administrador | CRUD de líneas, fallas y estructuras |

---

## 3. Arquitectura del sistema

### 3.1 Frontend

Aplicación web SPA.

**Tecnologías:**

* Vite
* React
* TypeScript
* Leaflet
* React Query

**Responsabilidades:**

* Renderizado del mapa.
* Gestión de filtros y paneles.
* Formularios de registro.
* Integración con Supabase y Edge Functions.

---

### 3.2 Backend (BaaS)

Basado en **Supabase**.

**Componentes:**

* PostgreSQL
* PostGIS (datos geoespaciales)
* Autenticación
* RLS (Row Level Security)

---

### 3.3 Edge Functions

Funciones serverless para procesamiento geoespacial.

#### import-kmz

Responsabilidades:

* Descomprimir archivo KMZ.
* Parsear KML.
* Extraer:

  * Tramos de línea.
  * Estructuras.
* Insertar datos en:

  * `linea_tramos`
  * `estructuras`
* Reconstruir geometría de la línea.

#### compute-fault-location

Responsabilidades:

* Recibir:

  * ID de línea
  * Kilómetro de falla
* Calcular coordenadas:

  * Interpolación sobre estructuras.
  * O interpolación sobre geometría de línea.
* Retornar latitud y longitud.

---

## 4. Modelo de datos principal

### 4.1 Tabla: `lineas`

| Campo  | Tipo                       |
| ------ | -------------------------- |
| id     | uuid                       |
| numero | text                       |
| nombre | text                       |
| geom   | geometry(LineString, 4326) |

---

### 4.2 Tabla: `linea_tramos`

| Campo    | Tipo                       |
| -------- | -------------------------- |
| id       | uuid                       |
| linea_id | uuid                       |
| geom     | geometry(LineString, 4326) |

---

### 4.3 Tabla: `estructuras`

| Campo    | Tipo                  |
| -------- | --------------------- |
| id       | uuid                  |
| linea_id | uuid                  |
| numero   | text                  |
| km       | numeric               |
| geom     | geometry(Point, 4326) |

---

### 4.4 Tabla: `fallas`

| Campo      | Tipo                  |
| ---------- | --------------------- |
| id         | uuid                  |
| linea_id   | uuid                  |
| km         | numeric               |
| estado     | text                  |
| geom       | geometry(Point, 4326) |
| created_at | timestamp             |

---

### 4.5 Tabla: `profiles`

| Campo | Tipo |
| ----- | ---- |
| id    | uuid |
| email | text |
| role  | text |

---

## 5. Flujo de operación técnico

### 5.5 Importación de línea

1. Usuario administrador sube archivo KMZ.
2. Frontend envía archivo a `import-kmz`.
3. Edge Function:

   * Extrae KML.
   * Inserta tramos y estructuras.
   * Ejecuta función SQL de reconstrucción.
4. La línea queda disponible en el mapa.

---

### 5.2 Registro de falla

1. Usuario administrador abre modal de registro.
2. Selecciona línea y kilómetro.
3. Frontend llama `compute-fault-location`.
4. Edge Function calcula coordenadas.
5. Frontend ejecuta RPC `insert_falla_with_wkt`.
6. La falla se guarda y se visualiza en el mapa.

---

## 6. Seguridad y control de acceso

### Autenticación

* Gestionada por Supabase Auth.

### Autorización

* Implementada mediante:

  * Tabla `profiles`.
  * Políticas RLS.
  * Función `is_admin()` en base de datos.

### Reglas generales

* Acceso público: solo lectura.
* Escritura: solo administradores.

---

## 7. Requisitos técnicos

### Entorno

* Node.js 18+
* Navegador moderno
* Cuenta de Supabase con PostGIS habilitado

---

## 8. Variables de entorno

Archivo `.env`:

```env
VITE_SUPABASE_URL=your_supabase_url
VITE_SUPABASE_ANON_KEY=your_anon_key
```

---

## 9. Comandos principales

### Instalación

```bash
npm install
```

### Desarrollo

```bash
npm run dev
```

### Build

```bash
npm run build
```

---

## 10. Despliegue de Edge Functions

```bash
supabase functions deploy import-kmz
supabase functions deploy compute-fault-location
```

Configurar secretos en Supabase:

```
SUPABASE_URL
SUPABASE_ANON_KEY
SUPABASE_SERVICE_ROLE_KEY
```

---

## 11. Flujo de pruebas básico

1. Crear usuario administrador.
2. Importar archivo KMZ.
3. Verificar que la línea aparece en el mapa.
4. Registrar una falla con kilómetro válido.
5. Confirmar visualización de la falla.

---
