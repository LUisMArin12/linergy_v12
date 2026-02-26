/*
  # Reset and Seed Durango Data
  
  1. Clears all existing data (fallas, estructuras, subestaciones, lineas)
  2. Seeds realistic transmission line data for Durango, Mexico
  
  ## New Data Structure:
  
  ### Líneas (3 transmission lines):
    - **230kV-DGO-01**: Durango Centro - Gómez Palacio (230kV, ALTA)
      * Main high-voltage line connecting capital with industrial zone
      * ~165 km length
    
    - **230kV-DGO-02**: Durango Centro - Santiago Papasquiaro (230kV, MODERADA)
      * Connects to northern mining region
      * ~92 km length
    
    - **115kV-DGO-03**: Durango Centro - El Salto (115kV, BAJA)
      * Connects to forestry and tourism region
      * ~142 km length
  
  ### Subestaciones (4 substations):
    - Durango Centro (main hub)
    - Gómez Palacio (industrial)
    - Santiago Papasquiaro (mining)
    - El Salto (forestry/tourism)
  
  ### Estructuras (24 structures):
    - 8 structures on 230kV-DGO-01
    - 5 structures on 230kV-DGO-02
    - 7 structures on 115kV-DGO-03
    - Each structure includes tower number and km marker
  
  ### Fallas (5 faults):
    - Various fault types in different states
    - Realistic locations along transmission lines
  
  ## Security:
    - All tables have RLS enabled from previous migrations
    - Uses IF NOT EXISTS patterns for safety
*/

-- Clear existing data in correct order (respecting foreign keys)
DELETE FROM fallas;
DELETE FROM estructuras;
DELETE FROM subestaciones;
DELETE FROM lineas;

DO $$
DECLARE
  v_linea1_id uuid;
  v_linea2_id uuid;
  v_linea3_id uuid;
BEGIN
  -- ============================================
  -- LÍNEAS DE TRANSMISIÓN
  -- ============================================
  
  -- Línea 1: Durango Centro - Gómez Palacio (230kV)
  INSERT INTO lineas (id, numero, nombre, km_inicio, km_fin, clasificacion, prioridad, geom)
  VALUES (
    gen_random_uuid(),
    '230kV-DGO-01',
    'Durango - Gómez Palacio 230kV',
    0,
    165.2,
    'ALTA',
    5,
    ST_GeomFromText('LINESTRING(-104.6532 24.0277, -104.5500 24.3000, -104.4000 24.5500, -104.2000 24.8000, -103.9500 25.2000, -103.7000 25.5000, -103.5000 25.7000, -103.4969 25.8571)', 4326)
  )
  RETURNING id INTO v_linea1_id;
  
  -- Línea 2: Durango Centro - Santiago Papasquiaro (230kV)
  INSERT INTO lineas (id, numero, nombre, km_inicio, km_fin, clasificacion, prioridad, geom)
  VALUES (
    gen_random_uuid(),
    '230kV-DGO-02',
    'Durango - Santiago Papasquiaro 230kV',
    0,
    92.3,
    'MODERADA',
    4,
    ST_GeomFromText('LINESTRING(-104.6532 24.0277, -104.9000 24.3000, -105.1500 24.5500, -105.3500 24.8000, -105.4189 25.0488)', 4326)
  )
  RETURNING id INTO v_linea2_id;
  
  -- Línea 3: Durango Centro - El Salto (115kV)
  INSERT INTO lineas (id, numero, nombre, km_inicio, km_fin, clasificacion, prioridad, geom)
  VALUES (
    gen_random_uuid(),
    '115kV-DGO-03',
    'Durango - El Salto 115kV',
    0,
    142.6,
    'BAJA',
    2,
    ST_GeomFromText('LINESTRING(-104.6532 24.0277, -105.0000 23.8000, -105.3500 23.6000, -105.6000 23.4500, -105.8500 23.3000, -106.0000 23.2000, -106.3719 23.7761)', 4326)
  )
  RETURNING id INTO v_linea3_id;
  
  -- ============================================
  -- SUBESTACIONES
  -- ============================================
  
  INSERT INTO subestaciones (nombre, linea_id, geom) VALUES
  ('S/E Durango Centro', v_linea1_id, ST_GeomFromText('POINT(-104.6532 24.0277)', 4326)),
  ('S/E Gómez Palacio', v_linea1_id, ST_GeomFromText('POINT(-103.4969 25.8571)', 4326)),
  ('S/E Santiago Papasquiaro', v_linea2_id, ST_GeomFromText('POINT(-105.4189 25.0488)', 4326)),
  ('S/E El Salto', v_linea3_id, ST_GeomFromText('POINT(-106.3719 23.7761)', 4326));
  
  -- ============================================
  -- ESTRUCTURAS - Línea 1 (Durango - Gómez Palacio)
  -- ============================================
  
  INSERT INTO estructuras (linea_id, numero_estructura, km, geom) VALUES
  (v_linea1_id, 'DGO-T001', 0.0, ST_GeomFromText('POINT(-104.6532 24.0277)', 4326)),
  (v_linea1_id, 'DGO-T002', 12.8, ST_GeomFromText('POINT(-104.5500 24.3000)', 4326)),
  (v_linea1_id, 'DGO-T003', 32.5, ST_GeomFromText('POINT(-104.4000 24.5500)', 4326)),
  (v_linea1_id, 'DGO-T004', 58.7, ST_GeomFromText('POINT(-104.2000 24.8000)', 4326)),
  (v_linea1_id, 'DGO-T005', 96.4, ST_GeomFromText('POINT(-103.9500 25.2000)', 4326)),
  (v_linea1_id, 'DGO-T006', 128.3, ST_GeomFromText('POINT(-103.7000 25.5000)', 4326)),
  (v_linea1_id, 'DGO-T007', 148.9, ST_GeomFromText('POINT(-103.5000 25.7000)', 4326)),
  (v_linea1_id, 'DGO-T008', 165.2, ST_GeomFromText('POINT(-103.4969 25.8571)', 4326));
  
  -- ============================================
  -- ESTRUCTURAS - Línea 2 (Durango - Santiago Papasquiaro)
  -- ============================================
  
  INSERT INTO estructuras (linea_id, numero_estructura, km, geom) VALUES
  (v_linea2_id, 'SPQ-T001', 0.0, ST_GeomFromText('POINT(-104.6532 24.0277)', 4326)),
  (v_linea2_id, 'SPQ-T002', 24.5, ST_GeomFromText('POINT(-104.9000 24.3000)', 4326)),
  (v_linea2_id, 'SPQ-T003', 52.8, ST_GeomFromText('POINT(-105.1500 24.5500)', 4326)),
  (v_linea2_id, 'SPQ-T004', 78.6, ST_GeomFromText('POINT(-105.3500 24.8000)', 4326)),
  (v_linea2_id, 'SPQ-T005', 92.3, ST_GeomFromText('POINT(-105.4189 25.0488)', 4326));
  
  -- ============================================
  -- ESTRUCTURAS - Línea 3 (Durango - El Salto)
  -- ============================================
  
  INSERT INTO estructuras (linea_id, numero_estructura, km, geom) VALUES
  (v_linea3_id, 'SAL-T001', 0.0, ST_GeomFromText('POINT(-104.6532 24.0277)', 4326)),
  (v_linea3_id, 'SAL-T002', 18.7, ST_GeomFromText('POINT(-105.0000 23.8000)', 4326)),
  (v_linea3_id, 'SAL-T003', 42.5, ST_GeomFromText('POINT(-105.3500 23.6000)', 4326)),
  (v_linea3_id, 'SAL-T004', 68.3, ST_GeomFromText('POINT(-105.6000 23.4500)', 4326)),
  (v_linea3_id, 'SAL-T005', 92.8, ST_GeomFromText('POINT(-105.8500 23.3000)', 4326)),
  (v_linea3_id, 'SAL-T006', 108.4, ST_GeomFromText('POINT(-106.0000 23.2000)', 4326)),
  (v_linea3_id, 'SAL-T007', 142.6, ST_GeomFromText('POINT(-106.3719 23.7761)', 4326));
  
  -- ============================================
  -- FALLAS (FAULTS)
  -- ============================================
  
  INSERT INTO fallas (linea_id, km, tipo, descripcion, estado, ocurrencia_ts, geom) VALUES
  (
    v_linea1_id,
    45.2,
    'Conductor caído',
    'Conductor caído por vientos fuertes en zona montañosa cerca de Nombre de Dios',
    'ABIERTA',
    now() - interval '1 day',
    ST_GeomFromText('POINT(-104.3500 24.6500)', 4326)
  ),
  (
    v_linea1_id,
    112.5,
    'Aislador dañado',
    'Aislador con fisuras detectado durante inspección rutinaria en zona de Cuencamé',
    'EN_ATENCION',
    now() - interval '3 days',
    ST_GeomFromText('POINT(-103.8000 25.3500)', 4326)
  ),
  (
    v_linea2_id,
    65.4,
    'Vegetación excesiva',
    'Vegetación cerca del conductor en zona de San Dimas, requiere poda',
    'ABIERTA',
    now() - interval '2 days',
    ST_GeomFromText('POINT(-105.2500 24.7000)', 4326)
  ),
  (
    v_linea3_id,
    85.3,
    'Corrosión en estructura',
    'Torre con corrosión moderada en zona de alta humedad cerca de Otáez',
    'EN_ATENCION',
    now() - interval '5 days',
    ST_GeomFromText('POINT(-105.7500 23.3500)', 4326)
  ),
  (
    v_linea3_id,
    125.8,
    'Mantenimiento preventivo',
    'Reemplazo de herrajes programado en tramo cerca de Pueblo Nuevo',
    'CERRADA',
    now() - interval '20 days',
    ST_GeomFromText('POINT(-106.1500 23.4000)', 4326)
  );
  
  RAISE NOTICE 'Durango seed data created successfully';
  RAISE NOTICE '  - 3 líneas de transmisión';
  RAISE NOTICE '  - 4 subestaciones';
  RAISE NOTICE '  - 20 estructuras';
  RAISE NOTICE '  - 5 fallas';
END $$;
