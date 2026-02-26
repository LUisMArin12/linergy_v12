/*
  # Seed Initial Data
  
  1. Creates 3 transmission lines with geographic coordinates in Chile
  2. Creates 15 structures distributed along the lines
  3. Creates 3 faults at specific locations
  4. Only runs if database is empty
*/

DO $$
DECLARE
  v_count int;
  v_linea1_id uuid;
  v_linea2_id uuid;
  v_linea3_id uuid;
  v_est1_id uuid;
  v_est2_id uuid;
BEGIN
  -- Check if data already exists
  SELECT COUNT(*) INTO v_count FROM lineas;
  
  IF v_count = 0 THEN
    -- Insert Línea 1: Santiago - Valparaíso (Alta tensión)
    INSERT INTO lineas (id, numero, nombre, km_inicio, km_fin, clasificacion, prioridad, geom)
    VALUES (
      gen_random_uuid(),
      'L-001',
      'Santiago - Valparaíso 220kV',
      0,
      120,
      'ALTA',
      5,
      ST_GeomFromText('LINESTRING(-70.6693 -33.4489, -70.7500 -33.3500, -70.8500 -33.2000, -71.0000 -33.0500, -71.6180 -33.0472)', 4326)
    )
    RETURNING id INTO v_linea1_id;
    
    -- Insert Línea 2: Concepción - Temuco (Moderada)
    INSERT INTO lineas (id, numero, nombre, km_inicio, km_fin, clasificacion, prioridad, geom)
    VALUES (
      gen_random_uuid(),
      'L-002',
      'Concepción - Temuco 154kV',
      0,
      250,
      'MODERADA',
      3,
      ST_GeomFromText('LINESTRING(-73.0495 -36.8270, -73.1000 -37.0000, -73.1500 -37.5000, -73.0000 -38.0000, -72.5986 -38.7395)', 4326)
    )
    RETURNING id INTO v_linea2_id;
    
    -- Insert Línea 3: Antofagasta - Calama (Baja)
    INSERT INTO lineas (id, numero, nombre, km_inicio, km_fin, clasificacion, prioridad, geom)
    VALUES (
      gen_random_uuid(),
      'L-003',
      'Antofagasta - Calama 110kV',
      0,
      200,
      'BAJA',
      2,
      ST_GeomFromText('LINESTRING(-70.4041 -23.6524, -69.9000 -23.4000, -69.5000 -23.0000, -69.0000 -22.7000, -68.9223 -22.4557)', 4326)
    )
    RETURNING id INTO v_linea3_id;
    
    -- Insert structures for Línea 1 (5 structures)
    INSERT INTO estructuras (linea_id, numero_estructura, km, geom) VALUES
    (v_linea1_id, 'E-001-001', 0, ST_GeomFromText('POINT(-70.6693 -33.4489)', 4326)),
    (v_linea1_id, 'E-001-002', 30, ST_GeomFromText('POINT(-70.7500 -33.3500)', 4326)),
    (v_linea1_id, 'E-001-003', 60, ST_GeomFromText('POINT(-70.8500 -33.2000)', 4326)),
    (v_linea1_id, 'E-001-004', 90, ST_GeomFromText('POINT(-71.0000 -33.0500)', 4326)),
    (v_linea1_id, 'E-001-005', 120, ST_GeomFromText('POINT(-71.6180 -33.0472)', 4326));
    
    -- Insert structures for Línea 2 (5 structures)
    INSERT INTO estructuras (linea_id, numero_estructura, km, geom) VALUES
    (v_linea2_id, 'E-002-001', 0, ST_GeomFromText('POINT(-73.0495 -36.8270)', 4326)),
    (v_linea2_id, 'E-002-002', 62.5, ST_GeomFromText('POINT(-73.1000 -37.0000)', 4326)),
    (v_linea2_id, 'E-002-003', 125, ST_GeomFromText('POINT(-73.1500 -37.5000)', 4326)),
    (v_linea2_id, 'E-002-004', 187.5, ST_GeomFromText('POINT(-73.0000 -38.0000)', 4326)),
    (v_linea2_id, 'E-002-005', 250, ST_GeomFromText('POINT(-72.5986 -38.7395)', 4326));
    
    -- Insert structures for Línea 3 (5 structures)
    INSERT INTO estructuras (linea_id, numero_estructura, km, geom) VALUES
    (v_linea3_id, 'E-003-001', 0, ST_GeomFromText('POINT(-70.4041 -23.6524)', 4326)),
    (v_linea3_id, 'E-003-002', 50, ST_GeomFromText('POINT(-69.9000 -23.4000)', 4326)),
    (v_linea3_id, 'E-003-003', 100, ST_GeomFromText('POINT(-69.5000 -23.0000)', 4326)),
    (v_linea3_id, 'E-003-004', 150, ST_GeomFromText('POINT(-69.0000 -22.7000)', 4326)),
    (v_linea3_id, 'E-003-005', 200, ST_GeomFromText('POINT(-68.9223 -22.4557)', 4326));
    
    -- Insert substations
    INSERT INTO subestaciones (nombre, linea_id, geom) VALUES
    ('Subestación Santiago Centro', v_linea1_id, ST_GeomFromText('POINT(-70.6693 -33.4489)', 4326)),
    ('Subestación Valparaíso', v_linea1_id, ST_GeomFromText('POINT(-71.6180 -33.0472)', 4326)),
    ('Subestación Concepción', v_linea2_id, ST_GeomFromText('POINT(-73.0495 -36.8270)', 4326)),
    ('Subestación Temuco', v_linea2_id, ST_GeomFromText('POINT(-72.5986 -38.7395)', 4326)),
    ('Subestación Antofagasta', v_linea3_id, ST_GeomFromText('POINT(-70.4041 -23.6524)', 4326)),
    ('Subestación Calama', v_linea3_id, ST_GeomFromText('POINT(-68.9223 -22.4557)', 4326));
    
    -- Get structure IDs for fault creation
    SELECT id INTO v_est1_id FROM estructuras WHERE linea_id = v_linea1_id AND km = 30 LIMIT 1;
    SELECT id INTO v_est2_id FROM estructuras WHERE linea_id = v_linea2_id AND km = 125 LIMIT 1;
    
    -- Insert faults (3 faults)
    INSERT INTO fallas (linea_id, km, tipo, descripcion, estado, ocurrencia_ts, geom) VALUES
    (
      v_linea1_id,
      45,
      'Conductor caído',
      'Conductor caído por viento fuerte en zona rural',
      'ABIERTA',
      now() - interval '2 days',
      ST_GeomFromText('POINT(-70.8000 -33.2750)', 4326)
    ),
    (
      v_linea2_id,
      125,
      'Aislador dañado',
      'Aislador con fisuras detectado en inspección',
      'EN_ATENCION',
      now() - interval '5 days',
      ST_GeomFromText('POINT(-73.1500 -37.5000)', 4326)
    ),
    (
      v_linea3_id,
      175,
      'Corrosión avanzada',
      'Estructura con corrosión avanzada requiere reemplazo',
      'CERRADA',
      now() - interval '30 days',
      ST_GeomFromText('POINT(-68.9500 -22.5500)', 4326)
    );
    
    RAISE NOTICE 'Seed data created successfully';
  ELSE
    RAISE NOTICE 'Data already exists, skipping seed';
  END IF;
END $$;
