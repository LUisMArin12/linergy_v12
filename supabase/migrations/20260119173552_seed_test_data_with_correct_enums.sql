/*
  # Datos de Prueba para LINERGY

  1. Nuevos Datos
    - 3 Líneas de subtransmisión con geometría realista:
      * 230kV-DGO-01 (ALTA prioridad, color magenta)
      * 230kV-DGO-02 (MODERADA prioridad, color magenta)
      * 115kV-DGO-03 (BAJA prioridad, color magenta)
    
    - Estructuras asociadas (5-10 por línea) con:
      * Número de estructura
      * Kilómetro
      * Coordenadas geográficas
    
    - Fallas de ejemplo (2-5) con distintos estados y tipos
  
  2. Notas
    - Estos datos simulan líneas de subtransmisión en Durango
    - Las coordenadas son aproximadas y representativas
    - Se incluyen diferentes tipos de fallas para demostración
*/

-- Insertar líneas de subtransmisión
INSERT INTO lineas (numero, nombre, km_inicio, km_fin, clasificacion, prioridad, geom) VALUES
(
  '230kV-DGO-01',
  'Durango - Nombre de Dios',
  0,
  15,
  'ALTA',
  5,
  ST_GeomFromText('LINESTRING(-104.6589 24.0277, -104.6450 24.0350, -104.6320 24.0420, -104.6180 24.0500, -104.6040 24.0580, -104.5900 24.0660, -104.5760 24.0740)', 4326)
),
(
  '230kV-DGO-02',
  'Durango - Mezquital',
  0,
  15,
  'MODERADA',
  4,
  ST_GeomFromText('LINESTRING(-104.6700 24.0200, -104.6850 24.0100, -104.7000 24.0000, -104.7150 23.9900, -104.7300 23.9800, -104.7450 23.9700)', 4326)
),
(
  '115kV-DGO-03',
  'Durango - Santiago Papasquiaro',
  0,
  8,
  'BAJA',
  2,
  ST_GeomFromText('LINESTRING(-104.6600 24.0300, -104.6500 24.0450, -104.6400 24.0600, -104.6300 24.0750, -104.6200 24.0900)', 4326)
);

-- Obtener IDs de las líneas recién insertadas
DO $$
DECLARE
  linea1_id uuid;
  linea2_id uuid;
  linea3_id uuid;
BEGIN
  -- Obtener IDs
  SELECT id INTO linea1_id FROM lineas WHERE numero = '230kV-DGO-01';
  SELECT id INTO linea2_id FROM lineas WHERE numero = '230kV-DGO-02';
  SELECT id INTO linea3_id FROM lineas WHERE numero = '115kV-DGO-03';

  -- Estructuras para 230kV-DGO-01
  INSERT INTO estructuras (linea_id, numero_estructura, km, geom) VALUES
  (linea1_id, 'E-001', 0.0, ST_GeomFromText('POINT(-104.6589 24.0277)', 4326)),
  (linea1_id, 'E-002', 2.5, ST_GeomFromText('POINT(-104.6450 24.0350)', 4326)),
  (linea1_id, 'E-003', 5.0, ST_GeomFromText('POINT(-104.6320 24.0420)', 4326)),
  (linea1_id, 'E-004', 7.5, ST_GeomFromText('POINT(-104.6180 24.0500)', 4326)),
  (linea1_id, 'E-005', 10.0, ST_GeomFromText('POINT(-104.6040 24.0580)', 4326)),
  (linea1_id, 'E-006', 12.5, ST_GeomFromText('POINT(-104.5900 24.0660)', 4326)),
  (linea1_id, 'E-007', 15.0, ST_GeomFromText('POINT(-104.5760 24.0740)', 4326));

  -- Estructuras para 230kV-DGO-02
  INSERT INTO estructuras (linea_id, numero_estructura, km, geom) VALUES
  (linea2_id, 'E-101', 0.0, ST_GeomFromText('POINT(-104.6700 24.0200)', 4326)),
  (linea2_id, 'E-102', 3.0, ST_GeomFromText('POINT(-104.6850 24.0100)', 4326)),
  (linea2_id, 'E-103', 6.0, ST_GeomFromText('POINT(-104.7000 24.0000)', 4326)),
  (linea2_id, 'E-104', 9.0, ST_GeomFromText('POINT(-104.7150 23.9900)', 4326)),
  (linea2_id, 'E-105', 12.0, ST_GeomFromText('POINT(-104.7300 23.9800)', 4326)),
  (linea2_id, 'E-106', 15.0, ST_GeomFromText('POINT(-104.7450 23.9700)', 4326));

  -- Estructuras para 115kV-DGO-03
  INSERT INTO estructuras (linea_id, numero_estructura, km, geom) VALUES
  (linea3_id, 'E-201', 0.0, ST_GeomFromText('POINT(-104.6600 24.0300)', 4326)),
  (linea3_id, 'E-202', 2.0, ST_GeomFromText('POINT(-104.6500 24.0450)', 4326)),
  (linea3_id, 'E-203', 4.0, ST_GeomFromText('POINT(-104.6400 24.0600)', 4326)),
  (linea3_id, 'E-204', 6.0, ST_GeomFromText('POINT(-104.6300 24.0750)', 4326)),
  (linea3_id, 'E-205', 8.0, ST_GeomFromText('POINT(-104.6200 24.0900)', 4326));

  -- Fallas de ejemplo
  INSERT INTO fallas (linea_id, km, tipo, descripcion, estado, ocurrencia_ts, geom) VALUES
  (linea1_id, 5.2, 'Conductor caído', 'Conductor de fase A caído por vientos fuertes', 'ABIERTA', NOW() - INTERVAL '2 hours', ST_GeomFromText('POINT(-104.6320 24.0420)', 4326)),
  (linea1_id, 10.8, 'Torre inclinada', 'Torre con inclinación de 15 grados', 'EN_ATENCION', NOW() - INTERVAL '1 day', ST_GeomFromText('POINT(-104.6040 24.0580)', 4326)),
  (linea2_id, 3.5, 'Aislador roto', 'Aislador dañado en fase B', 'ABIERTA', NOW() - INTERVAL '3 hours', ST_GeomFromText('POINT(-104.6850 24.0100)', 4326)),
  (linea2_id, 8.0, 'Vegetación cerca', 'Árbol a menos de 2 metros del conductor', 'CERRADA', NOW() - INTERVAL '5 days', ST_GeomFromText('POINT(-104.7100 23.9950)', 4326)),
  (linea3_id, 2.5, 'Corrosión', 'Corrosión severa en torre E-202', 'EN_ATENCION', NOW() - INTERVAL '12 hours', ST_GeomFromText('POINT(-104.6500 24.0450)', 4326));

  -- Subestaciones de ejemplo
  INSERT INTO subestaciones (linea_id, nombre, geom) VALUES
  (linea1_id, 'SE Durango Norte', ST_GeomFromText('POINT(-104.6589 24.0277)', 4326)),
  (linea1_id, 'SE Nombre de Dios', ST_GeomFromText('POINT(-104.5760 24.0740)', 4326)),
  (linea2_id, 'SE Durango Sur', ST_GeomFromText('POINT(-104.6700 24.0200)', 4326)),
  (linea3_id, 'SE Durango Central', ST_GeomFromText('POINT(-104.6600 24.0300)', 4326));

END $$;
