/*
  # Create LINERGY Schema
  
  1. New Tables
    - `lineas`: Transmission lines with geometric data
      - `id` (uuid, primary key)
      - `numero` (text, unique, not null) - Line number identifier
      - `nombre` (text, nullable) - Line name
      - `km_inicio` (float, nullable) - Starting kilometer
      - `km_fin` (float, nullable) - Ending kilometer
      - `clasificacion` (enum, default MODERADA) - Risk classification
      - `prioridad` (integer, nullable) - Priority 1-5
      - `geom` (geometry LineString, nullable) - Geographic line
      - `created_at`, `updated_at` (timestamps)
    
    - `estructuras`: Support structures along lines
      - `id` (uuid, primary key)
      - `linea_id` (uuid, foreign key, not null)
      - `numero_estructura` (text, not null) - Structure number
      - `km` (float, not null) - Kilometer position
      - `geom` (geometry Point, not null) - Geographic location
      - `created_at`, `updated_at` (timestamps)
    
    - `subestaciones`: Substations
      - `id` (uuid, primary key)
      - `nombre` (text, not null) - Substation name
      - `linea_id` (uuid, foreign key, nullable)
      - `geom` (geometry Point, not null) - Geographic location
      - `created_at`, `updated_at` (timestamps)
    
    - `fallas`: Faults/incidents on lines
      - `id` (uuid, primary key)
      - `linea_id` (uuid, foreign key, not null)
      - `km` (float, not null) - Kilometer where fault occurred
      - `tipo` (text, not null) - Fault type
      - `descripcion` (text, nullable) - Description
      - `estado` (enum, default ABIERTA) - Status
      - `ocurrencia_ts` (timestamp, default now) - When it occurred
      - `geom` (geometry Point, not null) - Geographic location
      - `created_at`, `updated_at` (timestamps)
  
  2. Security
    - Enable RLS on all tables
    - Public read access for all tables
    - Public write access only for fallas table
    - Admin operations require service role
*/

-- Create enum types
CREATE TYPE clasificacion_linea AS ENUM ('ALTA', 'MODERADA', 'BAJA');
CREATE TYPE estado_falla AS ENUM ('ABIERTA', 'EN_ATENCION', 'CERRADA');

-- Create lineas table
CREATE TABLE IF NOT EXISTS lineas (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  numero text UNIQUE NOT NULL,
  nombre text,
  km_inicio float,
  km_fin float,
  clasificacion clasificacion_linea DEFAULT 'MODERADA',
  prioridad int CHECK (prioridad >= 1 AND prioridad <= 5),
  geom geometry(LineString, 4326),
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- Create estructuras table
CREATE TABLE IF NOT EXISTS estructuras (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  linea_id uuid NOT NULL REFERENCES lineas(id) ON DELETE CASCADE,
  numero_estructura text NOT NULL,
  km float NOT NULL,
  geom geometry(Point, 4326) NOT NULL,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  UNIQUE(linea_id, numero_estructura)
);

-- Create subestaciones table
CREATE TABLE IF NOT EXISTS subestaciones (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  nombre text NOT NULL,
  linea_id uuid REFERENCES lineas(id) ON DELETE SET NULL,
  geom geometry(Point, 4326) NOT NULL,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- Create fallas table
CREATE TABLE IF NOT EXISTS fallas (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  linea_id uuid NOT NULL REFERENCES lineas(id) ON DELETE CASCADE,
  km float NOT NULL,
  tipo text NOT NULL,
  descripcion text,
  estado estado_falla DEFAULT 'ABIERTA',
  ocurrencia_ts timestamptz DEFAULT now(),
  geom geometry(Point, 4326) NOT NULL,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_lineas_numero ON lineas(numero);
CREATE INDEX IF NOT EXISTS idx_lineas_geom ON lineas USING GIST(geom);

CREATE INDEX IF NOT EXISTS idx_estructuras_linea ON estructuras(linea_id);
CREATE INDEX IF NOT EXISTS idx_estructuras_linea_km ON estructuras(linea_id, km);
CREATE INDEX IF NOT EXISTS idx_estructuras_geom ON estructuras USING GIST(geom);

CREATE INDEX IF NOT EXISTS idx_subestaciones_linea ON subestaciones(linea_id);
CREATE INDEX IF NOT EXISTS idx_subestaciones_geom ON subestaciones USING GIST(geom);

CREATE INDEX IF NOT EXISTS idx_fallas_linea ON fallas(linea_id);
CREATE INDEX IF NOT EXISTS idx_fallas_estado ON fallas(estado);
CREATE INDEX IF NOT EXISTS idx_fallas_ocurrencia ON fallas(ocurrencia_ts);
CREATE INDEX IF NOT EXISTS idx_fallas_geom ON fallas USING GIST(geom);

-- Create updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply updated_at triggers
CREATE TRIGGER update_lineas_updated_at BEFORE UPDATE ON lineas
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_estructuras_updated_at BEFORE UPDATE ON estructuras
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_subestaciones_updated_at BEFORE UPDATE ON subestaciones
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_fallas_updated_at BEFORE UPDATE ON fallas
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Enable Row Level Security
ALTER TABLE lineas ENABLE ROW LEVEL SECURITY;
ALTER TABLE estructuras ENABLE ROW LEVEL SECURITY;
ALTER TABLE subestaciones ENABLE ROW LEVEL SECURITY;
ALTER TABLE fallas ENABLE ROW LEVEL SECURITY;

-- RLS Policies for lineas
CREATE POLICY "Public read access for lineas"
  ON lineas FOR SELECT
  TO anon, authenticated
  USING (true);

CREATE POLICY "Service role can manage lineas"
  ON lineas FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

-- RLS Policies for estructuras
CREATE POLICY "Public read access for estructuras"
  ON estructuras FOR SELECT
  TO anon, authenticated
  USING (true);

CREATE POLICY "Service role can manage estructuras"
  ON estructuras FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

-- RLS Policies for subestaciones
CREATE POLICY "Public read access for subestaciones"
  ON subestaciones FOR SELECT
  TO anon, authenticated
  USING (true);

CREATE POLICY "Service role can manage subestaciones"
  ON subestaciones FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

-- RLS Policies for fallas
CREATE POLICY "Public read access for fallas"
  ON fallas FOR SELECT
  TO anon, authenticated
  USING (true);

CREATE POLICY "Public can create fallas"
  ON fallas FOR INSERT
  TO anon, authenticated
  WITH CHECK (true);

CREATE POLICY "Service role can manage fallas"
  ON fallas FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);
