/*
  # Create reportes table

  1. New Tables
    - `reportes`
      - `id` (uuid, primary key) - Unique identifier for the report
      - `falla_id` (uuid, foreign key) - References the fault this report is about
      - `linea_id` (uuid, foreign key) - References the line
      - `km` (double precision) - Kilometer location
      - `tipo` (text) - Type of fault
      - `descripcion` (text) - Description
      - `estado` (estado_falla enum) - Status of the report
      - `ocurrencia_ts` (timestamptz) - When the fault occurred
      - `geom` (geometry) - Geographic location
      - `created_at` (timestamptz) - When the report was created
      - `updated_at` (timestamptz) - When the report was last updated

  2. Security
    - Enable RLS on `reportes` table
    - Authenticated users can read, insert, update, and delete their reports
    - Service role has full access

  3. Notes
    - Reportes are independent of fallas
    - Deleting a falla does NOT delete its reportes
    - Deleting a reporte does NOT delete the falla
    - Reportes can reference fallas but are not dependent on them
*/

-- Create reportes table
CREATE TABLE IF NOT EXISTS reportes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  falla_id uuid REFERENCES fallas(id) ON DELETE SET NULL,
  linea_id uuid NOT NULL REFERENCES lineas(id) ON DELETE CASCADE,
  km double precision NOT NULL,
  tipo text NOT NULL,
  descripcion text,
  estado estado_falla DEFAULT 'ABIERTA',
  ocurrencia_ts timestamptz DEFAULT now(),
  geom geometry(Point, 4326) NOT NULL,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_reportes_falla_id ON reportes(falla_id);
CREATE INDEX IF NOT EXISTS idx_reportes_linea_id ON reportes(linea_id);
CREATE INDEX IF NOT EXISTS idx_reportes_estado ON reportes(estado);
CREATE INDEX IF NOT EXISTS idx_reportes_ocurrencia_ts ON reportes(ocurrencia_ts);
CREATE INDEX IF NOT EXISTS idx_reportes_geom ON reportes USING GIST(geom);

-- Enable RLS
ALTER TABLE reportes ENABLE ROW LEVEL SECURITY;

-- Create policies for authenticated users
CREATE POLICY "Authenticated users can read reportes"
  ON reportes
  FOR SELECT
  TO authenticated
  USING (true);

CREATE POLICY "Authenticated users can insert reportes"
  ON reportes
  FOR INSERT
  TO authenticated
  WITH CHECK (true);

CREATE POLICY "Authenticated users can update reportes"
  ON reportes
  FOR UPDATE
  TO authenticated
  USING (true)
  WITH CHECK (true);

CREATE POLICY "Authenticated users can delete reportes"
  ON reportes
  FOR DELETE
  TO authenticated
  USING (true);

-- Service role full access
CREATE POLICY "Service role full access on reportes"
  ON reportes
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

-- Create trigger to update updated_at
CREATE OR REPLACE FUNCTION update_reportes_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_reportes_updated_at
  BEFORE UPDATE ON reportes
  FOR EACH ROW
  EXECUTE FUNCTION update_reportes_updated_at();

-- Create function to get reportes as GeoJSON
CREATE OR REPLACE FUNCTION get_reportes_geojson()
RETURNS json
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  RETURN (
    SELECT json_build_object(
      'type', 'FeatureCollection',
      'features', COALESCE(json_agg(
        json_build_object(
          'type', 'Feature',
          'id', r.id,
          'geometry', ST_AsGeoJSON(r.geom)::json,
          'properties', json_build_object(
            'id', r.id,
            'falla_id', r.falla_id,
            'linea_id', r.linea_id,
            'km', r.km,
            'tipo', r.tipo,
            'descripcion', r.descripcion,
            'estado', r.estado,
            'ocurrencia_ts', r.ocurrencia_ts,
            'created_at', r.created_at,
            'updated_at', r.updated_at
          )
        )
      ), '[]'::json)
    )
    FROM reportes r
  );
END;
$$;