/*
  # KMZ Import Support - Line Segments and Geometry Reconstruction

  1. New Tables
    - `linea_tramos`: Individual LineString segments that compose a line
      - `id` (uuid, primary key)
      - `linea_id` (uuid, foreign key to lineas)
      - `tramo_codigo` (text, nullable) - Optional segment code from KML
      - `orden` (integer) - Order of the segment in the line
      - `geom` (geometry LineString) - Geographic segment
      - `created_at`, `updated_at` (timestamps)
      - Unique constraint on (linea_id, orden)

  2. Functions
    - `rebuild_linea_geom_from_tramos(p_linea_id uuid)`: Reconstructs the full line geometry from segments
    - `compute_estructuras_km_from_linea(p_linea_id uuid)`: Calculates km position for all structures on a line
    - `finalize_kmz_import_for_linea(p_linea_id uuid)`: Completes import by rebuilding geometry and computing km values

  3. Security
    - Enable RLS on linea_tramos
    - Public read access (anon, authenticated)
    - Full access for service_role only

  4. Indexes
    - B-tree index on linea_id
    - GIST index on geom for spatial queries

  Important Notes:
  - This migration supports KMZ imports where lines are composed of multiple LineString segments
  - After importing segments and structures, call `finalize_kmz_import_for_linea(linea_id)` to complete the process
  - The finalize function will:
    1. Merge all segments into a single line geometry
    2. Set km_inicio = 0 and km_fin = total length in kilometers
    3. Calculate km position for each structure based on its location along the line
*/

-- Create linea_tramos table to store individual segments
CREATE TABLE IF NOT EXISTS linea_tramos (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  linea_id uuid NOT NULL REFERENCES lineas(id) ON DELETE CASCADE,
  tramo_codigo text,
  orden int NOT NULL,
  geom geometry(LineString, 4326) NOT NULL,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  CONSTRAINT unique_linea_orden UNIQUE(linea_id, orden)
);

-- Create indexes for linea_tramos
CREATE INDEX IF NOT EXISTS idx_linea_tramos_linea ON linea_tramos(linea_id);
CREATE INDEX IF NOT EXISTS idx_linea_tramos_geom ON linea_tramos USING GIST(geom);

-- Apply updated_at trigger to linea_tramos
CREATE TRIGGER update_linea_tramos_updated_at BEFORE UPDATE ON linea_tramos
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Enable Row Level Security
ALTER TABLE linea_tramos ENABLE ROW LEVEL SECURITY;

-- RLS Policies for linea_tramos
CREATE POLICY "Public read access for linea_tramos"
  ON linea_tramos FOR SELECT
  TO anon, authenticated
  USING (true);

CREATE POLICY "Service role can manage linea_tramos"
  ON linea_tramos FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);

-- Function to rebuild line geometry from segments
CREATE OR REPLACE FUNCTION rebuild_linea_geom_from_tramos(p_linea_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_merged_geom geometry;
  v_length_km float;
  v_tramo_count int;
BEGIN
  -- Check if there are segments for this line
  SELECT COUNT(*) INTO v_tramo_count
  FROM linea_tramos
  WHERE linea_id = p_linea_id;

  IF v_tramo_count = 0 THEN
    RAISE EXCEPTION 'No segments (tramos) found for linea_id %', p_linea_id;
  END IF;

  -- Collect all segments ordered by 'orden' field and merge them
  -- ST_Collect creates a MultiLineString, ST_LineMerge attempts to merge connected segments
  SELECT ST_LineMerge(ST_Collect(geom ORDER BY orden))
  INTO v_merged_geom
  FROM linea_tramos
  WHERE linea_id = p_linea_id;

  -- If ST_LineMerge returns a MultiLineString, we still have disconnected segments
  -- In this case, we'll use the first linestring or force conversion
  -- For most cases, ST_LineMerge should work if segments are properly connected
  IF ST_GeometryType(v_merged_geom) = 'ST_MultiLineString' THEN
    -- Try to get the longest component or just the first one
    v_merged_geom := ST_GeometryN(v_merged_geom, 1);
  END IF;

  -- Ensure we have a LineString
  IF ST_GeometryType(v_merged_geom) != 'ST_LineString' THEN
    RAISE EXCEPTION 'Failed to merge segments into a single LineString for linea_id %. Result type: %',
      p_linea_id, ST_GeometryType(v_merged_geom);
  END IF;

  -- Calculate length in kilometers
  v_length_km := ST_Length(v_merged_geom::geography) / 1000.0;

  -- Update the lineas table
  UPDATE lineas
  SET
    geom = v_merged_geom,
    km_inicio = 0,
    km_fin = v_length_km,
    updated_at = now()
  WHERE id = p_linea_id;

  RAISE NOTICE 'Line geometry rebuilt for linea_id %. Length: % km', p_linea_id, v_length_km;
END;
$$;

-- Function to compute km values for all estructuras on a line
CREATE OR REPLACE FUNCTION compute_estructuras_km_from_linea(p_linea_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  v_linea_geom geometry;
  v_estructura_record record;
  v_fraction float;
  v_km_value float;
  v_total_length_m float;
  v_updated_count int := 0;
BEGIN
  -- Get the line geometry
  SELECT geom INTO v_linea_geom
  FROM lineas
  WHERE id = p_linea_id;

  IF v_linea_geom IS NULL THEN
    RAISE EXCEPTION 'Line geometry is null for linea_id %. Run rebuild_linea_geom_from_tramos first.', p_linea_id;
  END IF;

  -- Get total length in meters
  v_total_length_m := ST_Length(v_linea_geom::geography);

  -- Loop through all estructuras for this line
  FOR v_estructura_record IN
    SELECT id, geom, numero_estructura
    FROM estructuras
    WHERE linea_id = p_linea_id
  LOOP
    -- Calculate the fraction (0.0 to 1.0) along the line where the structure is located
    v_fraction := ST_LineLocatePoint(v_linea_geom, v_estructura_record.geom);

    -- Calculate km value
    v_km_value := (v_fraction * v_total_length_m) / 1000.0;

    -- Update the estructura
    UPDATE estructuras
    SET
      km = v_km_value,
      updated_at = now()
    WHERE id = v_estructura_record.id;

    v_updated_count := v_updated_count + 1;
  END LOOP;

  RAISE NOTICE 'Updated km values for % estructuras on linea_id %', v_updated_count, p_linea_id;
END;
$$;

-- Main function to finalize KMZ import for a line
CREATE OR REPLACE FUNCTION finalize_kmz_import_for_linea(p_linea_id uuid)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  -- Step 1: Rebuild line geometry from segments
  PERFORM rebuild_linea_geom_from_tramos(p_linea_id);

  -- Step 2: Compute km values for all estructuras
  PERFORM compute_estructuras_km_from_linea(p_linea_id);

  RAISE NOTICE 'KMZ import finalized successfully for linea_id %', p_linea_id;
END;
$$;