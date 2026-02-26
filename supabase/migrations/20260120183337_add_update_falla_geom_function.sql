/*
  # Add function to update falla geometry with WKT
  
  This migration creates a helper function to update the geometry of an existing
  falla record using WKT (Well-Known Text) format.
  
  ## Changes
  
  1. New Function
     - `update_falla_geom` - Updates the geometry of a falla record
     - Parameters:
       - p_falla_id: UUID of the falla to update
       - p_geom_wkt: Geometry in WKT format (e.g., "POINT(lon lat)")
     - Returns: void (updates the record in place)
  
  2. Security
     - Grant execute permission to anon, authenticated, and service_role users
*/

-- Create function to update falla geometry with WKT
CREATE OR REPLACE FUNCTION update_falla_geom(
  p_falla_id uuid,
  p_geom_wkt text
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
BEGIN
  UPDATE fallas
  SET geom = ST_GeomFromText(p_geom_wkt, 4326),
      updated_at = now()
  WHERE id = p_falla_id;
END;
$$;

-- Grant execute permission
GRANT EXECUTE ON FUNCTION update_falla_geom(uuid, text) TO anon, authenticated, service_role;
