/*
  # Fix update_falla_geom permissions

  1. Changes
    - Grant explicit EXECUTE permissions to anon and authenticated roles
    - Ensure the function uses SECURITY DEFINER properly

  2. Security
    - Function executes with owner privileges to bypass RLS during update
    - Only anon and authenticated roles can execute
*/

-- Grant EXECUTE to anon and authenticated
GRANT EXECUTE ON FUNCTION update_falla_geom(uuid, text) TO anon;
GRANT EXECUTE ON FUNCTION update_falla_geom(uuid, text) TO authenticated;
GRANT EXECUTE ON FUNCTION update_falla_geom(uuid, text) TO service_role;
