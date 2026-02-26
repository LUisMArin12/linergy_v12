/*
  # Simplify fallas deletion

  1. Changes
    - Drop all existing policies for fallas table
    - Create simple policies that allow authenticated users to perform all operations
    - Remove soft delete complexity
  
  2. Security
    - Authenticated users can read all non-deleted fallas
    - Authenticated users can insert, update, and delete fallas
    - Service role maintains full access
*/

-- Drop all existing policies
DROP POLICY IF EXISTS "Public read access for fallas" ON fallas;
DROP POLICY IF EXISTS "Public can create fallas" ON fallas;
DROP POLICY IF EXISTS "Authenticated users can update fallas" ON fallas;
DROP POLICY IF EXISTS "Authenticated users can delete fallas" ON fallas;
DROP POLICY IF EXISTS "Service role can manage fallas" ON fallas;

-- Create simple, permissive policies for authenticated users
CREATE POLICY "Authenticated users can read fallas"
  ON fallas
  FOR SELECT
  TO authenticated
  USING (deleted_at IS NULL);

CREATE POLICY "Authenticated users can insert fallas"
  ON fallas
  FOR INSERT
  TO authenticated
  WITH CHECK (true);

CREATE POLICY "Authenticated users can update fallas"
  ON fallas
  FOR UPDATE
  TO authenticated
  USING (true)
  WITH CHECK (true);

CREATE POLICY "Authenticated users can delete fallas"
  ON fallas
  FOR DELETE
  TO authenticated
  USING (true);

-- Service role full access
CREATE POLICY "Service role full access"
  ON fallas
  FOR ALL
  TO service_role
  USING (true)
  WITH CHECK (true);