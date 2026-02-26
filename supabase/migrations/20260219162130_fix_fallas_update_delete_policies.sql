/*
  # Fix fallas update and delete policies

  1. Changes
    - Drop existing update and delete policies for fallas
    - Create new policies that properly allow authenticated users to update and soft-delete fallas
  
  2. Security
    - Authenticated users can update fallas (including deleted_at field)
    - Maintains existing select policy that filters deleted records
*/

-- Drop existing update and delete policies
DROP POLICY IF EXISTS "Public can update fallas" ON fallas;
DROP POLICY IF EXISTS "Public can delete fallas" ON fallas;

-- Create new update policy for authenticated users
CREATE POLICY "Authenticated users can update fallas"
  ON fallas
  FOR UPDATE
  TO authenticated
  USING (true)
  WITH CHECK (true);

-- Create new delete policy for authenticated users
CREATE POLICY "Authenticated users can delete fallas"
  ON fallas
  FOR DELETE
  TO authenticated
  USING (true);