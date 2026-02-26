/*
  # Add delete_user function for admin user management

  1. New Functions
    - `delete_user`: Admin function to delete a user and their associated data
  
  2. Changes
    - Creates a secure function that only admins can execute
    - Deletes user profile and auth record
    - Cascading deletes will handle related data through foreign keys
  
  3. Security
    - Only accessible to users with admin role in their profile
    - Uses security definer to execute with elevated privileges
*/

-- Function to delete a user (admin only)
CREATE OR REPLACE FUNCTION delete_user(user_id uuid)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  caller_role text;
BEGIN
  -- Check if caller is admin
  SELECT role INTO caller_role
  FROM profiles
  WHERE id = auth.uid();
  
  IF caller_role != 'admin' THEN
    RAISE EXCEPTION 'Only admins can delete users';
  END IF;
  
  -- Delete profile (will cascade to related data if foreign keys are set up)
  DELETE FROM profiles WHERE id = user_id;
  
  -- Delete from auth.users (requires security definer)
  DELETE FROM auth.users WHERE id = user_id;
END;
$$;