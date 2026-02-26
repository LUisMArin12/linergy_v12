/*
  # Fix Admin User Management Permissions

  1. Changes
    - Drop and recreate functions with proper permissions
    - Grant necessary access to auth.users schema
    - Ensure SECURITY DEFINER works correctly
    
  2. Security
    - Functions validate admin role before executing
    - Proper error handling for unauthorized access
*/

-- Drop existing functions
DROP FUNCTION IF EXISTS get_all_users_with_profiles();
DROP FUNCTION IF EXISTS update_user_role(uuid, text);

-- Function to get all users with their profiles
CREATE OR REPLACE FUNCTION get_all_users_with_profiles()
RETURNS TABLE (
  id uuid,
  email text,
  role text,
  created_at timestamptz,
  last_sign_in_at timestamptz
) 
SECURITY DEFINER
SET search_path = public, auth
AS $$
BEGIN
  -- Check if the caller is an admin
  IF NOT EXISTS (
    SELECT 1 FROM public.profiles 
    WHERE profiles.id = auth.uid() 
    AND profiles.role = 'admin'
  ) THEN
    RAISE EXCEPTION 'Only admins can view all users';
  END IF;

  RETURN QUERY
  SELECT 
    au.id,
    au.email::text,
    COALESCE(p.role::text, 'user') as role,
    au.created_at,
    au.last_sign_in_at
  FROM auth.users au
  LEFT JOIN public.profiles p ON p.id = au.id
  ORDER BY au.created_at DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to update user role
CREATE OR REPLACE FUNCTION update_user_role(
  user_id uuid,
  new_role text
)
RETURNS void
SECURITY DEFINER
SET search_path = public
AS $$
BEGIN
  -- Check if the caller is an admin
  IF NOT EXISTS (
    SELECT 1 FROM profiles 
    WHERE profiles.id = auth.uid() 
    AND profiles.role = 'admin'
  ) THEN
    RAISE EXCEPTION 'Only admins can update user roles';
  END IF;

  -- Validate role
  IF new_role NOT IN ('admin', 'user') THEN
    RAISE EXCEPTION 'Invalid role. Must be either admin or user';
  END IF;

  -- Prevent admin from removing their own admin role
  IF user_id = auth.uid() AND new_role != 'admin' THEN
    RAISE EXCEPTION 'Cannot remove your own admin privileges';
  END IF;

  -- Update the role
  UPDATE profiles
  SET role = new_role::user_role
  WHERE id = user_id;

  -- If profile doesn't exist, create it
  IF NOT FOUND THEN
    INSERT INTO profiles (id, role)
    VALUES (user_id, new_role::user_role);
  END IF;
END;
$$ LANGUAGE plpgsql;

-- Grant execute permissions to authenticated users
GRANT EXECUTE ON FUNCTION get_all_users_with_profiles() TO authenticated;
GRANT EXECUTE ON FUNCTION update_user_role(uuid, text) TO authenticated;