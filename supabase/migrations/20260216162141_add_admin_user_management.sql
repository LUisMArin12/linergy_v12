/*
  # Admin User Management Functions

  1. New Functions
    - `get_all_users_with_profiles`: Returns all users with their profile information
    - `update_user_role`: Allows admins to update user roles
    
  2. Security
    - Only admins can execute these functions
    - Validates role values before updating
*/

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
AS $$
BEGIN
  -- Check if the caller is an admin
  IF NOT EXISTS (
    SELECT 1 FROM profiles 
    WHERE profiles.id = auth.uid() 
    AND profiles.role = 'admin'
  ) THEN
    RAISE EXCEPTION 'Only admins can view all users';
  END IF;

  RETURN QUERY
  SELECT 
    au.id,
    au.email::text,
    COALESCE(p.role, 'user')::text as role,
    au.created_at,
    au.last_sign_in_at
  FROM auth.users au
  LEFT JOIN profiles p ON p.id = au.id
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