/*
  # Sistema de Roles y Perfiles de Usuario

  1. Nueva Tabla
    - `profiles`
      - `id` (uuid, primary key, vinculado a auth.users)
      - `email` (text)
      - `role` (enum: 'admin', 'user')
      - `created_at` (timestamp)
      - `updated_at` (timestamp)

  2. Seguridad
    - Habilitar RLS en la tabla `profiles`
    - Política para que usuarios autenticados puedan leer su propio perfil
    - Política para que solo admins puedan actualizar roles

  3. Funciones y Triggers
    - Trigger automático para crear perfil cuando se registra un usuario nuevo
    - Por defecto, los nuevos usuarios tienen rol 'user'
*/

-- Crear enum para roles
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_role') THEN
    CREATE TYPE user_role AS ENUM ('admin', 'user');
  END IF;
END $$;

-- Crear tabla de perfiles
CREATE TABLE IF NOT EXISTS profiles (
  id uuid PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
  email text NOT NULL,
  role user_role NOT NULL DEFAULT 'user',
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- Habilitar RLS
ALTER TABLE profiles ENABLE ROW LEVEL SECURITY;

-- Política: Usuarios pueden leer su propio perfil
CREATE POLICY "Users can read own profile"
  ON profiles
  FOR SELECT
  TO authenticated
  USING (auth.uid() = id);

-- Política: Todos pueden ver todos los perfiles (para verificar roles)
CREATE POLICY "Anyone can read all profiles"
  ON profiles
  FOR SELECT
  TO authenticated
  USING (true);

-- Política: Solo admins pueden actualizar roles
CREATE POLICY "Only admins can update profiles"
  ON profiles
  FOR UPDATE
  TO authenticated
  USING (
    EXISTS (
      SELECT 1 FROM profiles
      WHERE profiles.id = auth.uid()
      AND profiles.role = 'admin'
    )
  );

-- Función para crear perfil automáticamente
CREATE OR REPLACE FUNCTION public.handle_new_user()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER SET search_path = public
AS $$
BEGIN
  INSERT INTO public.profiles (id, email, role)
  VALUES (new.id, new.email, 'user');
  RETURN new;
END;
$$;

-- Trigger para crear perfil al registrar usuario
DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
CREATE TRIGGER on_auth_user_created
  AFTER INSERT ON auth.users
  FOR EACH ROW EXECUTE FUNCTION public.handle_new_user();

-- Crear un usuario admin por defecto (opcional, puedes omitir esto si prefieres crear el admin manualmente)
-- NOTA: Este es solo un ejemplo, deberás crear el usuario real a través de la UI de Supabase
COMMENT ON TABLE profiles IS 'Perfiles de usuario con roles para control de acceso';