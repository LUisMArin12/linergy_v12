/*
  # Add Full Public Access Policies
  
  This migration adds UPDATE and DELETE policies for all tables to allow
  anonymous and authenticated users to fully manage records.
  
  ## Changes
  
  1. **lineas** table
     - Add UPDATE policy for anon/authenticated users
     - Add DELETE policy for anon/authenticated users
  
  2. **estructuras** table
     - Add INSERT policy for anon/authenticated users
     - Add UPDATE policy for anon/authenticated users
     - Add DELETE policy for anon/authenticated users
  
  3. **subestaciones** table
     - Add INSERT policy for anon/authenticated users
     - Add UPDATE policy for anon/authenticated users
     - Add DELETE policy for anon/authenticated users
  
  4. **fallas** table
     - Add UPDATE policy for anon/authenticated users
     - Add DELETE policy for anon/authenticated users
  
  5. **linea_tramos** table
     - Add INSERT policy for anon/authenticated users
     - Add UPDATE policy for anon/authenticated users
     - Add DELETE policy for anon/authenticated users
  
  ## Security Notes
  
  - All policies use `true` qualifier for development purposes
  - In production, these should be restricted based on authentication and ownership
*/

-- Policies for lineas
DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies 
    WHERE tablename = 'lineas' 
    AND policyname = 'Public can update lineas'
  ) THEN
    CREATE POLICY "Public can update lineas"
      ON lineas FOR UPDATE
      TO anon, authenticated
      USING (true)
      WITH CHECK (true);
  END IF;
END $$;

DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies 
    WHERE tablename = 'lineas' 
    AND policyname = 'Public can delete lineas'
  ) THEN
    CREATE POLICY "Public can delete lineas"
      ON lineas FOR DELETE
      TO anon, authenticated
      USING (true);
  END IF;
END $$;

DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies 
    WHERE tablename = 'lineas' 
    AND policyname = 'Public can insert lineas'
  ) THEN
    CREATE POLICY "Public can insert lineas"
      ON lineas FOR INSERT
      TO anon, authenticated
      WITH CHECK (true);
  END IF;
END $$;

-- Policies for estructuras
DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies 
    WHERE tablename = 'estructuras' 
    AND policyname = 'Public can insert estructuras'
  ) THEN
    CREATE POLICY "Public can insert estructuras"
      ON estructuras FOR INSERT
      TO anon, authenticated
      WITH CHECK (true);
  END IF;
END $$;

DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies 
    WHERE tablename = 'estructuras' 
    AND policyname = 'Public can update estructuras'
  ) THEN
    CREATE POLICY "Public can update estructuras"
      ON estructuras FOR UPDATE
      TO anon, authenticated
      USING (true)
      WITH CHECK (true);
  END IF;
END $$;

DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies 
    WHERE tablename = 'estructuras' 
    AND policyname = 'Public can delete estructuras'
  ) THEN
    CREATE POLICY "Public can delete estructuras"
      ON estructuras FOR DELETE
      TO anon, authenticated
      USING (true);
  END IF;
END $$;

-- Policies for subestaciones
DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies 
    WHERE tablename = 'subestaciones' 
    AND policyname = 'Public can insert subestaciones'
  ) THEN
    CREATE POLICY "Public can insert subestaciones"
      ON subestaciones FOR INSERT
      TO anon, authenticated
      WITH CHECK (true);
  END IF;
END $$;

DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies 
    WHERE tablename = 'subestaciones' 
    AND policyname = 'Public can update subestaciones'
  ) THEN
    CREATE POLICY "Public can update subestaciones"
      ON subestaciones FOR UPDATE
      TO anon, authenticated
      USING (true)
      WITH CHECK (true);
  END IF;
END $$;

DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies 
    WHERE tablename = 'subestaciones' 
    AND policyname = 'Public can delete subestaciones'
  ) THEN
    CREATE POLICY "Public can delete subestaciones"
      ON subestaciones FOR DELETE
      TO anon, authenticated
      USING (true);
  END IF;
END $$;

-- Policies for fallas
DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies 
    WHERE tablename = 'fallas' 
    AND policyname = 'Public can update fallas'
  ) THEN
    CREATE POLICY "Public can update fallas"
      ON fallas FOR UPDATE
      TO anon, authenticated
      USING (true)
      WITH CHECK (true);
  END IF;
END $$;

DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies 
    WHERE tablename = 'fallas' 
    AND policyname = 'Public can delete fallas'
  ) THEN
    CREATE POLICY "Public can delete fallas"
      ON fallas FOR DELETE
      TO anon, authenticated
      USING (true);
  END IF;
END $$;

-- Policies for linea_tramos
DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies 
    WHERE tablename = 'linea_tramos' 
    AND policyname = 'Public can insert linea_tramos'
  ) THEN
    CREATE POLICY "Public can insert linea_tramos"
      ON linea_tramos FOR INSERT
      TO anon, authenticated
      WITH CHECK (true);
  END IF;
END $$;

DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies 
    WHERE tablename = 'linea_tramos' 
    AND policyname = 'Public can update linea_tramos'
  ) THEN
    CREATE POLICY "Public can update linea_tramos"
      ON linea_tramos FOR UPDATE
      TO anon, authenticated
      USING (true)
      WITH CHECK (true);
  END IF;
END $$;

DO $$ 
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_policies 
    WHERE tablename = 'linea_tramos' 
    AND policyname = 'Public can delete linea_tramos'
  ) THEN
    CREATE POLICY "Public can delete linea_tramos"
      ON linea_tramos FOR DELETE
      TO anon, authenticated
      USING (true);
  END IF;
END $$;
