/*
  # Delete All Data
  
  1. Actions
    - Delete all records from all tables
    - Respects foreign key constraints by deleting in correct order
    
  2. Tables Affected
    - fallas (deleted first - references lineas)
    - linea_tramos (references lineas)
    - estructuras (references lineas)
    - subestaciones (references lineas)
    - lineas (deleted last - parent table)
    
  Important Notes:
  - This migration permanently deletes all data
  - Schema and tables remain intact
  - Only data is removed
*/

-- Delete all fallas (references lineas)
DELETE FROM fallas;

-- Delete all linea_tramos (references lineas)
DELETE FROM linea_tramos;

-- Delete all estructuras (references lineas)
DELETE FROM estructuras;

-- Delete all subestaciones (references lineas)
DELETE FROM subestaciones;

-- Delete all lineas (parent table)
DELETE FROM lineas;
