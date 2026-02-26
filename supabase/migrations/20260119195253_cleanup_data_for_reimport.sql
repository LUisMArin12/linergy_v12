/*
  # Cleanup Data for Reimport
  
  1. Actions
    - Delete all existing data to prepare for fresh import
    
  2. Tables Affected
    - All data tables will be cleared
    
  Important Notes:
  - This clears all data to enable clean re-import of test data
  - Schema remains intact
*/

DELETE FROM fallas;
DELETE FROM linea_tramos;
DELETE FROM estructuras;
DELETE FROM subestaciones;
DELETE FROM lineas;
