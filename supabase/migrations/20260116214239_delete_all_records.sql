/*
  # Eliminar todos los registros

  1. Limpieza de datos
    - Elimina todas las fallas
    - Elimina todas las estructuras
    - Elimina todas las subestaciones
    - Elimina todas las líneas
  
  2. Notas
    - Esta migración limpia completamente la base de datos
    - Los datos no se pueden recuperar después de ejecutar esta migración
    - Las tablas y la estructura de la base de datos permanecen intactas
*/

-- Eliminar todas las fallas primero (tienen FK a líneas)
DELETE FROM fallas;

-- Eliminar todas las estructuras (tienen FK a líneas)
DELETE FROM estructuras;

-- Eliminar todas las subestaciones (tienen FK a líneas)
DELETE FROM subestaciones;

-- Eliminar todas las líneas
DELETE FROM lineas;
