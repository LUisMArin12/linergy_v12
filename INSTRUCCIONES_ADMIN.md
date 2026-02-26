# Instrucciones para Crear el Primer Usuario Administrador

## Paso 1: Crear una cuenta desde la aplicación

1. Ve a la aplicación en tu navegador
2. Haz clic en "Acceder al Sistema"
3. En la página de login, haz clic en "¿No tienes cuenta? Regístrate"
4. Ingresa tu correo electrónico y contraseña
5. Haz clic en "Crear Cuenta"

## Paso 2: Convertir el usuario en administrador desde Supabase

### Opción A: Desde la interfaz de Supabase

1. Ve a tu proyecto en [Supabase Dashboard](https://supabase.com/dashboard)
2. En el menú lateral, ve a **Table Editor**
3. Selecciona la tabla **profiles**
4. Busca tu usuario (por email)
5. Haz clic en el botón de editar (lápiz) en la fila de tu usuario
6. Cambia el campo **role** de `user` a `admin`
7. Guarda los cambios

### Opción B: Desde el SQL Editor

1. Ve a tu proyecto en [Supabase Dashboard](https://supabase.com/dashboard)
2. En el menú lateral, ve a **SQL Editor**
3. Crea una nueva query y ejecuta:

```sql
-- Reemplaza 'tu@email.com' con tu correo electrónico
UPDATE profiles
SET role = 'admin'
WHERE email = 'tu@email.com';
```

4. Ejecuta la query

## Paso 3: Verificar

1. Cierra sesión en la aplicación (si ya habías iniciado sesión)
2. Inicia sesión nuevamente
3. Ahora deberías ver:
   - Las opciones de "Líneas" e "Importar KMZ" en el menú
   - Los botones para registrar, editar y eliminar fallas
   - Acceso completo a todas las funcionalidades de administrador

## Roles del Sistema

### Usuario (user)
- Ver el mapa y todas las líneas
- Ver reportes de fallas
- Ver información de líneas
- **NO puede**: Registrar, editar o eliminar fallas

### Administrador (admin)
- Todos los permisos de usuario
- Registrar nuevas fallas
- Editar fallas existentes
- Cambiar el estado de las fallas
- Eliminar fallas (borrado lógico)
- Gestionar líneas
- Importar archivos KMZ

## Crear Usuarios Adicionales

Para crear más usuarios administradores:

1. Pide a cada usuario que se registre en la aplicación
2. Como administrador, accede a Supabase Dashboard
3. Actualiza el rol de cada usuario en la tabla `profiles` según necesites

## Notas Importantes

- Los nuevos usuarios siempre se crean con rol `user` por defecto
- El cambio de rol debe hacerse desde la base de datos de Supabase
- No hay interfaz en la aplicación para cambiar roles (por seguridad)
- Se recomienda tener pocos usuarios con rol `admin` para mantener el control
