---
name: databricks-query
description: >
  Usar cuando el connector de Databricks está instalado y el usuario quiere
  ejecutar una consulta SQL, o cuando aparece cualquier error de Databricks.

  Activar cuando:
  - El usuario quiere datos y ~/projects/databricks_connector existe
  - Aparece DatabricksQueryError (error de consulta, permisos, tabla no encontrada)
  - Aparece AuthRequiredError (posible sesión expirada)
  - El usuario menciona un error de Databricks de cualquier tipo
---

# Databricks — Consultas y Manejo de Errores

## Patrón estándar para ejecutar consultas

```python
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/databricks_connector'))
from databricks_connector import query, DatabricksQueryError, AuthRequiredError

try:
    df = query("""
        SELECT ...
        FROM prd_refined.schema.tabla
        LIMIT 100
    """)
except AuthRequiredError:
    print("ERROR: Sesión expirada — ver instrucciones abajo")
except DatabricksQueryError as e:
    print(f"ERROR: {e}")
```

`query()` devuelve un `pandas.DataFrame`. El warehouse arranca automáticamente si estaba apagado (puede tomar 1-2 minutos en la primera consulta del día).

---

## Cuando aparece `AuthRequiredError` — verificar sesión primero

**NO le digas al usuario que se autentique de inmediato.** Primero verifica:

```bash
python3 ~/projects/databricks_connector/check_session.py
```

### Si el resultado es `Session expired` (exit code 1)

La sesión sí expiró. Dile al usuario:
> "Tu sesión de Databricks expiró (esto es normal, ocurre cada 8-24 horas).
> Solo necesitas volver a iniciar sesión."

Luego:
> "Voy a abrir una ventana de Chrome en tu pantalla de Windows.
> Cuando aparezca, inicia sesión con tu correo de Kavak (@kavak.com) como lo haces normalmente.
> No tienes que hacer nada más — la sesión se guardará sola cuando el login sea exitoso
> y el Chrome se cerrará automáticamente."

Ejecuta:
```bash
python3 ~/projects/databricks_connector/setup_auth.py
```

Volver a intentar la consulta original.

### Si el resultado es `Session valid` (exit code 0)

La sesión está activa — el error NO es de autenticación. Muestra el error original al usuario:
> "Tu sesión de Databricks está activa. El problema no es de autenticación.
> El error original fue: `[error completo]`
> ¿Quieres que lo investiguemos juntos?"

No sugieras re-autenticación en este caso.

---

## Cuando aparece `DatabricksQueryError`

**NO sugieras re-autenticación.** La sesión es válida — la consulta falló por otra razón.
Lee el mensaje del error y reacciona así:

### "Permission denied" o "PERMISSION_DENIED"

Dile:
> "No tienes permisos para acceder a esa tabla. Tu sesión está activa — esto es un problema de permisos, no de login.
> Para solicitar acceso, abre un ticket en el portal de Data Engineering:
> https://kavak.atlassian.net/servicedesk/customer/portal/79/create/1745
> Indica exactamente la tabla que necesitas: `[nombre.de.la.tabla]`"

### "TABLE_NOT_FOUND" o "Table or view not found"

Dile:
> "La tabla `[nombre]` no existe en Databricks. Verifica:
> 1. El nombre exacto del catálogo, esquema y tabla (respeta mayúsculas/minúsculas).
> 2. Que el catálogo sea correcto: `prd_refined`, `prd_raw`, etc."

### "SYNTAX_ERROR" o "ParseException"

Muestra el error exacto y ofrece corregir la consulta.

### Cualquier otro `DatabricksQueryError`

> "La consulta falló con este error: `[mensaje completo]`
> ¿Quieres que lo investiguemos juntos?"

---

## Catálogos y tablas

Usa el nombre de tabla exacto que el usuario mencione. Si no lo sabe, sugiere que lo consulte con el administrador de Databricks o con quien le compartió este acceso.

Formato estándar: `catalogo.esquema.tabla` — por ejemplo: `mi_catalogo.mi_esquema.mi_tabla`
