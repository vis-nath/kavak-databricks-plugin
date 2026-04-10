---
name: databricks-query
description: >
  Usar cuando el connector de Databricks está instalado y el usuario quiere
  ejecutar una consulta SQL, o cuando aparece cualquier error de Databricks.

  Activar cuando:
  - El usuario quiere datos y ~/projects/databricks_connector existe
  - Aparece DatabricksQueryError (error de consulta, permisos, tabla no encontrada)
  - Aparece AuthRequiredError (tokens expirados — ocurre cada 30-90 días)
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
    print("ERROR: Tokens expirados — ver instrucciones abajo")
except DatabricksQueryError as e:
    print(f"ERROR: {e}")
```

`query()` devuelve un `pandas.DataFrame`. El warehouse arranca automáticamente si estaba apagado (puede tomar 1-2 minutos en la primera consulta del día). **No se abre ningún navegador** durante las consultas normales — el SDK renueva el access token silenciosamente en background.

---

## Cuando aparece `AuthRequiredError` — verificar tokens primero

**NO le digas al usuario que se autentique de inmediato.** Primero verifica:

```bash
python3 ~/projects/databricks_connector/check_session.py
```

### Si el resultado es `Session expired` (exit code 1)

Los tokens expirados (esto ocurre cada 30-90 días, no es frecuente). Dile al usuario:

> "Tus tokens de Databricks expiraron — es normal, pasa cada 1-3 meses.
> Solo necesitas volver a iniciar sesión una vez."

Luego:
> "Voy a ejecutar el re-login. Se abrirá tu navegador predeterminado.
> Inicia sesión con tu correo @kavak.com usando 'Continuar con Google'.
> Después de hacer login, el browser puede cerrarse solo o puedes cerrarlo tú."

Ejecuta:
```bash
python3 ~/projects/databricks_connector/setup_auth.py
```

Resultado esperado:
```
✓ Autenticado como: nombre.apellido@kavak.com
```

Luego volver a intentar la consulta original.

### Si el resultado es `Session valid` (exit code 0)

Los tokens están activos — el error NO es de autenticación. Muestra el error original al usuario:
> "Tus tokens de Databricks están activos. El problema no es de autenticación.
> El error original fue: `[error completo]`
> ¿Quieres que lo investiguemos juntos?"

No sugieras re-autenticación en este caso. El SDK renueva el access token solo — si `check_session.py` dice `Session valid`, los tokens están en cache y el SDK los usa.

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

### "config.json no encontrado" o `RuntimeError` con mensaje de config

El archivo de configuración no existe o le faltan campos. Ejecuta:
```bash
cat ~/.databricks_connector/config.json 2>/dev/null || echo "ARCHIVO NO EXISTE"
```

Si no existe o le faltan campos `host` o `http_path`, usa el skill **`databricks-install`** para reconfigurar.

### Cualquier otro `DatabricksQueryError`

> "La consulta falló con este error: `[mensaje completo]`
> ¿Quieres que lo investiguemos juntos?"

---

## Ciclo de vida de los tokens

| Token | Duración | Qué hace el SDK |
|---|---|---|
| Access token | ~1 hora | Se renueva automáticamente — el usuario nunca lo nota |
| Refresh token | 30-90 días | Persiste en `~/.databricks/token-cache.json` — no hace falta login |
| Ambos expirados | Raro | `AuthRequiredError` → usuario corre `setup_auth.py` una vez |

En uso normal, el usuario nunca verá una ventana de login durante las consultas del día.

---

## Catálogos y tablas

Usa el nombre de tabla exacto que el usuario mencione. Si no lo sabe, sugiere que lo consulte con el administrador de Databricks o con quien le compartió este acceso.

Formato estándar: `catalogo.esquema.tabla` — por ejemplo: `prd_refined.seller_api_global_refined.variant_availability`
