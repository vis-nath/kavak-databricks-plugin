---
name: databricks-query
description: >
  Usar cuando el connector de Databricks está instalado y el usuario quiere
  ejecutar una consulta SQL, o cuando aparece cualquier error de Databricks.

  Activar cuando:
  - El usuario quiere datos y ~/projects/databricks_connector existe
  - Aparece DatabricksQueryError (error de consulta, permisos, tabla no encontrada)
  - Aparece AuthRequiredError (API key expirada ~5 días, o tokens OAuth cada 30-90 días)
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

`query()` devuelve un `pandas.DataFrame`. El warehouse arranca automáticamente si estaba apagado (puede tomar 1-2 minutos en la primera consulta del día).

**Autenticación:** Si `~/.databricks_connector/.env` contiene `DATABRICKS_TOKEN`, se usa API Key — nunca se abre el navegador. Si no hay token configurado, el SDK usa OAuth con tokens cacheados en `~/.databricks/token-cache.json` y renueva el access token silenciosamente en background.

---

## Cuando aparece `AuthRequiredError` — verificar credenciales primero

**NO le digas al usuario que se autentique de inmediato.** Primero verifica:

```bash
python3 ~/projects/databricks_connector/check_session.py
```

### Si el resultado es `Session expired` (exit code 1)

Verifica si el usuario tiene API Key configurada:
```bash
cat ~/.databricks_connector/.env 2>/dev/null || echo "NO_ENV"
```

**Si tiene `.env` con `DATABRICKS_TOKEN`:**

Las API Keys de Databricks de Kavak duran aproximadamente 5 días. Dile al usuario:

> "Tu API Key de Databricks expiró — duran unos 5 días.
> ¿Tienes una nueva? Puedes generarla en el portal de Databricks en Settings → Developer → Access Tokens."

Si el usuario proporciona un nuevo token, actualízalo reemplazando `[TOKEN]`:
```bash
cat > ~/.databricks_connector/.env << 'EOF'
DATABRICKS_TOKEN=[TOKEN]
EOF
chmod 600 ~/.databricks_connector/.env
echo "Token actualizado"
```

Luego volver a intentar la consulta original.

Si el usuario **no tiene** una nueva API Key todavía, ofrece el fallback OAuth:

> "Sin problema — podemos autenticarte con tu cuenta de Google por ahora mientras consigues el token nuevo."

Elimina el `.env` para que el conector use OAuth:
```bash
rm ~/.databricks_connector/.env
```

Luego ejecuta:
```bash
python3 ~/projects/databricks_connector/setup_auth.py
```

**Si NO tiene `.env` (usa OAuth):**
Los tokens OAuth expiraron (ocurre cada 30-90 días). Dile al usuario:

> "Tus tokens de Databricks expiraron — es normal, pasa cada 1-3 meses.
> Solo necesitas volver a iniciar sesión una vez."

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

Las credenciales están activas — el error NO es de autenticación. Muestra el error original al usuario:
> "Tus credenciales de Databricks están activas. El problema no es de autenticación.
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

## Ciclo de vida de las credenciales

### API Key (método recomendado)

| Credencial | Duración típica | Acción cuando expira |
|---|---|---|
| `DATABRICKS_TOKEN` en `.env` | ~5 días (política Kavak) | Pedir nuevo token al usuario → actualizar `.env` |

No hay renovación automática — el token se pasa directamente en cada query. Sin navegador, sin SSO.
Si el usuario no tiene un token nuevo disponible, puede eliminar `.env` y usar OAuth temporalmente.

### OAuth (alternativa)

| Token | Duración | Qué hace el SDK |
|---|---|---|
| Access token | ~1 hora | Se renueva automáticamente — el usuario nunca lo nota |
| Refresh token | 30-90 días | Persiste en `~/.databricks/token-cache.json` — no hace falta login |
| Ambos expirados | Raro | `AuthRequiredError` → usuario corre `setup_auth.py` una vez |

En uso normal con OAuth, el usuario nunca verá una ventana de login durante las consultas del día.

---

## Catálogos y tablas

Usa el nombre de tabla exacto que el usuario mencione. Si no lo sabe, sugiere que lo consulte con el administrador de Databricks o con quien le compartió este acceso.

Formato estándar: `catalogo.esquema.tabla` — por ejemplo: `prd_refined.seller_api_global_refined.variant_availability`
