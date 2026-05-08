---
name: kavak-query
description: >
  Ejecutar queries contra Databricks (siempre primero) o Redshift (solo fallback de migración).
  También maneja joins entre las dos fuentes vía Python.
  Triggers: query SQL, errores de Databricks/Redshift, solicitud de datos de cualquier tabla.
---

# kavak-query

## Índice de Decisión

| Situación | Acción |
|---|---|
| Tabla en formato `catalog.schema.table` | → Query Databricks |
| Tabla de Redshift o dato en migración | → Intentar Databricks primero, luego Redshift |
| Datos de dos tablas de fuentes distintas | → Cross-source join (sección abajo) |
| Error `AUTH_ERROR_DATABRICKS` | → Sección errores auth Databricks |
| Error `AUTH_ERROR_REDSHIFT` | → Sección errores auth Redshift |
| Error `QUERY_ERROR` | → Sección errores SQL |
| Error `CONFIG_ERROR` | → skill `kavak-install` |

---

## Contexto de Migración (IMPORTANTE)

Kavak está migrando datos de Redshift → Databricks. **Siempre intenta Databricks primero.**
Usa Redshift solo si:
1. La tabla no existe en Databricks (error `TABLE_NOT_FOUND`)
2. El usuario confirma que el dato aún no fue migrado

Cuando uses Redshift, avisa:
> ⚠️ Este dato aún no está en Databricks. Consultando Redshift como fuente temporal.

---

## Query a Databricks (patrón estándar)

```python
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector import query_databricks, QueryError, AuthRequiredError, ConfigNotFoundError

try:
    df = query_databricks("""
        SELECT * FROM prd_refined.schema.tabla LIMIT 1000
    """)
    print(df.to_string())
except AuthRequiredError:
    print("AUTH_ERROR_DATABRICKS")
except QueryError as e:
    print(f"QUERY_ERROR: {e}")
except ConfigNotFoundError as e:
    print(f"CONFIG_ERROR: {e}")
```

---

## Query a Redshift (solo fallback)

```python
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector import query_redshift, QueryError, AuthRequiredError, ConfigNotFoundError

try:
    df = query_redshift("""
        SELECT * FROM schema.tabla LIMIT 1000
    """)
    print(df.to_string())
except AuthRequiredError:
    print("AUTH_ERROR_REDSHIFT")
except QueryError as e:
    print(f"QUERY_ERROR: {e}")
except ConfigNotFoundError as e:
    print(f"CONFIG_ERROR: {e}")
```

---

## Cross-source Join (Databricks + Redshift vía Python)

Cuando una tabla está en Databricks y la otra aún en Redshift:

```python
import sys, pathlib, pandas as pd
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector import query_databricks, query_redshift

# Query 1: fuente Databricks
df_db = query_databricks("""
    SELECT dealer_id, nombre, region
    FROM prd_refined.commercial.dealers
""")

# Query 2: fuente Redshift (dato aún no migrado)
df_rs = query_redshift("""
    SELECT dealer_id, total_ventas
    FROM sales.dealer_summary
    WHERE fecha >= '2026-01-01'
""")

# Join en Python — resultado temporal mientras dure la migración
df_result = pd.merge(df_db, df_rs, on="dealer_id", how="left")
print(df_result.to_string())
```

Avisa al usuario: _"Resultado combinado de Databricks + Redshift. Este join es temporal — cuando la tabla migre completamente a Databricks, este paso ya no será necesario."_

---

## Manejo de Errores

### `AUTH_ERROR_DATABRICKS`

Verifica el método de auth activo:
```python
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector.databricks.auth import get_auth_method
print(get_auth_method())
```
- `token` → usa skill **`kavak-token-update`** para renovar el token
- `oauth` → pide al usuario ejecutar:
  ```bash
  cd ~/projects/kavak_connector && python3 setup_auth.py
  ```

### `AUTH_ERROR_REDSHIFT`

Las credenciales de Redshift son permanentes. Si hay error de auth, verifica `~/.kavak_connector/redshift.env`. Si cambiaron, ejecuta la sección Redshift de `kavak-install`.

### `QUERY_ERROR: TABLE_NOT_FOUND` en Databricks

Confirma con el usuario si el dato podría estar en Redshift (migración pendiente). Si sí → intenta el mismo query en Redshift con formato `schema.tabla`.

### `QUERY_ERROR: PERMISSION_DENIED`

Informa al usuario que no tiene acceso a esa tabla y que debe solicitarlo al equipo de datos.

### `QUERY_ERROR: SYNTAX_ERROR`

Lee el mensaje de error, identifica el token problemático y ofrece la query corregida.

### `CONFIG_ERROR`

→ skill **`kavak-install`**
