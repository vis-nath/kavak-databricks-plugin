---
name: kavak-query
description: >
  Ejecutar queries SQL contra Databricks o Redshift para datos de Kavak/Kuna.
  Detecta automáticamente la fuente por sintaxis de tabla. Diseñado para uso
  tanto interactivo (usuario) como agéntico (analyst-agent u otros subagentes).
  Triggers: query SQL, errores de Databricks/Redshift, solicitud de datos de cualquier tabla.
---

# kavak-query

## Índice de Decisión (leer siempre primero)

**Paso 1: ¿La fuente viene indicada por el agente que te llama?**
- SÍ (el agente especificó `SOURCE: databricks` o `SOURCE: redshift`) → ir directamente a esa sección
- NO (llamada interactiva del usuario) → Paso 2

**Paso 2: Detectar fuente por sintaxis de tabla en el SQL**

| Patrón detectado en el SQL | Fuente |
|---|---|
| Tabla con formato `palabra.palabra.palabra` (3 partes) | → Databricks |
| Tabla con formato `palabra.palabra` (2 partes) | → Redshift |
| No hay referencia a tabla con punto (ej. `SELECT 1`, tabla simple sin punto) | → Preguntar al usuario |

**Regla clave:** No conviertas ni adaptes la sintaxis. Si el SQL tiene sintaxis de Redshift, ejecútalo en Redshift tal cual. Si tiene sintaxis de Databricks, ejecútalo en Databricks tal cual.

---

## Contexto de Datos (Kavak y Kuna)

Kavak y Kuna comparten la misma infraestructura de datos. Ambas marcas tienen datos en Databricks (catálogo `prd_refined` principalmente) y en Redshift (datos en migración).

**Estado de migración:** Redshift → Databricks en curso. Redshift se usa solo para datos que aún no migraron. No intentes ejecutar en Databricks un query con sintaxis de Redshift ni viceversa.

---

## Ejecución en Databricks

Usa cuando el SQL contiene tablas con formato `catalog.schema.tabla` (3 partes) o cuando `SOURCE: databricks`.

```python
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector import query_databricks, QueryError, AuthRequiredError, ConfigNotFoundError

try:
    df = query_databricks("""
        PEGAR_QUERY_AQUI
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

## Ejecución en Redshift

Usa cuando el SQL contiene tablas con formato `schema.tabla` (2 partes) o cuando `SOURCE: redshift`.

```python
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector import query_redshift, QueryError, AuthRequiredError, ConfigNotFoundError

try:
    df = query_redshift("""
        PEGAR_QUERY_AQUI
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

Usa cuando necesitas combinar una tabla de Databricks (3 partes) con una de Redshift (2 partes).
El agente que llama a este skill puede indicar esto con `SOURCE: cross-join`.

```python
import sys, pathlib, pandas as pd
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector import query_databricks, query_redshift

df_db = query_databricks("""
    SELECT columna_clave, col1, col2
    FROM prd_refined.schema.tabla_databricks
""")

df_rs = query_redshift("""
    SELECT columna_clave, col3
    FROM schema.tabla_redshift
""")

# Join en Python usando la columna clave común
df_result = pd.merge(df_db, df_rs, on="columna_clave", how="left")
print(df_result.to_string())
```

Avisa siempre: _"Join temporal Databricks + Redshift. Cuando la tabla migre a Databricks este paso desaparecerá."_

---

## Para uso desde analyst-agent u otros subagentes

Cuando este skill es invocado por un agente, el agente debe indicar la fuente en su contexto:

```
SOURCE: databricks   → ejecutar en Databricks, no detectar sintaxis
SOURCE: redshift     → ejecutar en Redshift, no detectar sintaxis
SOURCE: cross-join   → patrón de join sección Cross-source
SOURCE: unknown      → aplicar detección por sintaxis
```

El resultado se devuelve como `pandas.DataFrame` en `df`. El agente que llama puede asumir que:
- Si no hay excepción → `df` contiene el resultado
- Si hay `AUTH_ERROR_*` → el agente debe pausar y avisar al usuario
- Si hay `QUERY_ERROR` → el agente debe reportar el error y no reintentar automáticamente
- Si hay `CONFIG_ERROR` → el agente debe invocar `kavak-install`

---

## Manejo de Errores

### `AUTH_ERROR_DATABRICKS`

```python
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector.databricks.auth import get_auth_method
print(get_auth_method())
```
- `token` → skill **`kavak-token-update`**
- `oauth` → ejecutar directamente una query de prueba; el browser se abre automáticamente para re-autenticar:
```python
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector import query_databricks
df = query_databricks('SELECT 1 AS ok')
print('Re-autenticado. OK:', df.iloc[0, 0])
```

### `AUTH_ERROR_REDSHIFT`

Credenciales permanentes. Verifica `~/.kavak_connector/redshift.env`. Si cambiaron → sección Redshift de `kavak-install`.

### `QUERY_ERROR: TABLE_OR_VIEW_NOT_FOUND` en Databricks

No intentes reescribir el query para Redshift. Informa: _"La tabla no existe en Databricks. ¿Está en Redshift? Si es así, proporciona el query con sintaxis `schema.tabla` y lo ejecutaré en Redshift."_

### `QUERY_ERROR: PERMISSION_DENIED`

Informa que se necesita acceso — contactar al equipo de datos.

### `QUERY_ERROR: SYNTAX_ERROR`

Lee el mensaje, identifica el token problemático, ofrece la query corregida manteniendo la fuente original.

### `CONFIG_ERROR`

→ skill **`kavak-install`**
