---
name: kavak-token-update
description: >
  Actualizar el token de Databricks (rota cada semana) o cambiar el método de auth
  entre token y OAuth. Triggers: "nuevo token", "token expirado", "cambiar auth",
  "cambiar a oauth", "cambiar a token", "dapi", "update token".
---

# kavak-token-update

## Índice de Acciones

| Solicitud | Ir a sección |
|---|---|
| "nuevo token" / token expirado / "dapi..." | → Renovar token |
| "cambiar a OAuth" / "no tengo token" | → Cambiar a OAuth |
| "cambiar a token" / "ya tengo token" | → Cambiar a token |

---

## Renovar Token

Paso 1: Pide al usuario que pegue el nuevo token `dapi...`

Paso 2: Guarda el token:
```python
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector.databricks.auth import save_token
save_token("PEGAR_TOKEN_AQUI")
print("Token guardado.")
```

Paso 3: Verificar:
```python
from kavak_connector import query_databricks
df = query_databricks("SELECT 1 AS ok")
print("Token válido:", df.iloc[0, 0])
```

Si falla: el token puede tener espacios extra o estar incompleto (debe empezar con `dapi`). Pide al usuario que lo copie de nuevo.

---

## Cambiar a OAuth (el usuario ya no tiene acceso a token)

Paso 1 — Cambiar el método de auth a oauth:
```python
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector.databricks.auth import set_auth_method
set_auth_method('oauth')
print("Método cambiado a oauth.")
```

Paso 2 — Ejecutar una query de prueba directamente (esto abre el navegador automáticamente):
```python
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector.databricks.query import query
df = query("SELECT 1 AS ok")
print("Autenticado. Resultado:", df.iloc[0, 0])
```

El navegador se abre solo al ejecutar el Paso 2. El usuario solo necesita iniciar sesión con su cuenta `@kavak.com` en el navegador — no hay ningún comando manual que ejecutar.

Si el proceso tarda más de 60 segundos, aumentar el timeout del Bash tool a 120000ms.

---

## Cambiar a Token (el usuario ahora tiene un token)

```python
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector.databricks.auth import set_auth_method
set_auth_method('token')
print("Método cambiado a token.")
```

Luego ir a la sección **Renovar Token** para guardar el nuevo token.
