---
name: kavak-install
description: >
  Instalar el conector kavak_connector o cambiar el método de autenticación de Databricks.
  Triggers: conector no encontrado, "instalar", "configurar", "cambiar auth", "nueva versión",
  "actualizar", "cambiar a oauth", "cambiar a token".
---

# kavak-install

## Índice de Acciones

| Situación | Ir a sección |
|---|---|
| Primera instalación | Paso Común → luego preguntar A o B |
| Actualizar conector (nueva versión) | Paso Común (git pull) |
| Cambiar de token → OAuth | Sección: Cambiar método de auth |
| Cambiar de OAuth → token | Sección: Cambiar método de auth |
| Configurar Redshift | Sección: Redshift |

---

## Paso Común (siempre ejecutar primero)

```bash
# Si es primera vez:
git clone git@github.com:vis-nath/kvk-connector.git ~/projects/kavak_connector

# Si ya existe (actualizar):
# cd ~/projects/kavak_connector && git pull

cd ~/projects/kavak_connector
pip install -r requirements.txt --break-system-packages
mkdir -p ~/.kavak_connector && chmod 700 ~/.kavak_connector
mkdir -p ~/.kavak_connector/cache
mkdir -p ~/.kavak_connector/agent_memory
```

Luego **configurar CLAUDE.md** para que Claude auto-invoque el plugin al detectar solicitudes de datos:

```python
import pathlib, re

claude_md = pathlib.Path.home() / ".claude" / "CLAUDE.md"
rule = (
    "- **Datos de Kavak o Kuna:** Si el usuario solicita datos de Kavak o Kuna "
    "(EaaS, reservas, entregas, STR, inventario, PIX, funnel, leads, afiliación, "
    "comisiones, Nicole, etc.), invoca SIEMPRE el skill `kavak-index` antes de "
    "cualquier otra acción. Aplica sin importar si el usuario mencione Databricks, "
    "Redshift o ninguno explícitamente."
)

content = claude_md.read_text() if claude_md.exists() else ""

# Reemplazar regla vieja de kavak-index si existe, o agregar al final
old_pattern = re.compile(r"- \*\*Datos de Kavak.*?kavak-index.*?\n", re.DOTALL)
if old_pattern.search(content):
    content = old_pattern.sub(rule + "\n", content)
    action = "actualizada"
else:
    content = content.rstrip() + "\n" + rule + "\n"
    action = "agregada"

claude_md.write_text(content)
print(f"Regla kavak-index {action} en ~/.claude/CLAUDE.md")
```

## Pregunta clave: ¿tienes acceso a un token `dapi...` de Databricks?

No todos los usuarios tienen acceso a un token de API. Pregunta al usuario antes de elegir el método.

- **Sí** → Método A (API Key)
- **No** → Método B (OAuth con Google SSO de Kavak)

---

## Método A: API Key

Pide al usuario: host, http_path, y el token `dapi...`.

```bash
# Crear databricks.json con auth_method=token
python3 -c "
import json, pathlib, os
config = {
    'host': 'PEGAR_HOST',
    'http_path': 'PEGAR_HTTP_PATH',
    'auth_method': 'token'
}
p = pathlib.Path.home() / '.kavak_connector/databricks.json'
p.write_text(json.dumps(config, indent=2))
os.chmod(p, 0o600)
print('databricks.json creado')
"

# Guardar el token
python3 -c "
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector.databricks.auth import save_token
save_token('PEGAR_TOKEN_DAPI')
print('Token guardado')
"
```

Verificar:
```bash
cd ~/projects/kavak_connector
python3 -c "
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector import query_databricks
df = query_databricks('SELECT 1 AS ok')
print('Databricks OK:', df.iloc[0,0])
"
```

---

## Método B: OAuth (Google SSO)

Pide al usuario: host y http_path. No necesita token.

```bash
python3 -c "
import json, pathlib, os
config = {
    'host': 'PEGAR_HOST',
    'http_path': 'PEGAR_HTTP_PATH',
    'auth_method': 'oauth'
}
p = pathlib.Path.home() / '.kavak_connector/databricks.json'
p.write_text(json.dumps(config, indent=2))
os.chmod(p, 0o600)
print('databricks.json creado (oauth)')
"
cd ~/projects/kavak_connector && python3 setup_auth.py
```

El browser abre una vez con cuenta `@kavak.com`. Los tokens se guardan automáticamente en `~/.databricks/token-cache.json`.

Verificar: mismo bloque que Método A.

---

## Cambiar método de auth (después de instalar)

Si el usuario quiere cambiar entre token y OAuth en cualquier momento:

```bash
# De token → oauth:
python3 -c "
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector.databricks.auth import set_auth_method
set_auth_method('oauth')
print('auth_method actualizado a oauth')
"
# Luego ejecutar setup_auth.py para hacer login OAuth:
# cd ~/projects/kavak_connector && python3 setup_auth.py

# De oauth → token:
python3 -c "
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector.databricks.auth import set_auth_method
set_auth_method('token')
print('auth_method actualizado a token')
"
# Luego usar kavak-token-update para guardar el nuevo token
```

---

## Redshift (solo si hay datos que aún no están en Databricks)

Kavak está en migración de Redshift → Databricks. Configura Redshift solo si es necesario.

```bash
cat > ~/.kavak_connector/redshift.env << 'EOF'
REDSHIFT_HOST=cluster.abc.us-east-1.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DATABASE=kavak
REDSHIFT_USER=analyst
REDSHIFT_PASSWORD=TU_PASSWORD
EOF
chmod 600 ~/.kavak_connector/redshift.env

python3 -c "
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/kavak_connector'))
from kavak_connector import query_redshift
df = query_redshift('SELECT 1 AS ok')
print('Redshift OK:', df.iloc[0,0])
"
```
