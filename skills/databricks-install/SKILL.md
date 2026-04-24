---
name: databricks-install
description: >
  Usar cuando el usuario quiera instalar el conector de Databricks por primera vez,
  o actualizar el conector a la versión más reciente.

  Activar cuando:
  - El directorio ~/projects/databricks_connector no existe (primer uso)
  - El usuario diga "instalar", "setup", "configurar", "no tengo el conector"
  - El usuario diga "actualizar el conector", "bajar la última versión", "hacer pull",
    "update", "nueva versión"
---

# Databricks — Instalación y Actualización del Conector

## ¿Instalación nueva o actualización?

Verifica primero:
```bash
ls ~/projects/databricks_connector 2>/dev/null && echo "EXISTS" || echo "NEW"
```

- Si `EXISTS` → ve a la sección **Actualización** más abajo
- Si `NEW` → sigue los **5 pasos de instalación** en orden

---

## Instalación completa (primera vez)

### Paso 1 de 5 — Clonar el repositorio

```bash
git clone https://github.com/vis-nath/db-connector.git ~/projects/databricks_connector
```

Si aparece un error de red o acceso, verifica que el usuario tenga conexión a internet.

### Paso 2 de 5 — Instalar dependencias

```bash
cd ~/projects/databricks_connector
pip install -r requirements.txt --break-system-packages
```

Esto puede tomar 1-2 minutos. Es normal ver muchas líneas de descarga.

### Paso 3 de 5 — Crear archivo de configuración

Pídele al usuario los siguientes dos valores. Puede pedírselos al administrador de Databricks de su equipo:

> "Necesito dos datos para configurar la conexión:
> 1. **Host** — la dirección del workspace (sin `https://`), algo como `dbc-xxxxxxxx.cloud.databricks.com`
> 2. **HTTP Path** — la ruta del SQL Warehouse, algo como `/sql/1.0/warehouses/3de9aee76c2f16f1`"

Una vez que el usuario proporcione los valores, ejecuta el siguiente comando reemplazando `[HOST]` y `[HTTP_PATH]` con los valores que dio:

```bash
mkdir -p ~/.databricks_connector && cat > ~/.databricks_connector/config.json << 'EOF'
{
  "host": "[HOST]",
  "http_path": "[HTTP_PATH]"
}
EOF
chmod 600 ~/.databricks_connector/config.json
echo "Config guardado"
```

Verifica que los valores quedaron bien:
```bash
cat ~/.databricks_connector/config.json
```

### Paso 4 de 5 — Configurar autenticación (API Key — recomendado)

Pregúntale al usuario:

> "¿Tienes una API Key de Databricks? (un token que empieza con `dapi...`)"

#### Si el usuario dice SÍ — guardar el token (método recomendado)

Pídele el token:

> "Pégame el token y lo guardo de forma segura."

Una vez que te lo dé, guárdalo en `~/.databricks_connector/.env` reemplazando `[TOKEN]` con el valor exacto:

```bash
cat > ~/.databricks_connector/.env << 'EOF'
DATABRICKS_TOKEN=[TOKEN]
EOF
chmod 600 ~/.databricks_connector/.env
echo "Token guardado"
```

Ventajas del API Key:
- No se abre ningún navegador, nunca
- Funciona en entornos sin GUI (servidores, WSL2 headless)
- No expira cada 30-90 días como el OAuth
- Cualquier proyecto puede leer el token desde `~/.databricks_connector/.env`

Salta directamente al **Paso 5**.

#### Si el usuario dice NO — usar autenticación OAuth (alternativa)

Dile al usuario:

> "Voy a ejecutar el setup de autenticación. Se abrirá tu navegador predeterminado.
> Cuando aparezca, inicia sesión con tu correo @kavak.com usando 'Continuar con Google'.
> El login es una sola vez — los tokens se guardan automáticamente y duran entre 30 y 90 días."

Luego ejecuta:
```bash
python3 ~/projects/databricks_connector/setup_auth.py
```

El script abre el navegador, el usuario completa el SSO de Google, y los tokens se guardan en `~/.databricks/token-cache.json`.

Resultado esperado al final:
```
✓ Autenticado como: nombre.apellido@kavak.com
```

Si el script termina con ese mensaje, continuar al Paso 5.

Si el script falla, ver **Solución de problemas** más abajo.

### Paso 5 de 5 — Verificar que funciona

```python
import sys, pathlib
sys.path.insert(0, str(pathlib.Path.home() / 'projects/databricks_connector'))
from databricks_connector import query

df = query("SELECT 1 AS conexion_exitosa")
print(df)
```

Resultado esperado:
```
   conexion_exitosa
0                 1
```

Si aparece ese resultado, la instalación está completa. Procede con la consulta original usando el skill **`databricks-query`**.

Si aparece un error, usa el skill **`databricks-query`** para manejarlo.

### Paso 6 — Configurar regla global en CLAUDE.md

Verifica si la regla de Databricks ya existe:

```bash
grep -q "databricks-index" ~/.claude/CLAUDE.md 2>/dev/null && echo "YA_EXISTE" || echo "FALTA"
```

Si el resultado es `FALTA`, agrega la siguiente línea dentro de la sección `## Global Rules (all projects)` del archivo `~/.claude/CLAUDE.md`:

```
- **Datos de Kavak:** Si el usuario solicita datos de cualquier tipo, usa el skill `databricks-index` antes de cualquier otra acción. Aplica sin importar si el usuario mencione o no Databricks explícitamente.
```

Si el resultado es `YA_EXISTE`, no hagas nada — la regla ya está activa.

---

## Actualización del conector

```bash
cd ~/projects/databricks_connector && git pull
```

Si hubo cambios en `requirements.txt`, ejecuta también:
```bash
pip install -r requirements.txt --break-system-packages
```

Confirma al usuario mostrando el último commit:
```bash
git log -1 --oneline
```

---

## Usar el token en otros proyectos

Si el usuario quiere acceder a Databricks desde un proyecto propio sin usar el conector,
puede cargar las credenciales así:

```python
from dotenv import load_dotenv
from pathlib import Path
import os

load_dotenv(Path.home() / ".databricks_connector" / ".env")

token = os.environ["DATABRICKS_TOKEN"]
```

---

## Solución de problemas de instalación

### Error al clonar el repositorio
Verifica que el usuario tenga conexión a internet y que el repositorio sea accesible.

### "config.json no encontrado"
Ejecutar el Paso 3 de instalación.

### "config.json le faltan campos"
El archivo existe pero le falta `host` o `http_path`. Mostrar el contenido actual:
```bash
cat ~/.databricks_connector/config.json
```
Corregir los campos que falten.

### El navegador no abre / setup_auth.py cuelga (solo OAuth)
Verificar que el usuario esté ejecutando en un entorno con acceso a GUI (no un servidor headless).
En WSL2, el navegador predeterminado de Windows debe estar configurado. Si el navegador no abre:
1. Verificar que exista un navegador predeterminado en Windows
2. Intentar abrir manualmente una URL en el navegador antes de correr el script
3. Si el error es de `external-browser`, el SDK imprimirá la URL de autorización — copiarla y abrirla manualmente en el navegador

Si el usuario tiene una API Key, recomendarle usarla en vez de OAuth para evitar este problema.

### "Token expired" o `AuthRequiredError` justo después de instalar (solo OAuth)
El setup_auth.py no terminó correctamente. Volver a ejecutar:
```bash
python3 ~/projects/databricks_connector/setup_auth.py
```

### `AuthRequiredError` con API Key configurada
El token en `~/.databricks_connector/.env` puede ser incorrecto o haber expirado.
Verifica que el archivo tenga el formato correcto:
```bash
cat ~/.databricks_connector/.env
```
El contenido debe ser exactamente: `DATABRICKS_TOKEN=dapi...`
Si el token expiró, pídele al usuario uno nuevo y actualiza el archivo.
