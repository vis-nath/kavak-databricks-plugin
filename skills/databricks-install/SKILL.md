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
- Si `NEW` → sigue los **6 pasos de instalación** en orden

---

## Instalación completa (primera vez)

### Paso 1 de 6 — Clonar el repositorio

```bash
git clone https://github.com/vis-nath/db-connector.git ~/projects/databricks_connector
```

Si aparece un error de red o acceso, verifica que el usuario tenga conexión a internet.

### Paso 2 de 6 — Instalar dependencias

```bash
cd ~/projects/databricks_connector
pip install -r requirements.txt --break-system-packages
playwright install chromium
```

Esto puede tomar 2-3 minutos. Es normal ver muchas líneas de descarga.

### Paso 3 de 6 — Crear archivo de configuración

Pídele al usuario los siguientes dos valores. Puede pedírselos al administrador de Databricks de su equipo:

> "Necesito dos datos para configurar la conexión:
> 1. **Host URL** — la dirección del workspace de Databricks (algo como `https://dbc-xxxxxxxx.cloud.databricks.com`)
> 2. **Warehouse ID** — el ID del SQL Warehouse (una cadena de letras y números, algo como `3de9aee76c2f16f1`)"

Una vez que el usuario proporcione los valores, ejecuta el siguiente comando reemplazando `[HOST_URL]` y `[WAREHOUSE_ID]` con los valores que dio:

```bash
mkdir -p ~/.databricks_connector && cat > ~/.databricks_connector/config.json << 'EOF'
{
  "host": "[HOST_URL]",
  "warehouse_id": "[WAREHOUSE_ID]"
}
EOF
chmod 600 ~/.databricks_connector/config.json
echo "Config guardado"
```

### Paso 4 de 6 — Iniciar sesión en Databricks

Dile al usuario:

> "Voy a abrir una ventana de Chrome en tu pantalla de Windows.
> Cuando aparezca, inicia sesión con tu correo de Kavak (@kavak.com) como lo haces normalmente.
> No tienes que hacer nada más — la sesión se guardará sola cuando el login sea exitoso
> y el Chrome se cerrará automáticamente."

Luego ejecuta:
```bash
python3 ~/projects/databricks_connector/setup_auth.py
```

El script abre Chrome, navega a la página de SQL Warehouses, espera a que el usuario complete el SSO, guarda la sesión y cierra el navegador automáticamente.

### Paso 5 de 6 — Verificar que funciona

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

Si aparece un error en lugar de ese resultado, usa el skill **`databricks-query`** para manejarlo.

### Paso 6 de 6 — Configurar regla global en CLAUDE.md

Verifica si la regla de Databricks ya existe:

```bash
grep -q "databricks-index" ~/.claude/CLAUDE.md 2>/dev/null && echo "YA_EXISTE" || echo "FALTA"
```

Si el resultado es `FALTA`, agrega la siguiente línea dentro de la sección `## Global Rules (all projects)` del archivo `~/.claude/CLAUDE.md`:

```
- **Datos de Kavak:** Si el usuario solicita datos de cualquier tipo, usa el skill `databricks-index` antes de cualquier otra acción. Aplica sin importar si el usuario mencione o no Databricks explícitamente.
```

Si el resultado es `YA_EXISTE`, no hagas nada — la regla ya está activa.

Confirma al usuario:
> "Listo. Claude ahora sabrá automáticamente que cuando pidas datos, debe preguntarte si están en Databricks antes de hacer cualquier otra cosa."

Procede con la consulta original usando el skill **`databricks-query`**.

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

## Solución de problemas de instalación

### Error al clonar el repositorio
Verifica que el usuario tenga conexión a internet y que el repositorio sea accesible.

### "config.json no encontrado"
Ejecutar el Paso 3 de instalación.

### El Chrome no abre / no se ve la ventana
```bash
echo $DISPLAY
```
Debe devolver algún valor (`:0`, `:1`). Si no devuelve nada, el usuario debe reiniciar WSL desde PowerShell:
```
wsl --shutdown
```
