# kavak-databricks — Plugin para Claude Code

Plugin interno de Kavak que conecta Claude con Databricks.
Una vez instalado, Claude te guía automáticamente cada vez que necesitas datos.

---

## Instalación (una sola vez)

**Paso 1 — Clonar este repositorio**
```bash
git clone https://github.com/vis-nath/kavak-databricks-plugin.git ~/projects/kavak-databricks-plugin
```

**Paso 2 — Instalar el plugin en Claude**
```bash
bash ~/projects/kavak-databricks-plugin/scripts/deploy-local.sh
```

**Paso 3 — Reiniciar Claude Code**

Cierra y vuelve a abrir Claude Code. Eso es todo.

---

## Primer uso

La próxima vez que le pidas datos a Claude (inventario, dealers, bookings, cualquier métrica), él tomará el control y te guiará paso a paso para configurar la conexión a Databricks.

No necesitas hacer nada más.

---

## Actualizar el plugin

```bash
cd ~/projects/kavak-databricks-plugin && git pull
bash ~/projects/kavak-databricks-plugin/scripts/deploy-local.sh
```

Reinicia Claude Code después de actualizar.

---

## Soporte

Habla con el equipo de EaaS o abre un issue en este repositorio.
