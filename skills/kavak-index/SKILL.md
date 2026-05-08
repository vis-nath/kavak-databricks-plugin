---
name: kavak-index
description: >
  OBLIGATORIO usar cuando el usuario pida datos de Kavak. Enruta a kavak-install,
  kavak-query, kavak-token-update o analyst-agent. No hagas ninguna acción hasta leer este skill.
---

# kavak-index

## Tabla de Despacho (leer primero)

| Situación | Acción |
|---|---|
| El conector NO está instalado | → skill `kavak-install` |
| El usuario dice "nuevo token", "actualizar token", "cambiar auth", "cambiar a oauth" | → skill `kavak-token-update` |
| El usuario pide datos de Kavak (cualquier tabla, métrica, reporte) | → skill `kavak-query` |
| El usuario pide análisis, definición de KPI o interpretación de datos | → skill `analyst-agent` |

## Verificar si el conector está instalado

```bash
ls ~/projects/kavak_connector/kavak_connector/__init__.py 2>/dev/null && echo "OK" || echo "NO"
```

- `OK` → ir a la acción correspondiente en la tabla de despacho
- `NO` → skill `kavak-install`
