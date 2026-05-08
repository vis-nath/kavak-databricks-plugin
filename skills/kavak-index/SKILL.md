---
name: kavak-index
description: >
  OBLIGATORIO usar cuando el usuario pida datos de Kavak o Kuna (datos de autos, inventario,
  dealers, ventas, reservas, leads, métricas, KPIs, reportes, tablas de cualquier tipo).
  Enruta a kavak-install, kavak-query, kavak-token-update o analyst-agent.
  No hagas ninguna acción hasta leer este skill.
---

# kavak-index

## Tabla de Despacho (leer primero)

| Situación | Acción |
|---|---|
| El conector NO está instalado | → skill `kavak-install` |
| El usuario dice "nuevo token", "actualizar token", "cambiar auth", "cambiar a oauth" | → skill `kavak-token-update` |
| El usuario pide datos, tablas, reportes o métricas de Kavak o Kuna | → skill `kavak-query` |
| El usuario pide análisis, definición de KPI, interpretación de datos o métricas de negocio | → skill `analyst-agent` |

## Verificar si el conector está instalado

```bash
ls ~/projects/kavak_connector/kavak_connector/__init__.py 2>/dev/null && echo "OK" || echo "NO"
```

- `OK` → ir a la acción correspondiente en la tabla de despacho
- `NO` → skill `kavak-install`

## Datos de Kavak y Kuna

Kavak y Kuna comparten la infraestructura de datos. Cualquier solicitud de datos de ambas marcas pasa por los mismos conectores (Databricks y Redshift).
