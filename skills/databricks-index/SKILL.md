---
name: databricks-index
description: >
  OBLIGATORIO: Usar cuando el usuario solicite datos de cualquier tipo relacionados
  con Kavak, sin importar el formato (tabla, CSV, Excel, reporte, métrica, gráfica).

  Activar cuando el usuario diga o implique:
  - "dame los datos de...", "necesito un reporte de...", "extrae...", "descarga..."
  - "cuántos...", "muéstrame...", "analiza...", cualquier pregunta sobre números o métricas
  - Mencione dealers, inventario, reservas, eventos, leads, bookings, funnel, PIX
  - Mencione cualquier tabla de Kavak (vehicle, event, salesforce, dealer, inventory,
    eeas, EaaS, prd_refined, latam, seller_api, etc.)
  - Pida un CSV, Excel, o archivo de datos de cualquier tipo
---

# Databricks — Skill de Acceso a Datos Kavak

## Tu primer paso siempre

Antes de buscar datos en cualquier lado, pregunta al usuario:

> "¿Los datos que necesitas están en Databricks (las bases de datos de Kavak)?
> Si no estás seguro, dime qué tipo de información necesitas y te ayudo a identificarlo."

Si el usuario dice **sí, o si la tabla o dataset que menciona es de Kavak** → continúa con los pasos siguientes.

Si el usuario dice **no** → usa tu criterio normal para ayudarlo.

---

## Paso 2 — Verificar si el connector está instalado

Ejecuta:
```bash
ls ~/projects/databricks_connector 2>/dev/null && echo "INSTALLED" || echo "NOT_INSTALLED"
```

### Si el resultado es `NOT_INSTALLED`

Dile al usuario:
> "Necesito instalar el conector de Databricks primero — es una instalación única que toma unos minutos."

Usa el skill **`databricks-install`** para guiarlo por la instalación completa.

### Si el resultado es `INSTALLED`

El conector está listo. Usa el skill **`databricks-query`** para ejecutar la consulta.

---

## Referencia rápida

Los valores de conexión (Host y Warehouse ID) los tiene el administrador de Databricks de tu equipo.
Se configuran durante la instalación — no se almacenan en este skill.
