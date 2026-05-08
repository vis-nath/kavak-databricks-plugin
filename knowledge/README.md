# Knowledge Files — Convención de Estructura

Cada dominio vive en un **subdirectorio propio** dentro de `knowledge/`.
El `analyst-agent` descubre automáticamente todos los dominios leyendo `knowledge/*/SKILL.md`.

## Estructura requerida por dominio

```
knowledge/<domain-name>/
├── SKILL.md            ← Índice + routing (REQUERIDO — el agent lo lee primero)
├── SKILL-reference.md  ← Referencia completa (opcional — leer solo sección necesaria)
├── CLASIFICACION_*.md  ← Lógica de clasificación Python/SQL (opcional)
└── references/
    └── *.sql           ← Queries SQL listas para ejecutar
```

## Frontmatter obligatorio en SKILL.md

Cada `SKILL.md` DEBE tener estos campos en su frontmatter YAML:

```yaml
---
name: <nombre-unico-del-dominio>
description: "<descripción corta — fuente de datos, propósito>"
source: redshift | databricks | mixed
topics: [lista, de, topics, que, cubre, este, dominio]
---
```

| Campo | Valores válidos | Uso |
|---|---|---|
| `source: redshift` | Todo el dominio usa Redshift | Kuna — todas las tablas en Redshift |
| `source: databricks` | Todo el dominio usa Databricks | (futuro) |
| `source: mixed` | El dominio mezcla fuentes | EaaS — Databricks principal, Redshift para KPIs específicos |

## Cómo usa el analyst-agent estos archivos

1. Lee **SOLO el frontmatter** de cada `knowledge/*/SKILL.md` → construye el índice de scope
2. Compara los `topics:` con la solicitud del usuario → detecta el dominio
3. Si hay match → carga el `SKILL.md` completo del dominio y sigue su routing interno
4. El routing interno indica qué `.sql` leer y si la fuente es Databricks, Redshift o ambas
5. Ejecuta vía `kavak-query` con el `SOURCE:` correspondiente

## Reglas de routing por source

| `source:` | Comportamiento del agent |
|---|---|
| `redshift` | Todas las queries del dominio van con `SOURCE: redshift` sin excepción |
| `databricks` | Todas las queries van con `SOURCE: databricks` |
| `mixed` | El routing interno del SKILL.md indica la fuente KPI por KPI (✅ = Databricks, ⚠️ SOLO REDSHIFT = Redshift) |

## Dominios actuales

| Directorio | Dominio | Source |
|---|---|---|
| `kavak-marketplace-eaas-databricks/` | EaaS Marketplace MX | mixed |
| `kuna-business-logic-skill/` | Kuna Dealer CRM | redshift |
