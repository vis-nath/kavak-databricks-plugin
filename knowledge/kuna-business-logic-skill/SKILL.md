---
name: kuna-business-logic-skill
description: "Kuna dealer CRM — índice de routing para agente analítico. Fuente: Redshift (playground + vehicle_documentation + kuna_dealers + communication_center schemas). Sin datos EaaS marketplace — para eso usar kavak-marketplace-eaas-databricks."
source: redshift
topics: [funnel kuna, leads, conversión, CTR, vencidos, en vuelo, reiniciados, afiliación, onboarding, agencias, churn, cohort, comisiones, expedientes, vehículo, documentos, pipeline, Nicole, WhatsApp, kuna, dealer CRM, KAVAK ALIADOS, ECOMERCEAAS, HERTZ]
---

# Kuna Business Logic — Índice del Skill

**Uso:** Lee SOLO este archivo al recibir un prompt. Ve a los archivos de detalle únicamente para la sección que necesitas.

**Idioma:** Responde siempre en español.

---

## Routing rápido — ¿qué necesito?

### 📊 Funnel de leads / conversión / CTR / Vencidos / En Vuelo / Reiniciados
**Query:** `references/query-kuna-funnel.sql` ✅  
**Fuente principal:** `playground.kua_sensitive_leads_data` + `kua_sensitive_formalization_summary`  
**Proyectos incluidos:** `KAVAK ALIADOS` · `HERTZ` · `ECOMERCEAAS`  
**Stages:** Leads → Consultados → Preaprobados → Handoffs → Aprobados → Contratos  
**Detalle:** `SKILL-reference.md` §1 (funnel, columnas, derivadas) + §2 (CTR, Vencidos, En Vuelo, Reiniciados)

### 🏢 Afiliación / onboarding de agencias / actividad / churn / cohort matrix
**Query:** `references/query-afiliacion.sql` ✅  
**Fuente principal:** `playground.dl_tickets_jira_data` + `playground.kua_sensitive_ao_nexus`  
**Una fila por agencia** (deduplicada — `rn = 1` por `agency_uuid`)  
**Detalle:** `SKILL-reference.md` §3

### 💰 Comisiones / expedientes de pago / SLA de dispersión y liberación
**Query:** `references/query-comision.sql` ✅  
**Fuente principal:** `playground.dl_comisiones___agencias_2_00__*`  
**Proyectos:** KAVAK ALIADOS + ECOMERCEAAS (siempre juntos)  
**Join clave:** `vin` → funnel Kuna  
**Detalle:** `SKILL-reference.md` §4

### 📁 Expedientes de vehículo / pipeline de documentos / estatus del auto
**Query:** `references/query-vehicle-files.sql` ✅  
**Fuente principal:** `vehicle_documentation_api_global_refined.*`  
**Una fila por `asset_id`** (último expediente)  
**Join clave:** `asset_id` → funnel Kuna  
**Detalle:** `SKILL-reference.md` §5

### 📱 Nicole / WhatsApp AI agent / funnel de conversación / mensajes enviados
**Queries:** `references/query-nicole.sql` + `references/query-nicole-channel.sql` ✅  
**Fuentes:** `kuna_dealers_api_global_refined.orchestrator_audit_events` + `communication_center_api_global_refined.communication`  
**Solo KAVAK ALIADOS** (ECOMERCEAAS no aplica a Nicole)  
**Join clave:** `id_lead` / `user_id` → funnel Kuna  
**Detalle:** `SKILL-reference.md` §6

### 🔗 Joins entre dominios (funnel + comisiones + expedientes + Nicole)
**Detalle:** `SKILL-reference.md` §7  

| Join | Key |
|------|-----|
| Funnel + Comisiones | `vin` |
| Funnel + Expedientes | `asset_id` |
| Funnel + Nicole events | `id_lead` |
| Funnel + Nicole channel | `user_id` |
| Afiliación → Funnel | `agency_id_final = agency_uuid` |

### 🏬 Comercial Hub (agencias en Notion)
**⚠️ Sin query Redshift — fuente es Notion API**  
Datos en `notion.db::afiliados` (cache del webapp). Para análisis ad-hoc: pedir export desde Notion.  
Campos clave: `nombre_compania`, `estado`, `status_compliance`, `status_training`, `hunter`, `trainer`, `prioridad_kuna`, `interesado_kuna/eeas/plan_piso`, `fecha_alta`, `fecha_creacion`.

---

## Exclusiones canónicas (aplicar siempre)

| Exclusión | Cuándo |
|-----------|--------|
| `criterios_proyecto = 'ECOMERCEAAS'` | **NO excluir** — incluir siempre junto a KAVAK ALIADOS en funnel y comisiones |
| `nombre_agencia = 'ALQUILADORA VEHICULOS AUTOMOT-MULTIMARCA-ALQUILADORA VEHICULOS AUTOMOT'` | Excluir en vistas comerciales, leaderboard y KAM performance |

---

## Reglas críticas

| Regla | Detalle |
|-------|---------|
| **agency_id** | Usar SIEMPRE `agency_id_final` (CTE corregida) — el `agency_id` raw tiene errores históricos |
| **Nombre agencia** | `nex.nombre_completo_agencia` — nunca `company.name` |
| **Ventana Vencidos** | 14 días desde `last_non_cancelled_or_entered_value_date` |
| **ECOMERCEAAS** | Leads EaaS con financiamiento Kuna — incluir en todo excepto Nicole |
| **Zona horaria** | Mexico City (UTC-6) — usar `CONVERT_TIMEZONE` en Redshift donde aplique |
| **EaaS Marketplace** | Para reservas, entregas, STR, inventario EaaS → usar skill `kavak-marketplace-eaas-databricks` |

---

## Archivos de este skill

```
kuna-business-logic-skill/
├── SKILL.md                            ← Este archivo (índice)
├── SKILL-reference.md                  ← Referencia completa (9 secciones)
└── references/
    ├── query-kuna-funnel.sql           ← Funnel principal (§1, §2)
    ├── query-afiliacion.sql            ← Afiliación agencias (§3)
    ├── query-comision.sql              ← Comisiones (§4)
    ├── query-vehicle-files.sql         ← Expedientes/pipeline (§5)
    ├── query-nicole.sql                ← Nicole events (§6)
    └── query-nicole-channel.sql        ← Nicole channel status (§6)
```

**Para detalle adicional:** leer solo la sección relevante de `SKILL-reference.md` usando los headers `§1`–`§9`.
