---
name: kuna-business-logic-skill-reference
description: "Referencia completa del dominio Kuna — columnas, joins, métricas derivadas, reglas de negocio por dominio. Leer solo la sección relevante al prompt."
---

# Kuna Business Logic — Referencia Completa

**Fuente de datos:** Redshift (`playground` schema + `vehicle_documentation_api_global_refined` + `kuna_dealers_api_global_refined` + `communication_center_api_global_refined`)

**No incluye:** EaaS funnel — usar skill `kavak-marketplace-eaas-databricks` para eso.

---

## §1 Funnel Principal Kuna

**Query:** `references/query-kuna-funnel.sql`

### Tablas fuente

| Tabla | Propósito |
|-------|-----------|
| `playground.kua_sensitive_leads_data` | Tabla principal — 1 fila por lead |
| `playground.kua_sensitive_formalization_summary` | Fechas de formalización (contratos, enganche, dispersión, etc.) |
| `playground.kua_diccionario_aliados` | Corrección de `agency_id` con errores históricos |
| `playground.kua_sensitive_ao_nexus` | Datos de agencia (nombre, estado, territorio, correo, KAM) |
| `playground.webapp_agencias_perfiladas` | Categoría de agencia prioritaria |

### Columnas principales de `kua_sensitive_leads_data`

| Columna | Tipo | Significado |
|---------|------|-------------|
| `id_lead` | PK | Identificador único del lead |
| `agency_id` | FK | UUID de la agencia (puede tener errores — usar `agency_id_final` del CTE) |
| `fecha_origen` | DATE | Fecha de creación del lead (clave de cohort) |
| `criterios_proyecto` | TEXT | `'KAVAK ALIADOS'`, `'HERTZ'`, `'ECOMERCEAAS'` |
| `profiling_status` | TEXT | Estado de score crediticio; NULL = no consultado |
| `statement` | TEXT | Estado actual del lead (`'APPROVED'`,`'REJECTED'`,`'CANCELLED'`, etc.) |
| `first_non_cancelled_or_entered_new_value` | TEXT | Primera resolución no-cancelada |
| `last_non_cancelled_or_entered_value_date` | TIMESTAMP | Fecha de la última resolución válida (base para Vencidos/En Vuelo) |
| `asset_id` | TEXT | Stock ID del auto (join a expedientes, comisiones) |
| `contract_id` | TEXT | FK a `kua_sensitive_formalization_summary` |
| `correo_agente` | TEXT | Email del KAM |
| `kuna_account_manager` | TEXT | Email del Financing Manager |
| `user_id` | TEXT | ID de usuario Kuna (join a Nicole channel) |

### Columnas derivadas en la query

| Columna | Derivación |
|---------|-----------|
| `agency_id_final` | `COALESCE(map.uuid_correcto, kl.agency_id)` — siempre usar este |
| `consultado` | `profiling_status IS NOT NULL` |
| `preaprobado` | `profiling_status IS NOT NULL AND RIGHT(profiling_status,1) != 'R'` |
| `rechazado` | `first_non_cancelled_or_entered_new_value = 'REJECTED' OR statement = 'REJECTED'` |
| `aprobado` | `first_non_cancelled_or_entered_new_value = 'APPROVED' OR statement = 'APPROVED'` |
| `etapa_lead` | CASE sobre flags — 9 valores (Cerrado, Auto desembolsado, Pendiente de desembolso, Pendiente de contrato, Pendiente de pago inicial, Rechazado, Pendiente de documentación, Pendiente de NIP, Rechazado en consulta NIP) |
| `etapa_formalizacion` | CASE sobre flags de formalización — 11 valores (Dispersado con Vobo, Pendiente de Tres Luces, Pendiente de Segunda Llave, Pendiente de GPS, Auto Dispersado si Vobo, Pendiente de Dispersion, Expediente Condicionado, Aprobado Reiniciado, Pendiente de Firma de Contrato, Pendiente de Enganche, Cerrado) |
| `profile_status_tier` | Tier A/B/C/D/E/F/X/Rechazados derivado de `profiling_status` |
| `agencia_prioritaria` | JOIN a `webapp_agencias_perfiladas` → categoría o `'Agencia no Prioritaria'` |
| `nombre_agencia` | `nex.nombre_completo_agencia` — siempre desde `kua_sensitive_ao_nexus` |

### Stages del funnel

| Stage | Fecha de evento (uncohorted) | Filtro adicional |
|-------|------------------------------|-----------------|
| Leads recibidos | `fecha_origen` | `criterios_proyecto IN (...)` |
| Consultados | `fecha_origen` | `AND consultado = 1` |
| Preaprobados | `fecha_origen` | `AND preaprobado = 1` |
| Handoffs | `fecha_origen` | `AND (aprobado=1 OR rechazado=1)` |
| Aprobados | `last_non_cancelled_or_entered_value_date` | `AND aprobado=1` |
| Contratos | `oldest_contract_date` | `AND oldest_contract_date IS NOT NULL` |

**Cohorted:** anclar todo a `fecha_origen` en el periodo. Aprobados cohortados requieren además `last_non_cancelled_or_entered_value_date >= fecha_origen` para evitar phantom approvals.

### Filtros del WHERE en la query

```sql
WHERE kl.criterios_proyecto IN ('KAVAK ALIADOS', 'HERTZ', 'ECOMERCEAAS')
   OR kl.asset_id IN ('10298236', '10301015')  -- overrides manuales
   OR (agency_id_final IN ('<lista UUIDs>') AND kl.criterios_proyecto != 'CREDITARIA')
```

**Nota ECOMERCEAAS:** Son leads EaaS que tramitan financiamiento Kuna. Incluirlos siempre en el funnel general.

---

## §2 Métricas Derivadas

### CTR (Contract-to-Risk Ratio)

```
CTR = contratos / (contratos + vencidos)
```

### Vencidos

Aprobados cuya ventana de 14 días expiró sin contrato:

```sql
-- Redshift
aprobado = 1
AND oldest_contract_date IS NULL
AND last_non_cancelled_or_entered_value_date IS NOT NULL
AND DATEADD(day, 14, last_non_cancelled_or_entered_value_date) < CURRENT_DATE
```

### En Vuelo

Aprobados dentro de la ventana de 14 días, sin contrato, sin cancelar:

```sql
-- Redshift
aprobado = 1
AND oldest_contract_date IS NULL
AND DATEDIFF(day, last_non_cancelled_or_entered_value_date, CURRENT_DATE) < 15
AND statement NOT IN ('CANCELLED', 'REJECTED')
```

### Reiniciados

Lead aprobado en ciclo anterior que volvió a entrar al pipeline en el periodo actual. Identificar por `fecha_origen` en el periodo actual y `aprobado=1` con `last_non_cancelled_or_entered_value_date` de un ciclo anterior.

### Leaderboard

| Rol | Columna email | Métrica principal |
|-----|---------------|-------------------|
| KAM | `correo_agente` | Contratos (`oldest_contract_date IS NOT NULL`) |
| Financing Manager | `kuna_account_manager` | HOs = `aprobado=1 OR rechazado=1` |

---

## §3 Afiliación

**Query:** `references/query-afiliacion.sql`

### Tablas fuente

| Tabla | Propósito |
|-------|-----------|
| `playground.dl_tickets_jira_data` | Tickets de onboarding por agencia |
| `playground.kua_sensitive_leads_data_kavak_aliados` | Histórico de leads por agencia (primer/último lead) |
| `playground.kua_sensitive_general_results` | Resumen de leads y contratos por agencia por periodo |
| `playground.kua_sensitive_ao_nexus` | Datos de la agencia (nombre, estado, territorio) |
| `playground.dl_bases_kavak_aliados_no_tickets_agencies` | Agencias activas sin ticket de Jira |

### Funnel de onboarding (por agencia)

| Stage | Columna fecha | SLA calculado |
|-------|---------------|---------------|
| Tickets Creados | `fecha_inicio` | — |
| Aprobado Data | `fecha_final_data` | `duracion_final_data_dias` |
| Aprobado Compliance | `fecha_final_compliance` | `duracion_final_compliance_dias` |
| Contrato Generado | `sla_contract_generated` | `duracion_contrato_generado` |
| Contrato Firmado | `sla_contract_signed_on` | `duracion_contrato_firmado_dealer` |
| Dealer Capacitado | `fecha_final_training` | `duracion_final_training_dias` |
| Alta total | — | `duracion_alta_agencia_dias` (inicio → training completo) |
| Primer lead | `fecha_primer_lead` | `duracion_hasta_primer_lead_dias` |
| Training → primer lead | — | `duracion_training_hasta_primer_lead_dias` |

### Lógica de `fecha_final_training`

```sql
CASE
    WHEN training_last_status = 'TRAINING COMPLETED' AND fecha_final_training IS NULL
        THEN fecha_inicio_training
    WHEN training_last_status = 'TRAINING COMPLETED' AND fecha_final_training IS NOT NULL
        THEN fecha_final_training
    ELSE NULL
END
```

### Agencias sin ticket de Jira

La query incluye agencias de `dl_bases_kavak_aliados_no_tickets_agencies` con `issue_key = 'Sin Ticket'` y `status = 'Completed without Ticket'`. Fechas de onboarding hardcodeadas en `'2025-03-03'`.

### Cohort matrix (agencias capacitadas → contratos)

1. Para cada mes de capacitación: obtener `agency_uuid` donde `fecha_final_training` cae en ese mes.
2. Para cada mes objetivo: contar contratos de esas agencias (`oldest_contract_date` en ese mes).
3. Join: `agency_id_final` (funnel) = `agency_uuid` (afiliación).

### Actividad de agencia (últimos 30/90 días)

| Flag | Condición |
|------|-----------|
| `agencia_inactiva_30d` | Sin leads O `fecha_ultimo_lead < CURRENT_DATE - 30 días` |
| `agencia_churn` | Sin leads O `fecha_ultimo_lead < CURRENT_DATE - 90 días` |
| `agencia_mas_5_leads` | `suma_leads_30dias > 5` |
| `agencia_mas_12_leads` | `suma_leads_30dias > 12` |
| `agencia_con_contratos` | `suma_contratos_30dias > 0` |

---

## §4 Comisiones

**Query:** `references/query-comision.sql`

### Tablas fuente

| Tabla | Propósito |
|-------|-----------|
| `playground.dl_comisiones___agencias_2_00__base_trabajable_comisiones_semanal` | Comisiones en proceso (activas, no históricas) |
| `playground.dl_comisiones___agencias_2_00__hist_rico` | Historial de comisiones liberadas |
| `playground.dl_expedientes_agencia_bd_historico_expedientes_data` | Estatus de expediente (liberación) |

**Nota:** La query hace UNION entre comisiones activas e históricas. `criterios_proyecto IN ('KAVAK ALIADOS','ECOMERCEAAS')` siempre.

### Columnas principales del resultado

| Columna | Significado |
|---------|-------------|
| `vin` | VIN del auto (join key con funnel por `vin`) |
| `asset_id` | Stock ID (join key con expedientes) |
| `fecha_de_contrato` | Fecha del contrato |
| `fecha_de_dispersion` | Fecha de dispersión del crédito |
| `fecha_de_carga` | Fecha de carga de la base trabajable |
| `fecha_de_liberacion` | Fecha de liberación del expediente |
| `comision_dealer` | Monto de comisión (`comision_punto_de_venta`) |
| `estatus_comision` | Estado derivado (6 valores — ver abajo) |
| `status_expediente_gabo` | Estado del expediente (sistema externo) |
| `substatus_expediente_gabo` | Sub-estado del expediente |
| `sla_dispersion` | Días contrato → dispersión |
| `sla_carga_base_trabajable` | Días dispersión → carga |
| `sla_expediente_completo` | Días carga → liberación expediente |
| `sla_factura_solicitada` | Días liberación → factura solicitada |
| `sla_factura_recibida` | Días factura solicitada → recibida |
| `sla_pago_comision` | Días factura solicitada → pago |
| `proyecto` | `'KAVAK ALIADOS'` o `'ECOMERCEAAS'` |

### Derivación de `estatus_comision`

```sql
CASE
    WHEN pagado IN ('Pagada','Si')                  THEN 'Comision Pagada'
    WHEN factura_recibida IN ('Recibida','Si')       THEN 'Pendiente de Pago de Comision'
    WHEN factura_solicitada IN ('Solicitada','Si')   THEN 'Pendiente de Recibo de Factura'
    WHEN estatus_expediente = 'EXPEDIENTE LIBERADO'  THEN 'Pendiente de Envio de Factura'
    WHEN fecha_de_liberacion IS NOT NULL             THEN 'Pendiente de Envio de Factura'
    ELSE 'Expediente No Liberado'
END
```

---

## §5 Expedientes / Pipeline

**Query:** `references/query-vehicle-files.sql`

### Tablas fuente (schema `vehicle_documentation_api_global_refined`)

| Tabla | Propósito |
|-------|-----------|
| `documents` | Documento principal, 1 fila por expediente |
| `review_requests` | Revisiones del expediente (primera y última) |
| `document_events` | Historial de eventos del documento |

**Resultado:** 1 fila por `asset_id` (último expediente por `created_at DESC`, `rank_num = 1`).

### Columnas del resultado

| Columna | Significado |
|---------|-------------|
| `asset_id` | Join key con funnel Kuna |
| `ultimo_estatus_expediente` | Estado actual (6 valores — ver abajo) |
| `fecha_ultimo_estatus` | `event_date` del documento más reciente |
| `expediente_aprobado` | `CLOSED` + `APPROVED` (o `PENDING` + evento `APPROVE`) |
| `expediente_rechazado` | `CLOSED` + `REJECT/REJECTED` |
| `expediente_reiniciado` | `CANCELLED` + evento `RESET` |
| `expediente_abierto` | `status IN ('IN_REVIEW','OPEN')` |
| `expediente_condicionado` | `ultimo_estatus_revision = 'CONDITIONED'` |

### Derivación de `ultimo_estatus_expediente`

```sql
CASE
    WHEN expediente_abierto AND expediente_condicionado THEN 'Expediente Condicionado'
    WHEN expediente_abierto                             THEN 'Expediente en revision'
    WHEN expediente_aprobado                            THEN 'Expediente Aprobado'
    WHEN expediente_reiniciado                          THEN 'Expediente Reiniciado'
    WHEN expediente_rechazado AND NOT expediente_reiniciado THEN 'Expediente Rechazado sin Reinicio'
    ELSE 'Expediente Cancelado'
END
```

### Join con funnel Kuna

```sql
SELECT f.id_lead, f.nombre_agencia, f.etapa_lead, vf.ultimo_estatus_expediente
FROM (<query-kuna-funnel>) f
LEFT JOIN (<query-vehicle-files>) vf ON f.asset_id = vf.asset_id
WHERE f.fecha_origen BETWEEN '<start>' AND '<end>'
```

---

## §6 Nicole (WhatsApp AI Agent)

**Queries:** `references/query-nicole.sql` + `references/query-nicole-channel.sql`

### Nicole Events (`query-nicole.sql`)

Fuente: `kuna_dealers_api_global_refined.orchestrator_audit_events`

Devuelve `ae.*` para leads de KAVAK ALIADOS con `profiling_status IS NOT NULL` (leads consultados).

Para derivar el funnel Nicole: agrupar por `lead_id`, usar `event_name` para identificar cada etapa.

### Nicole Channel Status (`query-nicole-channel.sql`)

Fuente: `communication_center_api_global_refined.communication`

| Columna | Significado |
|---------|-------------|
| `user_id` | Join key: `= playground.kua_sensitive_leads_data.user_id` |
| `channel_status` | Estado del canal WhatsApp → base para métrica "Enviados" |
| `created_at` | Timestamp del mensaje |

Filtro fijo: `created_at >= '2025-08-01'` (inicio del tracking en ETL).

### Funnel Nicole (métricas por cohort de `fecha_origen`)

| Métrica | Condición |
|---------|-----------|
| PreAprobados | Base del cohort (leads con `profiling_status IS NOT NULL`) |
| Enviados | `channel_status` válido para el `user_id` del lead |
| Contestados | `date_conversation_started` set OR `has_answered` truthy (`true`/`TRUE`/`1`) |
| Simulaciones Creadas | `date_simulation_offer_generated` set |
| Simulaciones Elegidas | `date_simulation_offer_selected` set |
| HOs Iniciados | `date_handoff_started` set |
| HOs Completados | `date_handoff_completed` set |
| HOs Completados BO | `rechazado OR aprobado` (resultado back-office) |

**Solo KAVAK ALIADOS.** ECOMERCEAAS no aplica a Nicole.

---

## §7 Joins entre dominios

| Join | Key | Patrón SQL |
|------|-----|-----------|
| Funnel + Comisiones | `vin` | `LEFT JOIN (<comision>) c ON f.vin = c.vin` |
| Funnel + Expedientes | `asset_id` | `LEFT JOIN (<vehicle-files>) vf ON f.asset_id = vf.asset_id` |
| Funnel + Nicole events | `id_lead` | `LEFT JOIN (<nicole>) n ON f.id_lead = n.lead_id` |
| Funnel + Nicole channel | `user_id` | `LEFT JOIN (<nicole-channel>) nc ON f.user_id = nc.user_id` |
| Afiliación + Funnel | `agency_id_final = agency_uuid` | `WHERE agency_id_final IN (SELECT agency_uuid FROM afiliacion WHERE ...)` |

---

## §8 Reglas que no se negocian

| Regla | Detalle |
|-------|---------|
| **agency_id** | Usar SIEMPRE `agency_id_final` (CTE corregida con `kua_diccionario_aliados`) — el `agency_id` raw tiene errores históricos |
| **Nombre de agencia** | `nex.nombre_completo_agencia` (alias `nombre_agencia`) — nunca `company.name` |
| **ECOMERCEAAS** | Incluir siempre junto a KAVAK ALIADOS en funnel y comisiones — son leads EaaS con financiamiento Kuna |
| **ECOMERCEAAS en Nicole** | NO aplica — solo KAVAK ALIADOS en las queries de Nicole |
| **Agencia test** | Excluir `'ALQUILADORA VEHICULOS AUTOMOT-MULTIMARCA-ALQUILADORA VEHICULOS AUTOMOT'` en vistas comerciales, leaderboard y KAM performance |
| **Ventana Vencidos** | 14 días desde `last_non_cancelled_or_entered_value_date` |
| **Zona horaria** | Mexico City (UTC-6); usar `CONVERT_TIMEZONE('UTC','America/Mexico_City',...)` en timestamps de Redshift donde aplique |
| **Cohort vs uncohorted** | Cohort: todo filtrado por `fecha_origen`. Uncohorted: cada etapa en su propia fecha de evento |

---

## §9 Fuentes de datos — schemas Redshift

| Schema | Propósito |
|--------|-----------|
| `playground` | Datos Kuna: leads, agencias, comisiones, afiliación, nexus, diccionario |
| `vehicle_documentation_api_global_refined` | Expedientes de vehículos |
| `kuna_dealers_api_global_refined` | Eventos del agente Nicole (orchestrator) |
| `communication_center_api_global_refined` | Canal de comunicación WhatsApp |

**Comercial Hub (Notion):** Datos de `notion.db::afiliados` — provienen de la API de Notion, NO de Redshift. Sin query SQL disponible; la tabla se introspecciona en runtime por el webapp. Para análisis ad-hoc: consultar directamente en Notion o pedir export. Campos clave: `nombre_compania`, `estado`, `status_compliance`, `status_training`, `hunter`, `trainer`, `prioridad_kuna`, `interesado_kuna`, `interesado_eeas`, `interesado_plan_piso`, `fecha_alta`, `fecha_creacion`.
