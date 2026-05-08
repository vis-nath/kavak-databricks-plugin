-- ============================================================
-- AFILIACIÓN AGENCIAS — query-afiliacion.sql
-- Fuente: Redshift (playground schema)
-- Una fila por agencia (deduplicada por agency_uuid, rn=1).
-- Columnas clave: agency_uuid, nombre_completos_agencia,
--   fecha_inicio, fecha_final_data, fecha_final_compliance,
--   fecha_final_legal, fecha_final_training,
--   sla_commercial_presentation, sla_commercial_successful_contact,
--   duracion_alta_agencia_dias, suma_leads_30dias,
--   agencia_inactiva_30d, agencia_churn
-- Filtrar por estado: AND estado = '<estado>'
-- Filtrar por territorio: AND territory_manager = '<manager>'
-- ============================================================
WITH
FechasCalculadas AS (
    SELECT
        CURRENT_DATE - INTERVAL '30 days' AS treinta_dias_atras,
        CURRENT_DATE - INTERVAL '90 days' AS noventa_dias_atras
),
PrimerosUltimosLeads AS (
    SELECT
        agency_id,
        MIN(fecha_origen) AS fecha_primer_lead,
        MAX(fecha_origen) AS fecha_ultimo_lead,
        MIN(id_lead) AS id_primer_lead,
        MAX(id_lead) AS id_ultimo_lead
    FROM playground.kua_sensitive_leads_data_kavak_aliados
    WHERE proyecto = 'KAVAK ALIADOS'
    GROUP BY agency_id
),
PrimerosUltimosContratos AS (
    SELECT
        kgr.agency_id,
        MIN(kgr.date) AS fecha_primer_contrato,
        MAX(kgr.date) AS fecha_ultimo_contrato
    FROM playground.kua_sensitive_general_results AS kgr
    LEFT JOIN playground.kua_sensitive_ao_nexus AS nex
        ON kgr.agency_id = nex.agency_id
    WHERE
        nex.proyecto_f = 'KAVAK ALIADOS'
        AND kgr.contratos >= 1
    GROUP BY
        kgr.agency_id
),
SumaLeads AS (
    SELECT
        kgr.agency_id,
        SUM(kgr.lead_count) AS suma_leads_30dias,
        SUM(kgr.contratos) AS suma_contratos_30dias,
        MAX(nex.proyecto_f) AS proyecto
    FROM playground.kua_sensitive_general_results AS kgr
    LEFT JOIN playground.kua_sensitive_ao_nexus AS nex ON kgr.agency_id  = nex.agency_id
    LEFT JOIN FechasCalculadas AS fc ON 1=1
    WHERE
        nex.proyecto_f = 'KAVAK ALIADOS' AND kgr.date >= fc.treinta_dias_atras
    GROUP BY
        kgr.agency_id
),
OriginalTicketsSheets AS (
    SELECT
        COALESCE(map.uuid_correcto, tic.agency_uuid) AS agency_uuid,
        tic.issue_key,
        tic.issue_type,
        tic.summary,
        tic.reporter,
        tic.status,
        tic.assignee,
        tic.jira_state,
        tic.jira_name,
        tic.jira_group,
        tic.jira_email,
        tic.commercial_wa_group_created_completed,
        tic.commercial_presentation_completed,
        tic.commercial_successful_contact_completed,
        tic.commercial_owner,
        tic.data_last_status,
        tic.vendor_last_status,
        tic.legal_last_status,
        tic.training_last_status,
        CASE
            WHEN tic.fecha_inicio IS NULL OR TRIM(tic.fecha_inicio) = '' OR tic.fecha_inicio = 'nan' OR tic.fecha_inicio !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.fecha_inicio, 'MM/DD/YYYY HH24:MI:SS')
        END AS fecha_inicio,
        CASE
            WHEN tic.fecha_inicio_data IS NULL OR TRIM(tic.fecha_inicio_data) = '' OR tic.fecha_inicio_data = 'nan' OR tic.fecha_inicio_data !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.fecha_inicio_data, 'DD/MM/YYYY HH24:MI:SS')
        END AS fecha_inicio_data,
        CASE
            WHEN tic.fecha_final_data IS NULL OR TRIM(tic.fecha_final_data) = '' OR tic.fecha_final_data = 'nan' OR tic.fecha_final_data !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.fecha_final_data, 'DD/MM/YYYY HH24:MI:SS')
        END AS fecha_final_data,
        CASE
            WHEN tic.fecha_inicio_vendor IS NULL OR TRIM(tic.fecha_inicio_vendor) = '' OR tic.fecha_inicio_vendor = 'nan' OR tic.fecha_inicio_vendor !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.fecha_inicio_vendor, 'DD/MM/YYYY HH24:MI:SS')
        END AS fecha_inicio_vendor,
        CASE
            WHEN tic.fecha_final_vendor IS NULL OR TRIM(tic.fecha_final_vendor) = '' OR tic.fecha_final_vendor = 'nan' OR tic.fecha_final_vendor !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.fecha_final_vendor, 'DD/MM/YYYY HH24:MI:SS')
        END AS fecha_final_vendor,
        CASE
            WHEN tic.fecha_inicio_legal IS NULL OR TRIM(tic.fecha_inicio_legal) = '' OR tic.fecha_inicio_legal = 'nan' OR tic.fecha_inicio_legal !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.fecha_inicio_legal, 'DD/MM/YYYY HH24:MI:SS')
        END AS fecha_inicio_legal,
        CASE
            WHEN tic.fecha_final_legal IS NULL OR TRIM(tic.fecha_final_legal) = '' OR tic.fecha_final_legal = 'nan' OR tic.fecha_final_legal !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.fecha_final_legal, 'DD/MM/YYYY HH24:MI:SS')
        END AS fecha_final_legal,
        CASE
            WHEN tic.fecha_inicio_training IS NULL OR TRIM(tic.fecha_inicio_training) = '' OR tic.fecha_inicio_training = 'nan' OR tic.fecha_inicio_training !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.fecha_inicio_training, 'DD/MM/YYYY HH24:MI:SS')
        END AS fecha_inicio_training,
        CASE
            WHEN tic.fecha_final_training IS NULL OR TRIM(tic.fecha_final_training) = '' OR tic.fecha_final_training = 'nan' OR tic.fecha_final_training !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.fecha_final_training, 'DD/MM/YYYY HH24:MI:SS')
        END AS fecha_final_training,
        CASE
            WHEN tic.sla_compliance_returned IS NULL OR TRIM(tic.sla_compliance_returned) = '' OR tic.sla_compliance_returned = 'nan' OR tic.sla_compliance_returned !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.sla_compliance_returned, 'MM/DD/YYYY HH24:MI:SS')
        END AS sla_compliance_returned,
        CASE
            WHEN tic.sla_compliance_approval IS NULL OR TRIM(tic.sla_compliance_approval) = '' OR tic.sla_compliance_approval = 'nan' OR tic.sla_compliance_approval !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.sla_compliance_approval, 'MM/DD/YYYY HH24:MI:SS')
        END AS fecha_final_compliance,
        CASE
            WHEN tic.sla_contract_generated IS NULL OR TRIM(tic.sla_contract_generated) = '' OR tic.sla_contract_generated = 'nan' OR tic.sla_contract_generated !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.sla_contract_generated, 'MM/DD/YYYY HH24:MI:SS')
        END AS sla_contract_generated,
        CASE
            WHEN tic.sla_commercial_wa_group_created IS NULL OR TRIM(tic.sla_commercial_wa_group_created) = '' OR tic.sla_commercial_wa_group_created = 'nan' OR tic.sla_commercial_wa_group_created !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.sla_commercial_wa_group_created, 'MM/DD/YYYY HH24:MI:SS')
        END AS sla_commercial_wa_group_created,
        CASE
            WHEN tic.sla_commercial_presentation IS NULL OR TRIM(tic.sla_commercial_presentation) = '' OR tic.sla_commercial_presentation = 'nan' OR tic.sla_commercial_presentation !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.sla_commercial_presentation, 'MM/DD/YYYY HH24:MI:SS')
        END AS sla_commercial_presentation,
        CASE
            WHEN tic.sla_commercial_successful_contact IS NULL OR TRIM(tic.sla_commercial_successful_contact) = '' OR tic.sla_commercial_successful_contact = 'nan' OR tic.sla_commercial_successful_contact !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.sla_commercial_successful_contact, 'MM/DD/YYYY HH24:MI:SS')
        END AS sla_commercial_successful_contact,
        CASE
            WHEN tic.sla_contract_signed_on IS NULL OR TRIM(tic.sla_contract_signed_on) = '' OR tic.sla_contract_signed_on = 'nan' OR tic.sla_contract_signed_on !~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}.*' THEN NULL
            ELSE TO_TIMESTAMP(tic.sla_contract_signed_on, 'MM/DD/YYYY HH24:MI:SS')
        END AS sla_contract_signed_on,
        pu.fecha_primer_lead,
        pu.fecha_ultimo_lead,
        pu.id_primer_lead,
        pu.id_ultimo_lead,
        sl.suma_leads_30dias,
        sl.suma_contratos_30dias,
        pulc.fecha_primer_contrato,
        pulc.fecha_ultimo_contrato
    FROM
        playground.dl_tickets_jira_data AS tic
    LEFT JOIN
        playground.kua_diccionario_aliados AS map ON tic.agency_uuid = map.uuid_incorrecto
    LEFT JOIN
        PrimerosUltimosLeads AS pu ON COALESCE(map.uuid_correcto, tic.agency_uuid) = pu.agency_id
    LEFT JOIN
        SumaLeads AS sl ON COALESCE(map.uuid_correcto, tic.agency_uuid) = sl.agency_id
    LEFT JOIN
        PrimerosUltimosContratos AS pulc ON COALESCE(map.uuid_correcto, tic.agency_uuid) = pulc.agency_id
),
TicketsSheets AS (
    SELECT
        *,
        CASE
            WHEN agency_uuid IS NULL OR agency_uuid = 'nan' THEN 1
            ELSE ROW_NUMBER() OVER(
                PARTITION BY agency_uuid
                ORDER BY fecha_inicio DESC, issue_key
            )
        END as rn
    FROM OriginalTicketsSheets
),
AgenciasConTickets AS (
    SELECT DISTINCT agency_uuid
    FROM OriginalTicketsSheets
    WHERE agency_uuid IS NOT NULL
),
NoTicketsAgencies AS (
    SELECT
        nta.uuid as agency_uuid,
        'Sin Ticket' AS issue_key,
        NULL AS issue_type,
        NULL AS summary,
        NULL AS reporter,
        'Completed without Ticket' AS status,
        NULL AS assignee,
        NULL AS jira_state,
        NULL AS jira_name,
        NULL AS jira_group,
        NULL AS jira_email,
        NULL AS commercial_wa_group_created_completed,
        NULL AS commercial_presentation_completed,
        NULL AS commercial_successful_contact_completed,
        NULL AS commercial_owner,
        NULL AS data_last_status,
        NULL AS vendor_last_status,
        NULL AS legal_last_status,
        NULL AS training_last_status,
        CAST('2025-03-03' AS TIMESTAMP) AS fecha_inicio,
        CAST(NULL AS TIMESTAMP) AS fecha_inicio_data,
        CAST('2025-03-03' AS TIMESTAMP) AS fecha_final_data,
        CAST(NULL AS TIMESTAMP) AS fecha_inicio_vendor,
        CAST('2025-03-03' AS TIMESTAMP) AS fecha_final_vendor,
        CAST(NULL AS TIMESTAMP) AS fecha_inicio_legal,
        CAST('2025-03-03' AS TIMESTAMP) AS fecha_final_legal,
        CAST(NULL AS TIMESTAMP) AS fecha_inicio_training,
        CAST('2025-03-03' AS TIMESTAMP) AS fecha_final_training,
        CAST(NULL AS TIMESTAMP) AS sla_compliance_returned,
        CAST('2025-03-03' AS TIMESTAMP) AS fecha_final_compliance,
        CAST(NULL AS TIMESTAMP) AS sla_contract_generated,
        CAST(NULL AS TIMESTAMP) AS sla_commercial_wa_group_created,
        CAST(NULL AS TIMESTAMP) AS sla_commercial_presentation,
        CAST(NULL AS TIMESTAMP) AS sla_commercial_successful_contact,
        CAST(NULL AS TIMESTAMP) AS sla_contract_signed_on,
        pl.fecha_primer_lead,
        pl.fecha_ultimo_lead,
        pl.id_primer_lead,
        pl.id_ultimo_lead,
        sl.suma_leads_30dias,
        sl.suma_contratos_30dias,
        pulc.fecha_primer_contrato,
        pulc.fecha_ultimo_contrato
    FROM
        playground.dl_bases_kavak_aliados_no_tickets_agencies AS nta
    LEFT JOIN
        PrimerosUltimosLeads AS pl ON nta.uuid = pl.agency_id
    LEFT JOIN
        SumaLeads AS sl ON nta.uuid = sl.agency_id
    LEFT JOIN
        PrimerosUltimosContratos AS pulc ON nta.uuid = pulc.agency_id
    LEFT JOIN AgenciasConTickets AS act
        ON nta.uuid = act.agency_uuid
    WHERE
        act.agency_uuid IS NULL
),
FinalJoin AS (
    SELECT
        agency_uuid, issue_key, issue_type, summary, reporter, status, assignee, jira_state, jira_name, jira_group, jira_email, commercial_wa_group_created_completed, commercial_presentation_completed, commercial_successful_contact_completed, commercial_owner, data_last_status, vendor_last_status, legal_last_status, training_last_status, fecha_inicio, fecha_inicio_data, fecha_final_data, fecha_inicio_vendor, fecha_final_vendor, fecha_inicio_legal, fecha_final_legal, fecha_inicio_training, fecha_final_training, sla_compliance_returned, fecha_final_compliance, sla_contract_generated, sla_commercial_wa_group_created, sla_commercial_presentation, sla_commercial_successful_contact, sla_contract_signed_on, fecha_primer_lead, fecha_ultimo_lead, id_primer_lead, id_ultimo_lead, suma_leads_30dias, suma_contratos_30dias, fecha_primer_contrato, fecha_ultimo_contrato
    FROM TicketsSheets
    WHERE rn = 1
    UNION ALL
    SELECT
        agency_uuid, issue_key, issue_type, summary, reporter, status, assignee, jira_state, jira_name, jira_group, jira_email, commercial_wa_group_created_completed, commercial_presentation_completed, commercial_successful_contact_completed, commercial_owner, data_last_status, vendor_last_status, legal_last_status, training_last_status, fecha_inicio, fecha_inicio_data, fecha_final_data, fecha_inicio_vendor, fecha_final_vendor, fecha_inicio_legal, fecha_final_legal, fecha_inicio_training, fecha_final_training, sla_compliance_returned, fecha_final_compliance, sla_contract_generated, sla_commercial_wa_group_created, sla_commercial_presentation, sla_commercial_successful_contact, sla_contract_signed_on, fecha_primer_lead, fecha_ultimo_lead, id_primer_lead, id_ultimo_lead, suma_leads_30dias, suma_contratos_30dias, fecha_primer_contrato, fecha_ultimo_contrato
    FROM NoTicketsAgencies
)
SELECT
    fj.issue_key,
    fj.summary,
    fj.fecha_inicio,
    fj.fecha_final_data,
    fj.fecha_final_compliance,
    fj.fecha_final_legal,
    CASE
        WHEN fj.training_last_status = 'TRAINING COMPLETED' AND fj.fecha_final_training IS NULL THEN fj.fecha_inicio_training
        WHEN fj.training_last_status = 'TRAINING COMPLETED' AND fj.fecha_final_training IS NOT NULL THEN fj.fecha_final_training
        ELSE NULL
    END AS fecha_final_training,
    fj.fecha_primer_lead,
    DATEDIFF(minute, CAST(fj.fecha_inicio_data AS TIMESTAMP), CAST(fj.fecha_final_data AS TIMESTAMP)) / 1440.0 AS duracion_final_data_dias,
    DATEDIFF(minute, CAST(fj.fecha_final_data AS TIMESTAMP), CAST(fj.fecha_final_compliance AS TIMESTAMP)) / 1440.0 AS duracion_final_compliance_dias,
    DATEDIFF(minute, CAST(fj.fecha_inicio_legal AS TIMESTAMP), CAST(fj.fecha_final_legal AS TIMESTAMP)) / 1440.0 AS duracion_final_legal_dias,
        CASE
            WHEN fj.training_last_status = 'TRAINING COMPLETED' AND fj.fecha_final_training IS NULL THEN DATEDIFF(minute, CAST(fj.fecha_final_legal AS TIMESTAMP), CAST(fj.fecha_inicio_training AS TIMESTAMP)) / 1440.0
            WHEN fj.training_last_status = 'TRAINING COMPLETED' AND fj.fecha_final_training IS NOT NULL THEN DATEDIFF(minute, CAST(fj.fecha_inicio_training AS TIMESTAMP), CAST(fj.fecha_final_training AS TIMESTAMP)) / 1440.0
            ELSE NULL
        END AS duracion_final_training_dias,
    CASE
        WHEN fj.training_last_status = 'TRAINING COMPLETED' AND fj.fecha_final_training IS NULL THEN DATEDIFF(minute, CAST(fj.fecha_inicio AS TIMESTAMP), CAST(fj.fecha_inicio_training AS TIMESTAMP)) / 1440.0
        WHEN fj.training_last_status = 'TRAINING COMPLETED' AND fj.fecha_final_training IS NOT NULL THEN DATEDIFF(minute, CAST(fj.fecha_inicio AS TIMESTAMP), CAST(fj.fecha_final_training AS TIMESTAMP)) / 1440.0
        ELSE NULL
    END AS duracion_alta_agencia_dias,
    DATEDIFF(minute, CAST(fj.fecha_inicio AS TIMESTAMP), CAST(fj.fecha_primer_lead AS TIMESTAMP)) / 1440.0 AS duracion_hasta_primer_lead_dias,
    fj.agency_uuid,
    nex.nombre_completo_agencia as nombre_completos_agencia,
    fj.id_primer_lead as primer_lead,
    fj.id_ultimo_lead as ultimo_lead,
    fj.fecha_ultimo_lead,
    CASE
        WHEN fj.id_primer_lead IS NULL THEN TRUE
        WHEN fj.fecha_ultimo_lead < fc.treinta_dias_atras THEN TRUE
        ELSE FALSE
        END AS agencia_inactiva_30d,
    CASE
        WHEN fj.id_primer_lead IS NULL THEN TRUE
        WHEN fj.fecha_ultimo_lead < fc.noventa_dias_atras THEN TRUE
        ELSE FALSE
    END AS agencia_churn,
    nex.estado_f as estado,
    nex.manager_bo___territory_manager as territory_manager,
    nex.supervisor as supervisor_bo,
    nex.implant___kam_secundario as kam_secundario,
    nex.agente as agente_bo,
    nex.correo_1 as correo_dealer,
    nex.numero_1 as celular_dealer,
    nex.estatus_agencia as estatus_de_agencia,
    nex.razon_social,
    nex.direccion_agencia,
    CASE
        WHEN fj.training_last_status = 'TRAINING COMPLETED' AND fj.fecha_final_training IS NULL THEN DATEDIFF(minute, CAST(fj.fecha_inicio_training AS TIMESTAMP), CAST(fj.fecha_primer_lead AS TIMESTAMP)) / 1440.0
        WHEN fj.training_last_status = 'TRAINING COMPLETED' AND fj.fecha_final_training IS NOT NULL THEN DATEDIFF(minute, CAST(fj.fecha_final_training AS TIMESTAMP), CAST(fj.fecha_primer_lead AS TIMESTAMP)) / 1440.0
        ELSE NULL
    END AS duracion_training_hasta_primer_lead_dias,
    CASE
        WHEN fj.suma_leads_30dias IS NULL THEN FALSE
        WHEN fj.suma_leads_30dias > 5 THEN TRUE
        ELSE FALSE
    END AS agencia_mas_5_leads,
    CASE
        WHEN fj.suma_leads_30dias IS NULL THEN FALSE
        WHEN fj.suma_leads_30dias > 12 THEN TRUE
        ELSE FALSE
    END AS agencia_mas_12_leads,
    CASE
        WHEN fj.suma_contratos_30dias IS NULL THEN FALSE
        WHEN fj.suma_contratos_30dias > 0 THEN TRUE
        ELSE FALSE
    END AS agencia_con_contratos,
    fj.status,
    fj.sla_compliance_returned,
    fj.sla_commercial_presentation,
    fj.sla_contract_signed_on,
    fj.fecha_final_vendor,
    fj.sla_contract_generated,
    DATEDIFF(minute, CAST(fj.fecha_inicio_vendor AS TIMESTAMP), CAST(fj.fecha_final_vendor AS TIMESTAMP)) / 1440.0 AS duracion_final_vendor_dias,
    DATEDIFF(minute, CAST(fj.fecha_inicio_legal AS TIMESTAMP), CAST(fj.sla_contract_generated AS TIMESTAMP)) / 1440.0 AS duracion_contrato_generado,
    DATEDIFF(minute, CAST(fj.fecha_inicio_legal AS TIMESTAMP), CAST(fj.sla_contract_signed_on AS TIMESTAMP)) / 1440.0 AS duracion_contrato_firmado_dealer,
    fj.suma_leads_30dias,
    fj.suma_contratos_30dias,
    fj.sla_commercial_successful_contact,
    fj.fecha_primer_contrato,
    fj.fecha_ultimo_contrato
FROM FinalJoin AS fj
LEFT JOIN playground.kua_sensitive_ao_nexus AS nex ON fj.agency_uuid  = nex.agency_id
JOIN FechasCalculadas AS fc ON 1=1;
