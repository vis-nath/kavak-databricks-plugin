-- ============================================================
-- EXPEDIENTES / VEHICLE FILES — query-vehicle-files.sql
-- Fuente: Redshift (vehicle_documentation_api_global_refined)
-- Una fila por asset_id (último expediente por ROW_NUMBER).
-- Columnas clave: asset_id, ultimo_estatus_expediente,
--   fecha_ultimo_estatus, expediente_aprobado, expediente_rechazado,
--   expediente_reiniciado, expediente_condicionado, expediente_abierto
-- Join con funnel: ON asset_id = query-kuna-funnel.asset_id
-- ============================================================
WITH
    DocumentosCerrados AS (
        SELECT
            id,
            asset_id,
            CONVERT_TIMEZONE('UTC', 'America/Mexico_City', created_at) AS created_at,
            CONVERT_TIMEZONE('UTC', 'America/Mexico_City', event_date) AS event_date,
            status,
            JSON_EXTRACT_PATH_TEXT(version_data, 'condition') AS condition
        FROM
            vehicle_documentation_api_global_refined.documents
    ),
    UltimaRevisionPorDocumento AS (
        SELECT
            r.document_id,
            r.status,
            r.reason,
            r.comment,
            CONVERT_TIMEZONE('UTC', 'America/Mexico_City', r.created_at) AS created_at,
            CONVERT_TIMEZONE('UTC', 'America/Mexico_City', r.reviewed_at) AS reviewed_at,
            ROW_NUMBER() OVER(PARTITION BY r.document_id ORDER BY r.reviewed_at DESC, r.created_at DESC) as rn
        FROM
            vehicle_documentation_api_global_refined.review_requests r
        INNER JOIN DocumentosCerrados dc ON r.document_id = dc.id
    ),
    PrimeraRevisionPorDocumento AS (
        SELECT
            r.document_id,
            r.requested_by,
            r.reviewed_by,
            CONVERT_TIMEZONE('UTC', 'America/Mexico_City', r.reviewed_at) AS reviewed_at,
            CONVERT_TIMEZONE('UTC', 'America/Mexico_City', r.created_at) AS created_at,
            ROW_NUMBER() OVER(PARTITION BY r.document_id ORDER BY r.reviewed_at ASC, r.created_at ASC) as rn_first
        FROM
            vehicle_documentation_api_global_refined.review_requests r
        INNER JOIN DocumentosCerrados dc ON r.document_id = dc.id
    ),
    UltimoEventoHistorial AS (
        SELECT
            e.document_id,
            e.event_name,
            e.event_data,
            CONVERT_TIMEZONE('UTC', 'America/Mexico_City', e.created_at) AS created_at,
            CONVERT_TIMEZONE('UTC', 'America/Mexico_City', e.event_date) AS event_date,
            ROW_NUMBER() OVER(PARTITION BY e.document_id ORDER BY e.event_date DESC, e.created_at DESC) as rn
        FROM
            vehicle_documentation_api_global_refined.document_events e
        INNER JOIN DocumentosCerrados dc ON e.document_id = dc.id
    ),
    ExpedientesConsolidados AS (
        SELECT
            dc.id as id_expediente,
            dc.asset_id,
            dc.condition as car_condition,
            dc.created_at,
            urd.reviewed_at as fecha_ultima_revision,
            dc.event_date as fecha_ultimo_estatus_vanilla,
            dc.status as estatus_vanilla_expediente,
            urd.status as ultimo_estatus_revision,
            urd.reason as razon_ultima_revision,
            urd.comment as comentario_ultima_revision,
            prp.requested_by as first_requested_by,
            prp.reviewed_by as first_reviewed_by,
            CASE
                WHEN (dc.status = 'CLOSED' AND urd.status = 'APPROVED') THEN TRUE
                WHEN (dc.status = 'CLOSED' AND urd.status = 'PENDING' AND ueh.event_name = 'APPROVE') THEN TRUE
                ELSE FALSE
            END AS expediente_aprobado,
            CASE
                WHEN (dc.status = 'CLOSED' AND urd.status IN ('REJECT', 'REJECTED')) THEN TRUE
                ELSE FALSE
            END AS expediente_rechazado,
            ueh.event_name as ultimo_registro_evento,
            ueh.event_data as comentario_ultimo_registro_evento,
            (dc.status = 'CANCELLED' AND ueh.event_name = 'RESET') AS expediente_reiniciado,
            (dc.status IN ('IN_REVIEW', 'OPEN')) AS expediente_abierto,
            (urd.status = 'CONDITIONED') AS expediente_condicionado
        FROM DocumentosCerrados dc
        LEFT JOIN UltimaRevisionPorDocumento urd ON dc.id = urd.document_id AND urd.rn = 1
        LEFT JOIN PrimeraRevisionPorDocumento prp ON dc.id = prp.document_id AND prp.rn_first = 1
        LEFT JOIN UltimoEventoHistorial ueh ON dc.id = ueh.document_id AND ueh.rn = 1
    ),
    TablaExpedientes AS (
        SELECT
            *,
            CASE
                WHEN expediente_abierto AND expediente_condicionado THEN 'Expediente Condicionado'
                WHEN expediente_abierto THEN 'Expediente en revision'
                WHEN expediente_aprobado THEN 'Expediente Aprobado'
                WHEN expediente_reiniciado THEN 'Expediente Reiniciado'
                WHEN expediente_rechazado AND NOT expediente_reiniciado THEN 'Expediente Rechazado sin Reinicio'
                ELSE 'Expediente Cancelado'
            END AS ultimo_estatus_expediente,
            ROW_NUMBER() OVER(PARTITION BY asset_id ORDER BY created_at DESC) as rank_num
        FROM ExpedientesConsolidados
    )
SELECT asset_id, ultimo_estatus_expediente, fecha_ultimo_estatus_vanilla AS fecha_ultimo_estatus
FROM TablaExpedientes
WHERE rank_num = 1;
