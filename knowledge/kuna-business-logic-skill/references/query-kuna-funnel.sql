-- ============================================================
-- KUNA FUNNEL PRINCIPAL — query-kuna-funnel.sql
-- Fuente: Redshift (playground schema)
-- Una fila por lead. Incluye KAVAK ALIADOS, HERTZ, ECOMERCEAAS.
-- Columnas clave: id_lead, fecha_origen, criterios_proyecto,
--   consultado, preaprobado, aprobado, rechazado,
--   oldest_contract_date, nombre_agencia, correo_agente,
--   kuna_account_manager, profile_status_tier, etapa_lead,
--   etapa_formalizacion, agencia_prioritaria
-- Filtrar por fecha: WHERE fecha_origen BETWEEN '<start>' AND '<end>'
-- Filtrar por agencia: AND nombre_agencia = '<agencia>'
-- ============================================================
WITH
    leads_data_corregida AS (
        SELECT
            COALESCE(map.uuid_correcto, kl.agency_id) AS agency_id_final,
            kl.*
        FROM
            playground.kua_sensitive_leads_data AS kl
        LEFT JOIN
            playground.kua_diccionario_aliados AS map ON kl.agency_id = map.uuid_incorrecto
        WHERE kl.criterios_proyecto IN ('KAVAK ALIADOS', 'HERTZ', 'ECOMERCEAAS') OR kl.asset_id IN ('10298236', '10301015') OR (agency_id_final IN ('7996f7e8-1797-4f41-9131-176f7134b68c', 'c1c6ed1e-6964-4cb4-8dda-e90fa7538360', '86367eb8-b82c-4b15-9dd0-612f1fb2f820', '7da43cf6-6b6c-42e1-9855-5a1b72d4c031', 'ad9e6a24-8e2d-47b1-840b-f9e7837a7c6c', 'f9e939a4-5ed2-4ad6-b79c-0410bb180538', '9c2eb6a7-85c5-4267-860c-aa512ff6c324') AND kl.criterios_proyecto != 'CREDITARIA')
    ),
    contratos_formalizacion AS (
        SELECT
            kfs.*
        FROM
            leads_data_corregida ldc
        JOIN
            playground.kua_sensitive_formalization_summary kfs
        ON
            ldc.contract_id = kfs.contract_id
        WHERE
            ldc.statement != 'CANCELLED' or ldc.statement is NULL
    ),
    funnel_columns AS (
    SELECT
        ld.*,
        (ld.profiling_status IS NOT NULL) AS consultado,
        (ld.profiling_status IS NOT NULL AND RIGHT(ld.profiling_status, 1) != 'R') AS preaprobado,
        (
            ld.first_non_cancelled_or_entered_new_value = 'REJECTED'
            OR ld.statement = 'REJECTED'
        ) AS rechazado,
        (
            (
                ld.first_non_cancelled_or_entered_new_value IS NOT NULL
                AND ld.first_non_cancelled_or_entered_new_value = 'APPROVED'
            )
            OR (
                ld.statement IS NOT NULL
                AND ld.statement = 'APPROVED'
            )
        ) AS aprobado,
        ld.names || ' ' || ld.surnames AS cliente_nombre_completo,
        cf.invoice_registered_first_date,
        cf.invoice_registered_last_date,
        cf.invoice_validated_first_date,
        cf.invoice_validated_last_date,
        cf.pennytest_transferred_first_date,
        cf.pennytest_transferred_last_date,
        cf.pennytest_validated_first_date,
        cf.pennytest_validated_last_date,
        cf.contract_generated_first_date,
        cf.contract_generated_last_date,
        cf.oldest_contract_date,
        cf.latest_contract_date,
        cf.three_lights_validated_first_date,
        cf.three_lights_validated_last_date,
        cf.second_key_validated_first_date,
        cf.second_key_validated_last_date,
        cf.gps_validated_first_date,
        cf.gps_validated_last_date,
        cf.vobo_first_date,
        cf.vobo_last_date,
        cf.bill_first_date,
        cf.bill_last_date,
        cf.payment_disbursed_first_date,
        cf.payment_disbursed_last_date,
        cf.marca,
        cf.cdyear,
        cf.cdmodel,
        cf.version,
        cf.vin,
        cf.price,
        cf.cdcondition,
        cf.pol_policy_number,
        cf.pol_broker,
        cf.pol_insurer,
        cf.aseguradora_anzen,
        cf.pol_gross_cost_raw,
        cf.poliza_seguro_impuestos,
        cf.marca || '-' || cf.cdmodel || '-' || cf.cdyear AS automovil,
        (cf.invoice_validated_last_date IS NOT NULL) AS enganche_validado,
        (cf.pennytest_validated_last_date IS NOT NULL) AS penny_test_validado,
        (cf.contract_generated_last_date IS NOT NULL) AS contrato_generado,
        (cf.latest_contract_date IS NOT NULL) AS contrato_firmado,
        (cf.three_lights_validated_last_date IS NOT NULL) AS validacion_tres_luces,
        (cf.bill_last_date IS NOT NULL) AS bill_payment,
        (cf.payment_disbursed_last_date IS NOT NULL) AS auto_dispersado,
        (cf.second_key_validated_last_date IS NOT NULL) AS segunda_llave,
        (cf.gps_validated_last_date IS NOT NULL) AS gps_validado,
        (cf.vobo_last_date IS NOT NULL) AS vobo,
        nex.grupo as grupo_nexus,
        nex.nombre_completo_agencia AS nombre_agencia,
        nex.estado_f,
        nex.direccion_agencia,
        nex.codigo_postal_f,
        nex.proyecto_f,
        nex.link_vendor,
        nex.vendor_id,
        nex.estatus_agencia,
        nex.es_baja,
        nex.nombre_contacto_1,
        nex.puesto_1,
        nex.correo_1,
        nex.numero_1,
        nex.razon_social,
        nex.cuenta_bancaria,
        nex.banco,
        nex.rfc as rfc_dealer,
        nex.agente,
        nex.correo_agente,
        nex.implant___kam_secundario AS kam_secundario,
        nex.correo_implant___kam_secundario AS email_kam_secundario,
        nex.supervisor AS account_lead,
        nex.correo_supervisor___lead_account AS email_account_lead,
        nex.manager_bo___territory_manager AS territory_manager,
        nex.correo_manager_bo___territory_manager AS email_territory_manager,
        nex.head_de_alianza,
        nex.correo_head_de_alianza
    FROM leads_data_corregida AS ld
    LEFT JOIN contratos_formalizacion AS cf
        ON ld.contract_id = cf.contract_id
       AND (ld.statement != 'CANCELLED' OR ld.statement IS NULL)
    LEFT JOIN playground.kua_sensitive_ao_nexus AS nex ON ld.agency_id_final = nex.agency_id
)
SELECT
    f.*,
    CASE
        WHEN f.statement = 'CANCELLED' THEN 'Cerrado'
        WHEN f.auto_dispersado THEN 'Auto desembolsado'
        WHEN f.contrato_firmado THEN 'Pendiente de desembolso'
        WHEN f.enganche_validado THEN 'Pendiente de contrato'
        WHEN f.aprobado THEN 'Pendiente de pago inicial'
        WHEN f.rechazado THEN 'Rechazado'
        WHEN f.preaprobado THEN 'Pendiente de documentación'
        WHEN NOT f.consultado THEN 'Pendiente de NIP'
        WHEN NOT f.preaprobado THEN 'Rechazado en consulta NIP'
        ELSE NULL
    END AS etapa_lead,
    CASE
        WHEN f.statement = 'CANCELLED' AND f.aprobado THEN 'Cerrado'
        WHEN f.auto_dispersado AND f.gps_validado AND f.segunda_llave AND f.vobo THEN 'Dispersado con Vobo'
        WHEN f.auto_dispersado AND NOT f.validacion_tres_luces AND f.fecha_incode IS NULL THEN 'Pendiente de Tres Luces'
        WHEN f.auto_dispersado AND NOT f.segunda_llave THEN 'Pendiente de Segunda Llave'
        WHEN f.auto_dispersado AND NOT f.gps_validado THEN 'Pendiente de GPS'
        WHEN f.auto_dispersado THEN 'Auto Dispersado si Vobo'
        WHEN f.contrato_firmado THEN 'Pendiente de Dispersion'
        WHEN f.statement = 'INCOMPLETE_DOCUMENT' AND f.aprobado THEN 'Expediente Condicionado'
        WHEN f.statement IS NULL AND f.aprobado THEN 'Aprobado Reiniciado'
        WHEN f.enganche_validado AND f.last_non_cancelled_or_entered_value_date > f.invoice_validated_last_date THEN 'Aprobado Reiniciado'
        WHEN f.enganche_validado THEN 'Pendiente de Firma de Contrato'
        WHEN NOT f.enganche_validado AND f.aprobado THEN 'Pendiente de Enganche'
        ELSE NULL
    END AS etapa_formalizacion,
    CASE
        WHEN ap.agency_uuid IS NOT NULL THEN ap.profile_category
        ELSE 'Agencia no Prioritaria'
    END AS agencia_prioritaria,
    CASE
        WHEN f.profiling_status LIKE '%_R' THEN 'Rechazados'
        WHEN f.profiling_status LIKE 'A%X' THEN 'Tier X'
        WHEN f.profiling_status LIKE 'A%' THEN 'Tier A'
        WHEN f.profiling_status LIKE 'B%' THEN 'Tier B'
        WHEN f.profiling_status LIKE 'C%' THEN 'Tier C'
        WHEN f.profiling_status LIKE 'D%' THEN 'Tier D'
        WHEN f.profiling_status LIKE 'E%' THEN 'Tier E'
        WHEN f.profiling_status LIKE 'F%' THEN 'Tier F'
        WHEN f.profiling_status LIKE '%_A' THEN 'Tier A'
        ELSE f.profiling_status
    END AS profile_status_tier
FROM funnel_columns AS f
LEFT JOIN playground.webapp_agencias_perfiladas AS ap
    ON f.agency_id_final = ap.agency_uuid;
