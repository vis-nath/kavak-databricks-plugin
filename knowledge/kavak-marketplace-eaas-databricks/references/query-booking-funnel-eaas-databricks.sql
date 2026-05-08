-- queries/bookings_databricks.sql — alineado al pipeline canónico Helio v3
-- Adaptaciones vs Redshift:
--   · Tablas con prefijo prd_refined / prd_serving / prd_datamx_serving
--   · Email cliente: r.emailopp__c (texto plano en DB, account.email__c enmascarado)
--   · finance.accounting_entry.entity_full_name ENMASCARADA — workaround:
--       · flag_kavak_bought general → inventory_transactions_netsuite_global
--         (todas las compras Kavak, sin distinguir supplier — vol. comparable a RS)
--       · flag_alquiladora → proxy con dealer.name='Carshop' del seller_api
--       · flag_element → NO identificable en DB; queda 0 (gap aceptado, ~3% bookings,
--         categoría 'Aliados', no afecta KPI EaaS BU)
--   · json_extract_path_text → get_json_object('$.vin')
--   · CONVERT_TIMEZONE → from_utc_timestamp

WITH
-- Alquiladora (Carshop) — proxy con dealer.name='Carshop*' en seller_api
stocks_alquiladora AS (
    SELECT DISTINCT vs.legacy_stock_id AS stock_id
    FROM prd_refined.seller_api_global_refined.variant_stock vs
    JOIN prd_refined.seller_api_global_refined.variant_availability va
        ON va.id = vs.variant_availability_id
    JOIN prd_refined.seller_api_global_refined.availability_zone az
        ON az.id = va.availability_zone_id
    JOIN prd_refined.seller_api_global_refined.company co
        ON co.id = az.company_id
    JOIN prd_refined.seller_api_global_refined.dealer d
        ON d.id = co.dealer_id
    WHERE LOWER(va.business_module) = 'third_party'
      AND d.name LIKE 'Carshop%'
      AND vs.legacy_stock_id IS NOT NULL
),
-- Element: no identificable en Databricks. Empty placeholder.
stocks_element AS (
    SELECT CAST(NULL AS STRING) AS stock_id WHERE 1=0
),
-- flag_kavak_bought general — desde inventory_transactions (incluye Alq + Element + retail)
-- Nota: incluye más stocks que Alquiladora+Element del Redshift canónico, pero el filtro
-- de universo (con se/ve flags) limita correctamente al canal EaaS.
stocks_kavak_bought AS (
    SELECT DISTINCT CAST(CAST(stock_id AS BIGINT) AS STRING) AS stock_id
    FROM prd_serving.inventory.inventory_transactions_netsuite_global
    WHERE subsidiary_name = 'UVI TECH SAPI MEXICO'
      AND account_name = 'AUTOS'
      AND transaction_type = 'Item Receipt'
      AND stock_id IS NOT NULL
),
stocks_eaas AS (
    SELECT DISTINCT v.stockid__c AS stock_id
    FROM prd_refined.salesforce_latam_refined.vehicle v
    WHERE v.stockid__c IS NOT NULL AND TRIM(v.stockid__c) <> ''
      AND LOWER(v.type__c) = 'third_party'
),
vins_eaas AS (
    SELECT DISTINCT get_json_object(va.details, '$.vin') AS vin
    FROM prd_refined.seller_api_global_refined.variant_availability va
    WHERE LOWER(va.business_module) = 'third_party'
      AND get_json_object(va.details, '$.vin') IS NOT NULL
      AND get_json_object(va.details, '$.vin') <> ''
),
deliveries_vt AS (
    SELECT
        vt.opportunity__c                                              AS opp_id,
        DATE(from_utc_timestamp(e.activitydatetime, 'America/Mexico_City')) AS fecha_entrega_vt,
        e.appointmenthubname__c                                         AS hub_name
    FROM prd_refined.salesforce_latam_refined.vehicletransfer__c vt
    LEFT JOIN prd_refined.salesforce_latam_refined.event e
        ON e.id = vt.eventid__c
        AND (e.isdeleted IS NULL OR e.isdeleted = 'false')
    WHERE (vt.isdeleted IS NULL OR vt.isdeleted = 'false')
      AND vt.status__c = 'Complete'
      AND vt.country__c = '484'
),
bh_data AS (
    SELECT
        bh.opportunity_id                          AS opp_id,
        DATE(bh.fecha_completada)                  AS bh_fecha_completada,
        DATE(bh.fecha_entrega)                     AS bh_fecha_entrega,
        DATE(bh.fecha_cancelacion_reserva)         AS fecha_cancelacion,
        DATE(bh.devolucion_date)                   AS devolucion_date,
        ROW_NUMBER() OVER (
            PARTITION BY bh.opportunity_id
            ORDER BY COALESCE(bh.fecha_completada, bh.fecha_entrega) DESC NULLS LAST
        ) AS rn
    FROM prd_datamx_serving.serving.bookings_history bh
    WHERE bh.opportunity_id IS NOT NULL
)

SELECT
    r.id                                                            AS reservation_id,
    r.opportunityid__c                                              AS opp_id,
    r.bookingid__c                                                  AS booking_id,
    r.stockid__c                                                    AS stock_id,
    DATE(from_utc_timestamp(r.createddate, 'America/Mexico_City'))  AS fecha_reserva,
    r.status__c                                                     AS reservation_status,
    r.transactiontype__c                                            AS transaction_type,
    r.iscarswap__c                                                  AS is_car_swap,
    r.origin__c                                                     AS origin,
    r.inventoryprice__c                                             AS inventory_price,

    LOWER(TRIM(r.emailopp__c))                                      AS customer_email,

    v.vin__c                                                        AS vin,
    v.carmake__c                                                    AS make,
    v.model__c                                                      AS model,
    v.modelyear__c                                                  AS year,
    v.version__c                                                    AS version,
    v.kmtachometer__c                                               AS km,
    v.type__c                                                       AS vehicle_type,

    opp.stagename                                                   AS opp_stagename,
    DATE(opp.closedate)                                             AS opp_closedate,
    COALESCE(opp.b2b__c, 'false')                                   AS opp_b2b_flag,
    opp.carpaymentmethod__c                                         AS opp_carpaymentmethod_code,
    CASE opp.carpaymentmethod__c
         WHEN '141' THEN 'Contado'
         WHEN '143' THEN 'Financiamiento'
         ELSE NULL
    END                                                             AS metodo_pago,

    CASE WHEN sa.stock_id IS NOT NULL THEN 1 ELSE 0 END             AS flag_alquiladora,
    CASE WHEN el.stock_id IS NOT NULL THEN 1 ELSE 0 END             AS flag_element,
    CASE WHEN kb.stock_id IS NOT NULL THEN 1 ELSE 0 END             AS flag_kavak_bought,
    CASE WHEN se.stock_id IS NOT NULL THEN 1 ELSE 0 END             AS flag_eaas_third_party,
    CASE WHEN ve.vin      IS NOT NULL THEN 1 ELSE 0 END             AS flag_vin_seller_api,

    COALESCE(
        ent.fecha_entrega_vt,
        bh.bh_fecha_completada,
        bh.bh_fecha_entrega,
        CASE WHEN opp.stagename ILIKE '%Closed Won%'
              OR opp.stagename ILIKE '%Cerrada Ganada%'
             THEN DATE(opp.closedate)
        END
    )                                                                AS fecha_entrega,
    CASE WHEN ent.fecha_entrega_vt IS NOT NULL THEN 1 ELSE 0 END     AS tiene_vt_complete,
    ent.hub_name                                                     AS hub_entrega_v2,
    bh.fecha_cancelacion                                             AS fecha_cancelacion,
    bh.devolucion_date                                               AS devolucion_date

FROM prd_refined.salesforce_latam_refined.reservation__c r
LEFT JOIN prd_refined.salesforce_latam_refined.opportunity opp ON opp.id = r.opportunityid__c
LEFT JOIN prd_refined.salesforce_latam_refined.vehicle    v   ON v.id  = r.vehicle__c
LEFT JOIN deliveries_vt                                   ent ON ent.opp_id = r.opportunityid__c
-- bh_data join: the bookings_history.opportunity_id is string already
LEFT JOIN bh_data                                         bh  ON bh.opp_id  = r.opportunityid__c AND bh.rn = 1
LEFT JOIN stocks_alquiladora                              sa  ON sa.stock_id = r.stockid__c
LEFT JOIN stocks_element                                  el  ON el.stock_id = r.stockid__c
LEFT JOIN stocks_kavak_bought                             kb  ON kb.stock_id = r.stockid__c
LEFT JOIN stocks_eaas                                     se  ON se.stock_id = r.stockid__c
LEFT JOIN vins_eaas                                       ve  ON ve.vin = v.vin__c

WHERE r.country__c = '484'
  AND (r.isdeleted IS NULL OR r.isdeleted = 'false')
  AND r.createddate >= TIMESTAMP '2025-07-01 00:00:00'
  -- Universo EaaS: stock SC (third_party actual o VIN ex-third_party)
  -- En DB usamos vins_eaas (ve) en lugar de la unión Alquiladora+Element del canónico
  -- porque entity_full_name está enmascarado y no podemos identificar Element directamente.
  AND (
      r.stockid__c LIKE '100%'      -- stock SC actual
      OR se.stock_id IS NOT NULL    -- third_party en SF vehicle
      OR ve.vin IS NOT NULL         -- VIN estuvo alguna vez en SC como third_party (caso FBK)
  )
  AND (r.emailopp__c IS NULL OR (
      LOWER(r.emailopp__c) NOT LIKE '%@kavak.com'
      AND LOWER(r.emailopp__c) NOT IN (
          'asdbr@gmail.com','eduardostorresu@gmail.com','usuariokaliados@gmail.com',
          'ventas2@autosicar.com.mx'
      )
  ))

ORDER BY r.createddate
LIMIT 400000
