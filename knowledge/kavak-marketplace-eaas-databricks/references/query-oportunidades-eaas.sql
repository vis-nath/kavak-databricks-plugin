-- =============================================================
-- OPORTUNIDADES EaaS — Autos de interés third_party + Alquiladora
-- Fuentes:
--   · opportunity + car_of_interest + vehicle + account (SF)
--   · inventory_history → primera fecha de publicación por stock
--   · accounting_entry → stocks comprados a Alquiladora
--
-- Filtros clave:
--   · opp.countryname__c = '484' (México)
--   · recordtype_name__c IN ('Venta','SalesAllies')
--   · vehicle.type__c = 'third_party' OR stock en Alquiladora
--   · SOLO retail: b2b__c = 'false'
--   · interés creado >= primera publicación del stock
--   · Excluir emails internos Kavak
--
-- Ajustar rango de fecha en WHERE: fecha_creacion_opp BETWEEN ... AND ...
-- =============================================================

WITH
stocks_alquiladora AS (
    SELECT DISTINCT TRIM(SPLIT_PART(item_name, ' ', 2)) AS stock_id
    FROM serving.accounting_entry
    WHERE subsidiary_name = 'UVI TECH SAPI MEXICO'
      AND account_number = '116-001'
      AND accounting_entry_transaction_type = 'Item Receipt'
      AND item_name LIKE 'AUTO %'
      AND entity_full_name ILIKE '%ALQUILADORA DE VEHICUL%'
      AND TRIM(SPLIT_PART(item_name, ' ', 2)) <> ''
),

first_publication AS (
    SELECT stock_id, MIN(inventory_date)::date AS first_published_date
    FROM serving.inventory_history
    WHERE flag_published = 1
      AND country_iso   = 'MX'
      AND stock_id IS NOT NULL
    GROUP BY stock_id
),

opps_con_autos AS (
    SELECT
        opp.id                              AS opp_id,
        opp.createddate::date               AS fecha_creacion_opp,
        opp.closedate::date                 AS closedate,
        opp.accountid                       AS account_id,
        opp.recordtype_name__c,
        opp.stagename,
        opp.name                            AS opportunity_name,
        opp.type                            AS opp_type,
        opp.b2b__c                          AS opp_b2b_flag,
        opp.stockid__c                      AS stockid_principal,
        opp.step__c                         AS opp_step,
        opp.initialstep__c                  AS opp_initial_step,
        coi.id                              AS car_interest_id,
        coi.carname__c                      AS carname_interes,
        coi.vehicle__c,
        coi.extid__c,
        coi.createddate::date               AS fecha_interes_auto,
        v.stockid__c                        AS stock_id_auto_interes,
        v.type__c                           AS vehicle_type__c,
        v.carmake__c                        AS make_interes,
        v.model__c                          AS model_interes,
        v.modelyear__c                      AS year_interes,
        v.vin__c                            AS vin_interes,
        acc.name                            AS account_name,
        acc.email__c                        AS email_opp,
        acc.phone                           AS phone_opp
    FROM salesforce_latam_refined.opportunity      AS opp
    JOIN salesforce_latam_refined.car_of_interest  AS coi ON coi.opportunity__c = opp.id
    LEFT JOIN salesforce_latam_refined.vehicle     AS v   ON v.id = coi.vehicle__c
    LEFT JOIN salesforce_latam_refined.account     AS acc ON acc.id = opp.accountid
    WHERE opp.countryname__c = '484'
      AND opp.recordtype_name__c IN ('Venta', 'SalesAllies')
      AND coi.extid__c IS NOT NULL
      AND coi.extid__c <> ''
)

SELECT
    oca.opp_id,
    oca.fecha_creacion_opp,
    oca.fecha_interes_auto,
    fp.first_published_date AS fecha_publicacion_stock,
    oca.account_id,
    oca.stagename,
    oca.opp_type,
    oca.stockid_principal,
    oca.opp_initial_step,
    oca.stock_id_auto_interes,
    oca.vehicle_type__c,
    oca.account_name,
    oca.email_opp,
    oca.phone_opp
FROM opps_con_autos oca
LEFT JOIN stocks_alquiladora sa  ON TRIM(oca.stock_id_auto_interes) = sa.stock_id
LEFT JOIN first_publication fp   ON fp.stock_id = oca.stock_id_auto_interes
WHERE
    (LOWER(oca.vehicle_type__c) = 'third_party' OR sa.stock_id IS NOT NULL)
  AND COALESCE(oca.opp_b2b_flag, 'false') = 'false'
  AND oca.fecha_creacion_opp BETWEEN DATE '2026-01-01' AND DATE '2026-03-31'
  AND (fp.first_published_date IS NULL OR oca.fecha_interes_auto >= fp.first_published_date)
  AND (
        oca.email_opp IS NULL
        OR (
             LOWER(oca.email_opp) NOT LIKE '%@kavak.com'
         AND LOWER(oca.email_opp) NOT IN (
                'alberto.tovar@kavak.com','asdbr@gmail.com','eduardo.chavez@kavak.com',
                'eduardo.torres@kavak.com','eduardo.torres+123@kavak.com',
                'eduardostorresu@gmail.com','gustavo.falco+123@kavak.com',
                'isain.sosa+23422349885769834785342342@kavak.com','juan.silva@kavak.com',
                'luis.castellanos@kavak.com','manuel.lopezp+123@kavak.com',
                'ricardo.sue@kavak.com','usuariokaliados@gmail.com','andres.mejias@kavak.com',
                'ventas2@autosicar.com.mx','eduardo.torres+1233212@kavak.com',
                'eduardo.torres+999999@kavak.com','eduardo.torres+9999999@kavak.com',
                'eduardo.torres+898989@kavak.com','angela.delduca+061001@kavak.com',
                'eduardo.torres+10101010@kavak.com'
            )
        )
      )
ORDER BY oca.fecha_interes_auto DESC, oca.opp_id, oca.stock_id_auto_interes
LIMIT 400000
