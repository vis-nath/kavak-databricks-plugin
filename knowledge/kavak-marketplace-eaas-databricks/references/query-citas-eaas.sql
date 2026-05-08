-- =============================================================
-- CITAS EaaS — Eventos tipo "Cita Auto%" en HUB para stocks EaaS
-- Fuentes:
--   · event (SF) → e.event_recordtype__c = 'AppointmentInHUB'
--                   e.type ILIKE 'Cita Auto%'
--                   e.status__c IN ('Scheduled','ConfirmedAppointment')
--   · opportunity + car_of_interest + vehicle + account (SF)
--   · inventory_history → primera publicación por stock
--   · accounting_entry → stocks Alquiladora
--
-- Filtros clave:
--   · Solo retail (b2b__c = 'false')
--   · vehicle.type__c = 'third_party' OR stock Alquiladora
--   · 1 cita por (opp_id, stockid__c) priorizando ConfirmedAppointment
--   · Excluir emails internos Kavak
--
-- Ajustar rango en: e.createddate::date BETWEEN ... AND ...
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
        opp.accountid                       AS account_id,
        opp.recordtype_name__c,
        opp.stagename,
        opp.b2b__c                          AS opp_b2b_flag,
        coi.id                              AS car_interest_id,
        coi.createddate::date               AS fecha_interes_auto,
        coi.vehicle__c,
        coi.extid__c,
        v.stockid__c                        AS stock_id_auto_interes,
        v.type__c                           AS vehicle_type__c,
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
),

citas_eventos AS (
    SELECT
        e.id                        AS event_id,
        e.opportunity__c            AS opp_id,
        e.createddate::date         AS createddate,
        e.activitydatetime::date    AS activitydatetime,
        e.confirmationdate__c::date AS confirmationdate__c,
        e.type,
        e.status__c,
        e.accountid,
        e.stockid__c,
        e.appointmenthubname__c,
        oca.vehicle_type__c,
        oca.opp_b2b_flag,
        fp.first_published_date,
        oca.account_name,
        oca.email_opp,
        oca.phone_opp,
        ROW_NUMBER() OVER (
            PARTITION BY e.opportunity__c, e.stockid__c
            ORDER BY
                CASE e.status__c
                    WHEN 'ConfirmedAppointment' THEN 2
                    WHEN 'Scheduled'            THEN 1
                    ELSE 0
                END DESC,
                e.activitydatetime DESC,
                e.createddate DESC,
                e.id DESC
        ) AS rn
    FROM opps_con_autos oca
    LEFT JOIN stocks_alquiladora sa ON TRIM(oca.stock_id_auto_interes) = sa.stock_id
    LEFT JOIN first_publication fp  ON fp.stock_id = oca.stock_id_auto_interes
    JOIN salesforce_latam_refined.event e
      ON e.opportunity__c = oca.opp_id
     AND e.stockid__c     = oca.stock_id_auto_interes
    WHERE
        (LOWER(oca.vehicle_type__c) = 'third_party' OR sa.stock_id IS NOT NULL)
      AND COALESCE(oca.opp_b2b_flag, 'false') = 'false'
      AND (fp.first_published_date IS NULL OR oca.fecha_interes_auto >= fp.first_published_date)
      AND e.event_recordtype__c = 'AppointmentInHUB'
      AND e.type ILIKE 'Cita Auto%'
      AND e.status__c IN ('Scheduled', 'ConfirmedAppointment')
      AND e.createddate::date BETWEEN DATE '2026-01-01' AND DATE '2026-03-31'
      AND (e.isdeleted IS NULL OR e.isdeleted = 'false')
)

SELECT
    event_id, opp_id, createddate, activitydatetime, confirmationdate__c,
    status__c, accountid, stockid__c, appointmenthubname__c,
    vehicle_type__c, first_published_date,
    account_name, email_opp, phone_opp
FROM citas_eventos
WHERE rn = 1
  AND (
        email_opp IS NULL
        OR (
             LOWER(email_opp) NOT LIKE '%@kavak.com'
         AND LOWER(email_opp) NOT IN (
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
ORDER BY createddate DESC, opp_id, stockid__c
LIMIT 400000
