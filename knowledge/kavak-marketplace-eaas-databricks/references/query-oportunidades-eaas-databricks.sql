-- queries/oportunidades_databricks.sql
-- Oportunidades únicas por stock_id y fecha — para gráfica semanal en modal.
-- Filtra solo oportunidades EaaS (b2b=false, third_party, país MX).
-- Versión Databricks de oportunidades.sql.
-- {stock_ids_quoted} reemplazado en Python como lista de STRINGs entre comillas.
--
-- Cambios vs Redshift:
--   · Schema: salesforce_latam_refined.* → prd_refined.salesforce_latam_refined.*
--   · DATEADD(month, -12, CURRENT_DATE) → ADD_MONTHS(CURRENT_DATE(), -12)
--   · ::date    → DATE(...)
--   · ::INTEGER → CAST(... AS BIGINT)
--   · account.email__c (enmascarado en DB) → reemplazado por opp.leademail__c
--     (texto plano, verificado en diccionario línea 30). Igual semántica:
--     filtra @kavak.com y emails internos. NO se usa el join a `account`.

WITH opps_con_autos AS (
    SELECT
        v.stockid__c                      AS stock_id,
        DATE(opp.createddate)             AS opp_date,
        opp.id                            AS opp_id,
        LOWER(opp.leademail__c)           AS email
    FROM prd_refined.salesforce_latam_refined.opportunity opp
    JOIN prd_refined.salesforce_latam_refined.car_of_interest coi
        ON coi.opportunity__c = opp.id
    LEFT JOIN prd_refined.salesforce_latam_refined.vehicle v
        ON v.id = coi.vehicle__c
    WHERE opp.countryname__c = '484'
      AND opp.recordtype_name__c IN ('Venta', 'SalesAllies')
      AND COALESCE(opp.b2b__c, 'false') = 'false'
      AND LOWER(v.type__c) = 'third_party'
      AND v.stockid__c IS NOT NULL
      AND v.stockid__c <> ''
      AND opp.createddate >= ADD_MONTHS(CURRENT_DATE(), -12)
      AND v.stockid__c IN ({stock_ids_quoted})
      AND (
          opp.leademail__c IS NULL
          OR (
              LOWER(opp.leademail__c) NOT LIKE '%@kavak.com'
              AND LOWER(opp.leademail__c) NOT IN (
                  'asdbr@gmail.com',
                  'eduardostorresu@gmail.com',
                  'usuariokaliados@gmail.com',
                  'ventas2@autosicar.com.mx'
              )
          )
      )
)
SELECT
    CAST(stock_id AS BIGINT)   AS stock_id,
    opp_date,
    COUNT(DISTINCT opp_id)     AS opps_unicas
FROM opps_con_autos
GROUP BY 1, 2
ORDER BY 1, 2
LIMIT 400000
