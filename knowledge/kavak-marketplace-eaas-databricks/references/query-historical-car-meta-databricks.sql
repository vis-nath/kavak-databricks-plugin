-- queries/historical_car_meta_databricks.sql
-- Metadatos del auto para stocks históricos (ya no publicados).
-- Trae: km, última región, market_price, fecha_publicacion.
-- Versión Databricks de historical_car_meta.sql.
-- {stock_ids} reemplazado en Python con la lista de IDs (enteros).
--
-- Cambios vs Redshift:
--   · seller_api_global_refined → prd_refined.seller_api_global_refined
--   · serving.inventory_history  → prd_serving.inventory.inventory_history (sin column renames)
--   · serving.pricing_stock_current → prd_pricing_serving.pricing.pricing_stock_current (sin renames)
--   · json_extract_path_text(va.details, 'km')::numeric
--       → CAST(get_json_object(va.details, '$.km') AS DECIMAL(20,2))
--   · ::INTEGER → CAST(... AS BIGINT)
--   · ::date    → DATE(...)
--   · ::varchar → CAST(... AS STRING)

SELECT
    sub.stock_id,
    sub.km,
    rh.region_name,
    rh.hub_name,
    pr.market_price,
    DATE(pr.publication_created_date_local) AS fecha_publicacion
FROM (
    SELECT
        CAST(vs.legacy_stock_id AS BIGINT)                          AS stock_id,
        CAST(get_json_object(va.details, '$.km') AS DECIMAL(20,2))  AS km,
        ROW_NUMBER() OVER (
            PARTITION BY vs.legacy_stock_id
            ORDER BY va.updated_date DESC
        )                                                            AS rn
    FROM prd_refined.seller_api_global_refined.variant_stock vs
    JOIN prd_refined.seller_api_global_refined.variant_availability va
        ON va.id = vs.variant_availability_id
    WHERE CAST(vs.legacy_stock_id AS BIGINT) IN ({stock_ids})
      AND LOWER(va.business_module) = 'third_party'
) sub
LEFT JOIN (
    SELECT stock_id, region_name, hub_name
    FROM (
        SELECT
            stock_id,
            region_name,
            hub_name,
            ROW_NUMBER() OVER (
                PARTITION BY stock_id
                ORDER BY inventory_date DESC
            ) AS rn
        FROM prd_serving.inventory.inventory_history
        WHERE country_iso = 'MX'
          AND flag_published = 1
          AND CAST(stock_id AS BIGINT) IN ({stock_ids})
    ) z
    WHERE rn = 1
) rh ON rh.stock_id = CAST(sub.stock_id AS STRING)
LEFT JOIN (
    SELECT
        CAST(stock_id AS BIGINT)             AS stock_id,
        market_price,
        publication_created_date_local
    FROM prd_pricing_serving.pricing.pricing_stock_current
    WHERE CAST(stock_id AS BIGINT) IN ({stock_ids})
) pr ON pr.stock_id = sub.stock_id
WHERE sub.rn = 1
LIMIT 400000
