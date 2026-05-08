-- queries/historical_reserved_databricks.sql
-- Historial de precios para stocks que ya fueron reservados/vendidos
-- (ya no están publicados actualmente).
-- Versión Databricks de historical_reserved.sql.
-- {stock_ids} reemplazado en Python con IDs de bookings.json.
--
-- Cambios vs Redshift:
--   · Schema: seller_api_global_refined → prd_refined.seller_api_global_refined
--   · json_extract_path_text(po.details, 'amount')::numeric
--       → CAST(get_json_object(po.details, '$.amount') AS DECIMAL(20,2))
--   · ::INTEGER → CAST(... AS BIGINT)
--   · ::date → DATE(...)

SELECT
    CAST(vs.legacy_stock_id AS BIGINT)                               AS stock_id,
    CAST(get_json_object(po.details, '$.amount') AS DECIMAL(20,2))   AS precio,
    DATE(po.updated_date)                                            AS fecha
FROM prd_refined.seller_api_global_refined.price_offer po
JOIN prd_refined.seller_api_global_refined.variant_stock vs
    ON vs.variant_availability_id = po.variant_availability_id
WHERE CAST(vs.legacy_stock_id AS BIGINT) IN ({stock_ids})
  AND get_json_object(po.details, '$.amount') IS NOT NULL
ORDER BY vs.legacy_stock_id, po.updated_date ASC
LIMIT 400000
