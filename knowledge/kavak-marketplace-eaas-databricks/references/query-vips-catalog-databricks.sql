-- queries/vips_catalog_databricks.sql
-- VIPs diarios de prd_datamx_serving.serving.catalog_inventory_velocity
-- {stock_ids} reemplazado en Python

SELECT
    CAST(bk_stock AS BIGINT)          AS stock_id,
    CAST(inv_date AS DATE)            AS inv_date,
    COALESCE(roll_01d_vips, 0)        AS roll_01d_vips,
    COALESCE(flag_published, 0)       AS flag_published
FROM prd_datamx_serving.serving.catalog_inventory_velocity
WHERE country_iso = 'MX'
  AND bk_stock IS NOT NULL
  AND inv_date >= ADD_MONTHS(CURRENT_DATE(), -6)
  AND CAST(bk_stock AS BIGINT) IN ({stock_ids})
ORDER BY stock_id, inv_date
LIMIT 2000000
