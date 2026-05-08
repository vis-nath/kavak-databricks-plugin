-- =============================================================
-- VIPs TOTALES POR AUTO — EaaS (Seller Center + Alquiladora)
-- Tabla Amplitude: serving.amplitude_vip_viewed_global_rs
--   · car_id = legacy_stock_id (seller_api)
--   · local_event_time = timestamp local MX
--   · path_prefix = '/mx' para México
--   · merged_amplitude_id / amplitude_id = usuario único
-- Inventory: serving.dl_catalog_inventory_velocity
--   · bk_stock = stock_id
--   · inv_date = fecha snapshot
--   · flag_published = 1 → publicado ese día
--   · roll_01d_vips = VIPs velocity del día
-- =============================================================
-- Ajustar date_floor según el rango deseado
-- =============================================================

WITH
compras_alquiladora AS (
  WITH compras AS (
      SELECT csf.car_stock_id::INTEGER AS car_stock_id
      FROM serving.car_supply_funnel csf
      WHERE csf.item_receipt_date::DATE >= DATE '2025-01-01'
        AND csf.country_iso = 'MX'
  ),
  contab AS (
      SELECT TRIM(SPLIT_PART(item_name, ' ', 2))::INTEGER AS car_stock_id, entity_full_name
      FROM serving.accounting_entry
      WHERE subsidiary_name = 'UVI TECH SAPI MEXICO'
        AND account_number = '116-001'
        AND accounting_entry_transaction_type = 'Item Receipt'
        AND item_name LIKE 'AUTO %'
  )
  SELECT DISTINCT c.car_stock_id
  FROM compras c
  JOIN contab ct ON c.car_stock_id = ct.car_stock_id
  WHERE ct.entity_full_name ILIKE '%ALQUILADORA DE VEHICUL%'
),

date_floor AS (
  SELECT
    DATE '2026-01-01' AS start_date,
    DATE '2026-03-31' AS end_date
),

seller_center_stocks AS (
  SELECT DISTINCT vs.legacy_stock_id::INTEGER AS stock_id
  FROM seller_api_global_refined.variant_availability va
  LEFT JOIN seller_api_global_refined.variant_stock vs ON vs.variant_availability_id = va.id
  LEFT JOIN seller_api_global_refined.availability_zone vz ON va.availability_zone_id = vz.id
  LEFT JOIN seller_api_global_refined.company c ON vz.company_id = c.id
  LEFT JOIN seller_api_global_refined.dealer d ON c.dealer_id = d.id
  WHERE LOWER(va.business_module) = 'third_party'
    AND LOWER(COALESCE(d.name, '')) <> 'test mexico'
    AND vs.legacy_stock_id IS NOT NULL
),

inv_stock AS (
  SELECT
    v.bk_stock,
    CASE
      WHEN v.stock_region ILIKE 'CDMX%'         THEN 'CDMX'
      WHEN v.stock_region ILIKE 'CUERNA-PUEBLA' THEN 'Puebla'
      WHEN v.stock_region ILIKE 'MONTERREY'     THEN 'Monterrey'
      WHEN v.stock_region ILIKE 'GUADALAJARA'   THEN 'Guadalajara'
      WHEN v.stock_region ILIKE 'QUERETARO'     THEN 'Queretaro'
      ELSE v.stock_region
    END AS stock_region,
    v.hub_name_ns AS stock_hub,
    COUNT(DISTINCT v.inv_date::date) AS dias_publicado,
    MAX(v.flag_auto_visitable)       AS flag_auto_visitable
  FROM serving.dl_catalog_inventory_velocity v
  JOIN date_floor df ON v.inv_date::date BETWEEN df.start_date AND df.end_date
  WHERE v.flag_published = 1
    AND v.bk_stock IS NOT NULL
    AND v.bk_stock::INTEGER IN (
        SELECT stock_id     FROM seller_center_stocks
        UNION
        SELECT car_stock_id FROM compras_alquiladora
    )
  GROUP BY 1, 2, 3
),

amp_unique_views AS (
  SELECT
    a.car_id AS bk_stock,
    COUNT(DISTINCT COALESCE(a.merged_amplitude_id, a.amplitude_id)) AS total_unique_vips
  FROM serving.amplitude_vip_viewed_global_rs a
  JOIN date_floor df ON a.local_event_time::date BETWEEN df.start_date AND df.end_date
  WHERE a.path_prefix = '/mx'
  GROUP BY 1
)

SELECT
  i.bk_stock,
  i.stock_region,
  i.stock_hub,
  i.dias_publicado,
  i.flag_auto_visitable,
  COALESCE(a.total_unique_vips, 0) AS total_unique_vips
FROM inv_stock i
LEFT JOIN amp_unique_views a USING (bk_stock)
ORDER BY stock_region, stock_hub, bk_stock
LIMIT 400000
