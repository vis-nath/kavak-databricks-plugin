-- ================================================================
-- VIPs DIARIOS POR DEALER — Tendencia histórica (Dic 2025 → hoy)
-- Genera: data_vips_daily.json → vips_daily_by_dealer
-- ================================================================
-- Output: dia | dealer_name | autos_publicados | total_vips | total_unique_vips | vips_per_auto_dia

WITH
-- Stocks comprados a Alquiladora (incluir en EaaS aunque no sean Seller Center)
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

-- Rango de fechas: desde Diciembre 2025
date_floor AS (SELECT DATE '2025-12-01' AS start_date),

-- Base: variant_availability con stock_id y business_module
base AS (
    SELECT va.id AS variant_availability_id, va.business_module AS unidad_de_negocio,
      vs.legacy_stock_id AS stock_id,
      vs.updated_date::date AS stock_updated_date,
      va.updated_date::date AS availability_updated_date
    FROM seller_api_global_refined.variant_availability va
    LEFT JOIN seller_api_global_refined.variant_stock vs ON vs.variant_availability_id = va.id
),

-- Dealer para cada variant_availability
dealer_info AS (
    SELECT va.id AS variant_availability_id, d.name AS dealer_name
    FROM seller_api_global_refined.variant_availability va
    LEFT JOIN seller_api_global_refined.availability_zone vz ON va.availability_zone_id = vz.id
    LEFT JOIN seller_api_global_refined.company c ON vz.company_id = c.id
    LEFT JOIN seller_api_global_refined.dealer d ON c.dealer_id = d.id
),

-- Une base + dealer, deduplica por variant_availability_id
joined_data AS (
    SELECT di.dealer_name, b.variant_availability_id, b.stock_id, b.unidad_de_negocio,
      ROW_NUMBER() OVER (
        PARTITION BY b.variant_availability_id
        ORDER BY COALESCE(b.availability_updated_date, b.stock_updated_date) DESC
      ) AS rn
    FROM base b LEFT JOIN dealer_info di ON di.variant_availability_id = b.variant_availability_id
),

-- Stocks EaaS del Seller Center
seller_center_stocks AS (
  SELECT DISTINCT j.stock_id::INTEGER AS stock_id FROM joined_data j
  WHERE j.rn = 1
    AND LOWER(j.unidad_de_negocio) = 'third_party'
    AND LOWER(j.dealer_name) <> 'test mexico'
    AND j.stock_id IS NOT NULL
),

-- Mapa stock_id → dealer_name
stock_dealer_map AS (
  SELECT DISTINCT stock_id::INTEGER AS stock_id, dealer_name FROM joined_data
  WHERE rn = 1
    AND LOWER(unidad_de_negocio) = 'third_party'
    AND LOWER(dealer_name) <> 'test mexico'
    AND stock_id IS NOT NULL
),

-- Inventario publicado por día (Seller Center + Alquiladora)
inv_day AS (
  SELECT v.bk_stock::INTEGER AS bk_stock, v.inv_date::date AS t_day,
    v.roll_01d_vips AS vips_roll
  FROM serving.dl_catalog_inventory_velocity v
  JOIN date_floor df ON v.inv_date::date >= df.start_date
  WHERE v.flag_published = 1
    AND v.bk_stock IS NOT NULL
    AND v.bk_stock::INTEGER IN (
          SELECT stock_id FROM seller_center_stocks
          UNION
          SELECT car_stock_id FROM compras_alquiladora
        )
),

-- VIPs únicos por día desde Amplitude
amp_views AS (
  SELECT a.car_id::INTEGER AS bk_stock, a.local_event_time::date AS event_date,
    COALESCE(a.merged_amplitude_id, a.amplitude_id) AS user_id
  FROM serving.amplitude_vip_viewed_global_rs a
  JOIN date_floor df ON a.local_event_time::date >= df.start_date
  WHERE a.path_prefix = '/mx' AND a.car_id IS NOT NULL
),
amp_views_in_inv AS (
  SELECT av.bk_stock, av.event_date, av.user_id FROM amp_views av
  INNER JOIN (SELECT DISTINCT bk_stock FROM inv_day) i USING (bk_stock)
),
daily_amp_uniques AS (
  SELECT bk_stock, event_date AS t_day, COUNT(DISTINCT user_id) AS daily_unique_users
  FROM amp_views_in_inv GROUP BY 1, 2
),

-- Une todo: inv_day + dealer + amplitude
joined_daily AS (
  SELECT i.t_day, sdm.dealer_name, i.bk_stock,
    COALESCE(u.daily_unique_users, 0) AS daily_unique_users,
    COALESCE(i.vips_roll, 0) AS vips_roll
  FROM inv_day i
  LEFT JOIN stock_dealer_map sdm ON sdm.stock_id = i.bk_stock
  LEFT JOIN daily_amp_uniques u ON u.bk_stock = i.bk_stock AND u.t_day = i.t_day
)

-- Salida agregada por dealer + día
SELECT
  t_day::VARCHAR                   AS dia,
  dealer_name,
  COUNT(DISTINCT bk_stock)         AS autos_publicados,
  SUM(vips_roll)                   AS total_vips,
  SUM(daily_unique_users)          AS total_unique_vips,
  ROUND(SUM(vips_roll)::FLOAT / NULLIF(COUNT(DISTINCT bk_stock), 0), 2) AS vips_per_auto_dia
FROM joined_daily
WHERE dealer_name IS NOT NULL
GROUP BY t_day, dealer_name
ORDER BY t_day, dealer_name;

-- NOTA: "raw" es palabra reservada en Redshift, usar "joined_daily" u otro alias
-- NOTA: ~783 filas para Dic 2025 → Feb 2026 con 10 dealers
