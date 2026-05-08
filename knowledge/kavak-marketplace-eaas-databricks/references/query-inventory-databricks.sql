-- queries/inventory_databricks.sql
-- Autos actualmente publicados en EaaS Seller Center.
-- Versión Databricks de inventory.sql.
--
-- Cambios vs Redshift:
--   · Schemas:
--       seller_api_global_refined.*  → prd_refined.seller_api_global_refined.*
--       serving.pricing_stock_current → prd_pricing_serving.pricing.pricing_stock_current
--       serving.car_centric_car_sku   → prd_pricing_serving.sku.sku_api_catalog
--       serving.inventory_history     → prd_serving.inventory.inventory_history
--       serving.pricing_mapping_sample_guia_autometrica → prd_pricing_serving.samples.pricing_mapping_sample_guia_autometrica
--       (ojo: el diccionario del vault sugería mapear a `pricing_mapping_sample` general,
--        pero la tabla GA original existe con su nombre completo en Databricks — verificado 2026-04-24)
--   · sku_api_catalog column renames:
--       jato_sku ← sku            (verificado 2026-04-24 — `variant.sku` UUIDs matchean sku_api_catalog.sku)
--       make    ← make_name
--       model   ← model_name
--       year    ← version_year
--       version ← name
--   · pricing_mapping_sample_guia_autometrica:
--       Sin renombres — `guia_price`, `guia_buy_price`, `guia_create_date`, `flag_guia_latest_price` existen tal cual
--   · json_extract_path_text(col, 'k')::numeric → CAST(get_json_object(col, '$.k') AS DECIMAL(20,2))
--   · ::date / ::varchar / ::INTEGER → DATE() / CAST(... AS STRING) / CAST(... AS BIGINT)
--   · UPPER/LOWER, ROW_NUMBER, COALESCE: misma sintaxis
--   · Outer ROW_NUMBER+filter restructurado con QUALIFY (Databricks ≥13)
--   · Fix Guía Autométrica preservado: WHERE price > 10000

WITH base AS (
    SELECT
        va.id                                                            AS variant_availability_id,
        va.status                                                        AS availability_status,
        va.business_module                                               AS unidad_de_negocio,
        DATE(va.created_date)                                            AS availability_created_date,
        DATE(va.updated_date)                                            AS availability_updated_date,
        va.published,
        vs.legacy_stock_id                                               AS stock_id,
        vs.car_id,
        CAST(get_json_object(va.details, '$.km')  AS DECIMAL(20,2))      AS kilometraje,
        get_json_object(va.details, '$.vin')                             AS vin,
        va.variant_id,
        va.availability_zone_id
    FROM prd_refined.seller_api_global_refined.variant_availability va
    LEFT JOIN prd_refined.seller_api_global_refined.variant_stock vs
        ON vs.variant_availability_id = va.id
    WHERE UPPER(va.status) IN ('AVAILABLE', 'BOOKED')
      AND va.published = TRUE
      AND LOWER(va.business_module) = 'third_party'
),
offers AS (
    SELECT
        po.variant_availability_id,
        CAST(get_json_object(po.details, '$.amount')          AS DECIMAL(20,2)) AS precio_base,
        CAST(get_json_object(po.details, '$.suggestedPrice')  AS DECIMAL(20,2)) AS precio_sugerido,
        DATE(po.updated_date)                                                    AS offer_updated_date,
        ROW_NUMBER() OVER (
            PARTITION BY po.variant_availability_id
            ORDER BY po.updated_date DESC, po.created_date DESC
        ) AS rn
    FROM prd_refined.seller_api_global_refined.price_offer po
),
dealer_info AS (
    SELECT
        va.id        AS variant_availability_id,
        d.name       AS dealer_name,
        vz.name      AS hub_name_sc,
        vz.id        AS availability_zone_id
    FROM prd_refined.seller_api_global_refined.variant_availability va
    LEFT JOIN prd_refined.seller_api_global_refined.availability_zone vz
        ON va.availability_zone_id = vz.id
    LEFT JOIN prd_refined.seller_api_global_refined.company c
        ON vz.company_id = c.id
    LEFT JOIN prd_refined.seller_api_global_refined.dealer d
        ON c.dealer_id = d.id
),
sku_info AS (
    SELECT v.id AS variant_id, v.sku AS jato_sku
    FROM prd_refined.seller_api_global_refined.variant v
),
pricing AS (
    SELECT
        stock_id,
        DATE(publication_created_date_local) AS publication_created_date_local,
        DATE(last_price_change_date)         AS last_price_change_date,
        market_price
    FROM prd_pricing_serving.pricing.pricing_stock_current
),
sku_master AS (
    SELECT DISTINCT
        sku          AS jato_sku,
        make_name    AS make,
        model_name   AS model,
        version_year AS year,
        name         AS version
    FROM prd_pricing_serving.sku.sku_api_catalog
    WHERE sku IS NOT NULL
),
inv_published_last_hub AS (
    SELECT
        CAST(stock_id AS STRING) AS stock_id,
        hub_name                 AS last_hub_name,
        region_name              AS last_region_name
    FROM (
        SELECT
            ih.stock_id,
            ih.hub_name,
            ih.region_name,
            ROW_NUMBER() OVER (
                PARTITION BY ih.stock_id
                ORDER BY ih.inventory_date DESC
            ) AS rn
        FROM prd_serving.inventory.inventory_history ih
        WHERE ih.flag_published = 1
          AND ih.country_iso = 'MX'
          AND UPPER(ih.inventory_status) IN ('AVAILABLE', 'BOOKED')
          AND ih.stock_id IS NOT NULL
    ) z
    WHERE rn = 1
),
-- Guía Autométrica: tabla original con columnas originales. Sin renombres.
-- Fix de corrupción ETL preservado: filtrar precios > 10000.
guia AS (
    SELECT jato_sku, guia_price, guia_buy_price
    FROM (
        SELECT
            ga.sku                                       AS jato_sku,
            ga.guia_price,
            ga.guia_buy_price,
            ROW_NUMBER() OVER (
                PARTITION BY ga.sku
                ORDER BY ga.guia_create_date DESC
            )                                            AS rn
        FROM prd_pricing_serving.samples.pricing_mapping_sample_guia_autometrica ga
        WHERE ga.guia_price > 10000
    )
    WHERE rn = 1
)

SELECT
    di.dealer_name,
    b.variant_availability_id,
    b.stock_id,
    si.jato_sku                           AS sku,
    sm.make,
    sm.model,
    sm.version,
    sm.year,
    b.kilometraje,
    b.vin,
    o.precio_base,
    o.precio_sugerido,
    pr.market_price,
    g.guia_price,
    g.guia_buy_price,
    b.availability_status,
    b.published,
    COALESCE(
        pr.publication_created_date_local,
        CASE WHEN b.availability_status = 'AVAILABLE' THEN b.availability_created_date END
    )                                     AS fecha_publicacion,
    b.availability_created_date           AS fecha_ingreso,
    CASE WHEN b.availability_status = 'BOOKED' THEN b.availability_updated_date END
                                          AS fecha_reserva,
    COALESCE(b.availability_updated_date, o.offer_updated_date) AS ultima_actualizacion,
    ih.last_hub_name,
    ih.last_region_name,
    b.availability_zone_id,
    di.hub_name_sc
FROM base b
LEFT JOIN offers              o   ON o.variant_availability_id = b.variant_availability_id AND o.rn = 1
LEFT JOIN dealer_info         di  ON di.variant_availability_id = b.variant_availability_id
LEFT JOIN sku_info            si  ON si.variant_id = b.variant_id
LEFT JOIN pricing             pr  ON pr.stock_id = CAST(b.stock_id AS STRING)
LEFT JOIN sku_master          sm  ON sm.jato_sku = si.jato_sku
LEFT JOIN inv_published_last_hub ih ON ih.stock_id = CAST(b.stock_id AS STRING)
LEFT JOIN guia                g   ON g.jato_sku = si.jato_sku
WHERE LOWER(COALESCE(di.dealer_name, '')) <> 'test mexico'
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY b.variant_availability_id
    ORDER BY COALESCE(b.availability_updated_date, o.offer_updated_date) DESC
) = 1
ORDER BY di.dealer_name, COALESCE(b.availability_updated_date, o.offer_updated_date) DESC
LIMIT 400000
