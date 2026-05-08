-- =============================================================
-- INVENTARIO DIARIO EaaS — Snapshot histórico por día
-- Fuente: serving.inventory_history
--   · inventory_date = fecha del snapshot (1 fila por stock por día)
--   · stock_id, inventory_status (AVAILABLE / BOOKED / SOLD)
--   · flag_published = 1 → publicado ese día
--   · hub_name, region_name, sku, kilometers, market_price
--   · regular_published_price = precio publicado
--   · country_iso = 'MX'
--
-- Cruzado con seller_api para identificar stocks third_party (EaaS)
-- Incluye unidad_de_negocio = 'third_party' | 'retail'
--
-- Para inventario promedio diario por mes:
--   GROUP BY DATE_TRUNC('month', inventory_date), contar stocks publicados
--   promedio = SUM(stocks_publicados_ese_dia) / COUNT(DISTINCT dias_del_mes)
--
-- Ajustar rango en WHERE: inventory_date >= ... AND inventory_date <= ...
-- =============================================================

WITH
third_party_stocks AS (
    SELECT DISTINCT vs.legacy_stock_id AS stock_id, va.business_module AS unidad_de_negocio
    FROM seller_api_global_refined.variant_availability va
    LEFT JOIN seller_api_global_refined.variant_stock vs ON vs.variant_availability_id = va.id
    LEFT JOIN seller_api_global_refined.availability_zone vz ON va.availability_zone_id = vz.id
    LEFT JOIN seller_api_global_refined.company c ON vz.company_id = c.id
    LEFT JOIN seller_api_global_refined.dealer d  ON c.dealer_id = d.id
    WHERE LOWER(va.business_module) = 'third_party'
      AND LOWER(COALESCE(d.name, '')) <> 'test mexico'
),

inv AS (
    SELECT
        inventory_date::date    AS inventory_date,
        stock_id,
        inventory_status,
        hub_name,
        region_name,
        sku,
        kilometers,
        market_price,
        regular_published_price,
        country_iso,
        flag_published
    FROM serving.inventory_history
    WHERE flag_published = 1
      AND country_iso = 'MX'
      AND UPPER(inventory_status) IN ('AVAILABLE', 'BOOKED')
      AND inventory_date >= '2026-01-01'
      AND inventory_date <= '2026-03-31'
)

SELECT
    i.inventory_date,
    i.stock_id,
    i.inventory_status,
    i.hub_name,
    i.region_name,
    i.sku,
    i.kilometers,
    i.market_price,
    i.regular_published_price,
    COALESCE(tp.unidad_de_negocio, 'retail') AS unidad_de_negocio
FROM inv i
LEFT JOIN third_party_stocks tp ON tp.stock_id = i.stock_id::varchar
-- Filtrar solo EaaS: descomentar si se quiere solo third_party
-- WHERE tp.stock_id IS NOT NULL
ORDER BY inventory_date, stock_id
LIMIT 400000
