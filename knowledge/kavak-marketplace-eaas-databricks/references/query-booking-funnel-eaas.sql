-- ============================================================
-- BOOKING FUNNEL EaaS — Query Principal desde Redshift
-- Fuente: serving.bookings_history + supply + velocity + Salesforce
-- Cubre: EaaS (THIRD_PARTY) + Alquiladora FBK
-- Última validación: 2026-03-03
-- ============================================================
-- CAMPOS CLAVE:
--   fecha_cancelacion_reserva  → Booking Cancellation (pre-entrega)
--   devolucion_date            → Return (post-entrega)
--   cancelacion_venta_real     → Cancelación real de venta
--   fecha_entrega_final        → Entrega consolidada (fecha_completada > fecha_entrega > SF CW)
--   fecha_cancelacion_final    → Cancelación final consolidada
--   tipo_de_auto               → 'THIRD_PARTY' = EaaS directo
-- ============================================================

WITH
-- ============================================================
-- 0. STOCKS COMPRADOS A "ALQUILADORA DE VEHICUL" (Retail FBK)
-- ============================================================
compras_alquiladora AS (
    WITH compras AS (
        SELECT
            csf.car_stock_id::INTEGER AS car_stock_id,
            csf.selected_offer_type
        FROM serving.car_supply_funnel csf
        WHERE csf.item_receipt_date::DATE >= DATE '2025-01-01'
          AND csf.country_iso = 'MX'
    ),
    contab AS (
        SELECT
            TRIM(SPLIT_PART(item_name, ' ', 2))::INTEGER AS car_stock_id,
            entity_full_name
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

-- ============================================================
-- 1. BOOKING DATA (RESERVAS)
-- ============================================================
salesforce_data AS (
    SELECT
        bh.booking_id,
        bh.stock,
        bh.customer_name,
        bh.email            AS customer_email,
        bh.telefono_2       AS customer_phone,
        bh.sales_id,
        bh.opportunity_id,
        bh.b2b,
        bh.tipo_de_auto,
        DATE(bh.booking_creation_date)  AS fecha_reserva,
        bh.fecha_cancelacion_reserva,   -- Booking Cancellation (pre-entrega)
        bh.fecha_venta_declarada,
        bh.fecha_entrega,
        bh.cancelacion_venta_real,      -- Cancelación real de venta
        bh.devolucion_date,             -- Return (post-entrega)
        bh.hub_entrega,
        bh.metodo_de_pago,
        bh.fecha_completada,
        bh.hub_entrega_v2
    FROM serving.bookings_history bh
    WHERE bh.booking_creation_date >= '2025-09-01'
      AND COALESCE(bh.b2b, 0) <> 1
      AND (
            bh.tipo_de_auto = 'THIRD_PARTY'
         OR bh.stock::INTEGER IN (SELECT car_stock_id FROM compras_alquiladora)
      )
      AND bh.email NOT IN (
        'alberto.tovar@kavak.com',
        'asdbr@gmail.com',
        'eduardo.chavez@kavak.com',
        'eduardo.torres@kavak.com',
        'eduardo.torres+123@kavak.com',
        'eduardostorresu@gmail.com',
        'gustavo.falco+123@kavak.com',
        'isain.sosa+23422349885769834785342342@kavak.com',
        'juan.silva@kavak.com',
        'luis.castellanos@kavak.com',
        'manuel.lopezp+123@kavak.com',
        'ricardo.sue@kavak.com',
        'usuariokaliados@gm',
        'andres.mejias@kavak.com',
        'ventas2@autosicar.com.mx',
        'eduardo.torres+1233212@kavak.com',
        'eduardo.torres+999999@kavak.com',
        'eduardo.torres+9999999@kavak.com',
        'eduardo.torres+898989@kavak.com',
        'angela.delduca+061001@kavak.com',
        'eduardo.torres+10101010@kavak.com'
      )
),

-- ============================================================
-- 2. SUPPLY: FECHA DE COMPRA POR STOCK
-- ============================================================
supply_data AS (
    SELECT
        csf.car_stock_id::INTEGER       AS stock_id,
        csf.item_receipt_date::DATE     AS fecha_compra,
        csf.mechanical_inspection_location AS lugar_compra
    FROM serving.car_supply_funnel csf
    WHERE csf.item_receipt_date IS NOT NULL
),

-- ============================================================
-- 3. INVENTORY VELOCITY (region del auto)
-- ============================================================
velocity AS (
    SELECT DISTINCT
        bk_stock,
        inv_date,
        CASE WHEN stock_region ILIKE '%CDMX%' THEN 'CDMX'
             ELSE stock_region
        END AS region
    FROM serving.dl_catalog_inventory_velocity
),

-- ============================================================
-- 4. OPORTUNIDADES SALESFORCE (Venta + SalesAllies)
-- ============================================================
opportunities AS (
    SELECT
        id,
        stagename,
        DATE(closedate) AS closedate,
        status__c,
        recordtype_name__c
    FROM salesforce_latam_refined.opportunity
    WHERE countryname__c = '484'
      AND recordtype_name__c IN ('Venta', 'SalesAllies')
)

-- ============================================================
-- QUERY FINAL
-- ============================================================
SELECT
    sf.booking_id,
    sf.stock,
    sf.customer_name,
    sf.customer_email,
    sf.customer_phone,
    sf.opportunity_id,
    sf.b2b,
    sf.tipo_de_auto,
    sf.fecha_reserva,
    sf.fecha_cancelacion_reserva,
    sf.fecha_venta_declarada,
    sf.fecha_entrega,
    sf.cancelacion_venta_real,
    sf.devolucion_date,
    sf.hub_entrega,
    sf.metodo_de_pago,
    sf.fecha_completada,
    sf.hub_entrega_v2,
    sd.fecha_compra,
    sd.lugar_compra,
    vel.region AS region_publicacion,
    opp.stagename,
    opp.closedate,
    opp.status__c,
    opp.recordtype_name__c,

    -- ✅ FECHA ENTREGA CONSOLIDADA (prioriza fecha_completada)
    CASE
        WHEN sf.fecha_completada IS NOT NULL THEN sf.fecha_completada
        WHEN sf.fecha_entrega IS NOT NULL    THEN sf.fecha_entrega
        WHEN sf.fecha_entrega IS NULL
             AND (
                    opp.stagename ILIKE 'Cerrada Ganada%'
                 OR opp.stagename ILIKE 'Closed Won%'
                 OR opp.status__c  ILIKE 'Closed Won%'
                 )
        THEN COALESCE(sf.fecha_venta_declarada, opp.closedate)
        ELSE NULL
    END AS fecha_entrega_final,

    -- ✅ FECHA CANCELACIÓN FINAL
    CASE
        WHEN sf.fecha_cancelacion_reserva IS NOT NULL THEN sf.fecha_cancelacion_reserva
        WHEN sf.fecha_cancelacion_reserva IS NULL
             AND (
                    opp.stagename ILIKE 'Cerrada Perdida%'
                 OR opp.stagename ILIKE 'Closed Lost%'
                 OR opp.status__c  ILIKE 'Closed Lost%'
                 )
        THEN COALESCE(sf.cancelacion_venta_real, sf.devolucion_date, opp.closedate)
        ELSE NULL
    END AS fecha_cancelacion_final

FROM salesforce_data sf
LEFT JOIN supply_data sd   ON sd.stock_id = sf.stock::INTEGER
LEFT JOIN velocity vel     ON vel.bk_stock = sf.stock
                          AND vel.inv_date = sf.fecha_reserva
LEFT JOIN opportunities opp ON opp.id = sf.opportunity_id

ORDER BY sf.fecha_reserva DESC;

-- ============================================================
-- MAPEO EXCEL → REDSHIFT
-- ============================================================
-- Excel column            | Redshift field
-- ----------------------- | -----------------------------------------
-- Booking_Cancellation=1  | fecha_cancelacion_reserva IS NOT NULL
-- Booking_Cancellation_Date | fecha_cancelacion_reserva
-- Return=1                | devolucion_date IS NOT NULL
-- Return_Date             | devolucion_date
-- Total_Cancellation=1    | fecha_cancelacion_reserva IS NOT NULL
--                         |   OR devolucion_date IS NOT NULL
-- Total_Cancellation_Date | COALESCE(fecha_cancelacion_reserva, devolucion_date)
-- Delivery=1              | fecha_entrega_final IS NOT NULL
-- Delivery_Date           | fecha_entrega_final
-- Active_Booking=1        | fecha_entrega_final IS NULL
--                         |   AND fecha_cancelacion_reserva IS NULL
--                         |   AND devolucion_date IS NULL
-- Fecha reserva           | fecha_reserva (= DATE(booking_creation_date))
-- metodo_pago_auto        | metodo_de_pago
-- vehicle_type__c THIRD_PARTY | tipo_de_auto = 'THIRD_PARTY'
-- vehicle_type__c KAVAK   | stock IN compras_alquiladora (Alquiladora FBK)
