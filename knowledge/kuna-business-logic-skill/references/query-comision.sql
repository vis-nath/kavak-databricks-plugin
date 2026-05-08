-- ============================================================
-- COMISIONES — query-comision.sql
-- Fuente: Redshift (playground schema)
-- Una fila por VIN (comisión). Incluye KAVAK ALIADOS y ECOMERCEAAS.
-- Columnas clave: vin, asset_id, fecha_de_contrato,
--   fecha_de_dispersion, comision_dealer, estatus_comision,
--   substatus_expediente_gabo, sla_dispersion,
--   sla_expediente_completo, sla_pago_comision, proyecto
-- Filtrar por proyecto: criterios_proyecto IN ('KAVAK ALIADOS','ECOMERCEAAS')
--   (ya incluido en WHERE de la query)
-- Join con funnel: ON vin = query-kuna-funnel.vin
-- ============================================================
WITH expedientes_gabo_limpia AS (
    SELECT
        vin,
        NULLIF(status_expediente, 'nan') AS status_expediente_gabo,
        NULLIF(substatus_expediente, 'nan') AS substatus_expediente_gabo,
        CASE
            WHEN fecha_de_liberacion ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN CAST(fecha_de_liberacion AS DATE)
            WHEN fecha_de_liberacion ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' THEN TO_DATE(fecha_de_liberacion, 'DD/MM/YYYY')
            ELSE NULL
        END AS fecha_de_liberacion,
        ROW_NUMBER() OVER (
            PARTITION BY vin
            ORDER BY
                CASE
                    WHEN fecha_de_liberacion ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN CAST(fecha_de_liberacion AS DATE)
                    WHEN fecha_de_liberacion ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' THEN TO_DATE(fecha_de_liberacion, 'DD/MM/YYYY')
                    ELSE NULL
                END DESC NULLS LAST
        ) AS rn
    FROM
        playground.dl_expedientes_agencia_bd_historico_expedientes_data
),
pago_expedientes AS (
    SELECT
        p.vin,
        p.stock_id::VARCHAR AS asset_id,
        CASE
            WHEN p.fecha_de_contrato ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
                THEN CAST(p.fecha_de_contrato AS DATE)
            WHEN p.fecha_de_contrato ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}'
                THEN TO_DATE(p.fecha_de_contrato, 'DD/MM/YYYY')
            ELSE NULL
        END AS fecha_de_contrato,
        CASE
            WHEN p.fecha_de_dispersion ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN CAST(p.fecha_de_dispersion AS DATE)
            WHEN p.fecha_de_dispersion ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' THEN TO_DATE(p.fecha_de_dispersion, 'DD/MM/YYYY')
            ELSE NULL
        END AS fecha_de_dispersion,
        CASE
            WHEN p.fecha_de_carga ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN CAST(p.fecha_de_carga AS DATE)
            WHEN p.fecha_de_carga ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' THEN TO_DATE(p.fecha_de_carga, 'DD/MM/YYYY')
            ELSE NULL
        END AS fecha_de_carga,
        p.nombre_de_cliente,
        p.inventory_item,
        p.monto_a_finaciar::NUMERIC(18,2) AS monto_a_finaciar,
        p.comision_punto_de_venta::NUMERIC(18,2) AS comision_dealer,
        p.criterios_proyecto AS proyecto,
        p.proyecto_comision AS proyecto_formulado_actual,
        p.estatus_expediente,
        p.factura_solicitada::VARCHAR,
        CASE
            WHEN p.fecha_factura_solicitada ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN CAST(p.fecha_factura_solicitada AS DATE)
            WHEN p.fecha_factura_solicitada ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' THEN TO_DATE(p.fecha_factura_solicitada, 'DD/MM/YYYY')
            ELSE NULL
        END AS fecha_factura_solicitada,
        p.factura_recibida::VARCHAR,
        CASE
            WHEN p.fecha_factura_recibida ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN CAST(p.fecha_factura_recibida AS DATE)
            WHEN p.fecha_factura_recibida ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' THEN TO_DATE(p.fecha_factura_recibida, 'DD/MM/YYYY')
            ELSE NULL
        END AS fecha_factura_recibida,
        NULL::VARCHAR AS pagado,
        NULL::DATE AS fecha_pago
    FROM
        playground.dl_comisiones___agencias_2_00__base_trabajable_comisiones_semanal AS p
    LEFT JOIN
        playground.dl_comisiones___agencias_2_00__hist_rico AS h
        ON p.vin = h.vin
        AND h.criterios_proyecto IN ('KAVAK ALIADOS', 'ECOMERCEAAS')
    WHERE
        p.criterios_proyecto IN ('KAVAK ALIADOS', 'ECOMERCEAAS')
        AND h.vin IS NULL
    UNION ALL
    SELECT
        vin,
        stock_id::VARCHAR AS asset_id,
        CASE
            WHEN fecha_de_contrato ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN CAST(fecha_de_contrato AS DATE)
            WHEN fecha_de_contrato ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' THEN TO_DATE(fecha_de_contrato, 'DD/MM/YYYY')
            ELSE NULL
        END AS fecha_de_contrato,
        CASE
            WHEN fecha_de_dispersion ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN CAST(fecha_de_dispersion AS DATE)
            WHEN fecha_de_dispersion ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' THEN TO_DATE(fecha_de_dispersion, 'DD/MM/YYYY')
            ELSE NULL
        END AS fecha_de_dispersion,
        CASE
            WHEN fecha_de_carga ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN CAST(fecha_de_carga AS DATE)
            WHEN fecha_de_carga ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' THEN TO_DATE(fecha_de_carga, 'DD/MM/YYYY')
            ELSE NULL
        END AS fecha_de_carga,
        nombre_de_cliente AS nombre_de_cliente,
        inventory_item,
        monto_a_finaciar::NUMERIC(18,2) AS monto_a_finaciar,
        comision_punto_de_venta::NUMERIC(18,2) AS comision_dealer,
        criterios_proyecto AS proyecto,
        proyecto_comision AS proyecto_formulado_actual,
        'EXPEDIENTE LIBERADO' AS estatus_expediente,
        factura_solicitada::VARCHAR AS factura_solicitada,
        CASE
            WHEN fecha_factura_solicitada ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN CAST(fecha_factura_solicitada AS DATE)
            WHEN fecha_factura_solicitada ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' THEN TO_DATE(fecha_factura_solicitada, 'DD/MM/YYYY')
            ELSE NULL
        END AS fecha_factura_solicitada,
        factura_recibida::VARCHAR,
        CASE
            WHEN fecha_factura_recibida ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN CAST(fecha_factura_recibida AS DATE)
            WHEN fecha_factura_recibida ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' THEN TO_DATE(fecha_factura_recibida, 'DD/MM/YYYY')
            ELSE NULL
        END AS fecha_factura_recibida,
        estatus_pago::VARCHAR as pagado,
        CASE
            WHEN fecha_pago ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN CAST(fecha_pago AS DATE)
            WHEN fecha_pago ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}' THEN TO_DATE(fecha_pago, 'DD/MM/YYYY')
            ELSE NULL
        END AS fecha_pago
    FROM
        playground.dl_comisiones___agencias_2_00__hist_rico
    WHERE
        criterios_proyecto IN ('KAVAK ALIADOS', 'ECOMERCEAAS')
)
SELECT
    pe.*,
    egl.status_expediente_gabo,
    egl.substatus_expediente_gabo,
    egl.fecha_de_liberacion,
    DATEDIFF(day, pe.fecha_de_contrato, pe.fecha_de_dispersion) AS sla_dispersion,
    DATEDIFF(day, pe.fecha_de_dispersion, pe.fecha_de_carga) AS sla_carga_base_trabajable,
    DATEDIFF(day, pe.fecha_de_carga, egl.fecha_de_liberacion) AS sla_expediente_completo,
    DATEDIFF(day, egl.fecha_de_liberacion, pe.fecha_factura_solicitada) AS sla_factura_solicitada,
    DATEDIFF(day, pe.fecha_factura_solicitada, pe.fecha_factura_recibida) AS sla_factura_recibida,
    DATEDIFF(day, pe.fecha_factura_solicitada, pe.fecha_pago) AS sla_pago_comision,
    CASE
        WHEN pe.pagado = 'Pagada' THEN 'Comision Pagada'
        WHEN pe.pagado = 'Si' THEN 'Comision Pagada'
        WHEN pe.factura_recibida = 'Recibida' THEN 'Pendiente de Pago de Comision'
        WHEN pe.factura_recibida = 'Si' THEN 'Pendiente de Pago de Comision'
        WHEN pe.factura_solicitada = 'Solicitada' THEN 'Pendiente de Recibo de Factura'
        WHEN pe.factura_solicitada = 'Si' THEN 'Pendiente de Recibo de Factura'
        WHEN pe.estatus_expediente = 'EXPEDIENTE LIBERADO' THEN 'Pendiente de Envio de Factura'
        WHEN egl.fecha_de_liberacion IS NOT NULL THEN 'Pendiente de Envio de Factura'
        ELSE 'Expediente No Liberado'
    END AS estatus_comision
FROM
    pago_expedientes AS pe
LEFT JOIN
    expedientes_gabo_limpia AS egl
    ON pe.vin = egl.vin
    AND egl.rn = 1;
