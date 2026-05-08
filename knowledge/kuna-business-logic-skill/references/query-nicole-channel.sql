-- ============================================================
-- NICOLE CHANNEL STATUS — query-nicole-channel.sql
-- Fuente: Redshift (communication_center_api_global_refined)
-- Estado del canal de comunicación WhatsApp por usuario.
-- Columnas: user_id, channel_status, created_at
-- Filtro base fijo: created_at >= '2025-08-01' (inicio del tracking)
-- Join con funnel: ON user_id = playground.kua_sensitive_leads_data.user_id
--   WHERE criterios_proyecto = 'KAVAK ALIADOS'
-- Usar channel_status para derivar la métrica "Enviados" en Nicole
-- ============================================================
SELECT
    c.user_id,
    c.channel_status,
    c.created_at
FROM communication_center_api_global_refined.communication c
WHERE c.created_at >= '2025-08-01'
AND EXISTS (
    SELECT 1
    FROM playground.kua_sensitive_leads_data l
    WHERE l.user_id = c.user_id
      AND l.criterios_proyecto = 'KAVAK ALIADOS'
      AND l.profiling_status IS NOT NULL
)
