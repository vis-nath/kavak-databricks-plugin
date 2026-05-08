-- ============================================================
-- NICOLE EVENTS — query-nicole.sql
-- Fuente: Redshift (kuna_dealers_api_global_refined)
-- Eventos del agente WhatsApp Nicole, filtrados a KAVAK ALIADOS
--   con profiling_status (leads consultados).
-- Columnas: todas las de orchestrator_audit_events (ae.*)
-- Join clave: lead_id = playground.kua_sensitive_leads_data.id_lead
-- Para funnel Nicole: agrupar por lead_id, usar event_name/timestamps
--   para derivar etapas (ver SKILL-reference.md §6)
-- Solo KAVAK ALIADOS (ECOMERCEAAS no aplica a Nicole)
-- ============================================================
SELECT
    ae.*
FROM kuna_dealers_api_global_refined.orchestrator_audit_events ae
WHERE EXISTS (
    SELECT 1
    FROM playground.kua_sensitive_leads_data l
    WHERE l.id_lead = ae.lead_id
      AND l.criterios_proyecto = 'KAVAK ALIADOS'
      AND l.profiling_status IS NOT NULL
)
