-- queries/eaas_history_databricks.sql
-- Set de pares (vin, email) que alguna vez tuvieron una reserva como third_party.
-- Usa r.emailopp__c (texto plano) en lugar de account.email__c (enmascarado en DB).

SELECT DISTINCT
    UPPER(TRIM(v.vin__c))            AS vin,
    LOWER(TRIM(r.emailopp__c))       AS email
FROM prd_refined.salesforce_latam_refined.reservation__c r
JOIN prd_refined.salesforce_latam_refined.vehicle v ON v.id = r.vehicle__c
WHERE (r.isdeleted IS NULL OR r.isdeleted = 'false')
  AND r.country__c = '484'
  AND LOWER(v.type__c) = 'third_party'
  AND v.vin__c IS NOT NULL AND v.vin__c <> ''
  AND r.emailopp__c IS NOT NULL AND r.emailopp__c <> ''
LIMIT 400000
