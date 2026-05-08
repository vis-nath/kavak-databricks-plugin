# Clasificación EaaS — Manual de Referencia Completo

**Versión:** 3.0
**Última actualización:** 2026-04-10
**Autor:** Helio Requena
**Estado:** Fuente de verdad para todos los KPIs del canal EaaS

---

## 🚨 REGLA DE ORO — Clasificación siempre desde Redshift

**NUNCA usar CSVs de caché para clasificar bookings.** La clasificación correcta requiere:
1. `stocks_eaas` CTE → `salesforce_latam_refined.vehicle` (type='third_party')
2. `vins_eaas` CTE → `seller_api_global_refined.variant_availability` (VIN en SC)
3. `eaas_prev_by_account` CTE → reservas históricas del mismo account+VIN como third_party
4. `b2b_flag` → `salesforce_latam_refined.opportunity.b2b__c`

**El único CSV permitido:** `references/returns_override.csv` — parche de devoluciones manuales no registradas en SF.

---

## ⚠️ Principio Central

La clasificación en 6 categorías (EaaS, EaaS FBK, EaaS B2B FBK, Retail FBK, B2B FBK, Third_Party B2B) se deriva del **VIN + account (email como proxy) + si Kavak compró el auto** de cada reserva única.

**Reserva única = `account + VIN`** — no `email + stock_id`. El mismo cliente puede generar múltiples reservas del mismo auto físico con distintos stock_ids (ej: el auto pasa de third_party a Kavak). Todas esas reservas son una sola unidad de negocio.

Es la misma lógica para **todos los KPIs de funnel**: Reservas, Ventas, Entregas, Cancelaciones, STR, SLAs.

---

## Las 6 Categorías

### 1. EaaS (Dealer aliado en Seller Center — cliente directo)
- Stock de la reserva es `type__c = 'third_party'` en SF vehicle
- **Y** Kavak NO compró el auto (`flag_kavak_bought = 0`)
- **Y** b2b = false
- **Dealers típicos:** Grupo Río, Grupo AlbaCar, Rada Motors, Dalcar, Alternativa Seminuevos, Finakar, Wecars, ISMO, NRFINANCE b2b=false, Real del Barrio, Carshop (cuando Kavak no compra), etc.
- **La entrega puede ser en hub KVK o hub Aliado** — es una dimensión adicional (Hub_Type)

### 2. EaaS FBK (Kavak compró el auto y lo vendió al MISMO cliente que lo había reservado como EaaS)
- `flag_kavak_bought = 1` — Kavak adquirió el auto (cualquier fuente: Alquiladora, Carshop, agencia)
- **Y** `had_eaas_res_same_account = 1` — el mismo account/email tenía una reserva previa de ese VIN como third_party
- **Y** b2b = false
- **Definición de negocio:** El cliente reservó el auto como EaaS, la venta no se concretó en ese canal, Kavak lo compró y se lo vendió directamente al mismo cliente. El cliente interactuó con EaaS — cuenta como EaaS BU.
- **Caso real:** VIN 93Y1R5F56RJ837420 — reservado en stock 1002236 (Carshop/third_party) por missbetty@live.com.mx, luego Kavak lo compra y lo vende bajo stock 480684 al mismo email.
- **Aplica para cualquier agencia**, no solo Alquiladora. Hasta ahora solo ocurrió con Carshop.

### 3. EaaS B2B FBK (mismo que EaaS FBK pero la venta final es B2B)
- `flag_kavak_bought = 1`
- **Y** `had_eaas_res_same_account = 1` — mismo account tuvo reserva previa del VIN como third_party
- **Y** b2b = true
- **Definición de negocio:** Igual que EaaS FBK pero la transacción final es B2B. Cuenta como EaaS BU porque el origen fue EaaS.

### 4. Retail FBK (Kavak compró el auto — cliente distinto al que lo había reservado como EaaS, o nunca estuvo en EaaS)
- `flag_kavak_bought = 1`
- **Y** `had_eaas_res_same_account = 0` — el account de la venta final NO tenía reserva previa del VIN como third_party
- **Y** b2b = false
- **Definición de negocio:** Kavak compró el auto (de Alquiladora, Element u otra fuente) y lo vendió a un cliente diferente, o el auto nunca estuvo en EaaS. No hay trazabilidad de cliente entre el canal EaaS y la venta final.

### 5. B2B FBK (Kavak compró el auto — cliente distinto o sin historia EaaS, venta B2B)
- `flag_kavak_bought = 1`
- **Y** `had_eaas_res_same_account = 0`
- **Y** b2b = true
- **Definición de negocio:** Igual que Retail FBK pero la venta final es B2B.

### 6. Third_Party B2B (Dealer aliado en SC — venta B2B, sin compra de Kavak)
- Stock es `type__c = 'third_party'` en SF vehicle
- **Y** `flag_kavak_bought = 0`
- **Y** b2b = true
- **Dealer conocido:** NRFINANCE (confirmado Apr-2026, primeras entregas Mar-26)
- **Definición de negocio:** El stock vive en Seller Center como third_party, pero la transacción es B2B. Se identifica y separa para P&L, pero **NO suma al canal EaaS** (KPIs de reservas, entregas, STR).
- **⚠️ No confundir con EaaS** — EaaS siempre es b2b=false y sin compra de Kavak.

---

## CTEs SQL Base (copiar en cualquier query)

```sql
-- ─── CTE 1: Todos los stocks que Kavak compró (cualquier fuente) ─────────────
-- Para breakdown de fuente usar flag_alquiladora / flag_element en Python
stocks_kavak_bought AS (
    SELECT DISTINCT TRIM(SPLIT_PART(item_name, ' ', 2)) AS stock_id
    FROM serving.accounting_entry
    WHERE subsidiary_name = 'UVI TECH SAPI MEXICO'
      AND account_number = '116-001'
      AND accounting_entry_transaction_type = 'Item Receipt'
      AND item_name LIKE 'AUTO %'
      AND TRIM(SPLIT_PART(item_name, ' ', 2)) <> ''
),

-- ─── CTE 2: Stocks de Alquiladora (Hertz/Carshop) — para breakdown ──────────
stocks_alquiladora AS (
    SELECT DISTINCT TRIM(SPLIT_PART(item_name, ' ', 2)) AS stock_id
    FROM serving.accounting_entry
    WHERE subsidiary_name = 'UVI TECH SAPI MEXICO'
      AND account_number = '116-001'
      AND accounting_entry_transaction_type = 'Item Receipt'
      AND item_name LIKE 'AUTO %'
      AND entity_full_name ILIKE '%ALQUILADORA DE VEHICUL%'
      AND TRIM(SPLIT_PART(item_name, ' ', 2)) <> ''
),

-- ─── CTE 3: Stocks de Element Fleet — para breakdown ────────────────────────
stocks_element AS (
    SELECT DISTINCT TRIM(SPLIT_PART(item_name, ' ', 2)) AS stock_id
    FROM serving.accounting_entry
    WHERE subsidiary_name = 'UVI TECH SAPI MEXICO'
      AND account_number = '116-001'
      AND accounting_entry_transaction_type = 'Item Receipt'
      AND item_name LIKE 'AUTO %'
      AND entity_full_name ILIKE '%ELEMENT FLEET%'
      AND TRIM(SPLIT_PART(item_name, ' ', 2)) <> ''
),

-- ─── CTE 4: Stocks EaaS por SF vehicle type='third_party' ──────────────────
stocks_eaas AS (
    SELECT DISTINCT v.stockid__c AS stock_id
    FROM salesforce_latam_refined.vehicle v
    WHERE v.stockid__c IS NOT NULL
      AND TRIM(v.stockid__c) <> ''
      AND LOWER(v.type__c) = 'third_party'
),

-- ─── CTE 5: VINs que alguna vez estuvieron en seller_api como third_party ───
vins_eaas AS (
    SELECT DISTINCT json_extract_path_text(va.details, 'vin') AS vin
    FROM seller_api_global_refined.variant_availability va
    WHERE LOWER(va.business_module) = 'third_party'
      AND json_extract_path_text(va.details, 'vin') IS NOT NULL
      AND json_extract_path_text(va.details, 'vin') <> ''
),

-- ─── CTE 6: Reservas previas EaaS por account (email) + VIN ─────────────────
-- Detecta si el mismo cliente ya había reservado ese VIN como third_party
-- "Mismo cliente" = account_id de SF. Email es proxy práctico si account no está disponible.
eaas_prev_by_account AS (
    SELECT DISTINCT
        v.vin__c          AS vin,
        opp.accountid     AS account_id,
        opp.leademail__c  AS email
    FROM salesforce_latam_refined.reservation__c r
    JOIN salesforce_latam_refined.opportunity opp ON opp.id = r.opportunityid__c
    JOIN salesforce_latam_refined.vehicle v       ON v.stockid__c = r.stockid__c
    WHERE LOWER(v.type__c) = 'third_party'
      AND v.vin__c IS NOT NULL
      AND v.vin__c <> ''
)
```

### Flags derivados (añadir al SELECT principal)

```sql
CASE WHEN kb.stock_id  IS NOT NULL THEN 1 ELSE 0 END AS flag_kavak_bought,
CASE WHEN sa.stock_id  IS NOT NULL THEN 1 ELSE 0 END AS flag_alquiladora,   -- para breakdown
CASE WHEN el.stock_id  IS NOT NULL THEN 1 ELSE 0 END AS flag_element,        -- para breakdown
CASE WHEN se.stock_id  IS NOT NULL THEN 1 ELSE 0 END AS flag_eaas_third_party,
CASE WHEN ve.vin       IS NOT NULL THEN 1 ELSE 0 END AS flag_vin_seller_api,
CASE WHEN ep.vin       IS NOT NULL THEN 1 ELSE 0 END AS had_eaas_res_same_account,
opp.b2b__c                                            AS opp_b2b_flag
-- El join para had_eaas_res_same_account usa el VIN del auto actual + email del cliente actual:
-- LEFT JOIN eaas_prev_by_account ep ON ep.vin = <vin_del_auto> AND ep.email = <email_cliente>
```

### Lógica de categoría (Python — `assign_categoria`)

```python
def assign_categoria(row):
    b2b      = str(row.get('opp_b2b_flag', 'false')).lower() == 'true'
    bought   = int(row.get('flag_kavak_bought', 0)) == 1         # Kavak compró el auto
    eaas_tp  = int(row.get('flag_eaas_third_party', 0)) == 1    # stock ES third_party en SF
    vin_sa   = int(row.get('flag_vin_seller_api', 0)) == 1       # VIN estuvo en SC
    same_cli = int(row.get('had_eaas_res_same_account', 0)) == 1 # mismo cliente reservó el VIN como EaaS
    in_sc    = eaas_tp or vin_sa

    if bought and same_cli and not b2b:  return 'EaaS FBK'        # Kavak compró + mismo cliente + b2c
    if bought and same_cli and b2b:      return 'EaaS B2B FBK'    # Kavak compró + mismo cliente + b2b
    if bought and not same_cli and b2b:  return 'B2B FBK'         # Kavak compró + otro cliente + b2b
    if bought and not same_cli:          return 'Retail FBK'      # Kavak compró + otro cliente + b2c
    if in_sc and b2b:                    return 'Third_Party B2B' # SC + b2b + Kavak no compró
    if in_sc:                            return 'EaaS'            # SC + b2c + Kavak no compró
    return 'Sin clasificar'
```

⚠️ **Campo b2b:** usar `opp_b2b_flag` (de `opp.b2b__c` en Salesforce) — NO `bh.b2b` de bookings_history.
⚠️ **`had_eaas_res_same_account`:** join por VIN + email/account_id del cliente de la reserva final contra `eaas_prev_by_account`.

**6 categorías canónicas (v3 — redefinidas Apr-2026):**

| Categoría | EaaS BU | Kavak compró | Mismo cliente | b2b | Fuente típica |
|-----------|:-------:|:------------:|:-------------:|:---:|---|
| **EaaS** | ✅ | ❌ | — | ❌ | Agencias: Grupo Río, AlbaCar, Rada, ISMO, Wecars... |
| **EaaS FBK** | ✅ | ✅ | ✅ | ❌ | Carshop (hasta ahora); cualquier agencia aplica |
| **EaaS B2B FBK** | ✅ | ✅ | ✅ | ✅ | Carshop/Alquiladora — mismo cliente, empresa |
| **Retail FBK** | ❌ | ✅ | ❌ | ❌ | Alquiladora, Element Fleet — cliente diferente |
| **B2B FBK** | ❌ | ✅ | ❌ | ✅ | Alquiladora, Element Fleet — empresa, diferente cliente |
| **Third_Party B2B** | ❌ | ❌ | — | ✅ | NRFINANCE — SC pero B2B |

> **Señales clave:**
> - `flag_kavak_bought` = Kavak adquirió el auto vía `accounting_entry` Item Receipt (cualquier entidad)
> - `had_eaas_res_same_account` = ese mismo VIN fue reservado como third_party por el mismo account/email
> - `flag_alquiladora` / `flag_element` = solo para breakdown de fuente en reportes
> - `in_sc = flag_eaas_third_party OR flag_vin_seller_api` — el VIN estuvo en Seller Center

> **⚠️ Números históricos invalidados:** la v3 requiere re-correr el pipeline completo con nueva lógica de dedup y `eaas_prev_by_account`.

---

## Buckets de Negocio

Las 6 categorías se agrupan en 2 buckets para reportes y KPIs:

| Bucket | Categorías incluidas | Descripción |
|--------|---------------------|-------------|
| **EaaS BU** | EaaS + EaaS FBK + EaaS B2B FBK | Dealer aliado participó activamente via Seller Center — KPI principal del canal |
| **Aliados EaaS** | Retail FBK + B2B FBK + Third_Party B2B | Flujo fuera de SC o B2B puro — se reporta informativo, no es el KPI EaaS |

```python
eaas_bu     = df[df['categoria'].isin(['EaaS', 'EaaS FBK', 'EaaS B2B FBK'])]
aliados_eas = df[df['categoria'].isin(['Retail FBK', 'B2B FBK', 'Third_Party B2B'])]
```

### Tabla de reporte canónica (estructura validada Apr-2026)

El reporte estándar desglosa por `categoria + dealer_name`. Estructura esperada:

```
TOTAL EaaS BU
  EaaS — Carshop          ← categoria='EaaS', dealer='Carshop'
  EaaS — Agencias         ← categoria='EaaS', dealer != 'Carshop'
  TOTAL EaaS (agencias)   ← subtotal de las dos filas anteriores
  EaaS FBK — Carshop      ← categoria='EaaS FBK'
  EaaS B2B FBK — Carshop  ← categoria='EaaS B2B FBK'
  TOTAL EaaS              ← suma del bucket EaaS BU

TOTAL Aliados EaaS
  Retail FBK — Carshop    ← categoria='Retail FBK', flag_alquiladora=1
  Retail FBK — Element    ← categoria='Retail FBK', flag_element=1
  B2B FBK — Carshop       ← categoria='B2B FBK', flag_alquiladora=1
  B2B FBK — Element       ← categoria='B2B FBK', flag_element=1
  TOTAL Aliados EaaS      ← suma del bucket Aliados

GRAN TOTAL
```

### Regla de oro para KPIs

```python
# ¿Cuántas entregas EaaS BU?
eaas_kpi = df[df['categoria'].isin(['EaaS', 'EaaS FBK', 'EaaS B2B FBK'])]['Delivery'].sum()

# ¿Cuántas entregas Aliados EaaS?
aliados_eaas = df[df['categoria'].isin(['B2B FBK', 'Retail FBK'])]['Delivery'].sum()

# Sub-breakdown por dealer dentro de cada categoría
df.groupby(['categoria', 'dealer_sub'])['Delivery'].sum()
# donde dealer_sub = 'Carshop' / 'Element' / 'Agencias'
```

### Cómo derivar `dealer_sub` en Python

```python
def get_dealer_sub(row):
    if row.get('flag_alquiladora') == 1:  return 'Carshop'
    if row.get('flag_element') == 1:      return 'Element'
    return 'Agencias'  # todos los demás dealers EaaS
```

**Nota sobre EaaS B2B FBK:** se incluye en el bucket EaaS porque el auto pasó por SC (`in_sc=1`). Si el acuerdo PnL cambia, consultar con el equipo de FP&A antes de mover.

---

## Dealer Name — Lookup Correcto

### Join chain en seller_api

```sql
variant_stock vs
  JOIN variant_availability va ON va.id = vs.variant_availability_id
  JOIN availability_zone az ON az.id = va.availability_zone_id
  JOIN company c ON c.id = az.company_id
  JOIN dealer d ON d.id = c.dealer_id
-- d.name = dealer_name (campo correcto)
```

**⚠️ NUNCA usar `company.name` para el dealer** — `company.name` puede decir "Hertz" mientras `dealer.name` dice "Carshop". Siempre usar `dealer.name`.

### Query maestro de dealers (Python)

```sql
SELECT DISTINCT
    json_extract_path_text(va.details, 'vin') AS vin,
    vs.legacy_stock_id                        AS stock_id,
    d.name                                    AS dealer_name
FROM seller_api_global_refined.variant_stock vs
JOIN seller_api_global_refined.variant_availability va ON va.id = vs.variant_availability_id
JOIN seller_api_global_refined.availability_zone az ON az.id = va.availability_zone_id
JOIN seller_api_global_refined.company c ON c.id = az.company_id
JOIN seller_api_global_refined.dealer d ON d.id = c.dealer_id
WHERE LOWER(va.business_module) = 'third_party'
  AND vs.legacy_stock_id IS NOT NULL
LIMIT 400000
```

### Orden de fallbacks para dealer_name en Python

1. **Match por VIN** — `df.merge(df_vin, on='vin')` → cubre la mayoría
2. **Match por stock_id** — `df.merge(df_stk, left_on='stock_id', right_on='stock_id')` → cubre VIN mismatch entre SF y seller_api
3. **Flags de accounting_entry:**
   - `flag_element=1` → `'Element Fleet'`
   - `flag_alquiladora=1` → `'Carshop'`
4. Si nada aplica → `'Sin clasificar'` (investigar)

### Dealers activos (2026-03)

| Dealer | Categoría típica | Notas |
|--------|-----------------|-------|
| Carshop | EaaS FBK / Retail FBK / B2B FBK | Carshop FBK = Alquiladora |
| Grupo Río | EaaS | Agencia independiente |
| Grupo AlbaCar | EaaS | Agencia independiente |
| Rada Motors | EaaS | Agencia independiente |
| Dalcar | EaaS | Agencia independiente |
| Alternativa Seminuevos | EaaS | Agencia independiente |
| Finakar | EaaS | Agencia independiente |
| Wecars | EaaS | Agencia independiente |
| ISMO | EaaS | Agencia independiente |
| NRFINANCE | EaaS | Agencia independiente |
| Real del Barrio | EaaS | Agencia independiente |
| Element Fleet | Retail FBK | Solo en accounting_entry, no en seller_api |

---

## Aplicación por KPI

### KPIs que usan stock_id como llave (todos)

| KPI | Tabla fuente | Llave para clasificar |
|-----|--------------|-----------------------|
| Inventario | `seller_api_global_refined.variant_stock` | `legacy_stock_id` → join a accounting_entry + vins_eaas |
| VIPs | `serving.analytics_events` o similar | stock_id de la URL/publicación |
| Oportunidades | `salesforce_latam_refined.opportunity` | via `car_of_interest` → `vehicle.stockid__c` |
| Citas | `salesforce_latam_refined.appointment__c` | via opportunity → vehicle.stockid__c |
| Reservas | `salesforce_latam_refined.reservation__c` | `r.stockid__c` directo |
| Ventas | mismo dataset que Reservas | `r.stockid__c` + `v.vin__c` |
| Entregas | mismo dataset que Reservas | mismo stock_id + VIN |
| Cancelaciones | mismo dataset que Reservas | mismo stock_id + VIN |
| STR | mismo dataset que Reservas | mismo stock_id + VIN |
| SLAs | mismo dataset que Reservas | mismo stock_id + VIN |

### Para Reservas/Ventas/Entregas/STR/Cancelaciones/SLAs

Todos usan el mismo dataset generado por `q_master.py`:

```bash
cd /Users/heliorequena/Documents/Claude_Projects/kavak_eaas_2026/scripts
python3 q_master.py
# Output: DataFrame en memoria — 1 fila por booking único, con dealer_name y categoria
```

El CSV tiene las columnas: `opp_id, stock_id, vin, email_account, dealer_name, categoria, flag_alquiladora, flag_element, flag_eaas_third_party, flag_vin_seller_api, opp_b2b_flag, Delivery, Return, Booking_Cancellation, Active_Booking, mes_entrega, mes_devolucion, Hub_Type, metodo_pago_auto, ...`

Para cualquier breakdown por categoría:
```python
df.groupby(['mes_entrega', 'categoria'])['Delivery'].sum()
df.groupby(['dealer_name', 'categoria'])['Delivery'].sum()
```

### Para Inventario, VIPs, Oportunidades, Citas

Los CTEs SQL base (arriba) se agregan al query de cada KPI. Ejemplo para Oportunidades:

```sql
WITH
stocks_alquiladora AS (...),  -- mismo CTE de siempre
stocks_element AS (...),
stocks_eaas AS (...),
vins_eaas AS (...)
SELECT
    opp.id,
    CASE WHEN sa.stock_id IS NOT NULL THEN 1 ELSE 0 END AS flag_alquiladora,
    CASE WHEN el.stock_id IS NOT NULL THEN 1 ELSE 0 END AS flag_element,
    CASE WHEN se.stock_id IS NOT NULL THEN 1 ELSE 0 END AS flag_eaas_third_party,
    CASE WHEN ve.vin      IS NOT NULL THEN 1 ELSE 0 END AS flag_vin_seller_api,
    opp.b2b__c                                           AS opp_b2b_flag
FROM salesforce_latam_refined.opportunity opp
JOIN salesforce_latam_refined.vehicle v ON v.id = opp.car_of_interest__c
LEFT JOIN stocks_alquiladora sa ON sa.stock_id = v.stockid__c
LEFT JOIN stocks_element el     ON el.stock_id = v.stockid__c
LEFT JOIN stocks_eaas se        ON se.stock_id = v.stockid__c
LEFT JOIN vins_eaas ve          ON ve.vin = v.vin__c
WHERE (sa.stock_id IS NOT NULL OR se.stock_id IS NOT NULL OR el.stock_id IS NOT NULL)
```

---

## Universo EaaS — Filtro WHERE

Para que un registro entre al universo EaaS debe cumplir:

```sql
-- Al menos uno de los tres orígenes:
(sa.stock_id IS NOT NULL    -- Alquiladora en accounting_entry
 OR se.stock_id IS NOT NULL -- SF vehicle type='third_party'
 OR el.stock_id IS NOT NULL -- Element Fleet en accounting_entry
)
-- Y condición B2B:
AND (
    COALESCE(opp.b2b__c, 'false') = 'false'  -- No B2B
    OR ve.vin IS NOT NULL                      -- O B2B pero VIN en seller_api
)
```

> **Nota B2B:** Los B2B que NO tienen VIN en seller_api se excluyen porque son transacciones B2B puras que no pasan por el flujo EaaS estándar.

---

## Dedup de Reservas (crítico para Ventas/STR)

**1 booking único = 1 fila por `account (email) + VIN`**

El mismo cliente puede generar múltiples reservas del mismo auto físico bajo distintos stock_ids (ej: primero como third_party, luego como Kavak tras compra). Son la misma unidad de negocio.

```python
# Paso 1: traer VIN de cada reserva (via SF vehicle.vin__c)
# Paso 2: agrupar por email + VIN — la más reciente es la válida
# Paso 3: fecha_reserva_original = fecha de la PRIMERA del grupo (no la última)
# Paso 4: las cancelaciones intermedias del mismo grupo NO cuentan como Booking_Cancellation

df_raw['_stage_priority'] = df_raw['opp_stagename'].apply(
    lambda s: 0 if any(w in str(s) for w in ['Closed Won', 'Cerrada Ganada']) else 1
)

# Fecha de la primera reserva del grupo email+VIN
df_raw['fecha_reserva_original'] = (
    df_raw.groupby(['email_account', 'vin'])['reservation_createddate'].transform('min')
)

# Dedup: por email+VIN, tomar la más reciente (priorizando Closed Won)
df = (df_raw
      .sort_values(['_stage_priority', 'reservation_createddate'], ascending=[True, False])
      .drop_duplicates(subset=['email_account', 'vin'], keep='first'))

# IMPORTANTE: las filas eliminadas por este dedup NO son cancelaciones reales.
# No sumar al STR denominador. Son artefactos del proceso operativo.
```

**Ejemplo real:**
- `a1cPb00000F6ywvIAB` — 2026-01-17, stock 1002236 (third_party), cancelada → se elimina en dedup
- `a1cPb00000F99EjIAJ` — cancelada → se elimina en dedup
- `a1cPb00000FBFfpIAH` — 2026-01-20, stock 480684 (Kavak) → **reserva válida, fecha_reserva_original = 2026-01-17**
- VIN: 93Y1R5F56RJ837420 | email: missbetty@live.com.mx → categoría: **EaaS FBK**

**⚠️ Si usas `keep='first'` sin prioridad Closed Won**, una Closed Lost posterior silencia la Closed Won con entrega real → perderás entregas.

---

## Delivery_Date — 3 niveles de fallback

| Prioridad | Fuente | Cuándo aplica |
|-----------|--------|---------------|
| 1° | `vehicletransfer__c.event_activitydatetime` | Siempre primero |
| 2° | `bookings_history.fecha_completada` o `fecha_entrega` | Si vehicletransfer no tiene la entrega |
| 3° | `opp.closedate` | Si opp es Closed Won/Cerrada Ganada y closedate no es null |

**⚠️ Iceberg S3 intermittente:** `vehicletransfer__c` puede devolver resultados distintos entre runs. Por eso existe el Fallback 3 (Closed Won closedate). Siempre correr el pipeline completo desde Redshift — no depender de CSVs de caché.

---

## Devoluciones

- **Fuente:** `serving.bookings_history.devolucion_date`
- **Se cuentan por `devolucion_date`** (NO por mes de entrega)
- **Devoluciones manuales** — casos que nunca se registraron en Salesforce (proceso omitido operativamente). No aparecen en ninguna fuente de datos, ni Redshift ni Databricks. Son un parche permanente mientras no se corrija el proceso.

**Fuente de verdad para estas devoluciones:**
```
~/.claude/skills/kavak-marketplace-eaas/references/returns_override.csv
```

Formato del CSV:
```
stock_id,email,devolucion_date,dealer
1002527,cynthiavalle.1916@gmail.com,2026-02-06,Carshop
1003307,pavo-real17@hotmail.com,2026-02-16,Dalcar
1002811,1219bere@gmail.com,2026-02-11,Carshop
481314,ara@ignia.vc,2026-02-23,Carshop
```

**Cómo aplicarlo en Python:**
```python
import pandas as pd, os

RETURNS_OVERRIDE = os.path.expanduser(
    '~/.claude/skills/kavak-marketplace-eaas/references/returns_override.csv'
)
df_override = pd.read_csv(RETURNS_OVERRIDE, dtype={'stock_id': str})
df_override['devolucion_date'] = pd.to_datetime(df_override['devolucion_date'])
df_override['_key'] = df_override['stock_id'] + '|' + df_override['email']

# Aplicar sobre el dataset principal
df['_key'] = df['stock_id'].astype(str) + '|' + df['email_account']
mask = df['_key'].isin(df_override['_key'])
df.loc[mask, 'Return'] = 1
df.loc[mask, 'devolucion_date'] = df.loc[mask, '_key'].map(
    df_override.set_index('_key')['devolucion_date']
)
df.drop(columns='_key', inplace=True)
```

**Match: `stock_id + email`** — nunca solo stock_id, para no afectar re-ventas del mismo auto a otro cliente.

> ⚠️ **Proceso:** cada vez que haya una devolución no registrada en SF, agregar una fila al CSV. No editar el código.

---

## Números de Referencia — ⚠️ PUEDEN ESTAR DESACTUALIZADOS

> **Corte: 2026-03-04.** Para números frescos correr `eaas_pipeline.py` (Flujo B en SKILL.md).
> NO usar estos números para reportes — son solo referencia para validar que el pipeline produce resultados coherentes.

## Números de Referencia (corte 2026-03-04, con Element Fleet)

### Distribución por categoría (502 unique bookings histórico)
| Categoría | Bookings |
|-----------|---------|
| EaaS | 395 |
| Retail FBK | 54 |
| EaaS FBK | 33 |
| B2B FBK | 20 |

### Entregas por mes — Vista Evento

| Mes | Brutas | Dev | Netas |
|-----|--------|-----|-------|
| Sep-25 | 4 | 0 | 4 |
| Oct-25 | 7 | 0 | 7 |
| Nov-25 | 18 | 1 | 17 |
| Dic-25 | 38 | 0 | 38 |
| Ene-26 | 50 | 0 | 50 |
| Feb-26 | 52 | 4 | 48 |
| **Total ventana** | **169** | **5** | **164** |
| **Total global** | **173** | **5** | **168** |

> **Diferencia vs validación anterior (Feb=49, Total=169):** se agregó Element Fleet (Retail FBK) que aporta +4 entregas en Feb-26 (+4 total global).

### STR Evento (con Element Fleet)
| Mes | Cerradas | STR Bruto | STR Neto |
|-----|----------|-----------|---------|
| Sep-25 | 13 | 30.8% | 30.8% |
| Oct-25 | 20 | 35.0% | 35.0% |
| Nov-25 | 60 | 30.0% | 28.3% |
| Dic-25 | 107 | 35.5% | 35.5% |
| Ene-26 | 132 | 37.9% | 36.4% |
| Feb-26 | 120 | 43.3% | 41.7% |
| **Global** | **459** | **37.7%** | **36.6%** |

---

## Errores Conocidos y Mitigaciones

| Error | Síntoma | Mitigación |
|-------|---------|------------|
| Iceberg S3 intermittente en `vehicletransfer__c` | N entregas distintas entre runs | Fallback 3 (Closed Won) — re-correr pipeline si hay inconsistencia |
| VIN mismatch entre SF vehicle y seller_api details | `flag_vin_seller_api=0` para stock realmente en EaaS | Usar `in_sa = vin_sa OR eaas_tp` en assign_categoria |
| `accounting_entry` tarda en propagarse | Stock nuevo no aparece en Alquiladora CTE | Fallback 3 (Closed Won) actúa como red de seguridad |
| Dedup sin prioridad Closed Won | Entrega real silenciada por Closed Lost posterior | Siempre sort por `_stage_priority` antes de dedup |
| `company.name` vs `dealer.name` | "Hertz" ≠ "Carshop" | Siempre join hasta `dealer` table, usar `dealer.name` |

---

## Archivos del Proyecto

```
/Users/heliorequena/Documents/Claude_Projects/kavak_eaas_2026/
├── scripts/
│   ├── q_master.py                  ← Script maestro — corre el pipeline en memoria
│   └── sql/
│       ├── eaas_booking_sf.sql      ← Query de reservas (fuente de verdad)
│       └── eaas_deliveries.sql      ← Query de entregas (vehicletransfer__c)
└── data/
    ├── deliveries.json              ← Entregas por mes (evento + cohort)
    └── bookings.json                ← STR por mes + pipeline activo
```

---

## CTEs SQL Base — Versión Databricks

> Usar estas CTEs cuando ejecutes `query-booking-funnel-eaas-databricks.sql` o cualquier query propia en Databricks. Las CTEs Redshift de la sección anterior siguen siendo válidas para queries Redshift.
>
> Paridad validada 2026-04-27. Gaps aceptados documentados en cada CTE.

```sql
-- ─── CTE 1: Todos los stocks que Kavak compró ────────────────────────────────
-- Fuente: inventory_transactions_netsuite_global (reemplaza serving.accounting_entry)
-- ⚠️ entity_full_name está ENMASCARADA en Databricks — no se puede distinguir
-- Alquiladora de otras compras por nombre. Ver CTE 2 para proxy Alquiladora.
stocks_kavak_bought AS (
    SELECT DISTINCT CAST(CAST(stock_id AS BIGINT) AS STRING) AS stock_id
    FROM prd_serving.inventory.inventory_transactions_netsuite_global
    WHERE subsidiary_name = 'UVI TECH SAPI MEXICO'
      AND account_name = 'AUTOS'
      AND transaction_type = 'Item Receipt'
      AND stock_id IS NOT NULL
),

-- ─── CTE 2: Stocks de Alquiladora (Carshop/Hertz) — proxy via seller_api ────
-- Redshift usaba entity_full_name ILIKE '%ALQUILADORA%' en accounting_entry.
-- En Databricks ese campo está enmascarado. Proxy: dealer.name LIKE 'Carshop%'.
-- Cobertura al ~71% — gap documentado y aceptado.
stocks_alquiladora AS (
    SELECT DISTINCT vs.legacy_stock_id AS stock_id
    FROM prd_refined.seller_api_global_refined.variant_stock vs
    JOIN prd_refined.seller_api_global_refined.variant_availability va
        ON va.id = vs.variant_availability_id
    JOIN prd_refined.seller_api_global_refined.availability_zone az
        ON az.id = va.availability_zone_id
    JOIN prd_refined.seller_api_global_refined.company co
        ON co.id = az.company_id
    JOIN prd_refined.seller_api_global_refined.dealer d
        ON d.id = co.dealer_id
    WHERE LOWER(va.business_module) = 'third_party'
      AND d.name LIKE 'Carshop%'
      AND vs.legacy_stock_id IS NOT NULL
),

-- ─── CTE 3: Stocks de Element Fleet — NO IDENTIFICABLE en Databricks ─────────
-- entity_full_name enmascarada. Gap aceptado (~3% bookings).
-- Impacto: ~3% de Aliados clasifican como EaaS — no afecta KPI EaaS BU.
stocks_element AS (
    SELECT CAST(NULL AS STRING) AS stock_id WHERE 1=0
),

-- ─── CTE 4: Stocks EaaS por SF vehicle type='third_party' ──────────────────
stocks_eaas AS (
    SELECT DISTINCT v.stockid__c AS stock_id
    FROM prd_refined.salesforce_latam_refined.vehicle v
    WHERE v.stockid__c IS NOT NULL
      AND TRIM(v.stockid__c) <> ''
      AND LOWER(v.type__c) = 'third_party'
),

-- ─── CTE 5: VINs que alguna vez estuvieron en seller_api como third_party ───
vins_eaas AS (
    SELECT DISTINCT get_json_object(va.details, '$.vin') AS vin
    FROM prd_refined.seller_api_global_refined.variant_availability va
    WHERE LOWER(va.business_module) = 'third_party'
      AND get_json_object(va.details, '$.vin') IS NOT NULL
      AND get_json_object(va.details, '$.vin') <> ''
),

-- ─── CTE 6: Reservas previas EaaS por account + VIN ─────────────────────────
-- ⚠️ Databricks: account.email__c enmascarado — usar opp.leademail__c como proxy.
eaas_prev_by_account AS (
    SELECT DISTINCT
        v.vin__c            AS vin,
        opp.accountid       AS account_id,
        opp.leademail__c    AS email
    FROM prd_refined.salesforce_latam_refined.reservation__c r
    JOIN prd_refined.salesforce_latam_refined.opportunity opp
        ON opp.id = r.opportunityid__c
    JOIN prd_refined.salesforce_latam_refined.vehicle v
        ON v.stockid__c = r.stockid__c
    WHERE LOWER(v.type__c) = 'third_party'
      AND v.vin__c IS NOT NULL
      AND v.vin__c <> ''
)
```

> La función Python `assign_categoria` y la lógica de flags derivados son idénticas en ambas versiones — no cambian entre Redshift y Databricks.
