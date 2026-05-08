---
name: kavak-marketplace-eaas-databricks
description: "EaaS Marketplace MX — índice de routing para agentes analíticos. Fuente primaria: Databricks (prd_* catalogs). Redshift = fallback en deprecación. Responde siempre en español."
source: mixed
topics: [entregas, ventas, STR, reservas, cancelaciones, SLAs, inventario, PIX, pricing, guía autométrica, VIPs, tráfico, oportunidades, citas, historial, clasificación, bookings, EaaS, EaaS marketplace, funnel EaaS, booking funnel]
---

# Kavak EaaS — Índice del skill (Agent Router)

**Uso:** Lee SOLO este archivo al recibir un prompt. Ve a los archivos de detalle únicamente para la sección que necesitas. No cargues `SKILL-reference.md` completo — es de 1,300 líneas y solo se necesita para edge cases.

**Idioma:** Responde siempre en español.

---

## Routing rápido — ¿qué necesito?

### 📦 Entregas / Ventas / STR / Reservas / Cancelaciones / SLAs
**Query primaria:** `references/query-booking-funnel-eaas-databricks.sql` ✅  
**Clasificar resultados:** `CLASIFICACION_EAAS.md` → sección "CTEs Versión Databricks" + `assign_categoria`  
**Dedup obligatorio antes de agregar:** ver §Dedup crítico abajo  
**Detalle por KPI:** `SKILL-reference.md`
- Entregas → §KPI 8 | Reservas → §KPI 6 | STR → §KPI 10
- Cancelaciones → §KPI 9 | Ventas → §KPI 7 | SLAs → §KPI 11

### 🏷️ Inventario publicado / conteo de autos / aging
**Query primaria:** `references/query-inventory-databricks.sql` ✅  
**Filtro publicado:** `status IN ('AVAILABLE', 'BOOKED') AND published = TRUE`  
**Detalle:** `SKILL-reference.md` §KPI 1

### 💰 PIX / Pricing / Guía Autométrica
**Query primaria:** `references/query-inventory-databricks.sql` ✅ (incluye `precio_base`, `market_price`, `guia_price`, `guia_buy_price`, `km_factor`)  
**Fix GA incluido en query:** `guia_price > 10000` + ROW_NUMBER por `guia_create_date DESC`  
**Detalle:** `SKILL-reference.md` §KPI 2

### 👁️ VIPs / tráfico / vistas al auto
**Query primaria:** `references/query-vips-catalog-databricks.sql` ✅  
**Campo stock_id:** `CAST(bk_stock AS BIGINT)` — usa `inv_date` (no `bk_date`) para fechas  
**VPD correcto:** `SUM(roll_01d_vips)` / días con `flag_published = 1` (no todos los días)  
**⚠️ Desglose diario por dealer:** `references/query-vips-daily-dealer.sql` (Redshift — deprecated)  
**Detalle:** `SKILL-reference.md` §KPI 3

### 🎯 Oportunidades (intención de compra)
**Query primaria:** `references/query-oportunidades-eaas-databricks.sql` ✅  
**Email internos:** filtrar `LOWER(opp.leademail__c) NOT LIKE '%@kavak.com'` (`account.email__c` está enmascarado en Databricks)  
**Gap conocido:** +13 filas vs Redshift (leademail__c=NULL no filtrado, no afecta KPI EaaS BU)  
**Detalle:** `SKILL-reference.md` §KPI 4

### 📅 Citas
**⚠️ SOLO REDSHIFT — sin equivalente Databricks aún:**  
**Query:** `references/query-citas-eaas.sql`  
**Filtro crítico obligatorio:** `LOWER(v.vehicle_type__c) = 'third_party'` — sin esto = 0 citas  
**Detalle:** `SKILL-reference.md` §KPI 5

### 📈 Historial / análisis longitudinal
```
references/query-eaas-history-databricks.sql        ← historial EaaS completo
references/query-historical-reserved-databricks.sql ← reservas históricas
references/query-historical-car-meta-databricks.sql ← metadata histórica por auto
references/query-price-changes-databricks.sql       ← cambios de precio
```

### 🏷️ Clasificar bookings por categoría EaaS
Lee `CLASIFICACION_EAAS.md` completo:
- Si ejecutas en **Databricks** → sección "CTEs SQL Base — Versión Databricks"
- Si ejecutas en **Redshift** → sección "CTEs SQL Base" (original)
- Función Python `assign_categoria` es idéntica en ambos casos

---

## Dedup crítico (aplicar SIEMPRE antes de agregar bookings)

```python
# 1. Priorizar Closed Won sobre otras etapas
df['_prio'] = df['opp_stagename'].apply(
    lambda s: 0 if any(w in str(s) for w in ['Closed Won','Cerrada Ganada']) else 1)
df = (df.sort_values(['_prio','fecha_reserva'], ascending=[True,False])
        .drop_duplicates(subset=['customer_email','stock_id'], keep='first')
        .drop(columns='_prio'))

# 2. Marcar reserva_unica (la más reciente por email+VIN = la real)
df['reserva_unica'] = (
    df.groupby(['customer_email','vin'])['fecha_reserva']
      .transform(lambda x: (x == x.max()).astype(int)))

# 3. bkg_cancels para STR: SOLO de reserva_unica=1
bkg_c = df.loc[df['reserva_unica']==1, 'Booking_Cancellation'].fillna(0).sum()
```

---

## Renombres columnas Databricks (aplica a queries `-databricks.sql`)

| Campo lógico | Nombre Redshift | Nombre Databricks | Tabla |
|---|---|---|---|
| Email cliente | `customer_email` | `email` (SHA-256 hashed) | bookings_history |
| ID oportunidad | `opp_id` | `opportunity_id` | bookings_history |
| Fecha reserva | `reservation_createddate` | `fecha_reserva` | bookings_history |
| Hub entrega | `hub_entrega_v2` | `hub_entrega` | bookings_history |
| Email plano | `account.email__c` ❌ enmascarado | `opp.leademail__c` ✅ | SF opportunity |
| Marca | `make` | `make_name` | sku_api_catalog |
| Modelo | `model` | `model_name` | sku_api_catalog |
| Año | `year` | `version_year` | sku_api_catalog |
| Versión | `version` | `name` | sku_api_catalog |
| Stock ID en VIPs | — | `CAST(bk_stock AS BIGINT)` | catalog_inventory_velocity |

---

## Catálogos Databricks (prefijos de 3 partes)

| Datos | Catálogo Databricks | Schema |
|---|---|---|
| Seller Center (autos, dealers, precios) | `prd_refined` | `seller_api_global_refined` |
| Salesforce (opp, reservas, citas, entregas) | `prd_refined` | `salesforce_latam_refined` |
| Bookings history | `prd_datamx_serving` | `serving` |
| VIPs diarios (catalog_inventory_velocity) | `prd_datamx_serving` | `serving` |
| Inventario histórico | `prd_serving` | `inventory` |
| Compras Kavak (flag_kavak_bought) | `prd_serving` | `inventory` → `inventory_transactions_netsuite_global` |
| Pricing actual | `prd_pricing_serving` | `pricing` → `pricing_stock_current` |
| SKUs / catálogo | `prd_pricing_serving` | `sku` → `sku_api_catalog` |
| Guía Autométrica | `prd_pricing_serving` | `samples` → `pricing_mapping_sample_guia_autometrica` |

---

## Gaps aceptados en Databricks (documentar en respuestas cuando aplique)

| Gap | Impacto | Workaround |
|---|---|---|
| `entity_full_name` enmascarada | `stocks_alquiladora` proxy ~71% | `dealer.name LIKE 'Carshop%'` |
| Element Fleet no identificable | ~3% bookings clasifican mal (no afecta EaaS BU) | `stocks_element` = placeholder vacío |
| `account.email__c` enmascarada | No filtrar internos por este campo | Usar `opp.leademail__c` |
| 23 stocks sin `fecha_entrega` | Undercount menor en historial | Gap documentado |
| +13 opps vs Redshift | `leademail__c=NULL` no filtrado | No afecta KPI EaaS BU |

---

## Reglas de negocio que no se negocian

| Regla | Detalle |
|---|---|
| **Dealer name** | Siempre `dealer.name` — nunca `company.name` |
| **Inventario publicado** | `status IN ('AVAILABLE','BOOKED') AND published = TRUE` — nunca solo `AVAILABLE` |
| **EaaS BU** | `categoria IN ('EaaS', 'EaaS FBK', 'EaaS B2B FBK')` |
| **Vista para reportes ejecutivos** | EVENTO (`fecha_entrega_final`) — no cohort (`fecha_reserva_original`) |
| **STR denominador** | `Ent.Brutas + Bkg.Cancels` — bkg_cancels SOLO de `reserva_unica=1` |
| **PIX thresholds** | ≤103% verde · 103–108% ámbar · >108% rojo |
| **PIX formato** | Siempre 2 decimales: `104.35%` |
| **Fix Guía Autométrica** | `WHERE guia_price > 10000` + ROW_NUMBER por `guia_create_date DESC` |
| **VPD correcto** | Dividir por días `flag_published=1`, no por todos los días |
| **Clasificación desde DB** | Nunca desde CSVs de caché — usar CTEs de `CLASIFICACION_EAAS.md` |

---

## Números de referencia validados (Ene–Mar 2026)

| Mes | Inv Pub Prom | Reservas | STR Neto | Ent. Netas | Opps únicas | VPD pub |
|-----|-------------|---------|---------|-----------|------------|--------|
| Ene-26 | 430.6 | 129 | 40.3% | 48 | 8,832 | 31.35 |
| Feb-26 | 465.5 | 107 | 38.9% | 35 | 6,985 | 24.32 |
| Mar-26 | 506.6 | 105 | 33.3% | 25 | 7,909 | 16.27 |

Si tus números difieren significativamente, revisar: dedup aplicado · `reserva_unica=1` en STR · vista EVENTO para entregas.

---

## Archivos de este skill

```
kavak-marketplace-eaas-databricks/
├── SKILL.md                    ← Este archivo (índice — leer primero)
├── SKILL-reference.md          ← Referencia completa 1,300 líneas (leer solo la sección necesaria)
├── CLASIFICACION_EAAS.md       ← CTEs + assign_categoria (Redshift y Databricks)
└── references/
    ├── [DATABRICKS — primarias] ✅
    ├── query-booking-funnel-eaas-databricks.sql   ← KPIs 6-11
    ├── query-inventory-databricks.sql             ← KPIs 1-2
    ├── query-vips-catalog-databricks.sql          ← KPI 3
    ├── query-oportunidades-eaas-databricks.sql    ← KPI 4
    ├── query-price-changes-databricks.sql
    ├── query-eaas-history-databricks.sql
    ├── query-historical-reserved-databricks.sql
    ├── query-historical-car-meta-databricks.sql
    ├── [REDSHIFT — fallback, en deprecación] ⚠️
    ├── query-booking-funnel-eaas.sql
    ├── query-citas-eaas.sql                       ← KPI 5 (sin equiv. Databricks)
    ├── query-inventory-daily-snapshot.sql         ← series temporales (sin equiv. Databricks)
    ├── query-oportunidades-eaas.sql
    ├── query-vips-daily-dealer.sql                ← desglose dealer (sin equiv. Databricks)
    ├── query-vips-totales-por-auto.sql
    └── returns_override.csv                       ← 4 devoluciones manuales (NO modificar)
```

**Para edge cases no cubiertos aquí:** leer solo la sección relevante de `SKILL-reference.md` (usar los headers `### KPI N`, `## Errores críticos`, `## Casos especiales`, etc.).
