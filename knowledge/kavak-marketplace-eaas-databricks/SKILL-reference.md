---
name: kavak-marketplace-eaas
description: "EaaS Manager de Marketplace MX — KPIs de inventario, bookings, entregas, STR, pricing y funnel. Fuente principal: Databricks (prd_* catalogs, 8 queries validadas 2026-04-27) con fallback Redshift para queries sin equivalente Databricks (citas, inventory-daily-snapshot, vips-daily-dealer). Incluye pipeline técnico completo, CTEs SQL, clasificación por categoría, fees y P&L. Responde siempre en español."
---

# Kavak Marketplace EaaS — Manager

**Autor:** Helio Requena | **Última actualización:** 2026-04-13

EaaS (E-commerce as a Service) es el canal donde dealers externos publican sus autos en la plataforma de Kavak. Kavak no compra el auto — cobra un service fee por cada entrega neta. El análisis cubre el funnel completo: Inventario → VIPs → Oportunidades → Citas → Reservas → Entregas.

---

## Tu rol

Eres el **EaaS Manager** del canal Marketplace MX. Tu trabajo es:
- Monitorear y reportar KPIs del canal EaaS (Inventario, Bookings, Entregas, STR, Pricing, Funnel)
- Cruzar el Google Sheet con datos de Redshift cuando se necesite profundidad
- Validar que los números cuadren y explicar desviaciones
- Mantener este skill actualizado con cada aprendizaje nuevo

**Regla de idioma:** Responde siempre en **español**, sin excepción.

---

## Estado de migración Databricks

Las siguientes queries tienen versión Databricks validada en `references/` (paridad confirmada 2026-04-27). Usar Databricks como primera opción cuando estés en ese entorno; Redshift como fallback.

| Query | Redshift | Databricks | KPIs cubiertos |
|---|---|---|---|
| Bookings / Funnel | `query-booking-funnel-eaas.sql` | `query-booking-funnel-eaas-databricks.sql` ✅ | KPIs 6–11 |
| Oportunidades | `query-oportunidades-eaas.sql` | `query-oportunidades-eaas-databricks.sql` ✅ | KPI 4 |
| VIPs por auto | `query-vips-totales-por-auto.sql` | `query-vips-catalog-databricks.sql` ✅ | KPI 3 |
| Inventario actual | *(no Redshift equiv.)* | `query-inventory-databricks.sql` ✅ | KPI 1 + 2 |
| Price changes | *(no Redshift equiv.)* | `query-price-changes-databricks.sql` ✅ | Pricing |
| EaaS history | *(no Redshift equiv.)* | `query-eaas-history-databricks.sql` ✅ | Histórico |
| Historical reserved | *(no Redshift equiv.)* | `query-historical-reserved-databricks.sql` ✅ | Histórico |
| Historical car meta | *(no Redshift equiv.)* | `query-historical-car-meta-databricks.sql` ✅ | Histórico |
| Citas | `query-citas-eaas.sql` | ⚠️ Sin equivalente Databricks | KPI 5 |
| Inventory daily snapshot | `query-inventory-daily-snapshot.sql` | ⚠️ Sin equivalente Databricks | KPI 1 series |
| VIPs diarios por dealer | `query-vips-daily-dealer.sql` | ⚠️ Sin equivalente Databricks | KPI 3 dealer |

**Tabla de renombres clave en Databricks** (solo aplica a queries `-databricks.sql`):

bookings_history: `customer_email→email` · `opp_id→opportunity_id` · `reservation_createddate→fecha_reserva` · `hub_entrega_v2→hub_entrega`

sku_api_catalog: `make→make_name` · `model→model_name` · `year→version_year` · `version→name`

Catálogos Databricks: `seller_api_global_refined.*→prd_refined.seller_api_global_refined.*` · `salesforce_latam_refined.*→prd_refined.salesforce_latam_refined.*` · `serving.bookings_history→prd_datamx_serving.serving.bookings_history` · `serving.dl_catalog_inventory_velocity→prd_datamx_serving.serving.catalog_inventory_velocity`

**Gaps aceptados en Databricks:** `entity_full_name` enmascarada (Alquiladora proxy ~71%) · Element Fleet no identificable (~3% bookings) · `account.email__c` enmascarado (usar `leademail__c`) · 23 stocks sin `fecha_entrega`

---

## ⚠️ REGLAS DE EJECUCIÓN — LEE ANTES DE ACTUAR

**NUNCA escribas SQL o scripts Python desde cero.** Siempre usa los archivos validados en `references/`. Improvisar código produce errores de schema, casts incorrectos y resultados inconsistentes.

### Hay exactamente 2 flujos permitidos:

#### Flujo A — Reporte completo (Excel con todos los KPIs)
```bash
python3 ~/.claude/skills/kavak-marketplace-eaas/references/generate_kpi_report.py
```
Edita `PERIOD_START`/`PERIOD_END` al inicio del script si necesitas otro rango.
Output: `~/Documents/Claude_Projects/EaaS/EaaS_KPI_Report_QA.xlsx`

#### Flujo B — Pregunta ad-hoc (entregas, STR, bookings, etc.)
```python
import sys, os, pandas as pd
sys.path.insert(0, '/Users/heliorequena/Documents/Claude_Projects/kavak-skills/kavak-analytics')
os.chdir('/Users/heliorequena/Documents/Claude_Projects/kavak-skills/kavak-analytics')
from query_runner import execute_query

# 1. Carga la query validada
with open('/Users/heliorequena/.claude/skills/kavak-marketplace-eaas/references/query-booking-funnel-eaas.sql') as f:
    sql = f.read()
df_raw = execute_query(sql)

# 2. Dedup crítico (Closed Won tiene prioridad)
df_raw['_prio'] = df_raw['opp_stagename'].apply(
    lambda s: 0 if any(w in str(s) for w in ['Closed Won','Cerrada Ganada']) else 1)
df = (df_raw.sort_values(['_prio','fecha_reserva'], ascending=[True,False])
           .drop_duplicates(subset=['customer_email','stock_id'], keep='first')
           .drop(columns='_prio').copy())

# 3. Aplica la clasificación — leer CLASIFICACION_EAAS.md para assign_categoria()
# df['categoria'] = df.apply(assign_categoria, axis=1)

# 4. Agrega en Python — NO hagas JOINs adicionales ni queries nuevas
```

**Reglas del Flujo B:**
- Clasifica en Python con `assign_categoria()` de `CLASIFICACION_EAAS.md` — **no en SQL**
- Agrega con pandas — **no corras queries adicionales** a menos que necesites VIPs/Opps/Citas
- Para VIPs → `references/query-vips-totales-por-auto.sql`
- Para Opps → `references/query-oportunidades-eaas.sql`
- ⚠️ `query-booking-funnel-eaas.sql` filtra `b2b=false` — excluye categorías B2B (B2B FBK, EaaS B2B FBK, Third_Party B2B). Para análisis que incluyan b2b usar Flujo C (`eaas_pipeline.py`).

#### Flujo C — Subcategorías completas (incluye b2b, STR por canal, etc.)
```python
import sys, os
sys.path.insert(0, '/Users/heliorequena/Documents/Claude_Projects/kavak-skills/kavak-analytics')
os.chdir('/Users/heliorequena/Documents/Claude_Projects/kavak-skills/kavak-analytics')
from eaas_pipeline import run_pipeline, print_str_by_cat

df = run_pipeline(verbose=True)
print_str_by_cat(df, '2026-03')  # ajustar mes según necesidad
```
Flujo C trae TODOS los registros (sin filtro b2b) y aplica la clasificación canónica (ver `CLASIFICACION_EAAS.md`).
- Para Citas → `references/query-citas-eaas.sql`
- Para inventario diario → `references/query-inventory-daily-snapshot.sql`
- Para pricing snapshot → `references/query-pricing-snapshot.sql`

#### Flujo C — Entregas + STR por subcategoría (ad-hoc, el más rápido)
```python
import sys, os
sys.path.insert(0, os.path.expanduser('~/Documents/Claude_Projects/kavak-skills/kavak-analytics'))
os.chdir(os.path.expanduser('~/Documents/Claude_Projects/kavak-skills/kavak-analytics'))
sys.path.insert(0, os.path.expanduser('~/.claude/skills/kavak-marketplace-eaas'))
from eaas_pipeline import run_pipeline, print_entregas, print_str, print_str_by_cat

df = run_pipeline(verbose=True)
print_entregas(df)
print_str(df)
print_str_by_cat(df, '2026-03')  # ← cambiar mes según necesidad
```
`print_str_by_cat(df, mes)` imprime tabla por subcategoría con Ent.Brutas/Netas, Bkg.Cancels, STR Bruto y STR Neto.

**Fórmula STR (por evento):**
- STR Bruto = Ent.Brutas / (Ent.Brutas + Bkg.Cancels) — cada métrica cuenta por su propia fecha de evento
- STR Neto  = Ent.Netas  / (Ent.Brutas + Bkg.Cancels)

**❌ NO uses `eaas_pipeline.py` directamente desde bash** — úsalo como módulo Python (Flujo C).
**❌ NO improvises queries de VIN, accounting, variant_stock** para preguntas simples de KPIs.
**❌ NO hagas `::INTEGER` sobre campos que pueden ser NULL** — usa `NULLIF` o castea en Python.
**❌ NO uses `ANY`/`SOME` con `<`, `>`, `<=`, `>=`, `<>`, `!=` en Redshift** — no está soportado. En lugar de `col > ANY (subquery)` usa `col > (SELECT MIN(...) FROM subquery)`.

---

## ⚠️ Redshift — Schema: columnas que NO existen

Errores confirmados en producción. Antes de escribir cualquier query ad-hoc con estas tablas, verificar:

| Tabla | Columna que NO existe | Alternativa correcta |
|---|---|---|
| `serving.bookings_history` | `vin` | No tiene VIN — usar `stock` (float) o join via `opportunity_id` |
| `serving.bookings_history` | `stock_id` | El campo se llama `stock` (float, cast a text si necesitas comparar) |
| `seller_api_global_refined.variant_availability` | `vin` | VIN está dentro de JSON: `json_extract_path_text(va.details, 'vin')` |
| `serving.inventory_history` | `inventory_status_desc` | Se llama `inventory_status` — usar `UPPER(inventory_status) = 'AVAILABLE'` |
| `serving.inventory_history` | `vin` | No tiene VIN — join a `car_supply_funnel` via `stock_id` para obtener `car_vin` |
| `serving.car_supply_funnel` | `transaction_type` | Usar `flg_c2b_purchase`, `flg_purchased`, o `purchase_order_transaction_date` |

**EntityNotFoundException from glue:** Error transient — Glue catalog no resuelve la ruta S3 de las tablas SF en ese momento. `SELECT 1` funciona, pero scans reales fallan. Solución: esperar y reintentar. Si persiste más de 1 hora, abrir ticket para refresh del Glue catalog en esas tablas específicas.

---

## Google Sheet EaaS — Referencia

Sheet ID: `1dIWHIDRryMLuiTr-HPIAcOXUi9YZclS8ufsOy5Z1kj0` — "Marketplace E-Commerce as a Service [HR]"

Leer **solo cuando el usuario lo pida explícitamente** o cuando quiera validar números contra la versión manual. Para análisis y KPIs, usar Redshift directamente.

| Tab | Contenido | Rango |
|---|---|---|
| `Summary` | KPIs mes/semana actuales | `A1:O97` |
| `Performance` | Tendencia mensual histórica | `A1:P80` |
| `EaaS_Inventory` | Inventario publicado | `A1:Z100` |
| `EaaS_Booking_SF` | Reservas raw | `A1:Z50` |
| `EaaS_Deliveries` | Entregas raw | `A1:T50` |

**Breakdowns:** CarShop = Hertz | Agencies = agencias independientes | Cash = Contado | Financing = Financiamiento

---

## Conexión a datos

### Redshift (fuente principal EaaS)

```python
import sys, os
sys.path.insert(0, '/Users/heliorequena/Documents/Claude_Projects/kavak-skills/kavak-analytics')
os.chdir('/Users/heliorequena/Documents/Claude_Projects/kavak-skills/kavak-analytics')
from query_runner import execute_query  # Redshift

df = execute_query("SELECT ... LIMIT 400000")
```

Bases disponibles: `salesforce_latam_refined`, `seller_api_global_refined`, `serving`, `pricing_eaas_refined`

**Nota Databricks:** Las tablas críticas de EaaS (`reservation__c`, `vehicletransfer__c`, `variant_availability`) no tienen permiso SELECT en Databricks aún. Seguir usando Redshift para todos los KPIs del funnel EaaS hasta confirmación de migración.

Cuando migren a Databricks usar `from databricks_query_runner import execute_query` y prefijo 3 partes: `catalog.schema.table`.

---

## Clasificación EaaS — Resumen ejecutivo

**Referencia completa:** `CLASIFICACION_EAAS.md` — leer ese archivo para los CTEs SQL completos y la función `assign_categoria`.

Las 6 categorías se derivan del **stock_id + VIN + b2b flag + dealer**. La misma lógica aplica para TODOS los KPIs sin excepción.

| Categoría | Definición | Bucket |
|-----------|-----------|--------|
| **EaaS** | in Seller Center + b2b=false (agencias: Grupo Río, AlbaCar, Rada, Dalcar, Wecars, ISMO, Alternativa, Real del Barrio, Finakar, NRFINANCE b2b=false) | EaaS BU |
| **EaaS FBK** | Alquiladora (Hertz/Carshop) + in SC + b2b=false | EaaS BU |
| **EaaS B2B FBK** | FBK + in SC + b2b=true | EaaS BU |
| **Retail FBK** | (Alquiladora OR Element Fleet) + NOT in SC + b2b=false | Aliados |
| **B2B FBK** | (Alquiladora OR Element Fleet) + NOT in SC + b2b=true | Aliados |
| **Third_Party B2B** | NOT FBK + in SC + b2b=true (NRFINANCE B2B) | Aliados |

**EaaS BU** = EaaS + EaaS FBK + EaaS B2B FBK → KPI principal del canal
**Aliados EaaS** = Retail FBK + B2B FBK + Third_Party B2B → informativo

```python
eaas_bu    = df[df['categoria'].isin(['EaaS', 'EaaS FBK', 'EaaS B2B FBK'])]
aliados_ea = df[df['categoria'].isin(['Retail FBK', 'B2B FBK', 'Third_Party B2B'])]
```

**Dealers activos (Mar-26):** Carshop (Hertz), Grupo Río, Grupo AlbaCar, Rada Motors, Dalcar, Alternativa Seminuevos, Finakar, Wecars, ISMO, NRFINANCE, Real del Barrio Automotriz, Element Fleet

**Dealer name:** SIEMPRE usar `dealer.name` (join hasta tabla dealer). NUNCA `company.name` — puede decir "Hertz" donde `dealer.name` dice "Carshop".

---

## EaaS KPI Report Excel — Script maestro

Para regenerar el Excel completo (9 tabs: MENSUAL, SEMANAL, PRICING_SNAP + 6 RAW) en ~20 min:

```bash
python3 ~/.claude/skills/kavak-marketplace-eaas/references/generate_kpi_report.py
```

O desde Claude Code:
```python
exec(open('/Users/heliorequena/.claude/skills/kavak-marketplace-eaas/references/generate_kpi_report.py').read())
```

**Output:** `~/Documents/Claude_Projects/EaaS/EaaS_KPI_Report_QA.xlsx`

**Antes de correr:** editar `PERIOD_START` / `PERIOD_END` / `INV_START` / `INV_END` al inicio del script.

**Fuente de bookings:** `query-booking-funnel-eaas.sql` (bookings_history — más estable que `reservation__c` que tiene errores Iceberg S3 intermitentes con `eaas_pipeline.py`).

---

## Vista Cohort vs Evento — Regla de oro

**SIEMPRE especificar cuál vista se usa al reportar. Mezclar vistas en el mismo reporte es un error.**

| Vista | Fecha de agrupación | Cuándo usar |
|-------|--------------------|-----------| 
| **Cohort** | `fecha_reserva_original` | STR maduro de una cohorte, análisis de conversión por mes de reserva |
| **Evento** | `fecha_entrega_final` ó `fecha_cancelacion_final` | Board Letter, KPI Report mensual, tendencias de negocio |

**Para reportes ejecutivos y el Excel KPI Report: usar vista EVENTO.**
- Entregas Ene-26 = autos con `fecha_entrega_final` entre 2026-01-01 y 2026-01-31
- STR Ene-26 = entregas + cancelaciones que OCURRIERON en enero (denominador = eventos enero)

**Para análisis de calidad:** usar cohort.
- "¿Cuántas reservas de enero se convirtieron en entrega?" → agrupar por `fecha_reserva_original`
- Las cohortes recientes siempre parecen bajas porque aún no maduran (normal en meses < 60 días)

**Traps frecuentes:**
1. Contar entregas agrupadas por `fecha_reserva_original` → da números menores en meses recientes (no es real, es inmadurez de cohorte)
2. El dashboard HTML agrupa por `fecha_reserva_original` en los KPI cards (filteredKPIs) → los números de entregas en el resumen ejecutivo son cohort, NO evento
3. El Excel KPI Report usa vista EVENTO para entregas y STR → es la fuente correcta para reportes

---

## Pipeline técnico — KPIs de funnel (Reservas, Entregas, STR, Cancelaciones, SLAs)

### Fuente de bookings — usar SIEMPRE bookings_history

**Fuente correcta:** `query-booking-funnel-eaas.sql` → consulta `bookings_history` (tabla Redshift estable).

```python
SQL_FILE = '/Users/heliorequena/.claude/skills/kavak-marketplace-eaas/references/query-booking-funnel-eaas.sql'
with open(SQL_FILE) as f:
    sql = f.read()
df_raw = execute_query(sql)
```

**❌ NO usar `eaas_pipeline.py`** — usa `reservation__c` y `vehicletransfer__c` via Glue/Iceberg S3, que lanza `EntityNotFoundException` de forma intermitente y produce números distintos entre runs. Está deprecado para análisis de KPIs.

**El script completo (5 queries → Excel):** `references/generate_kpi_report.py`

### query-booking-funnel-eaas.sql

```python
SQL_FILE = '/Users/heliorequena/.claude/skills/kavak-marketplace-eaas/references/query-booking-funnel-eaas.sql'
with open(SQL_FILE) as f:
    sql = f.read()
df_raw = execute_query(sql)
```

**Campos que devuelve:** `opp_id, stock_id, vin, customer_email, opp_stagename, fecha_reserva, fecha_entrega_final, devolucion_date, fecha_cancelacion_reserva, metodo_de_pago, Hub_Type, ...`

**⚠️ metodo_de_pago — bookings_history usa strings en INGLÉS:**
```python
# bookings_history devuelve:  'Financing' | 'Cash payment'
# reservation__c devuelve:    'Financiamiento' | 'Contado'
# Siempre normalizar después del query:
df['metodo_de_pago_norm'] = df['metodo_de_pago'].map({
    'Financing': 'Financiamiento',
    'Cash payment': 'Contado',
}).fillna(df['metodo_de_pago'])
```

### Dedup crítico (1 booking = email_account + stock_id)

```python
df['_stage_priority'] = df['opp_stagename'].apply(
    lambda s: 0 if any(w in str(s) for w in ['Closed Won', 'Cerrada Ganada']) else 1
)
df = (df.sort_values(['_stage_priority', 'fecha_reserva'], ascending=[True, False])
        .drop_duplicates(subset=['customer_email', 'stock_id'], keep='first'))
```

**Sin esta prioridad:** una Closed Lost posterior puede silenciar la Closed Won con entrega real.

### Delivery_Date — 3 niveles de fallback

| Prioridad | Fuente |
|-----------|--------|
| 1° | `vehicletransfer__c.event_activitydatetime` |
| 2° | `bookings_history.fecha_completada` o `fecha_entrega` |
| 3° | `opp.closedate` si stagename es Closed Won / Cerrada Ganada |

`vehicletransfer__c` tiene errores Iceberg S3 intermittentes (distintos resultados entre runs). Correr el pipeline completo en una sola sesión.

### Devoluciones manuales (override CSV)

4 devoluciones no registradas en Salesforce — parche permanente:

```
~/.claude/skills/kavak-marketplace-eaas/references/returns_override.csv
```

| stock_id | email | devolucion_date | dealer |
|---|---|---|---|
| 1002527 | cynthiavalle.1916@gmail.com | 2026-02-06 | Carshop |
| 1003307 | pavo-real17@hotmail.com | 2026-02-16 | Dalcar |
| 1002811 | 1219bere@gmail.com | 2026-02-11 | Carshop |
| 481314 | ara@ignia.vc | 2026-02-23 | Carshop |

Match por `stock_id + email` (no solo stock_id, para no afectar re-ventas del mismo auto a otro cliente).

```python
RETURNS_CSV = '/Users/heliorequena/.claude/skills/kavak-marketplace-eaas/references/returns_override.csv'
df_override = pd.read_csv(RETURNS_CSV, dtype={'stock_id': str})
df_override['devolucion_date'] = pd.to_datetime(df_override['devolucion_date'])
df_override['_key'] = df_override['stock_id'] + '|' + df_override['email']
df['_key'] = df['stock_id'].astype(str) + '|' + df['customer_email']
mask = df['_key'].isin(df_override['_key'])
df.loc[mask, 'Return'] = 1
df.loc[mask, 'devolucion_date'] = df.loc[mask, '_key'].map(
    df_override.set_index('_key')['devolucion_date'])
df.drop(columns='_key', inplace=True)
```

---

## Funnel EaaS — Vista completa

```
Inventario → VIPs → Oportunidades → Citas → Reservas → Entregas
```

| Paso | Métrica | Fórmula | Referencia Ene-26 |
|------|---------|---------|------------------|
| Inventario publicado | Inv Pub Prom | promedio diario stocks (AVAILABLE+BOOKED, published=True) | 430.6 stocks |
| VIPs | VPD (vistas/auto/día) | SUM(roll_01d_vips) / stocks / días | ~21 VPD |
| VIP → Opp | %CR VIP→Opp | OPD / VPD × 100 | ~1.5% |
| Oportunidades | OPD (opps/auto/día) | opps_únicas / inv_pub / días | ~22 OPD |
| Opp → Cita | %CR Opp→Cita | citas_únicas / opps_únicas × 100 | ~3.9% |
| Citas | CPD (citas/auto/día) | citas_únicas / inv_pub / días | ~0.020 CPD |
| Opp → Reserva | %CR Opp→Bkg | reservas / opps × 100 | ~1.46% |
| Reservas | I2B | reservas / inv_pub / días × 100 | 0.97% |
| Reserva → Entrega | STR Neto | ent_netas / (ent_brutas + bkg_cancels) × 100 | 35.7% |
| Entregas Netas | — | ent_brutas - devoluciones | 50 |

**Nota Hub:** Solo stocks en Hub KVK generan citas. Stocks en Hub Aliado no aplican para CPD.

---

## KPIs del funnel — Definiciones exactas

### KPI 1 — Inventario
> **✅ Databricks disponible:** usar `references/query-inventory-databricks.sql` — devuelve snapshot actual con pricing, PIX y metadata. Tablas: `prd_refined.seller_api_global_refined.*`, `prd_pricing_serving.*`. Sin Redshift equivalent para daily-snapshot — seguir usando `query-inventory-daily-snapshot.sql` (Redshift) para series temporales.

```
Published (snapshot) = published = True AND status IN ('AVAILABLE', 'BOOKED')
Daily Ave. Published  = promedio de snapshots diarios en el período
```

**Error frecuente:** usar solo `status = 'AVAILABLE'` — excluye BOOKED que siguen publicados.

**Status de availability:**

| Status | Descripción |
|---|---|
| AVAILABLE + published=True | Publicado en kavak.com |
| AVAILABLE + published=False | Aprobado pero no visible (PENDING_PUBLICATION) |
| BOOKED | Reservado |
| SOLD / SOLD_BY_DEALER | Vendido |
| DISABLED / REJECTED / BLOCKED_BY_DEALER / SYNC_ERROR | Estados inactivos |
| PENDING / PENDING_APPROVAL | En proceso de aprobación |

**KPIs de inventario a trackear:**
- Precargados: COUNT por `availability_created_date` (flujo mensual de autos ingresados)
- Publicados: `MIN(inventory_date)` de `serving.inventory_history` WHERE `UPPER(inventory_status) = 'AVAILABLE'`
- Despublicados: via `fecha_despublicacion` del SC o proxy `last_inventory_date < CURRENT_DATE - 1`
- Aging promedio: `DATEDIFF(day, COALESCE(first_inventory_date, availability_created_date), COALESCE(fecha_venta, fecha_despublicacion, CURRENT_DATE-1))`
- Aging >120d: alerta — autos publicados activos con >120 días

**Query maestro inventario:** `references/query-inventario-completo.sql` (Inventory_EaaS/references/)

**Hub_Type — regla definitiva:**
```python
def hub_type(hub):
    if pd.isna(hub) or hub is None: return 'Aliado'
    h = str(hub).lower()
    if 'kavak' in h or h.startswith('hq'): return 'KVK'
    return 'Aliado'
```
KVK = hubs Kavak (Lerma, Antara, Midtown GDL, Fortuna, Patio Santa Fe, Artz, etc.)
Aliado = Carshop - X, Agencia Carshop - X, agencias 3P

### KPI 2 — Pricing / PIX
> **✅ Databricks disponible:** datos de pricing incluidos en `references/query-inventory-databricks.sql` (columnas `precio_base`, `market_price`, `guia_price`, `guia_buy_price`, `km_factor`). Fix Guía Autométrica (`guia_price > 10000` + ROW_NUMBER por `guia_create_date DESC`) preservado en la query.

```
PIX Market    = precio_base / market_price
PIX GA Venta  = precio_base / guia_price
PIX GA V/km   = precio_base / (guia_price × (1 - km_factor))
```

**km_factor:** min(0.12, excess_km/20000 × 0.05). Castear `year` a `int()` y `km` a `float()` antes de calcular.

**Las 3 referencias de PIX — todas importantes:**

```
PIX Precio Mercado = precio_base / market_price   → competitividad vs mercado
PIX GA Venta       = precio_base / ga_venta        → margen dealer sobre precio GA
PIX GA Compra      = precio_base / ga_compra       → margen sobre precio de compra
```

**Fuentes:** `pricing_stock_current` (market_price) | `pricing_mapping_sample_guia_autometrica` (ga_venta, ga_compra, filtrar `flag_guia_latest_price IS TRUE`)

**Buckets de Ticket (precio base):**

| Bucket | Rango | Share actual (snap) |
|--------|-------|-------------------|
| < $200k | [0, 200K) | 5.0% |
| $200k–$350k | [200K, 350K) | 49.3% |
| $350k–$500k | [350K, 500K) | 21.1% |
| $500k–$700k | [500K, 700K) | 8.3% |
| $700k–$1M | [700K, 1M) | 12.0% |
| > $1M | 1M+ | 4.3% |

**Buckets de PIX Precio de Mercado:**

| Bucket | Semáforo |
|--------|---------|
| < 90% | 🔴 Por debajo mercado |
| 90–95% | 🟠 Ligeramente bajo |
| 95–100% | 🟡 Competitivo |
| 100–105% | 🟢 En línea |
| 105–110% | 🟢 Sobre mercado |
| > 110% | 🔵 Muy sobre mercado |

**Comisiones por dealer:**

Agencias (fija por entrega neta):
- $10,000: Grupo AlbaCar, Rada Motors, Grupo Río, Finakar, ISMO
- $12,000: Wecars, Alternativa Seminuevos, Dalcar, Real del Barrio

Carshop/Hertz (% sobre precio_base, escalonado por PIX GA V/km — `ga_compra` ajustada por km):
| PIX GA Compra/km | Fee % |
|-----------------|-------|
| ≥ 106% | 7% |
| ≥ 103% | 6% |
| ≥ 100% | 5% |
| ≥ 96% | 4% |
| ≥ 93% | 3% |
| ≥ 90% | 2.5% |
| < 90% | 2% |

**Reglas de fee = $0:** auto devuelto | método pago = Financiamiento | VIN con contrato Kuna | agencia nueva sin contrato confirmado

**Referencias técnicas de pricing:** `pricing_eaas/references/` (kpis.md, pricing-strategy.md, business-rules.md, competition-analysis.md)

### KPI 3 — VIPs (Vistas a la página del auto)
> **✅ Databricks disponible (parcial):** `references/query-vips-catalog-databricks.sql` — VIPs diarios por stock_id desde `prd_datamx_serving.serving.catalog_inventory_velocity` (campo `bk_stock` = stock_id, cast a BIGINT). ⚠️ Métrica `unique_users` (Amplitude) sin equivalente Databricks — seguir usando `query-vips-daily-dealer.sql` (Redshift) para desglose diario por dealer.

Fuentes:
- `serving.dl_catalog_inventory_velocity` — VIPs totales por auto por día (`roll_01d_vips`, campo fecha = `inv_date`)
- `serving.amplitude_vip_viewed_global_rs` — VIPs únicos por usuario (Amplitude, `path_prefix = '/mx'`)

**4 métricas de VIPs a trackear:**

```
vips_autos      = COUNT(DISTINCT stock_id) con al menos 1 VIP en el período
vips_total      = SUM(roll_01d_vips) total de vistas acumuladas
vpd_publicados  = SUM(roll_01d_vips) / COUNT(stock-days WHERE flag_published=1)  ← VPD correcto
pct_autos_con_vips = vips_autos / inv_pub_prom × 100

unique_users    = COUNT(DISTINCT COALESCE(user_id::VARCHAR, device_id::VARCHAR))
                  (Amplitude — usuarios únicos que vieron AL MENOS un auto EaaS en el mes)
```

**⚠️ VPD correcto usa días publicados, NO todos los días:**
- Campo fecha en `dl_catalog_inventory_velocity` es `inv_date` (NOT `bk_date`)
- Denominator: `COUNT(stock-days WHERE flag_published=1)` — días en que el auto estaba efectivamente publicado
- VPD "sucio" (÷ todos los días) da valores bajos (ej. 20.5 en Ene-26) — NO usar para reporting
- VPD "limpio" (÷ días publicados) es ~31 en Ene-26 — este es el benchmark correcto

**Nota:** Para VIPs TP NO filtrar `flag_crab_car`. Para Kavak total SÍ filtrar `flag_crab_car = 0`.

**Nota Amplitude:** `car_id` en `amplitude_vip_viewed` = `legacy_stock_id` en seller_api. Stocks EaaS con IDs >1M (entrada directa por Seller Center) solo existen en `amplitude` — NO en `catalog_inventory_velocity`. SOLD_BY_DEALER = auto vendido directo por el dealer fuera del flujo Kavak (no genera fee para Kavak).

**Nota unique_users:** Es único por el PERÍODO COMPLETO (ej. mes), no por día. Un usuario que vio 5 autos EaaS en enero = 1 unique_user para enero.

**Fix Amplitude — COALESCE de tipos:**
```python
# En Redshift: user_id es bigint, device_id es VARCHAR — deben castearse igual
COALESCE(a.user_id::VARCHAR, a.device_id::VARCHAR)  # ✓
# COALESCE(a.user_id, a.device_id)  → error: "cannot match bigint and varchar"
```

CTE identificar stocks EaaS para VIPs:
```sql
eaas_stocks AS (
    SELECT DISTINCT vs.legacy_stock_id::INTEGER AS stock_id, d.name AS dealer_name
    FROM seller_api_global_refined.variant_availability va
    JOIN seller_api_global_refined.variant_stock vs ON vs.variant_availability_id = va.id
    LEFT JOIN seller_api_global_refined.availability_zone vz ON va.availability_zone_id = vz.id
    LEFT JOIN seller_api_global_refined.company co ON vz.company_id = co.id
    LEFT JOIN seller_api_global_refined.dealer d ON co.dealer_id = d.id
    WHERE LOWER(va.business_module) = 'third_party'
      AND LOWER(COALESCE(d.name, '')) <> 'test mexico'
      AND vs.legacy_stock_id IS NOT NULL
)
```

**Query diario por dealer:** `references/query-vips-daily-dealer.sql` (Vips_EaaS/references/)

**Números validados (Ene-Mar 2026):**

| Mes | vips_autos | vips_total | vpd_pub | pct_con_vips | unique_users |
|-----|-----------|-----------|---------|-------------|-------------|
| Ene-26 | 772 | 397,892 | 31.35 | 99.2% | 217,928 |
| Feb-26 | 912 | 307,831 | 24.32 | 98.6% | 167,499 |
| Mar-26 | 1,028 | 250,783 | 16.27 | 95.3% | 140,909 |

### KPI 4 — Oportunidades
> **✅ Databricks disponible:** usar `references/query-oportunidades-eaas-databricks.sql`. Cambio crítico: `account.email__c` enmascarado en Databricks → usa `opp.leademail__c` para filtrar emails internos. Gap documentado: +13 filas vs Redshift (leademail__c=NULL no filtrado, no afecta KPI EaaS BU).

Una **Oportunidad** es intención de compra en Salesforce. Puede tener múltiples `car_of_interest`.

**Fuente:** `salesforce_latam_refined.opportunity` + `car_of_interest` + `vehicle`

**Filtros base (SIEMPRE aplicar):**
```sql
WHERE opp.countryname__c = '484'
  AND opp.recordtype_name__c IN ('Venta', 'SalesAllies')
  AND COALESCE(opp.b2b__c, 'false') = 'false'
  AND coi.extid__c IS NOT NULL AND coi.extid__c <> ''
  AND LOWER(v.type__c) = 'third_party'
```

**Emails excluidos (internos/pruebas):** `NOT LIKE '%@kavak.com'` y lista en `Oportunidades_EaaS/Oportunidades_EaaS.md`.

**Performance:** Query principal tarda ~8 min. No agregar CTEs de serving en la misma query. LIMIT 400000 obligatorio. Aggregations en Python post-query.

**Dos métricas de oportunidades — distintas, ambas importantes:**

```
opps_total  = COUNT(rows) = 1 fila por (opp_id × stock_id) → cuántos autos de interés tiene EaaS
opps_unicas = COUNT(DISTINCT opp_id)  → cuántos clientes únicos buscaron un auto EaaS
             (1 cliente con 5 autos de interés = 5 opps_total / 1 opp_unica)

opps_unicas/día = opps_unicas / días_del_mes
```

Usar `opps_unicas` para comparar con VIPs y reservas (cohort de clientes), `opps_total` para volumen de intención.

**Números validados (Ene-Mar 2026):**

| Mes | opps_total | opps_unicas | opps/día |
|-----|-----------|------------|---------|
| Ene-26 | 9,732 | 8,832 | 285 |
| Feb-26 | 7,599 | 6,985 | 249 |
| Mar-26 | 10,171 | 7,909 | 255 |

### KPI 5 — Citas
> **⚠️ Solo Redshift:** no hay query Databricks validada para Citas. Usar `references/query-citas-eaas.sql` (Redshift) con los filtros de esta sección.

Citas en hub físico Kavak para ver un auto EaaS.

**Filtro tabla `salesforce_latam_refined.event`:**
```sql
AND e.event_recordtype__c = 'AppointmentInHUB'
AND e.type ILIKE 'Cita Auto%'
AND e.status__c IN ('Scheduled', 'ConfirmedAppointment')
AND (e.isdeleted IS NULL OR e.isdeleted = 'false')
AND LOWER(v.vehicle_type__c) = 'third_party'   -- ⚠️ CRÍTICO: sin esto = 0 citas EaaS
AND COALESCE(opp.b2b__c, 'false') = 'false'
```

**⚠️ Error frecuente:** Olvidar `vehicle_type__c = 'THIRD_PARTY'` en el join. La tabla `event` tiene citas de Kavak retail y EaaS mezcladas. Sin el filtro = 0 citas EaaS.

**Dedup:** `ROW_NUMBER() OVER (PARTITION BY opportunity__c, stockid__c)` priorizando ConfirmedAppointment > Scheduled > activitydatetime reciente. Resultado: 1 cita por (opp_id, stockid__c).

**Dos fechas — distintas y ambas importantes:**
- `createddate` → cuándo SE CREÓ la cita → usar para `citas_creadas` por mes
- `activitydatetime` → cuándo ES la cita → usar para `citas_agendadas` por mes (calendario)

**6 métricas de citas:**
```
citas_creadas      = citas cuya createddate cae en el mes
citas_agendadas    = citas cuya activitydatetime cae en el mes
autos_con_cita     = COUNT(DISTINCT stock_id) con al menos 1 cita en el mes
pct_autos_con_cita = autos_con_cita / inv_pub_prom × 100
citas_por_auto_pub = citas_agendadas / inv_pub_prom / días_del_mes
opps_multi_cita    = opp_ids con más de 1 cita en el período
```

**Importante:** Solo autos en Hub_Type KVK generan citas — los de Hub Aliado no tienen citas porque el cliente no puede visitarlos en una sede Kavak.

**Números validados (Ene-Mar 2026):**

| Mes | citas_creadas | citas_agendadas | citas/auto_pub | pct_con_cita |
|-----|-------------|----------------|--------------|------------|
| Ene-26 | 337 | 315 | 0.024 | ~72% |
| Feb-26 | 294 | 306 | 0.023 | ~65% |
| Mar-26 | 132 | 140 | 0.009 | ~26% |

### KPI 6 — Reservas (Bookings)
> **✅ Databricks disponible:** usar `references/query-booking-funnel-eaas-databricks.sql`. Renombres de columnas en `bookings_history`: `customer_email→email` (SHA-256 hashed, usar `r.emailopp__c` para texto plano) · `opp_id→opportunity_id` · `reservation_createddate→fecha_reserva` · `hub_entrega_v2→hub_entrega`. CTEs Databricks en `CLASIFICACION_EAAS.md` §CTEs Databricks.

**Fuente:** `serving.bookings_history` via `query-booking-funnel-eaas.sql` (más estable). `salesforce_latam_refined.reservation__c` existe pero tiene errores Iceberg S3 intermitentes — no usar directamente.

**Campos del pipeline:**
- `fecha_reserva` = `reservation_createddate` (MIN para el par email+stock)
- `fecha_cancelacion_reserva` → Booking_Cancellation = 1 si NOT NULL
- `devolucion_date` → Return = 1 si NOT NULL
- `fecha_entrega_final` → Delivery = 1 si NOT NULL
- `Active_Booking` = todos los anteriores son NULL

**reserva_unica — campo crítico para separar real vs técnico:**

Cuando Kavak compra un auto de Carshop para venderlo como EaaS FBK, se crea un nuevo booking para el mismo cliente+auto. El booking ANTIGUO de EaaS se cancela técnicamente (cambio de stock/categoría). Por eso hay pares duplicados de (email+VIN).

```python
# reserva_unica = 1 → la reserva REAL (la más reciente por email+VIN)
# reserva_unica = 0 → la reserva antigua/técnica cancelada

df['fecha_reserva'] = pd.to_datetime(df['fecha_reserva'])
df['reserva_unica'] = (
    df.groupby(['customer_email', 'vin'])['fecha_reserva']
      .transform(lambda x: (x == x.max()).astype(int))
)
# fecha_reserva_original = MIN(fecha_reserva) por email+VIN  
# → cuándo el cliente PRIMERO expresó interés, para asignación mensual correcta
df['fecha_reserva_original'] = (
    df.groupby(['customer_email', 'vin'])['fecha_reserva'].transform('min')
)
```

**Uso de reserva_unica:**
- Para contar `reservas_unicas` por mes: usar solo `reserva_unica=1`, agrupado por `fecha_reserva_original`
- Para STR: `bkg_cancels` solo de `reserva_unica=1` (las técnicas no deben inflar denominador)
- Para `reservas_brutas`: contar TODAS (unique=0 y 1) — muestra flujo bruto de actividad

**Dealer name para EaaS FBK:**
- Stocks EaaS FBK (Carshop/Hertz) con IDs >1M entran directamente por SC
- Su `dealer_name` puede aparecer en blanco si el join dealer falla
- Fill: `df.loc[(df['categoria'].isin(['EaaS FBK','EaaS B2B FBK'])) & (df['dealer_name'].isna()), 'dealer_name'] = 'Carshop'`

**I2B (Inventory to Booking Rate):**
```
I2B = Reservas EaaS (unicas) / Inventario Publicado EaaS / Días del mes × 100
```

**STR (Sell-Through Rate) — ver KPI 10 para fórmulas completas.**

**Números validados (Ene-Mar 2026):**

| Mes | reservas_brutas | reservas_unicas | bookings_fin | pct_fin |
|-----|----------------|----------------|-------------|--------|
| Ene-26 | 129 | 110 | 54 | 49.1% |
| Feb-26 | 107 | 90 | 49 | 54.4% |
| Mar-26 | 105 | 93 | 49 | 52.7% |

### KPI 7 — Ventas (mismo dataset que reservas)

Ventas = subconjunto `Delivery = 1` del dataset de reservas. Se distingue:
- **Mes de reserva (cohort):** mes en que el cliente hizo la reserva
- **Mes de entrega (evento):** mes en que se entregó el auto → KPI principal para reportes

### KPI 8 — Entregas
> **✅ Databricks disponible:** entregas derivadas de `query-booking-funnel-eaas-databricks.sql` (misma query que KPI 6). Gap: 23 stocks sin `fecha_entrega` en Databricks (sin fallback `opp.closedate` confirmado).

```
Entrega Bruta = fecha_entrega_final IS NOT NULL (incluye las que se devolvieron)
Entrega Neta  = Entrega Bruta AND devolucion_date IS NULL
```

**⚠️ Campo correcto: `fecha_entrega_final`** — calculado con 3-level fallback en el pipeline:

| Prioridad | Fuente |
|-----------|--------|
| 1° | `vehicletransfer__c.event_activitydatetime` |
| 2° | `bookings_history.fecha_completada` ó `fecha_entrega` |
| 3° | `opp.closedate` si stagename = Closed Won / Cerrada Ganada |

**NO usar `fecha_entrega` directamente** — en el JSON del dashboard tiene `"NaT"` como string para registros cuya fecha viene del fallback Closed Won (~45% de las entregas). Esto causa undercount de ~45% en cualquier cálculo sobre el JSON raw.

**Fuente correcta para cálculos:** siempre usar `generate_kpi_report.py` → Excel, que calcula `fecha_entrega_final` correctamente en Python antes de agregar.

**Vista para reportes mensuales: EVENTO** (agrupar por `fecha_entrega_final`, no por `fecha_reserva_original`).

**Datos a contar por mes:** brutas, devoluciones, netas, por categoría, por dealer, por Hub_Type, por método de pago.

**Números validados (fuente: Excel KPI Report, vista EVENTO, corte Apr-12-2026):**

| Mes | Ent. Brutas | Devoluciones | Ent. Netas | Notas |
|-----|------------|-------------|-----------|-------|
| Ene-26 | 48 | 0 | 48 | — |
| Feb-26 | 36 | 1 | 35 | — |
| Mar-26 | 26 | 1 | 25 | mes en curso, puede subir |

> Los números de meses anteriores pueden variar ±5 entre corridas del pipeline por latencia de `vehicletransfer__c` (Iceberg S3). Usar la corrida más reciente del Excel como referencia.

### KPI 9 — Cancelaciones

Dos tipos:
- **Booking Cancellation:** pre-entrega (`fecha_cancelacion_reserva IS NOT NULL`)
- **Devolución (Return):** post-entrega (`devolucion_date IS NOT NULL`)

```python
# Total Cancellation = cualquiera de los dos
df['Total_Cancellation'] = (
    df['fecha_cancelacion_reserva'].notna() | df['devolucion_date'].notna()
).astype(int)

# Tasas
tasa_cancel = Total_Cancellation / Total_Reservas
tasa_dev    = Return / Total_Reservas
```

**Vista evento:** agrupar por `fecha_cancelacion_reserva` / `devolucion_date` (no por mes de reserva).

### KPI 10 — STR (Sell-Through Rate)

**Dos vistas:**
- **STR Cohort:** reservas del mes × sus outcomes (madurado cuando tiene ≥60d de antigüedad)
- **STR Evento:** entregas y cancelaciones que ocurrieron en el mes (para reportes en curso)

**Dos métricas:**
```
STR Bruto = Ent.Brutas / (Ent.Brutas + Bkg.Cancel)
STR Neto  = Ent.Netas  / (Ent.Netas  + Total.Cancel)
```

Los denominadores son iguales: `Ent.Brutas + Bkg.Cancel = Ent.Netas + Total.Cancel`.

```python
def str_bn_grp(df_):
    """Calcular STR. SOLO pasar reservas con reserva_unica=1 para bkg_cancels correcto."""
    ent_b = int(df_['Delivery'].fillna(0).sum())
    dev   = int(df_['Return'].fillna(0).sum())
    ent_n = ent_b - dev
    # ⚠️ Solo reservas únicas contribuyen a bkg_cancels
    bkg_c = int(df_.loc[df_['reserva_unica']==1, 'Booking_Cancellation'].fillna(0).sum())
    tot_c = int(df_.loc[df_['reserva_unica']==1, 'Total_Cancellation'].fillna(0).sum())
    den   = ent_b + bkg_c
    str_b = round(ent_b / den * 100, 1) if den > 0 else None
    str_n = round(ent_n / den * 100, 1) if den > 0 else None
    return ent_b, dev, ent_n, bkg_c, tot_c, den, str_b, str_n
```

**⚠️ BUG 1 — STR Evento: filtrar por fecha del EVENTO, no por mes de reserva:**
```python
# CORRECTO — STR Evento para un período (s, e):
del_p  = df_bu[(df_bu['fecha_entrega_final'] >= s) & (df_bu['fecha_entrega_final'] <= e)]
canc_p = df_bu[(df_bu['fecha_cancelacion_reserva'] >= s) & (df_bu['fecha_cancelacion_reserva'] <= e) & df_bu['fecha_cancelacion_reserva'].notna()]
dev_p  = df_bu[(df_bu['devolucion_date'] >= s) & (df_bu['devolucion_date'] <= e) & df_bu['devolucion_date'].notna()]
# INCORRECTO — NO filtrar df_bu por mes_reserva para STR Evento
```

**⚠️ BUG 2 — bkg_cancels inflado con cancelaciones técnicas:**
- Cuando Kavak compra un auto de Carshop, cancela el booking EaaS original (reserva_unica=0)
- Si se cuenta esa cancelación en bkg_cancels → denominador del STR sube artificialmente → STR baja
- Fix: `bkg_cancels` SOLO de rows donde `reserva_unica=1`
- Impacto en Mar-26: 42 cancels → 30 reales → STR sube de 32% a ~35%

**⚠️ BUG 3 — campo `fecha_entrega` con NaT en el dashboard JSON:**
- El JSON del dashboard (`dashboard_data.js`) exporta `fecha_entrega = "NaT"` para ~45% de entregas
- Causa: esos registros tienen fecha de entrega desde el fallback Closed Won, no de vehicletransfer
- **Nunca calcular STR ni entregas desde el raw del JSON** — usar la pre-agg de `DASH_DATA.summary.monthly`
- Para cálculos correctos: siempre usar el Excel KPI Report (genera en Python con `fecha_entrega_final`)

**Números validados (STR Evento, Excel KPI Report, reserva_unica fix, corte Apr-12-2026):**

| Mes | Ent. Brutas | Devol. | Ent. Netas | Bkg Cancels (únicos) | STR Bruto | STR Neto |
|-----|------------|--------|-----------|---------------------|----------|---------|
| Ene-26 | 48 | 0 | 48 | 71 | 40.3% | 40.3% |
| Feb-26 | 36 | 1 | 35 | 54 | 40.0% | 38.9% |
| Mar-26 | 26 | 1 | 25 | 49 | 34.7% | 33.3% |

> Números del mes en curso crecen con cada corrida del pipeline (entregas pendientes de registrar en SF).

Desgloses disponibles: por dealer, por método de pago (Contado/Financiamiento), por Hub_Type (KVK/Aliado), por categoría EaaS.

### KPI 11 — SLAs (tiempos del proceso de venta)

```python
df['sla_res_entrega']    = (Delivery_Date - Fecha_reserva).dt.days
df['sla_res_cancel']     = (Total_Cancellation_Date - Fecha_reserva).dt.days
df['sla_res_bkg_cancel'] = (Booking_Cancellation_Date - Fecha_reserva).dt.days
```

Solo calcular SLAs ≥ 0 días (negativos son errores de datos).

**Factores que afectan SLA:**
- Hub_Type KVK: mayor control → SLA más predecible
- Hub Aliado: coordinación externa → mayor variabilidad
- Financiamiento: requiere aprobación crediticia → SLA más largo

**Estadísticas:** N, Promedio, Mediana (P50), P25, P75, Max — calcular para global y por dealer.

---

## Grupos de análisis — grupo_canal y region

### grupo_canal — dos grandes buckets

```python
# Dos grupos de canal para comparar dinámicas de operación:
df['grupo_canal'] = df['dealer_name'].apply(
    lambda d: 'Carshop EaaS' if d == 'Carshop' else 'Agencias EaaS'
)
```

| grupo_canal | Dealers incluidos | Característica |
|------------|-------------------|---------------|
| Carshop EaaS | Carshop (Hertz) | Alquiladora — lotes propios, volumen alto, fee % |
| Agencias EaaS | Todos los demás | Agencias independientes — fee fijo por entrega |

**Números Ene-Mar 2026 (reservas únicas):**

| Mes | Carshop EaaS | Agencias EaaS | Carshop STR Neto | Agencias STR Neto |
|-----|-------------|--------------|----------------|-----------------|
| Ene-26 | 74 | 34 | 47.2% | 29.4% |
| Feb-26 | 58 | 36 | 43.1% | 30.3% |
| Mar-26 | 69 | 24 | 35.2% | 9.5% |

### region — distribución geográfica

La región se asigna por orden de prioridad:

1. **Agencias:** mapeo directo por dealer_name (estático)
2. **Carshop:** lookup por `variant_availability.availability_zone.name` en seller_api → `zone_name` → región
3. **Carshop fallback:** `hub_entrega_v2` (campo de booking) → patterns CDMX/MTY/GDL/etc.

```python
DEALER_REGION = {
    'Grupo Río': 'Monterrey', 'Grupo AlbaCar': 'Monterrey',
    'Rada Motors': 'Monterrey', 'Finakar': 'Monterrey',
    'Alternativa Seminuevos': 'Guadalajara',
    'Dalcar': 'Querétaro', 'Wecars': 'Chihuahua',
    'ISMO': 'Bajío', 'REAL DEL BARRIO AUTOMOTRIZ': 'Bajío',
}

HUB_REGION = {  # Para Carshop via availability_zone.name
    'Agencia Carshop - CDMX': 'CDMX',
    'Agencia Carshop - Guadalajara': 'Guadalajara',
    'Agencia Carshop - Monterrey': 'Monterrey',
    'Agencia Carshop - Cancún': 'Cancún',
    'Agencia Carshop - Querétaro': 'Querétaro',
    'Agencia Carshop - Mérida': 'Mérida',
    'Agencia Carshop - Puebla': 'Puebla',
    'Agencia Carshop - Leon Aeropuerto - Guanajuato': 'Bajío',
    'Agencia CDMX': 'CDMX',
    '<Ubicacion desactivada>': 'Carshop - Sin región',
}
```

**Query para recuperar región de stocks Carshop sin región:**
```sql
SELECT DISTINCT vs.legacy_stock_id::INTEGER AS stock_id, vz.name AS zone_name
FROM seller_api_global_refined.variant_stock vs
JOIN seller_api_global_refined.variant_availability va ON vs.variant_availability_id = va.id
JOIN seller_api_global_refined.availability_zone vz ON va.availability_zone_id = vz.id
WHERE vs.legacy_stock_id IN ({stocks_list})
  AND LOWER(va.business_module) = 'third_party'
LIMIT 500
```

**Distribución regiones (bookings Ene-Mar 2026):**
CDMX=202, Monterrey=186, Guadalajara=94, Querétaro=52, Cancún=42, Puebla=22, Bajío=12, Chihuahua=6, Mérida=5, Sin región=3

---

## Tablas principales

### Salesforce (`salesforce_latam_refined`)
- `reservation__c` — reservas (KPIs 6-11)
- `opportunity` — oportunidades (`countryname__c = '484'`, `recordtype_name__c IN ('Venta','SalesAllies')`)
- `vehicle` — autos (`type__c = 'third_party'` = EaaS, `stockid__c` = stock_id)
- `vehicletransfer__c` — entregas físicas (fecha real del evento)
- `event` — citas (`event_recordtype__c = 'AppointmentInHUB'`, `type ILIKE 'Cita Auto%'`)
- `car_of_interest` — link opp → stock (`extid__c` = stock_id)
- `account` — clientes (`email__c` para exclusión internos)

### Seller Center (`seller_api_global_refined`)
- `variant_availability` — autos en SC (`business_module = 'third_party'`, `published`, `status`)
- `variant_stock` — stock (`legacy_stock_id` = stock_id numérico)
- `availability_zone` — hubs/zonas (`company_id`)
- `company` — empresa por zona (`dealer_id`)
- `dealer` — dealer final (**usar `dealer.name` siempre**)
- `price_offer` — historial de precios (LOG: múltiples filas, usar `updated_date` para point-in-time)
- `variant` — SKU del auto

### Serving (`serving`)
- `accounting_entry` — compras Alquiladora/Element (`account_number='116-001'`, tipo `Item Receipt`)
- `bookings_history` — historial bookings (fechas cancelación, devolución)
- `inventory_history` — evolución diaria inventario (`country_iso='MX'`, `flag_published`, `hub_name`)
- `dl_catalog_inventory_velocity` — VIPs y tráfico diario por auto
- `amplitude_vip_viewed_global_rs` — VIPs únicos Amplitude (`path_prefix='/mx'`)
- `pricing_stock_current` — market_price actual por stock
- `car_centric_car_sku` — catálogo SKUs (make, model, year, version, jato_sku)
- `pricing_mapping_sample_guia_autometrica` — Guía Autométrica (filtrar `flag_guia_latest_price IS TRUE` para snapshot actual)

---

## Dashboard HTML — Arquitectura de datos y gotchas

**Archivo:** `~/Documents/Claude_Projects/EaaS/kavak_eaas_2026/dashboard_v2/dashboard.html`  
**Datos:** `dashboard_data.js` (mismo directorio) — regenerar con `generate_data.py` cuando expiren

### Objeto DASH_DATA — estructura

```
DASH_DATA
├── summary.monthly[]     ← pre-agg global EaaS BU (ent_brutas, ent_netas, str_neto_ev, opps_unique, citas_unique, vpd, ...)
├── bookings
│   ├── raw[]            ← 1 fila por booking (dealer_name, categoria, metodo_de_pago_norm, fecha_reserva_original, fecha_entrega, Delivery, Return, ...)
│   ├── monthly[]        ← pre-agg global (ent_brutas, ent_netas, str_neto_ev, reservas_unicas, bkg_cancels, ...)
│   ├── byCat[]          ← pre-agg por mes × categoria (ent_netas, ent_brutas, devoluciones, str_bruto, str_neto)
│   ├── byDealer[]       ← pre-agg por mes × dealer
│   └── slaByDealer[]   ← SLA P25/P50/P75 por dealer
├── inventory
│   ├── snapshot[]       ← foto actual publicados (dealer_name, region, stock_id)
│   └── daily[]          ← serie diaria de inv publicados
├── vips
│   ├── raw[]            ← vips por stock (vips_total, dias_pub, dealer_name)
│   └── daily[]          ← VPD diario
├── citas.monthly[]      ← citas creadas y agendadas por mes
├── pricing.snapshot[]   ← PIX y precios actuales por stock
├── compras.monthly[]    ← compras alquiladora por mes × tipo
└── aliados.monthly[]    ← pre-agg para canal Aliados
```

### Regla crítica: cuándo usar raw vs pre-agg

| Cálculo | Usar | Por qué |
|---------|------|---------|
| Entregas brutas/netas por mes | `summary.monthly.ent_brutas` / `ent_netas` | `raw.fecha_entrega` tiene "NaT" para ~45% de entregas |
| STR mensual | `summary.monthly.str_neto_ev` | Mismo problema NaT |
| Reservas por mes | `raw` filtrado por `fecha_reserva_original` | Sin NaT, funciona bien |
| Conteo por dealer/canal | `raw` con `_bkgBase()` | Pre-agg no tiene desglose por dealer |
| VIPs | `vips.raw` | Pre-agg tiene VPD pero sin desglose dealer |
| Opps y citas | `summary.monthly.opps_unique` / `citas_unique` | No hay raw de opps/citas en el JSON |

### Problema NaT — explicación

El `raw.fecha_entrega` exporta `"NaT"` (string) para registros donde la fecha de entrega vino del fallback Closed Won (`opp.closedate`). El pipeline Python calcula `fecha_entrega_final` correctamente, pero el JSON export del dashboard no distingue "NaT" del campo.

**Consecuencia:** filtrar `r.fecha_entrega.startsWith('2026-01')` descarta ~45% de entregas reales.

**Fix actual en el dashboard:** los charts de tendencia mensual usan `summary.monthly` (pre-agg correcta). Los KPI cards (`filteredKPIs`) calculan ent_netas y str desde raw usando `fecha_reserva_original` (cohort) — son correctos pero miden cohorte, no evento.

---

## Errores críticos — no cometer

| Error | Síntoma | Fix |
|-------|---------|-----|
| Dedup sin prioridad Closed Won | Pierdes entregas reales | Sort por `_stage_priority` antes del dedup |
| Usar `company.name` para dealer | "Hertz" en lugar de "Carshop" | Siempre join hasta tabla `dealer` |
| Re-correr vehicletransfer por separado | N entregas distintas entre runs | Correr pipeline completo en una sola sesión |
| Join dealer solo por VIN | Pierde stocks con VIN mismatch SF vs seller_api | Lookup secundario por `legacy_stock_id` |
| `assign_categoria` sin `eaas_tp` como fallback | "Sin clasificar" para EaaS reales | `in_sa = vin_sa OR eaas_tp` |
| Filtrar `raw.fecha_entrega` en dashboard JSON | Undercount ~45% entregas | Usar `summary.monthly` para totales mensuales |
| Vista cohort para reportes ejecutivos | Entregas mes reciente parecen bajas | Usar vista EVENTO (`fecha_entrega_final`) — solo en Excel |
| Contar devoluciones por `mes_entrega` | Mes equivocado | Usar `devolucion_date` para el período |
| `status = 'AVAILABLE'` solo para publicados | Excluye BOOKED publicados | Usar `status IN ('AVAILABLE', 'BOOKED') AND published = True` |
| bookings_master.csv como fuente | EaaS FBK y B2B FBK aparecen como 0 | NUNCA usar CSVs de caché para clasificar |
| Km/year como Decimal de Redshift | km_factor falla silenciosamente | Castear `year` a `int()` y `km` a `float()` |
| Citas sin filtro `vehicle_type__c = 'THIRD_PARTY'` | citas_unique = 0 todos los meses | Siempre filtrar por tipo de vehículo en join con vehicle |
| VPD dividido por TODOS los días del stock | VPD artificialmente bajo (ej. 20 vs 31) | Dividir solo por días donde `flag_published=1` en `dl_catalog_inventory_velocity` |
| Amplitude COALESCE sin castear | Error "bigint and varchar" | `COALESCE(user_id::VARCHAR, device_id::VARCHAR)` |
| bkg_cancels incluyendo reserva_unica=0 | STR artificialmente bajo | Solo contar cancels de `reserva_unica=1` |
| reserva_unica usando MIN fecha (el viejo) | La reserva real marcada como 0 | `reserva_unica=1` es MAX fecha (la más reciente = la real) |
| dealer_name en blanco para EaaS FBK | Carshop sin nombre en breakdowns | Fill `'Carshop'` para `categoria IN ['EaaS FBK','EaaS B2B FBK']` con dealer vacío |
| Pandas Categorical en safe_val/fillna | TypeError: Cannot setitem on Categorical | `df[col] = df[col].astype(str)` antes de aplicar fill |
| Usar `customer_email` en bookings_history (Databricks) | KeyError — campo no existe | Se llama `email` (SHA-256 hashed) en Databricks |
| Usar `account.email__c` para filtrar internos (Databricks) | Campo siempre NULL | Usar `opp.leademail__c` — texto plano |
| Usar `entity_full_name` para identificar Alquiladora (Databricks) | Campo siempre NULL | Proxy `dealer.name LIKE 'Carshop%'` vía seller_api |
| Asumir Element Fleet identificable en Databricks | Stocks clasifican como EaaS en lugar de Aliados | `stocks_element` = placeholder vacío — gap aceptado |
| Usar `bk_date` en catalog_inventory_velocity | Fecha incorrecta | Usar `inv_date` para fechas de VIP |
| `json_extract_path_text` en Databricks | Syntax error | Usar `get_json_object(col, '$.key')` |
| `DATEADD` en Databricks | Syntax error | Usar `ADD_MONTHS(date, -N)` |
| `::INTEGER` sobre campos nullable en Databricks | Runtime error | Usar `CAST(... AS BIGINT)` o `TRY_CAST` |

---

## Números de referencia validados

### Entregas Aliados (Retail FBK + B2B FBK)

| Mes | Retail FBK | B2B FBK | **Total** |
|-----|-----------|--------|-----------|
| Sep-25 | 4 | 49 | 53 |
| Oct-25 | 3 | 55 | 58 |
| Nov-25 | 5 | 21 | 26 |
| Dic-25 | 3 | 11 | 14 |
| Ene-26 | 2 | 7 | 9 |
| Feb-26 | 4 | 4 | 8 |

### STR Evento EaaS BU (bookings_history + reserva_unica fix, corte Apr-12-2026)

| Mes | Ent. Brutas | Devol. | Ent. Netas | Bkg Cancels | STR Bruto | STR Neto |
|-----|------------|--------|-----------|------------|----------|---------|
| Ene-26 | 48 | 0 | 48 | 71 | 40.3% | 40.3% |
| Feb-26 | 36 | 1 | 35 | 54 | 40.0% | 38.9% |
| Mar-26 | 26 | 1 | 25 | 49 | 34.7% | 33.3% |

**Nota:** bkg_cancels solo incluye reserva_unica=1. Sin este fix Ene-26 tenía 34.8% (falso).

### Reservas EaaS BU (Ene-Mar 2026)

| Mes | reservas_brutas | reservas_unicas | bookings_fin | pct_fin |
|-----|----------------|----------------|-------------|--------|
| Ene-26 | 129 | 110 | 54 | 49.1% |
| Feb-26 | 107 | 90 | 49 | 54.4% |
| Mar-26 | 105 | 93 | 49 | 52.7% |

### VIPs EaaS BU (Ene-Mar 2026)

| Mes | vips_autos | vips_total | vpd_pub | pct_con_vips | unique_users |
|-----|-----------|-----------|---------|-------------|-------------|
| Ene-26 | 772 | 397,892 | 31.35 | 99.2% | 217,928 |
| Feb-26 | 912 | 307,831 | 24.32 | 98.6% | 167,499 |
| Mar-26 | 1,028 | 250,783 | 16.27 | 95.3% | 140,909 |

### Oportunidades EaaS BU (Ene-Mar 2026)

| Mes | opps_total | opps_unicas | opps/día |
|-----|-----------|------------|---------|
| Ene-26 | 9,732 | 8,832 | 285 |
| Feb-26 | 7,599 | 6,985 | 249 |
| Mar-26 | 10,171 | 7,909 | 255 |

### Citas EaaS BU (Ene-Mar 2026)

| Mes | citas_creadas | citas_agendadas |
|-----|-------------|----------------|
| Ene-26 | 337 | 315 |
| Feb-26 | 294 | 306 |
| Mar-26 | 132 | 140 |

### Métricas de pricing (promedio autos publicados Feb-26)

| Mes | Autos pub | Ticket (K) | Market (K) | PIX Mkt | PIX GA V/km |
|-----|----------:|-----------:|-----------:|--------:|------------:|
| Dic-25 | 545 | $423 | $431 | 96.0% | 105.3% |
| Ene-26 | 574 | $437 | $442 | 97.3% | 108.3% |
| Feb-26 | 630 | $433 | $435 | 98.9% | 111.2% |

### VIPs (Feb-26): EaaS = 5.3% del tráfico Kavak | VPD EaaS = ~21 vs Kavak ~42

### KPIs consolidados EaaS BU (Ene–Mar 2026, fuente: bookings_history, corte Apr-12-2026)

| Mes | Inv Pub Prom | Reservas | I2B % | Ent. Netas | STR Neto | Opps | Citas |
|-----|------------|---------|-------|-----------|---------|------|-------|
| Ene-26 | 430.6 | 129 | 0.97% | 50 | 35.7% | 8,832 | 345 |
| Feb-26 | 465.5 | 107 | 0.82% | 39 | 35.5% | 6,985 | 300 |
| Mar-26 | 506.6 | 105 | 0.67% | 25 | 28.7% | 7,909 | 176 |

---

## Casos especiales

### Hertz / Carshop (EaaS FBK)

Hertz México vende autos de flota vía Seller Center. Entidad contable: **ALQUILADORA DE VEHICULOS** (en `accounting_entry`).

**Problema crítico:** Kavak compra el auto a Hertz y le asigna un nuevo stock_id. Las entregas quedan bajo el nuevo stock_id, no el original. Buscar por stock_id original devuelve ~1 entrega; buscar por VIN recupera ~100%.

**Metodología correcta:**
1. Obtener VINs de stocks originales via `salesforce_latam_refined.vehicle`
2. Buscar `vehicletransfer__c` por VIN
3. Fallback: Closed Won por VIN para los que vehicletransfer no cubre

**NO usar `salesforce_latam_refined.vehicle` para VIN↔stock_id de Hertz** — tiene stock_ids duplicados (49xxxx real + 100xxxx re-ingreso). Usar `serving.car_supply_funnel` que tiene `car_vin + stock_id` correctos.

**Dashboard CEO Hertz:** `~/Documents/Claude_Projects/Hertz_Dashboard_CEO.html`
**Skill completo:** `Hertz_EaaS/SKILL.md` — KPIs de referencia actualizados ahí.

### Board Letters EaaS

**Template:** HTML con Inter font, estilos inline.

**PDF via Chrome headless:**
```python
import subprocess, urllib.parse
html_path = "~/Documents/Claude_Projects/Board Letter/2026/N. Board Mes'YY/EaaS Board Meeting Mes YY.html"
pdf_path  = html_path.replace('.html', '.pdf')
file_url  = "file://" + urllib.parse.quote(html_path)
cmd = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "--headless=new", "--disable-gpu", "--no-sandbox",
    "--virtual-time-budget=5000",
    f"--print-to-pdf={pdf_path}",
    "--no-pdf-header-footer", file_url
]
subprocess.run(cmd, capture_output=True, text=True, timeout=30)
```

**Paleta EaaS:** `#2952CC` (primary) | `#0D2E6B` (dark) | `#EEF3FE` (light) | `#B8CEFA` (mid)

**Datos requeridos para cada Board Letter:**
- Gross/Net deliveries por canal con MoM%
- STR bruto/neto
- Active bookings al cierre
- Financing %
- PIX por canal
- Updates estratégicos (Hertz, nuevas agencias, Element)

**Checklist completo:** ver `Board_Letter_EaaS/SKILL.md`

### Dashboard EaaS

**Ubicación:** `~/Documents/Claude_Projects/kavak_eaas_2026/dashboard_v2/dashboard.html`
**Arquitectura:** HTML + Alpine.js + ECharts + JSON files generados por `generate_data.py`
**10 tabs:** Executive Summary, Inventory, VIPs & Traffic, Opps & Citas, Bookings, Deliveries, Funnel Completo, Supply/Compras, Pricing & PIX, Comparativas

**Skill técnico completo:** `Dashboard-EaaS/SKILL.md` (patrones Alpine.js, ECharts, helpers, CSS classes)

**Estado Fase 1:** aprobada, no iniciada. Prompt para retomar: "Lee ~/.claude/skills/kavak-marketplace-eaas/Dashboard-EaaS/SKILL.md y el plan de Fase 1 en brain (busca: dashboard eaas fase 1)."

### P&L — Net Deliveries y Fees

**Skill maestro P&L:** `~/.claude/skills/kavak-marketplace-fpna/` — query: `queries/q_net_deliveries_master.sql`

**Fecha de reconocimiento PNL:**
```
pnl_transaction_date = GREATEST(fecha_entrega, transaction_date)
  → Sin fecha_entrega → EXCLUIDO del P&L aunque tenga factura NS
```

**ic_sum en NS:** siempre -1, 0 o +1 en el mes.
- +1 = entregado | 0 = entregado + devuelto mismo mes | -1 = crédito de mes anterior

**EaaS puras (agencias) NO aparecen en NS 401-001** — Kavak cobra fee, no el auto. Clasifican como Entrega Bruta vía `has_ns=0 + fecha_entrega`.

**Lógica cancelaciones $3,500:** seguir cadena de bookings por email. Es ingreso EaaS solo si la cadena termina en entrega EaaS o cancelación definitiva sin reemplazo.

### Sales Type (clasificación ortogonal a Tipología)

| Sales Type | Condición |
|------------|-----------|
| 7d | `selected_offer_type = 'seven_days'` |
| Deal Hertz | `opp_name_supply ILIKE '%DEALS HERTZ%'` |
| EaaS Bulk | `opp_name_supply ILIKE '%HERTZ%'` (excl. DEALS) |
| Bulk | `opp_name_supply ILIKE '%BULK%'` |
| TAS | `supply_type='B2B' OR recordtype='B2B'` + no 'OPORTUNIDAD' |
| 48h | b2b=0 + aging ≤ 30 |
| Aging | 30 < aging ≤ 170 |
| No KVK | aging > 170 |
| Retail | reserva b2b=0, no aplica ninguna anterior |

---

## Formato de respuesta para KPIs

Cuando reportes KPIs del Sheet, usa este formato:

```
EaaS Performance — [Mes/Semana]

INVENTARIO
  Daily Ave. Published:  XXX  [CarShop: XX | Agencies: XX]
  Active Bookings:       XXX

BOOKINGS
  Gross Bookings:    XX    (vs LM: +X% | vs LMTD: +X%)
  Cancellations:     XX    (tasa: XX%)
  STR Bruto:         XX%

ENTREGAS
  Gross Deliveries:  XX    (CarShop: XX | Agencies: XX)
  Net Deliveries:    XX    (CarShop: XX | Agencies: XX)
  Returns:           XX
  STR Neto:          XX%

PRICING
  Ticket promedio:  $XX,XXX
  PIX:              XX%

Notas / Alertas:
  - [desvíos vs referencia]
  - [datos que no cuadran entre Sheet y pipeline]
```

**Formato de salida para análisis técnico:**
1. Tabla mensual consolidada — Sep-25 a la fecha, con Total
2. Breakdown por categoría — EaaS / EaaS FBK / Retail FBK / EaaS B2B FBK
3. Breakdown por dealer — ordenado por volumen descendente
4. Si hay inconsistencias vs números de referencia, señalarlas explícitamente

---

## Glosario EaaS

| Término | Significado |
|---|---|
| EaaS | E-commerce as a Service — canal de dealers externos en Kavak |
| Seller Center | Plataforma donde dealers suben su inventario |
| STR | Sell-Through Rate: % de reservas que terminan en entrega |
| PIX | Price Index: precio publicado vs guía de mercado (100% = en precio) |
| VIP | Vehicle Information Page: vista a la página del auto en kavak.com |
| VPD | VIPs por auto por día (métrica de eficiencia de tráfico) |
| Booking / Reserva | Pago del cliente para reservar un auto |
| Entrega bruta | Auto entregado físicamente |
| Entrega neta | Bruta − devoluciones = KPI principal |
| B2B | Venta a empresa, no a cliente final |
| FBK | Fleet-Back: autos de flota (Hertz/Carshop) |
| KVK | Inventario en ubicación operada por Kavak (convierte 5x más que Aliado) |
| Aliado | Inventario en ubicación propia del dealer |
| Service fee | Comisión que cobra Kavak por entrega neta de agencia |
| Arrendamiento | Renta mensual de flota Hertz |
| Afiliación | Proceso de dar de alta un nuevo dealer en EaaS |
| TAS | Trade-in Automotive Solution — canal de compra B2B |
| Iceberg S3 | Error intermittente en vehicletransfer__c que cambia resultados entre runs |
| OPD | Opps por auto por día |
| I2B | Inventory to Booking Rate |

---

## Archivos de referencia

```
/home/natanahelbaruch/projects/crm_eeas_dealers/kavak-marketplace-eaas-databricks/
├── SKILL.md                              ← Este archivo
├── CLASIFICACION_EAAS.md                 ← Redshift CTEs + Databricks CTEs (§CTEs Databricks)
└── references/
    ├── [REDSHIFT]
    ├── query-booking-funnel-eaas.sql         ← pipeline bookings (Redshift)
    ├── query-citas-eaas.sql                  ← citas (Redshift — sin equiv. DB)
    ├── query-inventory-daily-snapshot.sql    ← inventario diario (Redshift — sin equiv. DB)
    ├── query-oportunidades-eaas.sql          ← oportunidades (Redshift)
    ├── query-vips-daily-dealer.sql           ← VIPs por dealer (Redshift — sin equiv. DB)
    ├── query-vips-totales-por-auto.sql       ← VIPs por auto (Redshift)
    ├── returns_override.csv                  ← 4 devoluciones manuales (NO modificar)
    ├── [DATABRICKS - validadas 2026-04-27]
    ├── query-booking-funnel-eaas-databricks.sql   ← ✅ reemplaza query-booking-funnel
    ├── query-oportunidades-eaas-databricks.sql    ← ✅ reemplaza query-oportunidades
    ├── query-vips-catalog-databricks.sql          ← ✅ reemplaza query-vips-totales
    ├── query-inventory-databricks.sql             ← ✅ nueva (sin equiv. Redshift)
    ├── query-price-changes-databricks.sql         ← ✅ nueva
    ├── query-eaas-history-databricks.sql          ← ✅ nueva
    ├── query-historical-reserved-databricks.sql   ← ✅ nueva
    └── query-historical-car-meta-databricks.sql   ← ✅ nueva
```

**Subcarpetas absorbidas en este SKILL.md** — definiciones, lógica y código ya están aquí:
- `Cancelaciones_EaaS/` → KPI 9 | `Citas_EaaS/` → KPI 5 | `Entregas_EaaS/` → KPI 8
- `Oportunidades_EaaS/` → KPI 4 | `Reservas_EaaS/` → KPI 6 | `STR_EaaS/` → KPI 10
- `SLA_EaaS/` → KPI 11 | `Ventas_EaaS/` → KPI 7 | `Vips_EaaS/` → KPI 3

Los archivos `.sql` en `references/` contienen las queries completas listas para ejecutar — usarlos directamente.

---

## Changelog

| Fecha | Cambio |
|---|---|
| 2026-05-08 | Versión Databricks — 8 queries validadas agregadas como `*-databricks.sql`. Redshift conservado íntegro. Callouts de migración por KPI. CTEs Databricks en CLASIFICACION_EAAS.md. |
| 2026-04-06 | Reestructuración completa. Consolidado de 13 subcarpetas en SKILL.md autosuficiente. Integradas todas las definiciones de KPI, pipeline técnico, errores conocidos, clasificación, referencias. |
| 2026-04-01 | Rol EaaS Manager, Step 0 Google Sheet, estructura Sheet, formato de respuesta. |
| 2026-03-06 | Versión original — pipeline técnico, CTEs SQL, categorías, fees, P&L. |
