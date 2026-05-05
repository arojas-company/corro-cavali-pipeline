"""
BACKFILL HISTÓRICO v2.3 — Shopify → Google Sheets
===================================================
Jala datos desde 2024-01-01 hasta hoy para todos los meses, semanas y quarters.

FIXES v2.3 + Smartrr safe fix:
- Smartrr se consulta desde Python/backend y se escribe en Sheets; NO se expone API key en HTML.
- ql_run corregido para API 2025-10: `rows` devuelve OBJETOS JSON (dicts),
  NO rowData (eliminado) NI arrays de listas.
  Estructura real: tableData.rows = [{"col_name": "value", ...}, ...]
  Solo se pide `rows` en el query GQL, sin `rowData`.
- UNTIL date es e+1 cuando e == hoy para incluir el día actual en ShopifyQL
- new_vs_returning usa REST orders con customer.orders_count
- gross_profit viene directo de ShopifyQL (campo gross_profit)

EJECUCIÓN:
  python backfill.py
  (requiere env vars: SHOPIFY_TOKEN_CORRO, SHOPIFY_TOKEN_CAVALI, GOOGLE_CREDENTIALS)
  Opcional/recomendado para Cavali: SMARTRR_API_KEY_CAVALI

COMPORTAMIENTO EN SHEETS:
  ⚠️  BORRA y reescribe completamente los tabs:
      kpis_daily, revenue_share, new_vs_returning
      Además actualiza smartrr_subscribers para Cavali
"""

import os, json, time, random, requests, gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, date
import pytz
import re

TIMEZONE    = pytz.timezone("America/Bogota")
GQL_VERSION = "2025-10"

STORES = {
    "corro":  {
        # Usa tus mismos GitHub Secrets si existen; los defaults mantienen intacto el comportamiento anterior.
        "url":      os.environ.get("SHOPIFY_URL_CORRO", "equestrian-labs.myshopify.com"),
        "token":    os.environ["SHOPIFY_TOKEN_CORRO"],
        "sheet_id": os.environ.get("SHEET_ID_CORRO", "1nq8xkDzowAvhD3wpMBlVK2M3FZSNS2DrAiPxz-Y2tdU"),
    },
    "cavali": {
        # Usa tus mismos GitHub Secrets si existen; los defaults mantienen intacto el comportamiento anterior.
        "url":      os.environ.get("SHOPIFY_URL_CAVALI", "cavali-club.myshopify.com"),
        "token":    os.environ["SHOPIFY_TOKEN_CAVALI"],
        "sheet_id": os.environ.get("SHEET_ID_CAVALI", "1QUdJc2EIdElIX5nlLQxWxS98aAz-TgQnSg9glJpNtig"),
    },
}
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADERS_KPIS = [
    "updated_at", "period", "period_start", "period_end",
    "gross_sales", "net_sales", "total_discounts", "total_returns", "cogs",
    "gross_profit",
    "pct_discount", "pct_returns", "pct_gm",
    "nb_orders", "nb_units", "aov", "units_per_order",
    "sessions", "unique_visitors", "conversion_rate",
    "gross_sales_prev", "gross_sales_yoy",
    "net_sales_prev", "net_sales_yoy",
    "nb_orders_prev", "nb_orders_yoy",
    "aov_prev", "aov_yoy",
    "new_customers", "returning_customers",
    "new_revenue", "returning_revenue",
    "new_gross_profit", "returning_gross_profit",
]

SMARTRR_HEADERS = [
    "updated_at", "brand", "seasonal", "signature", "premier", "junior",
    "other", "total_subscribers", "source", "error",
]

SMARTRR_API_KEYS = {
    # Guardar en GitHub Actions Secrets. Nunca poner esta key en el HTML público.
    "cavali": os.environ.get("SMARTRR_API_KEY_CAVALI") or os.environ.get("SMARTRR_TOKEN_CAVALI") or "",
    "corro":  os.environ.get("SMARTRR_API_KEY_CORRO")  or os.environ.get("SMARTRR_TOKEN_CORRO")  or "",
}


# ─────────────────────────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────
def get_gc():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    return gspread.authorize(creds)

# ─────────────────────────────────────────────────────────────────
# FIX: UNTIL date helper
# ─────────────────────────────────────────────────────────────────
def _until(e):
    today = datetime.now(TIMEZONE).date()
    if e >= today:
        return e + timedelta(days=1)
    return e

# ─────────────────────────────────────────────────────────────────
# SHOPIFY GQL — raw request
# ─────────────────────────────────────────────────────────────────
def gql(store_url, token, query):
    url = f"https://{store_url}/admin/api/{GQL_VERSION}/graphql.json"
    r = requests.post(url,
        headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
        json={"query": query}, timeout=60)
    if r.status_code != 200:
        print(f"HTTP {r.status_code} — {r.text[:200]}")
        return None
    d = r.json()
    if d.get("errors"):
        print(f"GQL errors: {d['errors']}")
        return None
    return d.get("data")

# ─────────────────────────────────────────────────────────────────
# ql_run — DEFINITIVO (verificado contra docs.shopify.dev 2026-01)
#
# Estructura oficial ShopifyqlQueryResponse:
#   parseErrors  [String!]!    → [] si OK, ["msg..."] si error ShopifyQL
#   tableData    ShopifyqlTableData | null
#     columns    [ShopifyqlTableDataColumn!]!
#     rows       JSON!  → lista de dicts {"col_name": "value"}
# ─────────────────────────────────────────────────────────────────
def ql_run(store_url, token, ql_query):
    """
    Ejecuta una ShopifyQL query contra la Admin API 2025-10+.
    Devuelve lista de {columna: valor} o [] si no hay datos / error.
    """
    escaped = ql_query.replace("\\", "\\\\").replace('"', '\\"')

    # parseErrors NO tiene subfields — es [String!]! (scalar list)
    q = (
        f'{{ shopifyqlQuery(query: "{escaped}") {{ '
        f'tableData {{ columns {{ name }} rows }} '
        f'parseErrors }} }}'
    )
    data = gql(store_url, token, q)
    if not data:
        return []

    ql_obj = data.get("shopifyqlQuery") or {}

    # parseErrors = [String!]! — lista vacía [] cuando OK
    errs = ql_obj.get("parseErrors") or []
    if isinstance(errs, list) and len(errs) > 0:
        print(f"parseErrors: {errs}")
        return []

    # tableData es null cuando hay parseErrors
    td = ql_obj.get("tableData")
    if not td:
        return []

    # rows = JSON! scalar → lista de dicts {"col_name": "value"}
    rows = td.get("rows") or []
    if not rows:
        return []

    # Tipo esperado: lista de dicts
    if isinstance(rows, list) and isinstance(rows[0], dict):
        return rows

    # Fallback defensivo por si rows llega como string JSON
    if isinstance(rows, str):
        try:
            parsed = json.loads(rows)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass

    return []


def run_ql(store_url, token, ql_query):
    rows = ql_run(store_url, token, ql_query)
    return rows[-1] if rows else None


# ─────────────────────────────────────────────────────────────────
# SHOPIFY REST — rate-limit safe pagination
# ─────────────────────────────────────────────────────────────────
REST_MAX_RETRIES = 8
REST_BASE_SLEEP  = 1.25


def _sleep_for_shopify_rate_limit_(response=None, attempt=0, reason=""):
    """
    Shopify REST can return 429 during big backfills.
    This helper sleeps using Retry-After when present, otherwise exponential backoff.
    It also slows down when X-Shopify-Shop-Api-Call-Limit is close to the bucket limit.
    """
    sleep_for = 0.0

    if response is not None:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                sleep_for = max(sleep_for, float(retry_after))
            except Exception:
                pass

        call_limit = response.headers.get("X-Shopify-Shop-Api-Call-Limit", "")
        # Example: "39/40"
        try:
            used, limit = [int(x) for x in call_limit.split("/", 1)]
            if limit and used / limit >= 0.80:
                sleep_for = max(sleep_for, 2.0)
            elif limit and used / limit >= 0.65:
                sleep_for = max(sleep_for, 1.0)
        except Exception:
            pass

    # Exponential backoff + small jitter for 429/5xx/network errors
    if attempt > 0:
        sleep_for = max(
            sleep_for,
            min(60.0, REST_BASE_SLEEP * (2 ** (attempt - 1)) + random.uniform(0.15, 0.85))
        )

    if sleep_for > 0:
        msg = f"    ⏳ Shopify REST throttle {reason or ''} — sleeping {sleep_for:.1f}s"
        if response is not None:
            msg += f" [HTTP {response.status_code}, call-limit {response.headers.get('X-Shopify-Shop-Api-Call-Limit','?')}]"
        print(msg)
        time.sleep(sleep_for)


def _shopify_rest_get_with_retry_(url, headers, params=None):
    """GET with retry for Shopify 429/5xx/transient network errors."""
    last_error = None

    for attempt in range(REST_MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=90)

            # Success: still slow down a little if bucket is getting hot
            if 200 <= r.status_code < 300:
                _sleep_for_shopify_rate_limit_(r, 0, "near bucket")
                return r

            # Retry throttling and transient server errors
            if r.status_code == 429 or 500 <= r.status_code < 600:
                last_error = requests.HTTPError(f"HTTP {r.status_code}: {r.text[:300]}", response=r)
                if attempt < REST_MAX_RETRIES:
                    _sleep_for_shopify_rate_limit_(r, attempt + 1, "retry")
                    continue

            # Non-retryable errors fail normally
            r.raise_for_status()
            return r

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_error = e
            if attempt < REST_MAX_RETRIES:
                _sleep_for_shopify_rate_limit_(None, attempt + 1, "network")
                continue
            raise

    if last_error:
        raise last_error
    raise RuntimeError("Shopify REST request failed without response")


def rest_get(store_url, token, endpoint, params):
    """
    Shopify REST paginator with 429 protection.
    Keeps the previous output unchanged, but prevents the backfill from dying on:
      requests.exceptions.HTTPError: 429 Client Error: Too Many Requests
    """
    url     = f"https://{store_url}/admin/api/2024-01/{endpoint}"
    headers = {"X-Shopify-Access-Token": token}
    results = []
    page = 0

    while url:
        page += 1
        r = _shopify_rest_get_with_retry_(url, headers=headers, params=params)
        data = r.json()
        key = list(data.keys())[0]
        batch = data.get(key, []) or []
        results.extend(batch)

        link   = r.headers.get("Link", "") or r.headers.get("link", "") or ""
        url    = None
        params = {}  # page_info URLs already contain all query params

        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
                    break

        # Small steady pacing for long historical backfills.
        # This is intentionally conservative; it protects Corro/Cavali runs.
        if url:
            time.sleep(0.35)

    return results


def money(v):
    if v is None:
        return 0.0
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return 0.0


def gm_ratio(v):
    if v is None:
        return 0.0
    try:
        val = float(str(v).replace("%", "").replace(",", "").strip())
        return round(val * 100, 2) if abs(val) <= 1.0 else round(val, 2)
    except Exception:
        return 0.0

# ─────────────────────────────────────────────────────────────────
# FETCH: SALES
# ─────────────────────────────────────────────────────────────────
def fetch_sales(store_url, token, start, end):
    e_ql = _until(end)
    row  = run_ql(store_url, token,
        f"FROM sales SHOW gross_sales, discounts, returns, net_sales, "
        f"cost_of_goods_sold, gross_profit, gross_margin, orders "
        f"SINCE {start} UNTIL {e_ql}")

    empty = {k: None for k in ["gross_sales","discounts","returns","net_sales",
                                "cogs","gross_profit","pct_gm","orders"]}
    if not row:
        print(f"    ⚠ fetch_sales: sin datos para {start} → {e_ql}")
        return empty

    g  = round(money(row.get("gross_sales")),        2)
    d  = round(abs(money(row.get("discounts"))),      2)
    r  = round(abs(money(row.get("returns"))),        2)
    n  = round(money(row.get("net_sales")),           2)
    c  = round(money(row.get("cost_of_goods_sold")),  2)
    gp = round(money(row.get("gross_profit")),        2)
    gm = gm_ratio(row.get("gross_margin"))
    o  = int(abs(money(row.get("orders"))))

    print(f"    gross:{g:>12,.2f}  net:{n:>12,.2f}  gp:{gp:>10,.2f}  "
          f"gm:{gm:>5.1f}%  orders:{o}  [UNTIL {e_ql}]")

    return {"gross_sales": g, "discounts": d, "returns": r, "net_sales": n,
            "cogs": c, "gross_profit": gp, "pct_gm": gm, "orders": o}

# ─────────────────────────────────────────────────────────────────
# FETCH: SESSIONS
# ─────────────────────────────────────────────────────────────────
def fetch_sessions(store_url, token, start, end):
    e_ql = _until(end)
    row  = run_ql(store_url, token,
        f"FROM sessions SHOW sessions SINCE {start} UNTIL {e_ql}")
    if not row:
        return 0
    return int(abs(money(row.get("sessions", 0))))

# ─────────────────────────────────────────────────────────────────
# FETCH: ORDERS FULFILLED
# ─────────────────────────────────────────────────────────────────
def fetch_orders_fulfilled(store_url, token, start, end):
    e_ql = _until(end)
    row  = run_ql(store_url, token,
        f"FROM fulfillments SHOW orders_fulfilled SINCE {start} UNTIL {e_ql}")
    if not row:
        return None
    return int(abs(money(row.get("orders_fulfilled", 0))))

# ─────────────────────────────────────────────────────────────────
# FETCH: REST ORDERS
# ─────────────────────────────────────────────────────────────────
def fetch_orders(store_url, token, start, end):
    orders = rest_get(store_url, token, "orders.json", {
        "status":           "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min":   f"{start}T00:00:00-05:00",
        "created_at_max":   f"{end}T23:59:59-05:00",
        "limit":            250,
        "fields":           "id,subtotal_price,created_at,line_items,source_name,tags,customer",
    })
    return enrich_orders_with_customer_order_counts(store_url, token, orders)

# ─────────────────────────────────────────────────────────────────
# NEW vs RETURNING via REST
# ─────────────────────────────────────────────────────────────────


CUSTOMER_ORDER_COUNT_CACHE = {}
CUSTOMER_FIRST_ORDER_DATE_CACHE = {}


def _shopify_rest_get_json_with_retry(store_url, token, endpoint, params=None, max_retries=7):
    """Small REST GET helper for customer enrichment, with 429/5xx retry."""
    url = f"https://{store_url}/admin/api/2024-01/{endpoint}"
    headers = {"X-Shopify-Access-Token": token}
    params = params or {}
    last_resp = None
    for attempt in range(max_retries):
        r = requests.get(url, headers=headers, params=params, timeout=60)
        last_resp = r
        if r.status_code == 429 or r.status_code in (500, 502, 503, 504):
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                try:
                    sleep_for = float(retry_after)
                except Exception:
                    sleep_for = 2.0
            else:
                sleep_for = min(45, (2 ** attempt) + random.random())
            print(f"    Shopify REST {r.status_code} on {endpoint}; retrying in {sleep_for:.1f}s")
            time.sleep(sleep_for)
            continue
        r.raise_for_status()
        lim = r.headers.get("X-Shopify-Shop-Api-Call-Limit", "")
        try:
            used, cap = [int(x) for x in lim.split("/", 1)]
            if cap and used / cap >= 0.80:
                time.sleep(0.75)
        except Exception:
            pass
        return r.json()
    if last_resp is not None:
        last_resp.raise_for_status()
    return {}


def _order_customer_id(order):
    customer = order.get("customer") or {}
    cid = customer.get("id")
    return str(cid) if cid not in (None, "") else ""


def enrich_orders_with_customer_order_counts(store_url, token, orders):
    """
    Enrich each order with:
      - customer.orders_count when available
      - customer._first_order_created_at from Shopify Customer Orders

    New vs Returning is then date-aware:
      first-ever order before this order => Returning
      this is the customer's first-ever order => New
    This avoids the dashboard showing every Cavali subscription order as New.
    """
    ids = []
    seen = set()
    for o in orders or []:
        cid = _order_customer_id(o)
        if not cid or cid in seen:
            continue
        seen.add(cid)
        customer = o.get("customer") or {}
        current = customer.get("orders_count")
        if current not in (None, ""):
            try:
                CUSTOMER_ORDER_COUNT_CACHE[cid] = int(current)
            except Exception:
                pass
        if cid not in CUSTOMER_FIRST_ORDER_DATE_CACHE:
            ids.append(cid)

    for i, cid in enumerate(ids, 1):
        try:
            data = _shopify_rest_get_json_with_retry(
                store_url, token, f"customers/{cid}/orders.json",
                {"status": "any", "limit": 1, "order": "created_at asc", "fields": "id,created_at"}
            )
            first_order = (data.get("orders") or [{}])[0]
            CUSTOMER_FIRST_ORDER_DATE_CACHE[cid] = first_order.get("created_at") or ""
        except Exception as e:
            print(f"    ⚠ first-order fallback cid={cid}: {e}")
            CUSTOMER_FIRST_ORDER_DATE_CACHE[cid] = ""

        # orders_count is only used as fallback if first-order lookup fails.
        if cid not in CUSTOMER_ORDER_COUNT_CACHE:
            try:
                data = _shopify_rest_get_json_with_retry(store_url, token, f"customers/{cid}.json", {"fields": "id,orders_count"})
                customer = data.get("customer") or {}
                CUSTOMER_ORDER_COUNT_CACHE[cid] = int(customer.get("orders_count", 1) or 1)
            except Exception as e:
                print(f"    ⚠ customer orders_count fallback cid={cid}: {e}")
                CUSTOMER_ORDER_COUNT_CACHE[cid] = 1

        if i % 35 == 0:
            time.sleep(0.5)

    for o in orders or []:
        cid = _order_customer_id(o)
        if not cid:
            continue
        if not o.get("customer"):
            o["customer"] = {"id": cid}
        o["customer"]["orders_count"] = CUSTOMER_ORDER_COUNT_CACHE.get(cid, o["customer"].get("orders_count", 1))
        o["customer"]["_first_order_created_at"] = CUSTOMER_FIRST_ORDER_DATE_CACHE.get(cid, "")
    return orders


def calc_new_returning(orders):
    new_rev = ret_rev = 0.0
    new_nc  = ret_nc  = 0
    for o in orders:
        amt      = float(o.get("subtotal_price", 0) or 0)
        customer = o.get("customer") or {}
        first_dt = _parse_shopify_dt(customer.get("_first_order_created_at"))
        order_dt = _parse_shopify_dt(o.get("created_at"))
        if first_dt and order_dt:
            is_returning = first_dt < order_dt
        else:
            count = int(customer.get("orders_count", 1) or 1)
            is_returning = count > 1

        if is_returning:
            ret_nc  += 1
            ret_rev += amt
        else:
            new_nc  += 1
            new_rev += amt
    return {
        "new_customers":       new_nc,
        "returning_customers": ret_nc,
        "new_revenue":         round(new_rev, 2),
        "returning_revenue":   round(ret_rev, 2),
    }


def calc_units(orders):
    return sum(
        sum(int(li.get("quantity", 0) or 0) for li in o.get("line_items", []))
        for o in orders
    )


def calc_rs(orders):
    ch    = {"Wellington (POS)": 0., "Concierge": 0., "Online": 0., "Others": 0.}
    total = 0.
    for o in orders:
        amt    = float(o.get("subtotal_price", 0) or 0)
        total += amt
        src    = (o.get("source_name") or "").lower().strip()
        tags   = (o.get("tags") or "").lower()
        if src == "pos" or "wellington" in tags or "pos" in tags:
            ch["Wellington (POS)"] += amt
        elif "concierge" in tags or "concierge" in src:
            ch["Concierge"] += amt
        elif src in ("web", "shopify", "", "online_store") or not src:
            ch["Online"] += amt
        else:
            ch["Others"] += amt
    return {k: {"amount": round(v, 2), "pct": round(v / total * 100, 2) if total else 0}
            for k, v in ch.items()}

# ─────────────────────────────────────────────────────────────────
# BUILD KPI DICT
# ─────────────────────────────────────────────────────────────────
def build(ql, orders, sessions=0, orders_fulfilled=None):
    if ql.get("gross_sales") is not None:
        g  = ql["gross_sales"]
        d  = ql["discounts"]
        r  = ql["returns"]
        n  = ql["net_sales"]
        c  = ql["cogs"] or 0
        gp = ql.get("gross_profit") or 0
        gm = ql["pct_gm"] or 0
        nb = int(orders_fulfilled) if orders_fulfilled is not None else (ql["orders"] or len(orders))
    else:
        nb = int(orders_fulfilled) if orders_fulfilled is not None else len(orders)
        g  = sum(float(o.get("subtotal_price", 0) or 0) for o in orders)
        d  = r = c = gp = gm = 0.0
        n  = g

    nvr   = calc_new_returning(orders)
    units = calc_units(orders)
    aov   = round(n / nb,     2) if nb   else 0
    upo   = round(units / nb, 2) if nb   else 0
    pdisc = round(d / g * 100, 2) if g   else 0
    pret  = round(r / g * 100, 2) if g   else 0
    sess  = int(sessions or 0)

    gm_rate = gm / 100 if gm > 0 else (gp / n if n > 0 else 0)
    new_gp  = round(nvr["new_revenue"]       * gm_rate, 2)
    ret_gp  = round(nvr["returning_revenue"] * gm_rate, 2)

    return {
        "gross_sales":            round(g,  2),
        "net_sales":              round(n,  2),
        "total_discounts":        round(d,  2),
        "total_returns":          round(r,  2),
        "cogs":                   round(c,  2),
        "gross_profit":           round(gp, 2),
        "pct_discount":           pdisc,
        "pct_returns":            pret,
        "pct_gm":                 gm,
        "nb_orders":              nb,
        "nb_units":               units,
        "aov":                    aov,
        "units_per_order":        upo,
        "sessions":               sess,
        "unique_visitors":        round(sess * 0.85) if sess else 0,
        "conversion_rate":        round(nb / sess * 100, 4) if sess else 0,
        "new_customers":          nvr["new_customers"],
        "returning_customers":    nvr["returning_customers"],
        "new_revenue":            nvr["new_revenue"],
        "returning_revenue":      nvr["returning_revenue"],
        "new_gross_profit":       new_gp,
        "returning_gross_profit": ret_gp,
    }


def pct(c, p):
    if not p:
        return None
    return round((c - p) / p * 100, 2)


def last_day(y, m):
    return (date(y, m + 1, 1) - timedelta(days=1)) if m < 12 else date(y, 12, 31)


def monday_of(d):
    return d - timedelta(days=d.weekday())

# ─────────────────────────────────────────────────────────────────
# BUILD KPI ROW
# ─────────────────────────────────────────────────────────────────
def make_kpi_row(now_str, label, period_start, period_end, cur, prev, yoy):
    return [
        now_str, label, str(period_start), str(period_end),
        cur.get("gross_sales",            0),
        cur.get("net_sales",              0),
        cur.get("total_discounts",        0),
        cur.get("total_returns",          0),
        cur.get("cogs",                   0),
        cur.get("gross_profit",           0),
        cur.get("pct_discount",           0),
        cur.get("pct_returns",            0),
        cur.get("pct_gm",                 0),
        cur.get("nb_orders",              0),
        cur.get("nb_units",               0),
        cur.get("aov",                    0),
        cur.get("units_per_order",        0),
        cur.get("sessions",               0),
        cur.get("unique_visitors",        0),
        cur.get("conversion_rate",        0),
        pct(cur.get("gross_sales", 0), prev.get("gross_sales")),
        pct(cur.get("gross_sales", 0), yoy.get("gross_sales")),
        pct(cur.get("net_sales",   0), prev.get("net_sales")),
        pct(cur.get("net_sales",   0), yoy.get("net_sales")),
        pct(cur.get("nb_orders",   0), prev.get("nb_orders")),
        pct(cur.get("nb_orders",   0), yoy.get("nb_orders")),
        pct(cur.get("aov",         0), prev.get("aov")),
        pct(cur.get("aov",         0), yoy.get("aov")),
        cur.get("new_customers",          0),
        cur.get("returning_customers",    0),
        cur.get("new_revenue",            0),
        cur.get("returning_revenue",      0),
        cur.get("new_gross_profit",       0),
        cur.get("returning_gross_profit", 0),
    ]

# ─────────────────────────────────────────────────────────────────
# SMARTRR — fetch active subscriptions and split by real Cavali box/product
# ─────────────────────────────────────────────────────────────────

def _norm_txt(v):
    """Normalize text for robust Cavali box/product matching."""
    return re.sub(r"\s+", " ", str(v or "").strip().lower())


def _dig(obj, *paths):
    """Return first non-empty nested value from a dict/list using dot paths."""
    for path in paths:
        cur = obj
        ok = True
        for part in path.split("."):
            if isinstance(cur, dict):
                cur = cur.get(part)
            elif isinstance(cur, list):
                try:
                    cur = cur[int(part)]
                except Exception:
                    ok = False
                    break
            else:
                ok = False
                break
        if ok and cur not in (None, ""):
            return cur
    return ""


def _smartrr_items(payload):
    """Normalize Smartrr list responses into a list."""
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []

    for key in (
        "data", "items", "results", "records",
        "purchaseStates", "purchase_states", "purchaseState", "purchase_state",
        "subscriptions", "subscription_contracts", "contracts",
    ):
        val = payload.get(key)
        if isinstance(val, list):
            return val
        if isinstance(val, dict):
            nested = _smartrr_items(val)
            if nested:
                return nested
    return []


def _smartrr_total_hint(payload):
    if not isinstance(payload, dict):
        return None
    for path in ("total", "totalCount", "count", "meta.total", "pagination.total", "page.total"):
        val = _dig(payload, path)
        if val not in (None, ""):
            try:
                return int(float(str(val).replace(",", "")))
            except Exception:
                pass
    return None


def _collect_text(obj, depth=0, out=None):
    """Collect product/line-item text from nested Smartrr objects."""
    if out is None:
        out = []
    if depth > 7 or obj is None:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            lk = str(k).lower()
            # Product/line-item fields first. Plan/group fields are useful only when product title is absent.
            useful_key = any(x in lk for x in (
                "product", "variant", "lineitem", "line_item", "stline", "item", "sku",
                "title", "name", "label", "program", "plan"
            ))
            if isinstance(v, (dict, list)):
                _collect_text(v, depth + 1, out)
            elif useful_key and v not in (None, ""):
                out.append(str(v))
    elif isinstance(obj, list):
        for v in obj[:80]:
            _collect_text(v, depth + 1, out)
    return out


def _smartrr_plan_text(subscription):
    """
    Build classification text from Smartrr response.
    Important: Cavali Club Quarterly/Yearly Membership are plan/group names,
    NOT the Seasonal box. The actual boxes are Product values:
      - Cavali Club Membership        => Seasonal
      - The Signature Box             => Signature
      - The Premier Box               => Premier
      - Cavali Club Junior Membership => Junior
    """
    # Put the most product-specific fields first.
    vals = [
        _dig(subscription, "product.title"),
        _dig(subscription, "product.name"),
        _dig(subscription, "productTitle"),
        _dig(subscription, "product_title"),
        _dig(subscription, "productName"),
        _dig(subscription, "product_name"),
        _dig(subscription, "variant.title"),
        _dig(subscription, "variant.name"),
        _dig(subscription, "stLineItems.0.title"),
        _dig(subscription, "stLineItems.0.name"),
        _dig(subscription, "stLineItems.0.productTitle"),
        _dig(subscription, "stLineItems.0.product_title"),
        _dig(subscription, "stLineItems.0.productName"),
        _dig(subscription, "stLineItems.0.product_name"),
        _dig(subscription, "lineItems.0.title"),
        _dig(subscription, "lineItems.0.name"),
        _dig(subscription, "lineItems.0.productTitle"),
        _dig(subscription, "line_items.0.title"),
        _dig(subscription, "items.0.title"),
        _dig(subscription, "items.0.name"),
        _dig(subscription, "items.0.productTitle"),
        _dig(subscription, "subscriptionContractLine.title"),
        _dig(subscription, "subscription_contract_line.title"),
        # lower-priority plan/group text
        _dig(subscription, "sellingPlan.name"),
        _dig(subscription, "selling_plan.name"),
        _dig(subscription, "sellingPlanName"),
        _dig(subscription, "selling_plan_name"),
        _dig(subscription, "subscriptionProgram.name"),
        _dig(subscription, "subscription_program.name"),
        _dig(subscription, "planName"),
        _dig(subscription, "plan_name"),
        _dig(subscription, "name"),
        _dig(subscription, "title"),
    ]
    vals.extend(_collect_text(subscription))
    return " | ".join(str(v) for v in vals if v)


def _smartrr_is_active(subscription):
    status = str(
        _dig(subscription, "purchaseStateStatus") or
        _dig(subscription, "purchase_state_status") or
        _dig(subscription, "status") or
        _dig(subscription, "subscriptionStatus") or
        _dig(subscription, "subscription_status") or
        _dig(subscription, "state") or
        _dig(subscription, "sts.0.purchaseStateStatus") or
        _dig(subscription, "sts.0.status")
    ).strip().lower()

    cancelled = (
        _dig(subscription, "cancelledAt") or
        _dig(subscription, "cancelled_at") or
        _dig(subscription, "deletedAt") or
        _dig(subscription, "deleted_at")
    )
    if cancelled:
        return False

    # Endpoint is already filtered by ACTIVE. If no status field is returned, count it.
    return status in ("", "active", "activated")


def _smartrr_headers(api_key, mode="token"):
    if mode == "bearer":
        return {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
    return {
        "x-smartrr-access-token": api_key,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _smartrr_get(url, api_key, params=None):
    """Try the Smartrr access-token header first; retry bearer only if needed."""
    r = requests.get(url, headers=_smartrr_headers(api_key, "token"), params=params, timeout=60)
    if r.status_code in (401, 403):
        rb = requests.get(url, headers=_smartrr_headers(api_key, "bearer"), params=params, timeout=60)
        if rb.status_code < 400:
            return rb
    return r


def _parse_shopify_dt(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None


def _smartrr_empty_row(now_str, brand_name, source, reason):
    """Write an explicit empty/error row. No hardcoded reviewed fallback values."""
    return [now_str, brand_name, 0, 0, 0, 0, 0, 0, source, reason or ""]


def _find_contract_gid(obj, depth=0):
    """Find gid://shopify/SubscriptionContract/... anywhere in the Smartrr object."""
    if depth > 8 or obj is None:
        return ""
    if isinstance(obj, str):
        m = re.search(r'gid://shopify/SubscriptionContract/(\d+)', obj)
        if m:
            return f"gid://shopify/SubscriptionContract/{m.group(1)}"
        m = re.search(r'SubscriptionContract/(\d+)', obj)
        if m:
            return f"gid://shopify/SubscriptionContract/{m.group(1)}"
        return ""
    if isinstance(obj, dict):
        # Prefer obvious id fields first.
        for key in (
            "shopifyId", "shopify_id", "shopifySubscriptionContractId", "shopify_subscription_contract_id",
            "subscriptionContractId", "subscription_contract_id", "externalSubscriptionId", "external_subscription_id",
        ):
            val = obj.get(key)
            found = _find_contract_gid(val, depth + 1)
            if found:
                return found
            if val is not None:
                s = str(val).strip()
                if re.fullmatch(r'\d{6,}', s) and "contract" in key.lower():
                    return f"gid://shopify/SubscriptionContract/{s}"
        for v in obj.values():
            found = _find_contract_gid(v, depth + 1)
            if found:
                return found
    if isinstance(obj, list):
        for v in obj[:80]:
            found = _find_contract_gid(v, depth + 1)
            if found:
                return found
    return ""


def _classify_box_text(text):
    """Map Cavali product/box names to dashboard buckets."""
    t = _norm_txt(text)
    if not t:
        return "other"
    # Specific first so Junior Membership does not get swallowed by generic Membership.
    if "cavali club junior membership" in t or "junior membership" in t or re.search(r'\bjunior\b', t):
        return "junior"
    if "the signature box" in t or "signature box" in t or re.search(r'\bsignature\b', t):
        return "signature"
    if "the premier box" in t or "premier box" in t or re.search(r'\bpremier\b', t):
        return "premier"
    if re.search(r'\bcavali club membership\b', t) or re.search(r'\bseasonal\b', t):
        return "seasonal"
    return "other"


def fetch_shopify_subscription_contract_titles(store_url, token, contract_gids):
    """
    Smartrr purchase-state can return active subscription IDs without product labels.
    Resolve those Shopify SubscriptionContract IDs through Shopify GraphQL and read line titles.
    """
    ids = sorted({gid for gid in contract_gids if gid and str(gid).startswith("gid://shopify/SubscriptionContract/")})
    result = {}
    if not ids:
        return result

    for i in range(0, len(ids), 50):
        chunk = ids[i:i + 50]
        ids_arg = ",".join(json.dumps(x) for x in chunk)
        q = f'''
        {{
          nodes(ids: [{ids_arg}]) {{
            id
            ... on SubscriptionContract {{
              lines(first: 10) {{
                edges {{
                  node {{
                    title
                    variantTitle
                    productId
                    variantId
                  }}
                }}
              }}
            }}
          }}
        }}
        '''
        data = gql(store_url, token, q)
        nodes = (data or {}).get("nodes") or []
        for node in nodes:
            if not node or not node.get("id"):
                continue
            parts = []
            for edge in (((node.get("lines") or {}).get("edges")) or []):
                ln = edge.get("node") or {}
                for key in ("title", "variantTitle", "productId", "variantId"):
                    if ln.get(key):
                        parts.append(str(ln.get(key)))
            result[node["id"]] = " | ".join(parts)
        # Be gentle with Shopify GraphQL during large active-sub lists.
        if i + 50 < len(ids):
            time.sleep(0.35)
    return result


def fetch_smartrr_active_subs(brand_name, store_url=None, token=None):
    """
    Returns one Sheets row for smartrr_subscribers.

    Correct source of truth for Cavali box split:
      1) Smartrr ACTIVE purchase states for the active subscriber universe.
      2) Product/box name from Smartrr payload when present.
      3) If Smartrr does not expose product labels, resolve Shopify SubscriptionContract
         line titles through Shopify GraphQL and classify:
           Cavali Club Membership        -> Seasonal
           The Signature Box             -> Signature
           The Premier Box               -> Premier
           Cavali Club Junior Membership -> Junior
    """
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
    key = SMARTRR_API_KEYS.get(brand_name, "")
    source = "Smartrr ACTIVE purchase-state + Shopify SubscriptionContract lines"
    if brand_name != "cavali":
        return None
    if not key:
        return _smartrr_empty_row(now_str, brand_name, source, "SMARTRR_API_KEY_CAVALI missing")

    base_url = "https://api.smartrr.com/vendor/purchase-state"
    active_subs = []
    seen_ids = set()
    last_status = ""

    try:
        page_size = 250
        page_number = 0
        total_hint = None

        while page_number < 200:
            params = {
                "pageSize": page_size,
                "pageNumber": page_number,
                "filterEquals[purchaseStateStatus]": "ACTIVE",
                "include": "items,lineItems,stLineItems,product,variant,subscriptionProgram,sellingPlan",
            }
            r = _smartrr_get(base_url, key, params=params)
            last_status = f"HTTP {r.status_code}"
            if r.status_code >= 400:
                body = (r.text or "")[:350]
                return _smartrr_empty_row(now_str, brand_name, source, f"Smartrr {last_status}: {body}")

            payload = r.json()
            items = _smartrr_items(payload)
            total_hint = _smartrr_total_hint(payload)
            if not items:
                break

            for sub in items:
                raw_id = str(
                    _dig(sub, "id") or _dig(sub, "purchaseStateId") or _dig(sub, "purchase_state_id") or
                    _dig(sub, "shopifyId") or _dig(sub, "shopify_id") or
                    _dig(sub, "externalSubscriptionId") or _dig(sub, "external_subscription_id") or
                    _dig(sub, "subscriptionId") or _dig(sub, "subscription_id") or ""
                )
                contract_gid = _find_contract_gid(sub)
                dedupe_id = contract_gid or raw_id or json.dumps(sub, sort_keys=True)[:200]
                if dedupe_id in seen_ids:
                    continue
                seen_ids.add(dedupe_id)

                if not _smartrr_is_active(sub):
                    continue

                direct_text = _smartrr_plan_text(sub)
                direct_bucket = _classify_box_text(direct_text)
                active_subs.append({
                    "bucket": direct_bucket,
                    "direct_text": direct_text,
                    "contract_gid": contract_gid,
                })

            if len(items) < page_size:
                break
            if total_hint is not None and (page_number + 1) * page_size >= total_hint:
                break
            page_number += 1

        if not active_subs:
            return _smartrr_empty_row(now_str, brand_name, source, f"Smartrr returned 0 ACTIVE rows. Last status: {last_status or 'no response'}")

        # Resolve the records that Smartrr could not classify by product label.
        unresolved_gids = [s["contract_gid"] for s in active_subs if s["bucket"] == "other" and s.get("contract_gid")]
        contract_titles = {}
        if unresolved_gids and store_url and token:
            contract_titles = fetch_shopify_subscription_contract_titles(store_url, token, unresolved_gids)

        counts = {"seasonal": 0, "signature": 0, "premier": 0, "junior": 0, "other": 0}
        unknown_examples = []
        for sub in active_subs:
            bucket = sub["bucket"]
            if bucket == "other" and sub.get("contract_gid"):
                title_text = contract_titles.get(sub["contract_gid"], "")
                resolved_bucket = _classify_box_text(title_text)
                if resolved_bucket != "other":
                    bucket = resolved_bucket
                elif title_text and len(unknown_examples) < 5:
                    unknown_examples.append(title_text[:180])
            elif bucket == "other" and sub.get("direct_text") and len(unknown_examples) < 5:
                unknown_examples.append(sub["direct_text"][:180])
            counts[bucket] += 1

        total = sum(counts.values())
        error = ""
        if counts["other"] > 0:
            error = "Unmapped active subscriptions remained after product lookup: " + "; ".join(unknown_examples[:5])

        print(
            f"    smartrr: seasonal={counts['seasonal']} signature={counts['signature']} "
            f"premier={counts['premier']} junior={counts['junior']} other={counts['other']} total={total}"
        )
        return [
            now_str, brand_name,
            counts["seasonal"], counts["signature"], counts["premier"], counts["junior"],
            counts["other"], total, source, error,
        ]

    except Exception as e:
        print(f"    ⚠ smartrr error: {e}")
        return _smartrr_empty_row(now_str, brand_name, source, str(e))



def write_smartrr(gc, sheet_id, smartrr_row):
    if not smartrr_row:
        return
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet("smartrr_subscribers")
    except Exception:
        ws = sh.add_worksheet("smartrr_subscribers", rows=50, cols=len(SMARTRR_HEADERS))

    ws.clear()
    ws.append_row(SMARTRR_HEADERS)
    ws.append_row(smartrr_row, value_input_option="USER_ENTERED")
    print("    smartrr_subscribers: 1 row")

# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    gc      = get_gc()
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
    today   = datetime.now(TIMEZONE).date()

    # ── Limpiar tabs antes del backfill completo ──────────────────
    print("Limpiando tabs existentes...")
    for brand, cfg in STORES.items():
        sh = gc.open_by_key(cfg["sheet_id"])
        for tab, headers in [
            ("kpis_daily",      HEADERS_KPIS),
            ("revenue_share",   ["updated_at","period","channel","amount","pct"]),
            ("new_vs_returning",["updated_at","period","period_start","period_end",
                                  "new_customers","returning_customers",
                                  "new_revenue","returning_revenue",
                                  "new_gross_profit","returning_gross_profit"]),
        ]:
            try:
                ws = sh.worksheet(tab)
                ws.clear()
                ws.append_row(headers)
                print(f"  ✓ Limpiado {tab} para {brand}")
            except Exception as ex:
                print(f"  ✗ No se pudo limpiar {tab} para {brand}: {ex}")

    for brand, cfg in STORES.items():
        print(f"\n{'='*55}\n  {brand.upper()} — BACKFILL 2024-01 → {today}\n{'='*55}")
        url, token, sid = cfg["url"], cfg["token"], cfg["sheet_id"]
        kpi_rows, rs_rows, nvr_rows = [], [], []

        # ── MONTHLY ──────────────────────────────────────────────
        for y in range(2024, today.year + 1):
            m_start = 1
            m_end   = today.month if y == today.year else 12
            for m in range(m_start, m_end + 1):
                mo_start     = date(y, m, 1)
                mo_end       = last_day(y, m) if (y < today.year or m < today.month) else today
                period_label = f"{y}-{str(m).zfill(2)}"
                mtd_label    = f"mtd_{period_label}"

                print(f"\n  Month {period_label} ({mo_start} → {mo_end})")

                ql_cur = fetch_sales(url, token, mo_start, mo_end)
                s_cur  = fetch_sessions(url, token, mo_start, mo_end)
                of_cur = fetch_orders_fulfilled(url, token, mo_start, mo_end)
                o_cur  = fetch_orders(url, token, mo_start, mo_end)
                cur    = build(ql_cur, o_cur, s_cur, of_cur)

                # Prev month
                pm = m - 1 if m > 1 else 12
                py = y if m > 1 else y - 1
                prev_start = date(py, pm, 1)
                prev_end   = last_day(py, pm)
                ql_prev    = fetch_sales(url, token, prev_start, prev_end)
                of_prev    = fetch_orders_fulfilled(url, token, prev_start, prev_end)
                o_prev     = fetch_orders(url, token, prev_start, prev_end)
                prev       = build(ql_prev, o_prev, 0, of_prev)

                # YOY
                if y > 2024:
                    yoy_start = date(y - 1, m, 1)
                    yoy_end   = last_day(y - 1, m)
                    ql_yoy    = fetch_sales(url, token, yoy_start, yoy_end)
                    of_yoy    = fetch_orders_fulfilled(url, token, yoy_start, yoy_end)
                    o_yoy     = fetch_orders(url, token, yoy_start, yoy_end)
                    yoy       = build(ql_yoy, o_yoy, 0, of_yoy)
                else:
                    yoy = {}

                # Revenue share
                rs = calc_rs(o_cur)
                for ch, v in rs.items():
                    rs_rows.append([now_str, period_label, ch, v["amount"], v["pct"]])
                    rs_rows.append([now_str, mtd_label,    ch, v["amount"], v["pct"]])

                # New vs returning
                nvr     = calc_new_returning(o_cur)
                gm_rate = (cur.get("pct_gm", 0) or 0) / 100
                for lbl in [period_label, mtd_label]:
                    nvr_rows.append([
                        now_str, lbl, str(mo_start), str(mo_end),
                        nvr["new_customers"],
                        nvr["returning_customers"],
                        nvr["new_revenue"],
                        nvr["returning_revenue"],
                        round(nvr["new_revenue"]       * gm_rate, 2),
                        round(nvr["returning_revenue"] * gm_rate, 2),
                    ])

                kpi_rows.append(make_kpi_row(now_str, period_label, mo_start, mo_end, cur, prev, yoy))
                kpi_rows.append(make_kpi_row(now_str, mtd_label,    mo_start, mo_end, cur, prev, yoy))

        # ── WEEKLY ───────────────────────────────────────────────
        wk_start = monday_of(date(2024, 1, 1))
        while wk_start <= today:
            wk_end   = min(wk_start + timedelta(days=6), today)
            wk_label = f"week_{wk_start}"

            print(f"\n  Week {wk_label} ({wk_start} → {wk_end})")

            ql_cur = fetch_sales(url, token, wk_start, wk_end)
            s_cur  = fetch_sessions(url, token, wk_start, wk_end)
            of_cur = fetch_orders_fulfilled(url, token, wk_start, wk_end)
            o_cur  = fetch_orders(url, token, wk_start, wk_end)
            cur    = build(ql_cur, o_cur, s_cur, of_cur)

            pws    = wk_start - timedelta(days=7)
            pwe    = pws + timedelta(days=(wk_end - wk_start).days)
            ql_prev = fetch_sales(url, token, pws, pwe)
            of_prev = fetch_orders_fulfilled(url, token, pws, pwe)
            o_prev  = fetch_orders(url, token, pws, pwe)
            prev    = build(ql_prev, o_prev, 0, of_prev)

            yws    = wk_start - timedelta(days=364)
            ywe    = wk_end   - timedelta(days=364)
            ql_yoy = fetch_sales(url, token, yws, ywe)
            of_yoy = fetch_orders_fulfilled(url, token, yws, ywe)
            o_yoy  = fetch_orders(url, token, yws, ywe)
            yoy    = build(ql_yoy, o_yoy, 0, of_yoy)

            rs  = calc_rs(o_cur)
            nvr = calc_new_returning(o_cur)
            gm_rate = (cur.get("pct_gm", 0) or 0) / 100

            for ch, v in rs.items():
                rs_rows.append([now_str, wk_label, ch, v["amount"], v["pct"]])

            nvr_rows.append([
                now_str, wk_label, str(wk_start), str(wk_end),
                nvr["new_customers"],
                nvr["returning_customers"],
                nvr["new_revenue"],
                nvr["returning_revenue"],
                round(nvr["new_revenue"]       * gm_rate, 2),
                round(nvr["returning_revenue"] * gm_rate, 2),
            ])

            kpi_rows.append(make_kpi_row(now_str, wk_label, wk_start, wk_end, cur, prev, yoy))
            wk_start += timedelta(days=7)

        # ── QUARTERLY ────────────────────────────────────────────
        for y in range(2024, today.year + 1):
            max_q = ((today.month - 1) // 3) + 1 if y == today.year else 4
            for q in range(1, max_q + 1):
                q_start = date(y, (q - 1) * 3 + 1, 1)
                q_end_m = q * 3
                q_end   = last_day(y, q_end_m) if (y < today.year or q < max_q) else today
                q_label = f"q{q}_{y}"

                print(f"\n  Quarter {q_label} ({q_start} → {q_end})")

                ql_q   = fetch_sales(url, token, q_start, q_end)
                s_q    = fetch_sessions(url, token, q_start, q_end)
                of_q   = fetch_orders_fulfilled(url, token, q_start, q_end)
                o_q    = fetch_orders(url, token, q_start, q_end)
                cur_q  = build(ql_q, o_q, s_q, of_q)

                pq_    = q - 1 if q > 1 else 4
                py_    = y if q > 1 else y - 1
                pq_start = date(py_, (pq_ - 1) * 3 + 1, 1)
                pq_end   = last_day(py_, pq_ * 3)
                ql_pq    = fetch_sales(url, token, pq_start, pq_end)
                of_pq    = fetch_orders_fulfilled(url, token, pq_start, pq_end)
                o_pq     = fetch_orders(url, token, pq_start, pq_end)
                prev_q   = build(ql_pq, o_pq, 0, of_pq)

                if y > 2024:
                    yq_start = date(y - 1, (q - 1) * 3 + 1, 1)
                    yq_end   = last_day(y - 1, q * 3)
                    ql_yq    = fetch_sales(url, token, yq_start, yq_end)
                    of_yq    = fetch_orders_fulfilled(url, token, yq_start, yq_end)
                    o_yq     = fetch_orders(url, token, yq_start, yq_end)
                    yoy_q    = build(ql_yq, o_yq, 0, of_yq)
                else:
                    yoy_q = {}

                rs_q  = calc_rs(o_q)
                nvr_q = calc_new_returning(o_q)
                gm_rate = (cur_q.get("pct_gm", 0) or 0) / 100

                for ch, v in rs_q.items():
                    rs_rows.append([now_str, q_label, ch, v["amount"], v["pct"]])

                nvr_rows.append([
                    now_str, q_label, str(q_start), str(q_end),
                    nvr_q["new_customers"],
                    nvr_q["returning_customers"],
                    nvr_q["new_revenue"],
                    nvr_q["returning_revenue"],
                    round(nvr_q["new_revenue"]       * gm_rate, 2),
                    round(nvr_q["returning_revenue"] * gm_rate, 2),
                ])

                kpi_rows.append(make_kpi_row(now_str, q_label, q_start, q_end, cur_q, prev_q, yoy_q))

        # ── WRITE ────────────────────────────────────────────────
        print(f"\n  Escribiendo {len(kpi_rows)} filas KPI...")
        sh    = gc.open_by_key(sid)
        ws    = sh.worksheet("kpis_daily")
        ws_rs = sh.worksheet("revenue_share")

        try:
            ws_nvr = sh.worksheet("new_vs_returning")
        except Exception:
            ws_nvr = sh.add_worksheet("new_vs_returning", rows=600, cols=12)
            ws_nvr.append_row([
                "updated_at","period","period_start","period_end",
                "new_customers","returning_customers",
                "new_revenue","returning_revenue",
                "new_gross_profit","returning_gross_profit",
            ])

        for i in range(0, len(kpi_rows), 50):
            ws.append_rows(kpi_rows[i:i+50])
            print(f"  KPI batch {i//50+1} escrito")

        for i in range(0, len(rs_rows), 50):
            ws_rs.append_rows(rs_rows[i:i+50])
            print(f"  RS batch {i//50+1} escrito")

        for i in range(0, len(nvr_rows), 50):
            ws_nvr.append_rows(nvr_rows[i:i+50])
            print(f"  NVR batch {i//50+1} escrito")

        # Smartrr no es histórico por periodo: es snapshot actual de suscriptores activos.
        # Se escribe aquí para que, al correr backfill, el dashboard también tenga esta pestaña lista.
        smartrr_row = fetch_smartrr_active_subs(brand, url, token)
        write_smartrr(gc, sid, smartrr_row)

        print(f"\n  ✓ {brand.upper()} completado: "
              f"{len(kpi_rows)} KPI + {len(rs_rows)} RS + {len(nvr_rows)} NVR filas")


if __name__ == "__main__":
    main()
