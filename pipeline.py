"""
Pipeline CORRO / CAVALI v4 — FIXED
====================================
FIXES vs original:
1. HEADERS index correcto — new_customers en posición 20 (0-based) ✓
2. nc_by_month usa índice numérico seguro con HEADERS.index()
3. pct_gm normalizado siempre como float (no queda como "0.45" string)
4. gross_profit nunca queda en None — default 0.0
5. calendar import movido a top-level (no import dentro de loop)
6. fetch_new_vs_returning: maneja 'first_time' y 'repeat' como aliases
7. CAC auto: safe int cast con try/except
8. write_all: rs_rows siempre tiene len=10 antes de append (pad)
9. orders_fulfilled fallback a sales.orders si ShopifyQL retorna None
10. _gm() devuelve float coherente (no multiplica si ya es porcentaje entero)
"""

import os, json, requests, gspread, calendar
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, date
import pytz

TIMEZONE    = pytz.timezone("America/Bogota")
GQL_VERSION = "2025-10"

STORES = {
    "corro": {
        "url":      "equestrian-labs.myshopify.com",
        "token":    os.environ["SHOPIFY_TOKEN_CORRO"],
        "sheet_id": "1nq8xkDzowAvhD3wpMBlVK2M3FZSNS2DrAiPxz-Y2tdU",
    },
    "cavali": {
        "url":      "cavali-club.myshopify.com",
        "token":    os.environ["SHOPIFY_TOKEN_CAVALI"],
        "sheet_id": "1QUdJc2EIdElIX5nlLQxWxS98aAz-TgQnSg9glJpNtig",
    },
}
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ── kpis_daily columns ─────────────────────────────────────────────
HEADERS = [
    "updated_at",           # 0
    "period",               # 1
    "period_start",         # 2
    "period_end",           # 3
    "gross_sales",          # 4
    "net_sales",            # 5
    "gross_profit",         # 6   ← VISIBLE EN DASHBOARD
    "total_discounts",      # 7
    "total_returns",        # 8
    "cogs",                 # 9
    "pct_discount",         # 10
    "pct_returns",          # 11
    "pct_gm",               # 12
    "nb_orders",            # 13
    "nb_units",             # 14
    "aov",                  # 15
    "units_per_order",      # 16
    "sessions",             # 17
    "unique_visitors",      # 18
    "conversion_rate",      # 19
    "new_customers",        # 20
    "returning_customers",  # 21
    "new_revenue",          # 22
    "returning_revenue",    # 23
    "new_gross_profit",     # 24
    "returning_gross_profit", # 25
]

# ── Ad Spend (feed from Stats.xlsx before running) ─────────────────
AD_SPEND_DATA = {
    "corro": {
        "2024-01": {"spend": 82069,  "roas": 2.12, "cos": 0.472},
        "2024-02": {"spend": 38738,  "roas": 2.94, "cos": 0.341},
        "2024-03": {"spend": 39391,  "roas": 3.24, "cos": 0.309},
        "2024-04": {"spend": 16371,  "roas": 6.22, "cos": 0.161},
        "2024-05": {"spend": 7909,   "roas": 13.78,"cos": 0.073},
        "2024-06": {"spend": 19752,  "roas": 4.98, "cos": 0.201},
        "2024-07": {"spend": 10491,  "roas": 6.21, "cos": 0.161},
        "2024-08": {"spend": 16110,  "roas": 5.34, "cos": 0.187},
        "2024-09": {"spend": 18786,  "roas": 4.54, "cos": 0.220},
        "2024-10": {"spend": 22284,  "roas": 3.95, "cos": 0.253},
        "2024-11": {"spend": 30959,  "roas": 3.77, "cos": 0.265},
        "2024-12": {"spend": 22994,  "roas": 4.84, "cos": 0.207},
        "2025-01": {"spend": 32136,  "roas": 2.77, "cos": 0.362},
        "2025-02": {"spend": 26531,  "roas": 4.16, "cos": 0.240},
        "2025-03": {"spend": 32810,  "roas": 3.64, "cos": 0.275},
        "2025-04": {"spend": 40677,  "roas": 3.19, "cos": 0.313},
        "2025-05": {"spend": 59424,  "roas": 2.88, "cos": 0.348},
        "2025-06": {"spend": 45524,  "roas": 3.23, "cos": 0.310},
        "2025-07": {"spend": 51788,  "roas": 3.10, "cos": 0.322},
        "2025-08": {"spend": 27828,  "roas": 3.72, "cos": 0.269},
        "2025-09": {"spend": 36960,  "roas": 3.34, "cos": 0.300},
        "2025-10": {"spend": 45790,  "roas": 2.95, "cos": 0.339},
        "2025-11": {"spend": 41051,  "roas": 4.08, "cos": 0.245},
        "2025-12": {"spend": 36657,  "roas": 3.55, "cos": 0.282},
        "2026-01": {"spend": 33133,  "roas": 3.77, "cos": 0.265},
        "2026-02": {"spend": 16470,  "roas": 4.56, "cos": 0.219},
        "2026-03": {"spend": 0,      "roas": 0,    "cos": 0},
        "2026-04": {"spend": 7883,   "roas": 3.85, "cos": 0.260},
    },
    "cavali": {},
}

# ─────────────────────────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────
def get_gc():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    return gspread.authorize(creds)

# ─────────────────────────────────────────────────────────────────
# SHOPIFY HELPERS
# ─────────────────────────────────────────────────────────────────
def gql(store_url, token, query):
    r = requests.post(
        f"https://{store_url}/admin/api/{GQL_VERSION}/graphql.json",
        headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
        json={"query": query}, timeout=60,
    )
    if r.status_code != 200:
        print(f"    HTTP {r.status_code}"); return None
    d = r.json()
    if d.get("errors"):
        print(f"    GQL errors: {d['errors']}"); return None
    return d.get("data")

def rest(store_url, token, endpoint, params):
    url     = f"https://{store_url}/admin/api/2024-01/{endpoint}"
    headers = {"X-Shopify-Access-Token": token}
    results = []
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        key  = list(data.keys())[0]
        results.extend(data[key])
        link = r.headers.get("Link", ""); url = None; params = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
    return results

# ─────────────────────────────────────────────────────────────────
# SHOPIFYQL HELPERS
# ─────────────────────────────────────────────────────────────────
def ql_run(store_url, token, ql_query):
    """Runs ShopifyQL, returns list of {col: val} dicts."""
    q = ('{ shopifyqlQuery(query: "%s") { tableData { columns { name } rows } parseErrors } }'
         % ql_query.replace('"', '\\"'))
    data = gql(store_url, token, q)
    if not data:
        return []
    ql   = data.get("shopifyqlQuery") or {}
    errs = ql.get("parseErrors") or []
    if errs:
        print(f"    parseErrors: {errs}"); return []
    td   = ql.get("tableData") or {}
    cols = [c["name"] for c in (td.get("columns") or [])]
    rows = td.get("rows") or []
    return [{cols[i]: row[i] for i in range(len(cols))} for row in rows]

def ql_row(store_url, token, ql_query):
    """Returns the last row (totals for aggregate queries)."""
    rows = ql_run(store_url, token, ql_query)
    return rows[-1] if rows else None

def _m(v):
    """Converts any value to a safe float."""
    if v is None: return 0.0
    try:   return float(str(v).replace(",", "").strip())
    except: return 0.0

def _gm(v):
    """
    Normalizes gross_margin from ShopifyQL.
    ShopifyQL may return 0.423 (decimal) or 42.3 (percentage).
    Always returns as percentage (42.3).
    FIX: compare abs(f) < 1.5 to detect decimal format.
    """
    if v is None: return 0.0
    try:
        s = str(v).replace("%", "").replace(",", "").strip()
        f = float(s)
        # If ShopifyQL returns 0.423 → convert to 42.3
        # If it returns 42.3 → leave as-is
        if abs(f) < 1.5:
            return round(f * 100, 2)
        return round(f, 2)
    except:
        return 0.0

# ─────────────────────────────────────────────────────────────────
# FETCH: SALES (gross_profit real desde ShopifyQL)
# ─────────────────────────────────────────────────────────────────
def fetch_sales(url, token, s, e):
    """
    FROM sales — devuelve gross_sales, net_sales, gross_profit, cogs, gm%, orders.
    gross_profit es campo REAL confirmado en ShopifyQL.
    FIX: garantiza que gross_profit nunca sea None.
    """
    row = ql_row(url, token,
        f"FROM sales SHOW gross_sales, discounts, returns, net_sales, "
        f"cost_of_goods_sold, gross_profit, gross_margin, orders "
        f"SINCE {s} UNTIL {e}")

    if not row:
        return {k: 0.0 for k in
                ["gross_sales","discounts","returns","net_sales",
                 "cogs","gross_profit","pct_gm","orders"]}

    g  = round(_m(row.get("gross_sales")),      2)
    d  = round(abs(_m(row.get("discounts"))),   2)
    r  = round(abs(_m(row.get("returns"))),     2)
    n  = round(_m(row.get("net_sales")),        2)
    c  = round(_m(row.get("cost_of_goods_sold")), 2)
    # FIX: gross_profit with explicit fallback
    gp_raw = row.get("gross_profit")
    gp = round(_m(gp_raw), 2) if gp_raw is not None else round(n - c, 2)
    gm = _gm(row.get("gross_margin"))
    # FIX: if gm=0 but we have gp and n, calculate it
    if gm == 0.0 and n > 0 and gp != 0:
        gm = round(gp / n * 100, 2)
    o  = int(abs(_m(row.get("orders"))))

    print(f"    gross:{g:>12,.2f}  net:{n:>12,.2f}  gp:{gp:>10,.2f}  "
          f"cogs:{c:>9,.2f}  gm:{gm:>5.1f}%  orders:{o}")
    return {
        "gross_sales":  g,
        "discounts":    d,
        "returns":      r,
        "net_sales":    n,
        "cogs":         c,
        "gross_profit": gp,
        "pct_gm":       gm,
        "orders":       o,
    }

# ─────────────────────────────────────────────────────────────────
# FETCH: SESSIONS
# ─────────────────────────────────────────────────────────────────
def fetch_sessions(url, token, s, e):
    row = ql_row(url, token, f"FROM sessions SHOW sessions SINCE {s} UNTIL {e}")
    if not row: return 0
    v = int(abs(_m(row.get("sessions", 0))))
    print(f"    sessions: {v:,}")
    return v

# ─────────────────────────────────────────────────────────────────
# FETCH: ORDERS FULFILLED
# ─────────────────────────────────────────────────────────────────
def fetch_orders_fulfilled(url, token, s, e):
    row = ql_row(url, token,
        f"FROM fulfillments SHOW orders_fulfilled SINCE {s} UNTIL {e}")
    if not row: return None
    v = int(abs(_m(row.get("orders_fulfilled", 0))))
    print(f"    orders_fulfilled: {v:,}")
    return v

# ─────────────────────────────────────────────────────────────────
# FETCH: NEW vs RETURNING (ShopifyQL GROUP BY customer_type)
# ─────────────────────────────────────────────────────────────────
def fetch_new_vs_returning(url, token, s, e):
    """
    ShopifyQL GROUP BY customer_type → real values.
    FIX: handles all possible Shopify aliases:
      new / first_time / new_customer
      returning / repeat / returning_customer
    FIX: gross_profit por segmento con fallback a 0.0.
    """
    rows = ql_run(url, token,
        f"FROM sales SHOW customer_type, net_sales, orders, gross_profit "
        f"SINCE {s} UNTIL {e} GROUP BY customer_type")

    result = {
        "new_customers":          0,
        "returning_customers":    0,
        "new_revenue":            0.0,
        "returning_revenue":      0.0,
        "new_gross_profit":       0.0,
        "returning_gross_profit": 0.0,
    }

    NEW_ALIASES      = {"new", "first_time", "new_customer", "first-time"}
    RETURNING_ALIASES = {"returning", "repeat", "returning_customer", "repeat_customer"}

    for row in rows:
        ctype  = str(row.get("customer_type") or "").lower().strip()
        rev    = round(_m(row.get("net_sales")), 2)
        orders = int(abs(_m(row.get("orders", 0))))
        gp_raw = row.get("gross_profit")
        gp     = round(_m(gp_raw), 2) if gp_raw is not None else 0.0

        if ctype in NEW_ALIASES:
            result["new_revenue"]      += rev
            result["new_customers"]    += orders
            result["new_gross_profit"] += gp
        elif ctype in RETURNING_ALIASES:
            result["returning_revenue"]      += rev
            result["returning_customers"]    += orders
            result["returning_gross_profit"] += gp

    # Round final values
    for k in result:
        if isinstance(result[k], float):
            result[k] = round(result[k], 2)

    print(f"    new_customers:{result['new_customers']:>6}  "
          f"new_rev:{result['new_revenue']:>10,.2f}  "
          f"new_gp:{result['new_gross_profit']:>10,.2f}  "
          f"ret_customers:{result['returning_customers']:>6}  "
          f"ret_rev:{result['returning_revenue']:>10,.2f}  "
          f"ret_gp:{result['returning_gross_profit']:>10,.2f}")
    return result

# ─────────────────────────────────────────────────────────────────
# FETCH: REST ORDERS (para units + revenue share)
# ─────────────────────────────────────────────────────────────────
def fetch_orders(url, token, s, e):
    return rest(url, token, "orders.json", {
        "status":           "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min":   f"{s}T00:00:00-05:00",
        "created_at_max":   f"{e}T23:59:59-05:00",
        "limit":            250,
        "fields":           "id,subtotal_price,line_items,source_name,tags",
    })

def calc_units(orders):
    return sum(
        sum(int(li.get("quantity", 0) or 0) for li in o.get("line_items", []))
        for o in orders
    )

# ─────────────────────────────────────────────────────────────────
# REVENUE SHARE BY CHANNEL (net_sales + GP estimado por GM%)
# ─────────────────────────────────────────────────────────────────
def calc_rs(orders, overall_gm_pct):
    """
    Revenue share by channel using subtotal_price (net per order).
    GP per channel = channel_net_sales * overall_gm_pct / 100
    FIX: overall_gm_pct is already normalized as a percentage (e.g. 42.3).
    """
    ch = {
        "Wellington (POS)": 0.0,
        "Concierge":        0.0,
        "Online":           0.0,
        "Others":           0.0,
    }
    total = 0.0
    for o in orders:
        amt   = float(o.get("subtotal_price", 0) or 0)
        total += amt
        src   = (o.get("source_name") or "").lower().strip()
        tags  = (o.get("tags") or "").lower()
        if src == "pos" or "wellington" in tags or "pos" in tags:
            ch["Wellington (POS)"] += amt
        elif "concierge" in tags or "concierge" in src:
            ch["Concierge"] += amt
        elif src in ("web", "shopify", "", "online_store") or not src:
            ch["Online"] += amt
        else:
            ch["Others"] += amt

    result = {}
    for k, v in ch.items():
        pct    = round(v / total * 100, 2) if total else 0.0
        # FIX: overall_gm_pct is already in % form (42.3), divide by 100
        est_gp = round(v * overall_gm_pct / 100, 2)
        est_gm = round(overall_gm_pct, 2)
        result[k] = {
            "amount":         round(v, 2),
            "pct":            pct,
            "gross_profit":   est_gp,
            "gross_margin":   est_gm,
            "gp_is_estimate": True,
        }
    return result

# ─────────────────────────────────────────────────────────────────
# BUILD KPI DICT
# ─────────────────────────────────────────────────────────────────
def build(sales, orders, nvr, sessions=0, orders_fulfilled=None):
    """Combines all fetched data into a single KPI dict."""
    g   = sales.get("gross_sales",  0.0)
    d   = sales.get("discounts",    0.0)
    r   = sales.get("returns",      0.0)
    n   = sales.get("net_sales",    0.0)
    c   = sales.get("cogs",         0.0)
    gp  = sales.get("gross_profit", 0.0)
    gm  = sales.get("pct_gm",       0.0)

    # FIX: if gross_profit is 0 but we have net_sales and cogs, compute it
    if gp == 0.0 and n > 0 and c >= 0:
        gp = round(n - c, 2)

    # FIX: fallback orders_fulfilled to sales.orders if missing
    nb = (int(orders_fulfilled) if orders_fulfilled is not None
          else (sales.get("orders", 0) or len(orders)))

    units   = calc_units(orders)
    aov     = round(n / nb,    2) if nb    else 0.0
    upo     = round(units / nb, 2) if nb   else 0.0
    pdisc   = round(d / g * 100, 2) if g   else 0.0
    pret    = round(r / g * 100, 2) if g   else 0.0
    sess    = int(sessions or 0)
    uv      = round(sess * 0.85)    if sess else 0
    cr      = round(nb / sess * 100, 4) if sess else 0.0

    return {
        "gross_sales":            g,
        "net_sales":              n,
        "gross_profit":           gp,
        "total_discounts":        d,
        "total_returns":          r,
        "cogs":                   c,
        "pct_discount":           pdisc,
        "pct_returns":            pret,
        "pct_gm":                 gm,
        "nb_orders":              nb,
        "nb_units":               units,
        "aov":                    aov,
        "units_per_order":        upo,
        "sessions":               sess,
        "unique_visitors":        uv,
        "conversion_rate":        cr,
        # new vs returning (real de ShopifyQL)
        "new_customers":          nvr.get("new_customers",          0),
        "returning_customers":    nvr.get("returning_customers",    0),
        "new_revenue":            nvr.get("new_revenue",            0.0),
        "returning_revenue":      nvr.get("returning_revenue",      0.0),
        "new_gross_profit":       nvr.get("new_gross_profit",       0.0),
        "returning_gross_profit": nvr.get("returning_gross_profit", 0.0),
    }

# ─────────────────────────────────────────────────────────────────
# BUILD KPI ROW
# FIX: usa HEADERS list para garantizar orden correcto
# ─────────────────────────────────────────────────────────────────
def make_kpi_row(now_str, period_key, s, e, cur):
    """
    Construye una fila en el mismo orden que HEADERS.
    FIX: construido explícitamente para evitar errores de orden.
    """
    return [
        now_str,                              # 0  updated_at
        period_key,                           # 1  period
        str(s),                               # 2  period_start
        str(e),                               # 3  period_end
        cur.get("gross_sales",            0), # 4
        cur.get("net_sales",              0), # 5
        cur.get("gross_profit",           0), # 6  ← CLAVE
        cur.get("total_discounts",        0), # 7
        cur.get("total_returns",          0), # 8
        cur.get("cogs",                   0), # 9
        cur.get("pct_discount",           0), # 10
        cur.get("pct_returns",            0), # 11
        cur.get("pct_gm",                 0), # 12
        cur.get("nb_orders",              0), # 13
        cur.get("nb_units",               0), # 14
        cur.get("aov",                    0), # 15
        cur.get("units_per_order",        0), # 16
        cur.get("sessions",               0), # 17
        cur.get("unique_visitors",        0), # 18
        cur.get("conversion_rate",        0), # 19
        cur.get("new_customers",          0), # 20
        cur.get("returning_customers",    0), # 21
        cur.get("new_revenue",            0), # 22
        cur.get("returning_revenue",      0), # 23
        cur.get("new_gross_profit",       0), # 24
        cur.get("returning_gross_profit", 0), # 25
    ]

# ─────────────────────────────────────────────────────────────────
# PERIOD HELPERS
# ─────────────────────────────────────────────────────────────────
def _safe_date(v):
    try:    return datetime.strptime(str(v), "%Y-%m-%d").date()
    except: return date(1900, 1, 1)

def _row_to_map(headers, row):
    return {h: (row[i] if i < len(row) else "") for i, h in enumerate(headers)}

def _map_to_row(headers, m):
    return [m.get(h, "") for h in headers]

# ─────────────────────────────────────────────────────────────────
# PERIODS
# ─────────────────────────────────────────────────────────────────
def get_periods():
    today = datetime.now(TIMEZONE).date()
    dow   = today.weekday()  # 0=Lun

    mtd_s  = today.replace(day=1)
    mtd_e  = today
    mtd_pk = f"mtd_{today.strftime('%Y-%m')}"

    prev_mo_end   = mtd_s - timedelta(days=1)
    prev_mo_s     = prev_mo_end.replace(day=1)
    prev_mo_mtd_e = prev_mo_end.replace(day=min(today.day, prev_mo_end.day))

    yoy_mtd_s = mtd_s.replace(year=mtd_s.year - 1)
    yoy_mtd_e = today.replace(year=today.year - 1)

    wk_s  = today - timedelta(days=dow)
    wk_e  = today
    wk_pk = f"week_{wk_s}"

    pwk_e  = wk_s - timedelta(days=1)
    pwk_s  = pwk_e - timedelta(days=6)
    pwk_pk = f"week_{pwk_s}"

    yoy_wk_s = wk_s - timedelta(days=364)
    yoy_wk_e = wk_e - timedelta(days=364)

    mo_e  = mtd_s - timedelta(days=1)
    mo_s  = mo_e.replace(day=1)
    mo_pk = mo_s.strftime("%Y-%m")

    pmo_e  = mo_s - timedelta(days=1)
    pmo_s  = pmo_e.replace(day=1)
    pmo_pk = pmo_s.strftime("%Y-%m")

    yoy_mo_s = mo_s.replace(year=mo_s.year - 1)
    yoy_mo_e = mo_e.replace(year=mo_e.year - 1)

    q_num = (today.month - 1) // 3 + 1
    q_s   = today.replace(month=(q_num - 1) * 3 + 1, day=1)
    q_e   = today
    q_pk  = f"q{q_num}_{today.year}"

    pq    = q_num - 1 if q_num > 1 else 4
    pq_y  = today.year if q_num > 1 else today.year - 1
    pq_s  = date(pq_y, (pq - 1) * 3 + 1, 1)
    pq_em = pq * 3
    pq_e  = date(pq_y, pq_em, calendar.monthrange(pq_y, pq_em)[1])
    pq_pk = f"q{pq}_{pq_y}"

    yoy_q_s  = q_s.replace(year=q_s.year - 1)
    yoy_q_e  = today.replace(year=today.year - 1)
    yoy_q_pk = f"q{q_num}_{today.year - 1}"

    return {
        "mtd":          (mtd_s,       mtd_e,         mtd_pk),
        "mtd_prev":     (prev_mo_s,   prev_mo_mtd_e, None),
        "mtd_yoy":      (yoy_mtd_s,   yoy_mtd_e,     None),
        "week":         (wk_s,        wk_e,          wk_pk),
        "week_prev":    (pwk_s,       pwk_e,         pwk_pk),
        "week_yoy":     (yoy_wk_s,    yoy_wk_e,      None),
        "month":        (mo_s,        mo_e,          mo_pk),
        "month_prev":   (pmo_s,       pmo_e,         pmo_pk),
        "month_yoy":    (yoy_mo_s,    yoy_mo_e,      None),
        "quarter":      (q_s,         q_e,           q_pk),
        "quarter_prev": (pq_s,        pq_e,          pq_pk),
        "quarter_yoy":  (yoy_q_s,     yoy_q_e,       yoy_q_pk),
    }

# ─────────────────────────────────────────────────────────────────
# WRITE TO GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────
def write_all(gc, sheet_id, kpi_rows, rs_rows, nvr_rows, brand_name):
    sh = gc.open_by_key(sheet_id)

    # ── kpis_daily ──────────────────────────────────────────────
    try:    ws = sh.worksheet("kpis_daily")
    except: ws = sh.add_worksheet("kpis_daily", rows=600, cols=40)

    existing_vals = ws.get_all_values()
    existing = {}
    if len(existing_vals) >= 2:
        ex_h = existing_vals[0]
        for r in existing_vals[1:]:
            m  = _row_to_map(ex_h, r)
            pk = str(m.get("period", "")).strip()
            if pk:
                # FIX: rebuild with current HEADERS to prevent column misalignment
                existing[pk] = _map_to_row(HEADERS, m)

    for r in kpi_rows:
        existing[str(r[1]).strip()] = r

    merged = sorted(
        existing.values(),
        key=lambda r: (_safe_date(r[2]), str(r[1]))
    )
    ws.clear()
    ws.append_row(HEADERS)
    if merged:
        ws.append_rows(merged, value_input_option="USER_ENTERED")
    print(f"    kpis_daily: {len(merged)} rows")

    # ── revenue_share ────────────────────────────────────────────
    try:    ws_rs = sh.worksheet("revenue_share")
    except: ws_rs = sh.add_worksheet("revenue_share", rows=600, cols=12)

    rs_headers = [
        "updated_at", "period", "channel",
        "amount", "pct",
        "gross_profit", "gross_margin",
        "pct_prev", "pct_chg",
        "gp_is_estimate",
    ]
    rs_vals = ws_rs.get_all_values()
    existing_rs = {}
    if len(rs_vals) >= 2:
        ex_h = rs_vals[0]
        for r in rs_vals[1:]:
            m  = _row_to_map(ex_h, r)
            p  = str(m.get("period",  "")).strip()
            ch = str(m.get("channel", "")).strip()
            if p and ch:
                existing_rs[(p, ch)] = _map_to_row(rs_headers, m)

    for r in rs_rows:
        # FIX: padding to guarantee correct row length
        while len(r) < len(rs_headers):
            r.append("")
        existing_rs[(str(r[1]).strip(), str(r[2]).strip())] = r

    # Calculate pct_prev and pct_chg
    sorted_rs = sorted(existing_rs.values(), key=lambda r: (str(r[2]), str(r[1])))
    rs_idx    = {(str(r[2]).strip(), str(r[1]).strip()): r for r in sorted_rs}

    for r in sorted_rs:
        ch = str(r[2]).strip()
        pk = str(r[1]).strip()
        prev_pk = None
        if pk.startswith("mtd_"):
            try:
                yr, mo  = map(int, pk[4:].split("-"))
                pmo     = mo - 1 if mo > 1 else 12
                py      = yr if mo > 1 else yr - 1
                prev_pk = f"mtd_{py}-{str(pmo).zfill(2)}"
            except: pass
        elif pk.startswith("week_"):
            try:
                d       = datetime.strptime(pk[5:], "%Y-%m-%d").date()
                prev_pk = f"week_{d - timedelta(days=7)}"
            except: pass
        elif len(pk) == 7 and "-" in pk:
            try:
                yr, mo  = int(pk[:4]), int(pk[5:])
                pmo     = mo - 1 if mo > 1 else 12
                py      = yr if mo > 1 else yr - 1
                prev_pk = f"{py}-{str(pmo).zfill(2)}"
            except: pass
        elif pk.startswith("q") and "_" in pk:
            try:
                parts   = pk[1:].split("_")
                q, yr   = int(parts[0]), int(parts[1])
                pq      = q - 1 if q > 1 else 4
                py      = yr if q > 1 else yr - 1
                prev_pk = f"q{pq}_{py}"
            except: pass

        prev_row = rs_idx.get((ch, prev_pk)) if prev_pk else None
        try:    pct_now  = float(r[4]) if str(r[4]) not in ("", "None") else None
        except: pct_now  = None
        try:    pct_prev = float(prev_row[4]) if prev_row and str(prev_row[4]) not in ("", "None") else None
        except: pct_prev = None
        pct_chg = (round(pct_now - pct_prev, 2)
                   if pct_now is not None and pct_prev is not None else None)
        while len(r) < len(rs_headers):
            r.append("")
        r[7] = pct_prev if pct_prev is not None else ""
        r[8] = pct_chg  if pct_chg  is not None else ""

    merged_rs = sorted(existing_rs.values(), key=lambda r: (str(r[1]), str(r[2])))
    ws_rs.clear()
    ws_rs.append_row(rs_headers)
    if merged_rs:
        ws_rs.append_rows(merged_rs, value_input_option="USER_ENTERED")
    print(f"    revenue_share: {len(merged_rs)} rows")

    # ── new_vs_returning ─────────────────────────────────────────
    try:    ws_nvr = sh.worksheet("new_vs_returning")
    except: ws_nvr = sh.add_worksheet("new_vs_returning", rows=300, cols=12)

    nvr_headers = [
        "updated_at", "period", "period_start", "period_end",
        "new_customers", "returning_customers",
        "new_revenue", "returning_revenue",
        "new_gross_profit", "returning_gross_profit",
    ]
    nvr_vals = ws_nvr.get_all_values()
    existing_nvr = {}
    if len(nvr_vals) >= 2:
        ex_h = nvr_vals[0]
        for r in nvr_vals[1:]:
            m  = _row_to_map(ex_h, r)
            pk = str(m.get("period", "")).strip()
            if pk:
                existing_nvr[pk] = _map_to_row(nvr_headers, m)

    for r in nvr_rows:
        existing_nvr[str(r[1]).strip()] = r

    merged_nvr = sorted(
        existing_nvr.values(),
        key=lambda r: (_safe_date(r[2]), str(r[1]))
    )
    ws_nvr.clear()
    ws_nvr.append_row(nvr_headers)
    if merged_nvr:
        ws_nvr.append_rows(merged_nvr, value_input_option="USER_ENTERED")
    print(f"    new_vs_returning: {len(merged_nvr)} rows")

    # ── ad_spend ─────────────────────────────────────────────────
    try:    ws_ad = sh.worksheet("ad_spend")
    except: ws_ad = sh.add_worksheet("ad_spend", rows=200, cols=10)

    ad_headers = [
        "updated_at", "brand", "period", "period_start", "period_end",
        "ad_spend", "roas", "cos",
        "cac_auto",  # = ad_spend / new_customers del mes
    ]
    now_str    = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
    brand_data = AD_SPEND_DATA.get(brand_name, {})

    # FIX: nc_by_month usando HEADERS.index() — robusto al orden
    nc_idx = HEADERS.index("new_customers")
    nc_by_month = {}
    for r in merged:
        pk = str(r[1]).strip()
        # Only full-month rows (format YYYY-MM, not mtd_/week_/q)
        if (len(pk) == 7 and "-" in pk
                and not pk.startswith("mtd_")
                and not pk.startswith("week_")
                and not pk.startswith("q")):
            try:
                nc_by_month[pk] = int(float(r[nc_idx] or 0))
            except:
                nc_by_month[pk] = 0

    ad_rows = []
    for mo, vals in sorted(brand_data.items()):
        spend = vals.get("spend", 0)
        if not spend:
            continue
        yr, mn  = int(mo[:4]), int(mo[5:])
        ps      = f"{mo}-01"
        pe      = f"{mo}-{calendar.monthrange(yr, mn)[1]:02d}"
        nc      = nc_by_month.get(mo, 0)
        # FIX: CAC auto with try/except guard
        try:
            cac_auto = round(spend / nc, 2) if nc > 0 else ""
        except:
            cac_auto = ""

        ad_rows.append([
            now_str,
            brand_name,
            mo,
            ps,
            pe,
            spend,
            vals.get("roas", 0),
            vals.get("cos",  0),
            cac_auto,
        ])

    ws_ad.clear()
    ws_ad.append_row(ad_headers)
    if ad_rows:
        ws_ad.append_rows(ad_rows, value_input_option="USER_ENTERED")
    print(f"    ad_spend: {len(ad_rows)} months  "
          f"(cac_auto populated for {sum(1 for r in ad_rows if r[8]!='')} months)")

# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    gc      = get_gc()
    P       = get_periods()
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

    for brand_name, cfg in STORES.items():
        print(f"\n{'='*60}\n  {brand_name.upper()}\n{'='*60}")
        url, token = cfg["url"], cfg["token"]
        kpi_rows, rs_rows, nvr_rows = [], [], []

        periods_to_run = [
            {"label": "MTD",          "cur": "mtd",          "is_snapshot": False},
            {"label": "WEEK",         "cur": "week",         "is_snapshot": False},
            {"label": "MONTH",        "cur": "month",        "is_snapshot": False},
            {"label": "QUARTER",      "cur": "quarter",      "is_snapshot": False},
            {"label": "WEEK_PREV",    "cur": "week_prev",    "is_snapshot": True},
            {"label": "MONTH_PREV",   "cur": "month_prev",   "is_snapshot": True},
            {"label": "QUARTER_PREV", "cur": "quarter_prev", "is_snapshot": True},
        ]

        for it in periods_to_run:
            label       = it["label"]
            cur_k       = it["cur"]
            s, e, pk    = P[cur_k]

            print(f"\n  [{label}] {s} → {e}  (period='{pk}')")

            sal  = fetch_sales(url, token, s, e)
            sess = fetch_sessions(url, token, s, e)
            of   = fetch_orders_fulfilled(url, token, s, e)
            ords = fetch_orders(url, token, s, e)
            nvr  = fetch_new_vs_returning(url, token, s, e)

            cur  = build(sal, ords, nvr, sess, of)
            kpi_rows.append(make_kpi_row(now_str, pk, s, e, cur))

            # Revenue share
            gm_pct = sal.get("pct_gm", 0)
            rs     = calc_rs(ords, gm_pct)
            for ch, v in rs.items():
                rs_rows.append([
                    now_str, pk, ch,
                    v["amount"],  v["pct"],
                    v["gross_profit"], v["gross_margin"],
                    "", "",  # pct_prev, pct_chg se calculan en write_all
                    str(v["gp_is_estimate"]),
                ])

            # New vs returning
            nvr_rows.append([
                now_str, pk, str(s), str(e),
                nvr.get("new_customers",          0),
                nvr.get("returning_customers",    0),
                nvr.get("new_revenue",            0),
                nvr.get("returning_revenue",      0),
                nvr.get("new_gross_profit",       0),
                nvr.get("returning_gross_profit", 0),
            ])

        write_all(gc, cfg["sheet_id"], kpi_rows, rs_rows, nvr_rows, brand_name)

        print(f"\n  ✓ {brand_name.upper()} — {len(kpi_rows)} períodos escritos")
        for row in kpi_rows:
            gp_val  = row[6]
            gm_val  = row[12]
            nc_val  = row[20]
            print(f"    {row[1]:<24}  {row[2]} → {row[3]}"
                  f"  gross:{float(row[4] or 0):>12,.2f}"
                  f"  net:{float(row[5] or 0):>12,.2f}"
                  f"  gp:{float(gp_val or 0):>10,.2f}"
                  f"  gm:{float(gm_val or 0):>5.1f}%"
                  f"  new_cust:{int(nc_val or 0):>5}")

if __name__ == "__main__":
    main()
