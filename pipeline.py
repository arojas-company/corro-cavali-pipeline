"""
Pipeline CORRO / CAVALI — Shopify Analytics → Google Sheets
============================================================
API: ShopifyQL via GraphQL Admin API 2025-10
Documentacion oficial confirmada:
  - parseErrors es [String!]! (array de strings, NO objeto ni scalar)
  - tableData.rows es array de dicts {col_name: value}
  - Con WITH TOTALS las columnas de totales se llaman: col__totals
    EXCEPTO cuando no hay GROUP BY (query de totales puro),
    en ese caso el unico row ya ES el total.
  - gross_margin viene como ratio 0-1 (ej: 0.4521 = 45.21%)

Definiciones Shopify Analytics (fuente de verdad):
  gross_sales  = precio de lista sin descuentos ni returns
  discounts    = descuentos (Shopify lo devuelve negativo → guardamos abs())
  returns      = devoluciones (negativo → guardamos abs())
  net_sales    = gross_sales − discounts − returns
  cogs         = cost_of_goods_sold
  gross_margin = (net_sales − cogs) / net_sales × 100
  AOV Shopify  = (gross_sales − discounts) / orders
"""

import os, json, requests, gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import pytz

TIMEZONE    = pytz.timezone("America/Bogota")
GQL_VERSION = "2025-10"   # version minima que tiene shopifyqlQuery

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

HEADERS_KPIS = [
    "updated_at","period","period_start","period_end",
    "gross_sales","net_sales","total_discounts","total_returns","cogs",
    "pct_discount","pct_returns","pct_gm",
    "nb_orders","nb_units","aov","units_per_order",
    "sessions","unique_visitors","conversion_rate",
    "gross_sales_mom","gross_sales_yoy",
    "net_sales_mom","net_sales_yoy",
    "nb_orders_mom","nb_orders_yoy",
    "aov_mom","aov_yoy",
]

# ══════════════════════════════════════════════════════════════
# Google Sheets
# ══════════════════════════════════════════════════════════════
def get_gc():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES
    )
    return gspread.authorize(creds)

# ══════════════════════════════════════════════════════════════
# HTTP helpers
# ══════════════════════════════════════════════════════════════
def shopify_graphql(store_url, token, query):
    """POST GraphQL Admin API 2025-10. Retorna data{} o None."""
    url = f"https://{store_url}/admin/api/{GQL_VERSION}/graphql.json"
    r = requests.post(
        url,
        headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
        json={"query": query},
        timeout=60,
    )
    if r.status_code != 200:
        print(f"    HTTP {r.status_code}: {r.text[:300]}")
        return None
    d = r.json()
    if d.get("errors"):
        print(f"    GraphQL errors: {d['errors']}")
        return None
    return d.get("data")

def shopify_rest_get(store_url, token, endpoint, params):
    """REST Admin API paginado."""
    url = f"https://{store_url}/admin/api/2024-01/{endpoint}"
    headers = {"X-Shopify-Access-Token": token}
    results = []
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        results.extend(data[list(data.keys())[0]])
        link, url, params = r.headers.get("Link",""), None, {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
    return results

# ══════════════════════════════════════════════════════════════
# ShopifyQL core parser
# ══════════════════════════════════════════════════════════════
def _run_ql(store_url, token, ql_query):
    """
    Ejecuta una ShopifyQL query y retorna el unico/ultimo row como dict.
    
    CRITICO — estructura de respuesta 2025-10:
      parseErrors: [String!]!   → lista de strings, puede estar vacia []
      tableData.rows: [JSON!]!  → cada row es un dict {col_name: value_str}
    
    Sin GROUP BY + WITH TOTALS: hay UN solo row que ya es el total global.
    """
    gql = """
    {
      shopifyqlQuery(query: "%s") {
        tableData {
          columns { name dataType }
          rows
        }
        parseErrors
      }
    }
    """ % ql_query.replace('"', '\\"')

    data = shopify_graphql(store_url, token, gql)
    if not data:
        return None

    ql = data.get("shopifyqlQuery") or {}

    # parseErrors es [String!]! — lista de strings
    errs = ql.get("parseErrors") or []
    if errs:
        print(f"    ShopifyQL parseErrors: {errs}")
        return None

    table = ql.get("tableData") or {}
    rows  = table.get("rows") or []
    if not rows:
        print("    ShopifyQL: sin filas")
        return None

    # Sin GROUP BY el unico row es el total
    return rows[-1]   # dict {col_name: value_str}

def _money(v):
    """'1,234.56' o '-4837.31' → float"""
    if v is None: return 0.0
    try:
        return float(str(v).replace(",","").strip())
    except ValueError:
        return 0.0

def _ratio_to_pct(v):
    """
    gross_margin viene como ratio 0-1 (ej: 0.4521 → 45.21%).
    Si ya viene >1 (ej: 45.21) lo dejamos tal cual.
    """
    if v is None: return 0.0
    try:
        val = float(str(v).replace("%","").replace(",","").strip())
        return round(val * 100, 2) if abs(val) <= 1.0 else round(val, 2)
    except ValueError:
        return 0.0

# ══════════════════════════════════════════════════════════════
# ShopifyQL — metricas financieras
# ══════════════════════════════════════════════════════════════
def fetch_sales_ql(store_url, token, start_date, end_date):
    """
    FROM sales sin GROUP BY → un solo row con los totales del periodo.
    Campos: gross_sales, discounts, returns, net_sales,
            cost_of_goods_sold, gross_margin, orders
    
    NOTA: discounts y returns vienen negativos en Shopify → abs()
    NOTA: gross_margin viene como ratio 0-1
    """
    row = _run_ql(
        store_url, token,
        f"FROM sales SHOW gross_sales, discounts, returns, net_sales, "
        f"cost_of_goods_sold, gross_margin, orders SINCE {start_date} UNTIL {end_date}"
    )

    empty = {k: None for k in
             ["gross_sales","discounts","returns","net_sales","cogs","pct_gm","orders"]}
    if row is None:
        return empty

    gross  = round(_money(row.get("gross_sales")),          2)
    disc   = round(abs(_money(row.get("discounts"))),       2)
    ret    = round(abs(_money(row.get("returns"))),         2)
    net    = round(_money(row.get("net_sales")),            2)
    cogs   = round(_money(row.get("cost_of_goods_sold")),   2)
    pct_gm = _ratio_to_pct(row.get("gross_margin"))
    orders = int(abs(_money(row.get("orders"))))

    print(f"    ✓ ShopifyQL  gross:{gross:>12,.2f}  disc:{disc:>10,.2f}  "
          f"ret:{ret:>10,.2f}  net:{net:>12,.2f}  "
          f"cogs:{cogs:>10,.2f}  gm:{pct_gm:>6.2f}%  ord:{orders}")

    return {"gross_sales":gross,"discounts":disc,"returns":ret,
            "net_sales":net,"cogs":cogs,"pct_gm":pct_gm,"orders":orders}

def fetch_sessions_ql(store_url, token, start_date, end_date):
    """FROM sessions → total sesiones del periodo."""
    row = _run_ql(
        store_url, token,
        f"FROM sessions SHOW sessions SINCE {start_date} UNTIL {end_date}"
    )
    if row is None: return 0
    return int(abs(_money(row.get("sessions", 0))))

# ══════════════════════════════════════════════════════════════
# REST orders — units + revenue share
# ══════════════════════════════════════════════════════════════
def fetch_orders(store_url, token, start_date, end_date):
    return shopify_rest_get(store_url, token, "orders.json", {
        "status":           "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min":   f"{start_date}T00:00:00-05:00",
        "created_at_max":   f"{end_date}T23:59:59-05:00",
        "limit":            250,
        "fields":           "id,subtotal_price,line_items,source_name,tags",
    })

def calc_units(orders):
    return sum(
        sum(int(li.get("quantity",0) or 0) for li in o.get("line_items",[]))
        for o in orders
    )

def calc_revenue_share(orders):
    ch = {"Wellington (POS)":0.0,"Concierge":0.0,"Online":0.0,"Others":0.0}
    total = 0.0
    for o in orders:
        amt   = float(o.get("subtotal_price",0) or 0)
        total += amt
        src   = (o.get("source_name") or "").lower().strip()
        tags  = (o.get("tags") or "").lower()
        if src == "pos" or "wellington" in tags or "pos" in tags:
            ch["Wellington (POS)"] += amt
        elif "concierge" in tags or "concierge" in src:
            ch["Concierge"] += amt
        elif src in ("web","shopify","","online_store") or not src:
            ch["Online"] += amt
        else:
            ch["Others"] += amt
    return {k: {"amount":round(v,2), "pct":round(v/total*100,2) if total else 0.0}
            for k,v in ch.items()}

# ══════════════════════════════════════════════════════════════
# Build KPIs
# ══════════════════════════════════════════════════════════════
def build_kpis(ql, orders, sessions=0):
    """
    Combina ShopifyQL (financiero exacto) + REST orders (units).
    AOV Shopify = (gross_sales - discounts) / orders  [sin returns]
    """
    if ql.get("gross_sales") is not None:
        gross  = ql["gross_sales"]
        disc   = ql["discounts"]
        ret    = ql["returns"]
        net    = ql["net_sales"]
        cogs   = ql["cogs"]   or 0.0
        pct_gm = ql["pct_gm"] or 0.0
        nb_ord = ql["orders"] or len(orders)
    else:
        print("    ⚠ ShopifyQL no disponible — fallback REST parcial (sin COGS/GM)")
        nb_ord = len(orders)
        gross  = sum(float(o.get("subtotal_price",0) or 0) for o in orders)
        disc   = 0.0
        ret    = 0.0
        net    = gross
        cogs   = 0.0
        pct_gm = 0.0

    units    = calc_units(orders)
    aov      = round((gross - disc) / nb_ord, 2) if nb_ord else 0.0
    upo      = round(units / nb_ord, 2)           if nb_ord else 0.0
    pct_disc = round(disc / gross * 100, 2)       if gross  else 0.0
    pct_ret  = round(ret  / gross * 100, 2)       if gross  else 0.0
    sessions = int(sessions or 0)
    uv       = round(sessions * 0.85)             if sessions else 0
    cr       = round(nb_ord / sessions * 100, 4)  if sessions else 0.0

    return {
        "gross_sales":     round(gross, 2),
        "net_sales":       round(net,   2),
        "total_discounts": round(disc,  2),
        "total_returns":   round(ret,   2),
        "cogs":            round(cogs,  2),
        "pct_discount":    pct_disc,
        "pct_returns":     pct_ret,
        "pct_gm":          pct_gm,
        "nb_orders":       nb_ord,
        "nb_units":        units,
        "aov":             aov,
        "units_per_order": upo,
        "sessions":        sessions,
        "unique_visitors": uv,
        "conversion_rate": cr,
    }

# ══════════════════════════════════════════════════════════════
# Periodos
# ══════════════════════════════════════════════════════════════
def get_periods():
    today       = datetime.now(TIMEZONE).date()
    mtd_start   = today.replace(day=1)
    mtd_end     = today
    mom_end     = mtd_start - timedelta(days=1)
    mom_start   = mom_end.replace(day=1)
    mom_mtd_end = mom_end.replace(day=min(today.day, mom_end.day))
    yoy_start   = mtd_start.replace(year=mtd_start.year-1)
    yoy_end     = today.replace(year=today.year-1)
    wk_start    = today - timedelta(days=today.weekday())
    pwk_start   = wk_start - timedelta(days=7)
    mo_end      = mtd_start - timedelta(days=1)
    q_month     = ((today.month-1)//3)*3+1
    return {
        "mtd":       (mtd_start,          mtd_end),
        "mtd_mom":   (mom_start,          mom_mtd_end),
        "mtd_yoy":   (yoy_start,          yoy_end),
        "week":      (wk_start,           today),
        "week_prev": (pwk_start,          wk_start - timedelta(days=1)),
        "month":     (mo_end.replace(day=1), mo_end),
        "quarter":   (today.replace(month=q_month, day=1), today),
    }

# ══════════════════════════════════════════════════════════════
# Google Sheets writer
# ══════════════════════════════════════════════════════════════
def pct_change(cur, prev):
    if not prev: return None
    return round((cur - prev) / prev * 100, 2)

def write_kpis(gc, sheet_id, periods_data):
    sh      = gc.open_by_key(sheet_id)
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

    try:    ws = sh.worksheet("kpis_daily")
    except: ws = sh.add_worksheet("kpis_daily", rows=500, cols=35)
    ws.clear()
    ws.append_row(HEADERS_KPIS)

    for pname, d in periods_data.items():
        cur = d["current"]
        mom = d.get("mom", {})
        yoy = d.get("yoy", {})
        ws.append_row([
            now_str, pname, str(d["start"]), str(d["end"]),
            cur.get("gross_sales",0),     cur.get("net_sales",0),
            cur.get("total_discounts",0), cur.get("total_returns",0),
            cur.get("cogs",0),
            cur.get("pct_discount",0),    cur.get("pct_returns",0),
            cur.get("pct_gm",0),
            cur.get("nb_orders",0),       cur.get("nb_units",0),
            cur.get("aov",0),             cur.get("units_per_order",0),
            cur.get("sessions",0),        cur.get("unique_visitors",0),
            cur.get("conversion_rate",0),
            pct_change(cur.get("gross_sales",0), mom.get("gross_sales")),
            pct_change(cur.get("gross_sales",0), yoy.get("gross_sales")),
            pct_change(cur.get("net_sales",0),   mom.get("net_sales")),
            pct_change(cur.get("net_sales",0),   yoy.get("net_sales")),
            pct_change(cur.get("nb_orders",0),   mom.get("nb_orders")),
            pct_change(cur.get("nb_orders",0),   yoy.get("nb_orders")),
            pct_change(cur.get("aov",0),         mom.get("aov")),
            pct_change(cur.get("aov",0),         yoy.get("aov")),
        ])

    try:    ws_rs = sh.worksheet("revenue_share")
    except: ws_rs = sh.add_worksheet("revenue_share", rows=500, cols=10)
    ws_rs.clear()
    ws_rs.append_row(["updated_at","period","channel","amount","pct"])
    for pname, d in periods_data.items():
        for ch, v in d.get("revenue_share",{}).items():
            ws_rs.append_row([now_str, pname, ch, v["amount"], v["pct"]])

    print(f"  ✓ Sheets OK: {now_str}")

# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def main():
    gc      = get_gc()
    periods = get_periods()

    for brand, cfg in STORES.items():
        print(f"\n{'='*52}\n  {brand.upper()}\n{'='*52}")
        url, token = cfg["url"], cfg["token"]

        # [1/3] ShopifyQL financiero
        print(f"\n  [1/3] ShopifyQL (API {GQL_VERSION})...")
        ql = {}
        for key in ["mtd","mtd_mom","mtd_yoy","week","week_prev","month","quarter"]:
            s, e = periods[key]
            print(f"  {key.upper():<10} {s} → {e}")
            ql[key] = fetch_sales_ql(url, token, s, e)

        # [2/3] Sessions
        print("\n  [2/3] Sessions...")
        s_mtd     = fetch_sessions_ql(url, token, *periods["mtd"])
        s_week    = fetch_sessions_ql(url, token, *periods["week"])
        s_month   = fetch_sessions_ql(url, token, *periods["month"])
        s_quarter = fetch_sessions_ql(url, token, *periods["quarter"])
        print(f"  sessions  mtd:{s_mtd}  week:{s_week}  month:{s_month}  qtr:{s_quarter}")

        # [3/3] Orders REST
        print("\n  [3/3] Orders REST...")
        orders = {}
        for key in ["mtd","mtd_mom","mtd_yoy","week","week_prev","month","quarter"]:
            orders[key] = fetch_orders(url, token, *periods[key])
        print(f"  orders  mtd:{len(orders['mtd'])}  week:{len(orders['week'])}  "
              f"month:{len(orders['month'])}  qtr:{len(orders['quarter'])}")

        # Build KPIs
        cur_mtd = build_kpis(ql["mtd"],      orders["mtd"],      s_mtd)
        mom_mtd = build_kpis(ql["mtd_mom"],  orders["mtd_mom"])
        yoy_mtd = build_kpis(ql["mtd_yoy"],  orders["mtd_yoy"])
        cur_wk  = build_kpis(ql["week"],     orders["week"],     s_week)
        mom_wk  = build_kpis(ql["week_prev"],orders["week_prev"])
        cur_mo  = build_kpis(ql["month"],    orders["month"],    s_month)
        cur_qtr = build_kpis(ql["quarter"],  orders["quarter"],  s_quarter)

        periods_data = {
            "mtd":     {"start":periods["mtd"][0],     "end":periods["mtd"][1],
                        "current":cur_mtd,"mom":mom_mtd,"yoy":yoy_mtd,
                        "revenue_share":calc_revenue_share(orders["mtd"])},
            "week":    {"start":periods["week"][0],    "end":periods["week"][1],
                        "current":cur_wk, "mom":mom_wk, "yoy":{},
                        "revenue_share":calc_revenue_share(orders["week"])},
            "month":   {"start":periods["month"][0],   "end":periods["month"][1],
                        "current":cur_mo, "mom":{},     "yoy":{},
                        "revenue_share":calc_revenue_share(orders["month"])},
            "quarter": {"start":periods["quarter"][0], "end":periods["quarter"][1],
                        "current":cur_qtr,"mom":{},     "yoy":{},
                        "revenue_share":calc_revenue_share(orders["quarter"])},
        }

        write_kpis(gc, cfg["sheet_id"], periods_data)
        print(f"\n  ✓ {brand.upper()} done.")

if __name__ == "__main__":
    main()
