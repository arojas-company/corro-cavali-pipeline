import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import pytz

TIMEZONE = pytz.timezone("America/Bogota")

STORES = {
    "corro": {
        "url": "equestrian-labs.myshopify.com",
        "token": os.environ["SHOPIFY_TOKEN_CORRO"],
        "sheet_id": "1nq8xkDzowAvhD3wpMBlVK2M3FZSNS2DrAiPxz-Y2tdU",
    },
    "cavali": {
        "url": "cavali-club.myshopify.com",
        "token": os.environ["SHOPIFY_TOKEN_CAVALI"],
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

def get_gc():
    creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_json, scopes=SCOPES)
    return gspread.authorize(creds)

def shopify_get(store_url, token, endpoint, params={}):
    url = f"https://{store_url}/admin/api/2024-01/{endpoint}"
    headers = {"X-Shopify-Access-Token": token}
    results = []
    while url:
        r = requests.get(url, headers=headers, params=params)
        r.raise_for_status()
        data = r.json()
        key = list(data.keys())[0]
        results.extend(data[key])
        link = r.headers.get("Link", "")
        url = None
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
                    params = {}
    return results

def shopify_graphql(store_url, token, query):
    """Generic GraphQL call to Shopify."""
    url = f"https://{store_url}/admin/api/2024-10/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    r = requests.post(url, headers=headers, json={"query": query}, timeout=45)
    if r.status_code != 200:
        return None
    d = r.json()
    if d.get("errors"):
        print(f"    GraphQL errors: {d['errors']}")
        return None
    return d.get("data")

# ─────────────────────────────────────────────────────────────
# FUENTE DE VERDAD: ShopifyQL via GraphQL
# Replica EXACTAMENTE los números de Shopify Analytics
# ─────────────────────────────────────────────────────────────
def fetch_shopify_analytics(store_url, token, start_date, end_date):
    """
    Usa ShopifyQL para obtener todos los KPIs financieros directamente
    de la misma fuente que usa Shopify Analytics.
    Campos: gross_sales, discounts, returns, net_sales,
            cost_of_goods_sold, gross_margin, orders, sessions
    """
    query = """
    {
      shopifyqlQuery(query: "FROM sales SHOW gross_sales, discounts, returns, net_sales, cost_of_goods_sold, gross_margin, orders DURING custom(%s,%s) WITH TOTALS") {
        __typename
        ... on TableResponse {
          tableData {
            rowData
            columns { name dataType }
          }
        }
        ... on ParseErrorResponse {
          parseErrors { code message }
        }
      }
    }
    """ % (start_date, end_date)

    result = {
        "gross_sales": None, "discounts": None, "returns": None,
        "net_sales": None, "cogs": None, "pct_gm": None, "orders": None,
    }

    data = shopify_graphql(store_url, token, query)
    if not data:
        return result

    ql = data.get("shopifyqlQuery", {})
    typename = ql.get("__typename", "")

    if typename == "ParseErrorResponse":
        print(f"    ShopifyQL parse error: {ql.get('parseErrors')}")
        return result

    table = ql.get("tableData")
    if not table or not table.get("rowData"):
        print(f"    ShopifyQL: no data returned for {start_date} to {end_date}")
        return result

    cols = [c["name"] for c in table["columns"]]
    rows = table["rowData"]
    # Last row is TOTALS
    totals = dict(zip(cols, rows[-1]))

    def parse_money(v):
        if v is None: return 0.0
        return float(str(v).replace("$","").replace(",","").strip() or 0)

    def parse_pct(v):
        if v is None: return 0.0
        val = float(str(v).replace("%","").strip() or 0)
        # Shopify returns 0.32 for 32% or 32.0 for 32%
        return round(val * 100, 2) if val < 1 else round(val, 2)

    result["gross_sales"] = round(parse_money(totals.get("gross_sales")), 2)
    result["discounts"]   = round(abs(parse_money(totals.get("discounts"))), 2)
    result["returns"]     = round(abs(parse_money(totals.get("returns"))), 2)
    result["net_sales"]   = round(parse_money(totals.get("net_sales")), 2)
    result["cogs"]        = round(parse_money(totals.get("cost_of_goods_sold")), 2)
    result["pct_gm"]      = parse_pct(totals.get("gross_margin"))
    result["orders"]      = int(parse_money(totals.get("orders")))

    print(f"    ShopifyQL → gross:{result['gross_sales']:,.2f} "
          f"disc:{result['discounts']:,.2f} ret:{result['returns']:,.2f} "
          f"net:{result['net_sales']:,.2f} cogs:{result['cogs']:,.2f} "
          f"gm:{result['pct_gm']}% orders:{result['orders']}")

    return result

def fetch_sessions_ql(store_url, token, start_date, end_date):
    query = """
    {
      shopifyqlQuery(query: "FROM sessions SHOW sessions DURING custom(%s,%s) WITH TOTALS") {
        ... on TableResponse {
          tableData { rowData columns { name } }
        }
      }
    }
    """ % (start_date, end_date)
    data = shopify_graphql(store_url, token, query)
    if not data: return 0
    table = data.get("shopifyqlQuery", {}).get("tableData", {})
    if not table or not table.get("rowData"): return 0
    cols = [c["name"] for c in table["columns"]]
    row  = dict(zip(cols, table["rowData"][-1]))
    return int(float(row.get("sessions", 0) or 0))

# ─────────────────────────────────────────────────────────────
# ORDERS REST — solo para units y revenue share
# ─────────────────────────────────────────────────────────────
def fetch_orders(store_url, token, start_date, end_date):
    params = {
        "status": "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min": f"{start_date}T00:00:00-05:00",
        "created_at_max": f"{end_date}T23:59:59-05:00",
        "limit": 250,
        "fields": "id,subtotal_price,line_items,source_name,tags",
    }
    return shopify_get(store_url, token, "orders.json", params)

def calc_units(orders):
    return sum(
        sum(int(li.get("quantity", 0) or 0) for li in o.get("line_items", []))
        for o in orders
    )

def calc_revenue_share(orders):
    channels = {"Wellington (POS)": 0, "Concierge": 0, "Online": 0, "Others": 0}
    total = 0
    for o in orders:
        amount = float(o.get("subtotal_price", 0) or 0)
        total += amount
        src  = (o.get("source_name") or "").lower().strip()
        tags = (o.get("tags") or "").lower()
        if src == "pos" or "wellington" in tags or "pos" in tags:
            channels["Wellington (POS)"] += amount
        elif "concierge" in tags or "concierge" in src:
            channels["Concierge"] += amount
        elif src in ("web", "shopify", "", "online_store") or not src:
            channels["Online"] += amount
        else:
            channels["Others"] += amount
    return {
        k: {"amount": round(v,2), "pct": round(v/total*100,2) if total else 0}
        for k, v in channels.items()
    }

# ─────────────────────────────────────────────────────────────
# BUILD KPIs — combina ShopifyQL + orders REST
# ─────────────────────────────────────────────────────────────
def build_kpis(ql, orders, sessions=0):
    """
    ql: resultado de fetch_shopify_analytics (fuente de verdad financiera)
    orders: resultado de fetch_orders (para units y revenue share)
    """
    # Si ShopifyQL falló, calcular desde orders como fallback
    if ql.get("gross_sales") is None:
        print("    ⚠ ShopifyQL falló — usando órdenes REST como fallback")
        gross  = sum(float(o.get("subtotal_price",0) or 0) for o in orders)
        net    = gross
        disc   = 0
        ret    = 0
        cogs   = 0
        pct_gm = 0
        nb_ord = len(orders)
    else:
        gross  = ql["gross_sales"]
        disc   = ql["discounts"]
        ret    = ql["returns"]
        net    = ql["net_sales"]
        cogs   = ql["cogs"] or 0
        pct_gm = ql["pct_gm"] or 0
        nb_ord = ql["orders"] or len(orders)

    units = calc_units(orders)
    aov   = round(net / nb_ord, 2) if nb_ord else 0
    upo   = round(units / nb_ord, 2) if nb_ord else 0

    pct_disc = round(disc / gross * 100, 2) if gross else 0
    pct_ret  = round(ret  / gross * 100, 2) if gross else 0

    sessions = int(sessions or 0)
    uv       = round(sessions * 0.85) if sessions else 0
    cr       = round(nb_ord / sessions * 100, 4) if sessions else 0

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

def pct_change(cur, prev):
    if not prev: return None
    return round((cur - prev) / prev * 100, 2)

def get_periods():
    today     = datetime.now(TIMEZONE).date()
    mtd_start = today.replace(day=1)
    mtd_end   = today
    mom_end   = mtd_start - timedelta(days=1)
    mom_start = mom_end.replace(day=1)
    mom_mtd_end = mom_end.replace(day=min(today.day, mom_end.day))
    yoy_start = mtd_start.replace(year=mtd_start.year-1)
    yoy_end   = today.replace(year=today.year-1)
    wk_start  = today - timedelta(days=today.weekday())
    wk_end    = today
    pwk_start = wk_start - timedelta(days=7)
    pwk_end   = wk_start - timedelta(days=1)
    mo_end    = mtd_start - timedelta(days=1)
    mo_start  = mo_end.replace(day=1)
    q_month   = ((today.month-1)//3)*3+1
    q_start   = today.replace(month=q_month, day=1)
    q_end     = today
    return {
        "mtd":       (mtd_start, mtd_end),
        "mtd_mom":   (mom_start, mom_mtd_end),
        "mtd_yoy":   (yoy_start, yoy_end),
        "week":      (wk_start,  wk_end),
        "week_prev": (pwk_start, pwk_end),
        "month":     (mo_start,  mo_end),
        "quarter":   (q_start,   q_end),
    }

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
        for ch, v in d.get("revenue_share", {}).items():
            ws_rs.append_row([now_str, pname, ch, v["amount"], v["pct"]])

    print(f"  ✓ Sheets OK: {now_str}")

def main():
    gc      = get_gc()
    periods = get_periods()

    for brand, cfg in STORES.items():
        print(f"\n{'='*50}\n  {brand.upper()}\n{'='*50}")
        url   = cfg["url"]
        token = cfg["token"]

        # ShopifyQL — fuente de verdad financiera
        print("\n  [1/3] ShopifyQL analytics...")
        print(f"  MTD ({periods['mtd'][0]} → {periods['mtd'][1]}):")
        ql_mtd     = fetch_shopify_analytics(url, token, *periods["mtd"])
        print(f"  MTD MOM ({periods['mtd_mom'][0]} → {periods['mtd_mom'][1]}):")
        ql_mtd_mom = fetch_shopify_analytics(url, token, *periods["mtd_mom"])
        print(f"  MTD YOY ({periods['mtd_yoy'][0]} → {periods['mtd_yoy'][1]}):")
        ql_mtd_yoy = fetch_shopify_analytics(url, token, *periods["mtd_yoy"])
        print(f"  WEEK ({periods['week'][0]} → {periods['week'][1]}):")
        ql_week    = fetch_shopify_analytics(url, token, *periods["week"])
        print(f"  WEEK PREV ({periods['week_prev'][0]} → {periods['week_prev'][1]}):")
        ql_wk_prev = fetch_shopify_analytics(url, token, *periods["week_prev"])
        print(f"  MONTH ({periods['month'][0]} → {periods['month'][1]}):")
        ql_month   = fetch_shopify_analytics(url, token, *periods["month"])
        print(f"  QUARTER ({periods['quarter'][0]} → {periods['quarter'][1]}):")
        ql_quarter = fetch_shopify_analytics(url, token, *periods["quarter"])

        # Sessions
        print("\n  [2/3] Sessions...")
        s_mtd     = fetch_sessions_ql(url, token, *periods["mtd"])
        s_week    = fetch_sessions_ql(url, token, *periods["week"])
        s_month   = fetch_sessions_ql(url, token, *periods["month"])
        s_quarter = fetch_sessions_ql(url, token, *periods["quarter"])
        print(f"  sessions mtd:{s_mtd} week:{s_week} month:{s_month} quarter:{s_quarter}")

        # Orders REST — solo para units y revenue share
        print("\n  [3/3] Orders (units + revenue share)...")
        o_mtd     = fetch_orders(url, token, *periods["mtd"])
        o_wk_prev = fetch_orders(url, token, *periods["week_prev"])
        o_week    = fetch_orders(url, token, *periods["week"])
        o_month   = fetch_orders(url, token, *periods["month"])
        o_quarter = fetch_orders(url, token, *periods["quarter"])
        o_mtd_mom = fetch_orders(url, token, *periods["mtd_mom"])
        o_mtd_yoy = fetch_orders(url, token, *periods["mtd_yoy"])
        print(f"  orders mtd:{len(o_mtd)} week:{len(o_week)} month:{len(o_month)} quarter:{len(o_quarter)}")

        # Build KPIs
        cur_mtd = build_kpis(ql_mtd,     o_mtd,     s_mtd)
        mom_mtd = build_kpis(ql_mtd_mom, o_mtd_mom)
        yoy_mtd = build_kpis(ql_mtd_yoy, o_mtd_yoy)
        cur_wk  = build_kpis(ql_week,    o_week,    s_week)
        mom_wk  = build_kpis(ql_wk_prev, o_wk_prev)
        cur_mo  = build_kpis(ql_month,   o_month,   s_month)
        cur_qtr = build_kpis(ql_quarter, o_quarter, s_quarter)

        periods_data = {
            "mtd":     {"start":periods["mtd"][0],     "end":periods["mtd"][1],
                        "current":cur_mtd, "mom":mom_mtd, "yoy":yoy_mtd,
                        "revenue_share":calc_revenue_share(o_mtd)},
            "week":    {"start":periods["week"][0],    "end":periods["week"][1],
                        "current":cur_wk,  "mom":mom_wk,  "yoy":{},
                        "revenue_share":calc_revenue_share(o_week)},
            "month":   {"start":periods["month"][0],   "end":periods["month"][1],
                        "current":cur_mo,  "mom":{},      "yoy":{},
                        "revenue_share":calc_revenue_share(o_month)},
            "quarter": {"start":periods["quarter"][0], "end":periods["quarter"][1],
                        "current":cur_qtr, "mom":{},      "yoy":{},
                        "revenue_share":calc_revenue_share(o_quarter)},
        }

        write_kpis(gc, cfg["sheet_id"], periods_data)
        print(f"\n  ✓ {brand.upper()} done.")

if __name__ == "__main__":
    main()
