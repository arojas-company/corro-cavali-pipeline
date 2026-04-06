import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, date
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
    "gross_sales","net_sales","total_discounts","total_returns",
    "pct_discount","pct_returns","pct_gm",
    "nb_orders","nb_units","aov","units_per_order",
    "sessions","unique_visitors","conversion_rate",
    "gross_sales_mom","gross_sales_yoy",
    "net_sales_mom","net_sales_yoy",
    "nb_orders_mom","nb_orders_yoy",
    "pct_discount_mom","pct_discount_yoy",
    "aov_mom","aov_yoy",
]

# ── GOOGLE SHEETS ──────────────────────────────────────────
def get_gc():
    creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_json, scopes=SCOPES)
    return gspread.authorize(creds)

# ── SHOPIFY REST ───────────────────────────────────────────
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

# ── SHOPIFY GRAPHQL (ShopifyQL para COGS y Sessions) ───────
def shopify_ql(store_url, token, start_date, end_date):
    """
    Usa ShopifyQL para obtener directamente:
    - gross_sales, discounts, returns, net_sales, cost_of_goods_sold
    - sessions, orders, gross_margin_percentage
    Esto replica exactamente lo que muestra Shopify Analytics.
    """
    graphql_url = f"https://{store_url}/admin/api/2024-01/graphql.json"
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
    }
    query = f"""
    {{
      shopifyqlQuery(query: "FROM sales SHOW gross_sales, discounts, returns, net_sales, cost_of_goods_sold, gross_margin, orders, sessions DURING custom(\\"{start_date}\\",\\"{end_date}\\") WITH TOTALS") {{
        __typename
        ... on TableResponse {{
          tableData {{
            rowData
            columns {{ name dataType }}
          }}
        }}
        ... on ParseErrorResponse {{
          parseErrors {{ message code }}
        }}
      }}
    }}
    """
    result = {
        "gross_sales": 0, "discounts": 0, "returns": 0,
        "net_sales": 0, "cogs": 0, "pct_gm": 0,
        "orders": 0, "sessions": 0,
    }
    try:
        r = requests.post(graphql_url, headers=headers, json={"query": query}, timeout=30)
        if r.status_code != 200:
            print(f"  ShopifyQL HTTP {r.status_code}")
            return result
        data = r.json()
        if "errors" in data:
            print(f"  ShopifyQL errors: {data['errors']}")
            return result
        table_data = data.get("data", {}).get("shopifyqlQuery", {}).get("tableData")
        if not table_data:
            print(f"  ShopifyQL: no tableData returned")
            return result
        cols = [c["name"] for c in table_data.get("columns", [])]
        rows = table_data.get("rowData", [])
        if not rows:
            return result
        # Last row is TOTALS
        totals = dict(zip(cols, rows[-1]))
        def g(k): 
            v = totals.get(k, 0)
            try: return float(str(v).replace("$","").replace(",","").strip() or 0)
            except: return 0
        result["gross_sales"] = g("gross_sales")
        result["discounts"]   = abs(g("discounts"))
        result["returns"]     = abs(g("returns"))
        result["net_sales"]   = g("net_sales")
        result["cogs"]        = g("cost_of_goods_sold")
        result["pct_gm"]      = round(g("gross_margin") * 100, 1) if g("gross_margin") < 1 else round(g("gross_margin"), 1)
        result["orders"]      = int(g("orders"))
        result["sessions"]    = int(g("sessions"))
        print(f"  ShopifyQL OK — gross:{result['gross_sales']} net:{result['net_sales']} gm:{result['pct_gm']}% sessions:{result['sessions']}")
    except Exception as e:
        print(f"  ShopifyQL exception: {e}")
    return result

# ── ÓRDENES (para units, revenue share) ───────────────────
def fetch_orders(store_url, token, start_date, end_date):
    params = {
        "status": "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min": f"{start_date}T00:00:00-05:00",
        "created_at_max": f"{end_date}T23:59:59-05:00",
        "limit": 250,
        "fields": "id,subtotal_price,total_discounts,total_line_items_price,line_items,source_name,tags,refunds",
    }
    return shopify_get(store_url, token, "orders.json", params)

# ── REVENUE SHARE ──────────────────────────────────────────
def calc_revenue_share(orders):
    channels = {"Wellington (POS)": 0, "Concierge": 0, "Online": 0, "Others": 0}
    total = 0
    for o in orders:
        amount = float(o.get("subtotal_price", 0))
        total += amount
        src  = (o.get("source_name") or "").lower()
        tags = (o.get("tags") or "").lower()
        if src == "pos" or "wellington" in tags or "pos" in tags:
            channels["Wellington (POS)"] += amount
        elif "concierge" in tags or "concierge" in src:
            channels["Concierge"] += amount
        elif src in ("web", "shopify", "", "online_store") or not src:
            channels["Online"] += amount
        else:
            channels["Others"] += amount
    return {k: {"amount": round(v, 2), "pct": round(v/total*100, 2) if total else 0} for k, v in channels.items()}

# ── UNITS (desde orders REST) ──────────────────────────────
def calc_units(orders):
    nb_units = sum(
        sum(int(li.get("quantity", 0)) for li in o.get("line_items", []))
        for o in orders
    )
    return nb_units

# ── PERÍODOS ───────────────────────────────────────────────
def get_periods():
    today = datetime.now(TIMEZONE).date()
    mtd_start   = today.replace(day=1)
    mtd_end     = today
    mom_end     = mtd_start - timedelta(days=1)
    mom_start   = mom_end.replace(day=1)
    mom_mtd_end = mom_end.replace(day=min(today.day, mom_end.day))
    yoy_start   = mtd_start.replace(year=mtd_start.year - 1)
    yoy_end     = today.replace(year=today.year - 1)
    week_start  = today - timedelta(days=today.weekday())
    week_end    = today
    pw_start    = week_start - timedelta(days=7)
    pw_end      = week_start - timedelta(days=1)
    month_end   = mtd_start - timedelta(days=1)
    month_start = month_end.replace(day=1)
    q_month     = ((today.month - 1) // 3) * 3 + 1
    q_start     = today.replace(month=q_month, day=1)
    q_end       = today
    return {
        "mtd":      (mtd_start, mtd_end),
        "mtd_mom":  (mom_start, mom_mtd_end),
        "mtd_yoy":  (yoy_start, yoy_end),
        "week":     (week_start, week_end),
        "week_prev":(pw_start, pw_end),
        "month":    (month_start, month_end),
        "quarter":  (q_start, q_end),
    }

# ── PCT CHANGE ─────────────────────────────────────────────
def pct_chg(cur, prev):
    if not prev: return None
    return round(((cur - prev) / prev) * 100, 2)

# ── WRITE TO SHEETS ────────────────────────────────────────
def write_to_sheets(gc, sheet_id, periods_data, now_str):
    sh = gc.open_by_key(sheet_id)

    # kpis_daily
    try:    ws = sh.worksheet("kpis_daily")
    except: ws = sh.add_worksheet("kpis_daily", rows=500, cols=35)
    existing = ws.get_all_values()
    if not existing or existing[0] != HEADERS_KPIS:
        ws.clear()
        ws.append_row(HEADERS_KPIS)

    for pname, d in periods_data.items():
        cur = d["current"]
        mom = d.get("mom", {})
        yoy = d.get("yoy", {})
        nb  = cur.get("nb_orders", 0)
        s   = cur.get("sessions", 0)
        cr  = round((nb / s * 100), 2) if s else 0
        row = [
            now_str, pname, str(d["start"]), str(d["end"]),
            cur.get("gross_sales",0), cur.get("net_sales",0),
            cur.get("discounts",0),   cur.get("returns",0),
            cur.get("pct_discount",0),cur.get("pct_returns",0),
            cur.get("pct_gm",0),
            nb, cur.get("nb_units",0),
            cur.get("aov",0), cur.get("units_per_order",0),
            s, round(s*0.85) if s else 0, cr,
            pct_chg(cur.get("gross_sales",0), mom.get("gross_sales")),
            pct_chg(cur.get("gross_sales",0), yoy.get("gross_sales")),
            pct_chg(cur.get("net_sales",0),   mom.get("net_sales")),
            pct_chg(cur.get("net_sales",0),   yoy.get("net_sales")),
            pct_chg(nb,                        mom.get("nb_orders")),
            pct_chg(nb,                        yoy.get("nb_orders")),
            pct_chg(cur.get("pct_discount",0), mom.get("pct_discount")),
            pct_chg(cur.get("pct_discount",0), yoy.get("pct_discount")),
            pct_chg(cur.get("aov",0),          mom.get("aov")),
            pct_chg(cur.get("aov",0),          yoy.get("aov")),
        ]
        ws.append_row(row)

    # revenue_share
    try:    ws_rs = sh.worksheet("revenue_share")
    except: ws_rs = sh.add_worksheet("revenue_share", rows=500, cols=10)
    rs_hdrs = ["updated_at","period","channel","amount","pct"]
    ex_rs = ws_rs.get_all_values()
    if not ex_rs or ex_rs[0] != rs_hdrs:
        ws_rs.clear()
        ws_rs.append_row(rs_hdrs)
    for pname, d in periods_data.items():
        for ch, v in d.get("revenue_share", {}).items():
            ws_rs.append_row([now_str, pname, ch, v["amount"], v["pct"]])

    print(f"  Sheets OK at {now_str}")

# ── MAIN ───────────────────────────────────────────────────
def main():
    gc      = get_gc()
    periods = get_periods()
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

    for brand, cfg in STORES.items():
        print(f"\n{'='*40}\nProcessing {brand.upper()}...")
        url   = cfg["url"]
        token = cfg["token"]

        # ShopifyQL — fuente de verdad para financials
        print("  Fetching ShopifyQL data...")
        ql_mtd     = shopify_ql(url, token, *periods["mtd"])
        ql_mtd_mom = shopify_ql(url, token, *periods["mtd_mom"])
        ql_mtd_yoy = shopify_ql(url, token, *periods["mtd_yoy"])
        ql_week    = shopify_ql(url, token, *periods["week"])
        ql_wk_prev = shopify_ql(url, token, *periods["week_prev"])
        ql_month   = shopify_ql(url, token, *periods["month"])
        ql_quarter = shopify_ql(url, token, *periods["quarter"])

        # Orders REST — solo para units y revenue share
        print("  Fetching orders for units & revenue share...")
        ord_mtd     = fetch_orders(url, token, *periods["mtd"])
        ord_wk      = fetch_orders(url, token, *periods["week"])
        ord_month   = fetch_orders(url, token, *periods["month"])
        ord_quarter = fetch_orders(url, token, *periods["quarter"])

        def build(ql, ql_mom, ql_yoy, orders):
            nb  = ql.get("orders", 0)
            gs  = ql.get("gross_sales", 0)
            ns  = ql.get("net_sales", 0)
            dis = ql.get("discounts", 0)
            ret = ql.get("returns", 0)
            units = calc_units(orders)
            return {
                "gross_sales":    round(gs, 2),
                "net_sales":      round(ns, 2),
                "discounts":      round(dis, 2),
                "returns":        round(ret, 2),
                "pct_discount":   round(dis/gs*100, 2) if gs else 0,
                "pct_returns":    round(ret/gs*100, 2) if gs else 0,
                "pct_gm":         ql.get("pct_gm", 0),
                "nb_orders":      nb,
                "nb_units":       units,
                "aov":            round(ns/nb, 2) if nb else 0,
                "units_per_order":round(units/nb, 2) if nb else 0,
                "sessions":       ql.get("sessions", 0),
            }, {
                "gross_sales": ql_mom.get("gross_sales", 0),
                "net_sales":   ql_mom.get("net_sales", 0),
                "nb_orders":   ql_mom.get("orders", 0),
                "pct_discount":round(ql_mom.get("discounts",0)/ql_mom.get("gross_sales",1)*100, 2) if ql_mom.get("gross_sales") else 0,
                "aov":         round(ql_mom.get("net_sales",0)/ql_mom.get("orders",1), 2) if ql_mom.get("orders") else 0,
            }, {
                "gross_sales": ql_yoy.get("gross_sales", 0),
                "net_sales":   ql_yoy.get("net_sales", 0),
                "nb_orders":   ql_yoy.get("orders", 0),
                "pct_discount":round(ql_yoy.get("discounts",0)/ql_yoy.get("gross_sales",1)*100, 2) if ql_yoy.get("gross_sales") else 0,
                "aov":         round(ql_yoy.get("net_sales",0)/ql_yoy.get("orders",1), 2) if ql_yoy.get("orders") else 0,
            }

        cur_mtd,  mom_mtd,  yoy_mtd  = build(ql_mtd,  ql_mtd_mom, ql_mtd_yoy, ord_mtd)
        cur_wk,   mom_wk,   _        = build(ql_week,  ql_wk_prev, {},          ord_wk)
        cur_month,_,        _        = build(ql_month, {},          {},          ord_month)
        cur_qtr,  _,        _        = build(ql_quarter,{},         {},          ord_quarter)

        periods_data = {
            "mtd":     {"start":periods["mtd"][0],     "end":periods["mtd"][1],     "current":cur_mtd,   "mom":mom_mtd,  "yoy":yoy_mtd,  "revenue_share":calc_revenue_share(ord_mtd)},
            "week":    {"start":periods["week"][0],    "end":periods["week"][1],    "current":cur_wk,    "mom":mom_wk,   "yoy":{},       "revenue_share":calc_revenue_share(ord_wk)},
            "month":   {"start":periods["month"][0],   "end":periods["month"][1],   "current":cur_month, "mom":{},       "yoy":{},       "revenue_share":calc_revenue_share(ord_month)},
            "quarter": {"start":periods["quarter"][0], "end":periods["quarter"][1], "current":cur_qtr,   "mom":{},       "yoy":{},       "revenue_share":calc_revenue_share(ord_quarter)},
        }

        write_to_sheets(gc, cfg["sheet_id"], periods_data, now_str)
        print(f"{brand.upper()} done.")

if __name__ == "__main__":
    main()
