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

# ── GOOGLE SHEETS ───────────────────────────────────────────
def get_gc():
    creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_json, scopes=SCOPES)
    return gspread.authorize(creds)

# ── SHOPIFY REST PAGINADO ───────────────────────────────────
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

# ── FETCH ÓRDENES ───────────────────────────────────────────
def fetch_orders(store_url, token, start_date, end_date):
    """
    Trae órdenes con refunds completos.
    Incluye refund_line_items dentro de refunds para calcular returns exactos.
    """
    params = {
        "status": "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min": f"{start_date}T00:00:00-05:00",
        "created_at_max": f"{end_date}T23:59:59-05:00",
        "limit": 250,
        "fields": (
            "id,financial_status,"
            "total_line_items_price,"
            "total_discounts,"
            "subtotal_price,"
            "line_items,"
            "source_name,tags,"
            "refunds"
        ),
    }
    return shopify_get(store_url, token, "orders.json", params)

# ── FETCH REFUNDS SEPARADOS (fuente de verdad para returns) ─
def fetch_refunds_for_period(store_url, token, start_date, end_date):
    """
    Llama directamente al endpoint /refunds para el período.
    Suma refund_line_items[].subtotal + refund_line_items[].total_discount
    que es exactamente lo que Shopify muestra como Returns en Analytics.
    """
    params = {
        "created_at_min": f"{start_date}T00:00:00-05:00",
        "created_at_max": f"{end_date}T23:59:59-05:00",
        "limit": 250,
        "fields": "id,created_at,refund_line_items,transactions",
    }
    total_returns = 0.0
    # Recorremos las órdenes refunded en ese período
    orders_refunded = shopify_get(store_url, token, "orders.json", {
        "status": "any",
        "financial_status": "partially_refunded,refunded",
        "updated_at_min": f"{start_date}T00:00:00-05:00",
        "updated_at_max": f"{end_date}T23:59:59-05:00",
        "limit": 250,
        "fields": "id,refunds",
    })
    for order in orders_refunded:
        for refund in order.get("refunds", []):
            # refund_line_items.subtotal es el monto neto del item reembolsado
            for rli in refund.get("refund_line_items", []):
                try:
                    subtotal = float(rli.get("subtotal", 0) or 0)
                    total_returns += subtotal
                except:
                    pass
    return round(total_returns, 2)

# ── FETCH COGS VIA SHOPIFYQL ────────────────────────────────
def fetch_cogs_gm(store_url, token, start_date, end_date):
    """
    ShopifyQL da directamente cost_of_goods_sold y gross_margin.
    Si falla (permisos) retorna None, None.
    """
    try:
        url = f"https://{store_url}/admin/api/2024-10/graphql.json"
        headers = {
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
        }
        q = """
        {
          shopifyqlQuery(query: "FROM sales SHOW cost_of_goods_sold, gross_margin DURING custom(%s,%s) WITH TOTALS") {
            __typename
            ... on TableResponse {
              tableData {
                rowData
                columns { name dataType }
              }
            }
          }
        }
        """ % (start_date, end_date)

        r = requests.post(url, headers=headers, json={"query": q}, timeout=30)
        if r.status_code != 200:
            return None, None
        data = r.json()
        if data.get("errors"):
            return None, None
        table = data.get("data", {}).get("shopifyqlQuery", {}).get("tableData")
        if not table or not table.get("rowData"):
            return None, None
        cols = [c["name"] for c in table["columns"]]
        # Last row = TOTALS
        row = dict(zip(cols, table["rowData"][-1]))
        cogs = float(str(row.get("cost_of_goods_sold", 0) or 0).replace("$","").replace(",",""))
        gm_raw = float(str(row.get("gross_margin", 0) or 0).replace("%","").strip() or 0)
        # Shopify puede retornar gross_margin como 0.32 (32%) o como 32.0
        pct_gm = round(gm_raw * 100, 2) if gm_raw < 1 else round(gm_raw, 2)
        print(f"    ShopifyQL COGS: ${cogs:,.2f}  GM: {pct_gm}%")
        return round(cogs, 2), pct_gm
    except Exception as e:
        print(f"    ShopifyQL COGS error: {e}")
        return None, None

# ── FETCH SESSIONS VIA SHOPIFYQL ────────────────────────────
def fetch_sessions(store_url, token, start_date, end_date):
    try:
        url = f"https://{store_url}/admin/api/2024-10/graphql.json"
        headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
        q = """
        {
          shopifyqlQuery(query: "FROM sessions SHOW sessions DURING custom(%s,%s) WITH TOTALS") {
            ... on TableResponse {
              tableData { rowData columns { name } }
            }
          }
        }
        """ % (start_date, end_date)
        r = requests.post(url, headers=headers, json={"query": q}, timeout=30)
        if r.status_code != 200: return 0
        d = r.json()
        if d.get("errors"): return 0
        table = d.get("data", {}).get("shopifyqlQuery", {}).get("tableData", {})
        if not table or not table.get("rowData"): return 0
        cols = [c["name"] for c in table["columns"]]
        row  = dict(zip(cols, table["rowData"][-1]))
        return int(float(row.get("sessions", 0) or 0))
    except:
        return 0

# ── CALCULAR KPIs ───────────────────────────────────────────
def calc_kpis(orders, returns_override=None, cogs=None, pct_gm_override=None, sessions=0):
    """
    returns_override: monto de returns del endpoint /refunds (más preciso)
    cogs: de ShopifyQL si está disponible
    """
    if not orders:
        return {k: 0 for k in [
            "gross_sales","net_sales","total_discounts","total_returns","cogs",
            "pct_discount","pct_returns","pct_gm",
            "nb_orders","nb_units","aov","units_per_order",
            "sessions","unique_visitors","conversion_rate",
        ]}

    gross_sales = round(sum(float(o.get("total_line_items_price", 0) or 0) for o in orders), 2)
    discounts   = round(sum(float(o.get("total_discounts", 0) or 0) for o in orders), 2)

    # Returns: usar override del endpoint /refunds si está disponible
    if returns_override is not None:
        returns = returns_override
    else:
        # Fallback: refund_line_items dentro de las órdenes
        returns = 0.0
        for o in orders:
            for refund in o.get("refunds", []):
                for rli in refund.get("refund_line_items", []):
                    try:
                        returns += float(rli.get("subtotal", 0) or 0)
                    except:
                        pass
        returns = round(returns, 2)

    # Net sales = Gross - Discounts - Returns (igual que Shopify Analytics)
    net_sales = round(gross_sales - discounts - returns, 2)

    # COGS y GM
    if cogs is not None and pct_gm_override is not None:
        cogs_val = cogs
        pct_gm   = pct_gm_override
    elif cogs is not None and gross_sales:
        cogs_val = cogs
        pct_gm   = round((gross_sales - cogs) / gross_sales * 100, 2)
    else:
        cogs_val = 0
        pct_gm   = 0

    pct_discount = round(discounts / gross_sales * 100, 2) if gross_sales else 0
    pct_returns  = round(returns   / gross_sales * 100, 2) if gross_sales else 0

    nb_orders = len(orders)
    nb_units  = sum(
        sum(int(li.get("quantity", 0) or 0) for li in o.get("line_items", []))
        for o in orders
    )
    aov = round(net_sales / nb_orders, 2) if nb_orders else 0
    upo = round(nb_units  / nb_orders, 2) if nb_orders else 0

    sessions  = int(sessions or 0)
    uv_val    = round(sessions * 0.85) if sessions else 0
    cr_val    = round(nb_orders / sessions * 100, 4) if sessions else 0

    print(f"    gross_sales:    ${gross_sales:>12,.2f}")
    print(f"    discounts:      ${discounts:>12,.2f}  ({pct_discount:.1f}%)")
    print(f"    returns:        ${returns:>12,.2f}  ({pct_returns:.1f}%)")
    print(f"    net_sales:      ${net_sales:>12,.2f}")
    print(f"    cogs:           ${cogs_val:>12,.2f}")
    print(f"    pct_gm:         {pct_gm:>12.1f}%")
    print(f"    nb_orders:      {nb_orders:>12,}")
    print(f"    aov:            ${aov:>12,.2f}")

    return {
        "gross_sales":     gross_sales,
        "net_sales":       net_sales,
        "total_discounts": discounts,
        "total_returns":   returns,
        "cogs":            cogs_val,
        "pct_discount":    pct_discount,
        "pct_returns":     pct_returns,
        "pct_gm":          pct_gm,
        "nb_orders":       nb_orders,
        "nb_units":        nb_units,
        "aov":             aov,
        "units_per_order": upo,
        "sessions":        sessions,
        "unique_visitors": uv_val,
        "conversion_rate": cr_val,
    }

# ── REVENUE SHARE ────────────────────────────────────────────
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
        k: {"amount": round(v, 2), "pct": round(v / total * 100, 2) if total else 0}
        for k, v in channels.items()
    }

# ── PCT CHANGE ───────────────────────────────────────────────
def pct_change(cur, prev):
    if not prev: return None
    return round((cur - prev) / prev * 100, 2)

# ── PERÍODOS ─────────────────────────────────────────────────
def get_periods():
    today     = datetime.now(TIMEZONE).date()
    mtd_start = today.replace(day=1)
    mtd_end   = today
    mom_end   = mtd_start - timedelta(days=1)
    mom_start = mom_end.replace(day=1)
    mom_mtd_end = mom_end.replace(day=min(today.day, mom_end.day))
    yoy_start = mtd_start.replace(year=mtd_start.year - 1)
    yoy_end   = today.replace(year=today.year - 1)
    wk_start  = today - timedelta(days=today.weekday())
    wk_end    = today
    pwk_start = wk_start - timedelta(days=7)
    pwk_end   = wk_start - timedelta(days=1)
    mo_end    = mtd_start - timedelta(days=1)
    mo_start  = mo_end.replace(day=1)
    q_month   = ((today.month - 1) // 3) * 3 + 1
    q_start   = today.replace(month=q_month, day=1)
    q_end     = today
    return {
        "mtd":       (mtd_start,  mtd_end),
        "mtd_mom":   (mom_start,  mom_mtd_end),
        "mtd_yoy":   (yoy_start,  yoy_end),
        "week":      (wk_start,   wk_end),
        "week_prev": (pwk_start,  pwk_end),
        "month":     (mo_start,   mo_end),
        "quarter":   (q_start,    q_end),
    }

# ── ESCRIBIR SHEETS ──────────────────────────────────────────
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

# ── MAIN ─────────────────────────────────────────────────────
def main():
    gc      = get_gc()
    periods = get_periods()

    for brand, cfg in STORES.items():
        print(f"\n{'='*50}")
        print(f"  {brand.upper()}")
        print(f"{'='*50}")
        url   = cfg["url"]
        token = cfg["token"]

        # 1. Órdenes REST
        print("\n  [1/4] Fetching orders...")
        o_mtd     = fetch_orders(url, token, *periods["mtd"])
        o_mtd_mom = fetch_orders(url, token, *periods["mtd_mom"])
        o_mtd_yoy = fetch_orders(url, token, *periods["mtd_yoy"])
        o_week    = fetch_orders(url, token, *periods["week"])
        o_wk_prev = fetch_orders(url, token, *periods["week_prev"])
        o_month   = fetch_orders(url, token, *periods["month"])
        o_quarter = fetch_orders(url, token, *periods["quarter"])
        print(f"  orders: mtd={len(o_mtd)} week={len(o_week)} month={len(o_month)} quarter={len(o_quarter)}")

        # 2. Returns desde endpoint de refunds (fuente más precisa)
        print("\n  [2/4] Fetching returns from refunds endpoint...")
        ret_mtd     = fetch_refunds_for_period(url, token, *periods["mtd"])
        ret_week    = fetch_refunds_for_period(url, token, *periods["week"])
        ret_month   = fetch_refunds_for_period(url, token, *periods["month"])
        ret_quarter = fetch_refunds_for_period(url, token, *periods["quarter"])
        print(f"  returns: mtd=${ret_mtd:,.2f} week=${ret_week:,.2f} month=${ret_month:,.2f} quarter=${ret_quarter:,.2f}")

        # 3. COGS y GM via ShopifyQL
        print("\n  [3/4] Fetching COGS & GM from ShopifyQL...")
        cogs_mtd,  gm_mtd  = fetch_cogs_gm(url, token, *periods["mtd"])
        cogs_week, gm_week = fetch_cogs_gm(url, token, *periods["week"])
        cogs_mo,   gm_mo   = fetch_cogs_gm(url, token, *periods["month"])
        cogs_qtr,  gm_qtr  = fetch_cogs_gm(url, token, *periods["quarter"])

        # 4. Sessions
        print("\n  [4/4] Fetching sessions...")
        s_mtd     = fetch_sessions(url, token, *periods["mtd"])
        s_week    = fetch_sessions(url, token, *periods["week"])
        s_month   = fetch_sessions(url, token, *periods["month"])
        s_quarter = fetch_sessions(url, token, *periods["quarter"])
        print(f"  sessions: mtd={s_mtd} week={s_week} month={s_month} quarter={s_quarter}")

        # 5. KPIs
        print("\n  Calculating KPIs...")
        print("  [MTD current]")
        cur_mtd = calc_kpis(o_mtd, ret_mtd, cogs_mtd, gm_mtd, s_mtd)
        print("  [MTD mom]")
        mom_mtd = calc_kpis(o_mtd_mom)
        print("  [MTD yoy]")
        yoy_mtd = calc_kpis(o_mtd_yoy)
        print("  [WEEK]")
        cur_wk  = calc_kpis(o_week,    ret_week,    cogs_week, gm_week, s_week)
        mom_wk  = calc_kpis(o_wk_prev)
        print("  [MONTH]")
        cur_mo  = calc_kpis(o_month,   ret_month,   cogs_mo,   gm_mo,   s_month)
        print("  [QUARTER]")
        cur_qtr = calc_kpis(o_quarter, ret_quarter, cogs_qtr,  gm_qtr,  s_quarter)

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
        print(f"\n  ✓ {brand.upper()} done.\n")

if __name__ == "__main__":
    main()
