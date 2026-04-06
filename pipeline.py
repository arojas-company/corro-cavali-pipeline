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

# ── GOOGLE SHEETS ───────────────────────────────────────────
def get_gc():
    creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_json, scopes=SCOPES)
    return gspread.authorize(creds)

# ── SHOPIFY REST ────────────────────────────────────────────
def shopify_get(store_url, token, endpoint, params={}):
    url = f"https://{store_url}/admin/api/2024-01/{endpoint}"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
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

# ── PERÍODOS ────────────────────────────────────────────────
def get_periods():
    now   = datetime.now(TIMEZONE)
    today = now.date()

    mtd_start = today.replace(day=1)
    mtd_end   = today

    mom_end       = mtd_start - timedelta(days=1)
    mom_start     = mom_end.replace(day=1)
    mom_mtd_end   = mom_end.replace(day=min(today.day, mom_end.day))

    yoy_start = mtd_start.replace(year=mtd_start.year - 1)
    yoy_end   = today.replace(year=today.year - 1)

    week_start      = today - timedelta(days=today.weekday())
    week_end        = today
    prev_week_start = week_start - timedelta(days=7)
    prev_week_end   = week_start - timedelta(days=1)

    full_month_end   = mtd_start - timedelta(days=1)
    full_month_start = full_month_end.replace(day=1)

    q_month       = ((today.month - 1) // 3) * 3 + 1
    quarter_start = today.replace(month=q_month, day=1)
    quarter_end   = today

    return {
        "mtd":       (mtd_start, mtd_end),
        "mtd_mom":   (mom_start, mom_mtd_end),
        "mtd_yoy":   (yoy_start, yoy_end),
        "week":      (week_start, week_end),
        "week_prev": (prev_week_start, prev_week_end),
        "month":     (full_month_start, full_month_end),
        "quarter":   (quarter_start, quarter_end),
    }

# ── FETCH ÓRDENES ───────────────────────────────────────────
def fetch_orders(store_url, token, start_date, end_date):
    params = {
        "status": "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min": f"{start_date}T00:00:00-05:00",
        "created_at_max": f"{end_date}T23:59:59-05:00",
        "limit": 250,
        "fields": "id,created_at,financial_status,total_price,subtotal_price,"
                  "total_discounts,total_line_items_price,line_items,"
                  "source_name,gateway,tags,refunds,cancel_reason",
    }
    return shopify_get(store_url, token, "orders.json", params)

# ── FETCH SESSIONS (ShopifyQL API 2024-10+) ─────────────────
def fetch_sessions(store_url, token, start_date, end_date):
    """Solo busca sessions via ShopifyQL. Si falla, retorna 0."""
    try:
        graphql_url = f"https://{store_url}/admin/api/2024-10/graphql.json"
        headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}

        query = f"""
        {{
          shopifyqlQuery(query: "FROM sessions SHOW sessions DURING custom({start_date},{end_date}) WITH TOTALS") {{
            __typename
            ... on TableResponse {{
              tableData {{
                rowData
                columns {{ name dataType }}
              }}
            }}
          }}
        }}
        """

        r = requests.post(graphql_url, headers=headers, json={"query": query}, timeout=30)
        if r.status_code != 200:
            return 0

        data = r.json()
        if data.get("errors"):
            return 0

        table = data.get("data", {}).get("shopifyqlQuery", {}).get("tableData", {})
        if not table or not table.get("rowData"):
            return 0

        cols = [c["name"] for c in table["columns"]]
        row  = dict(zip(cols, table["rowData"][-1]))
        return int(float(row.get("sessions", 0) or 0))

    except Exception:
        return 0

# ── CALCULAR FINANCIEROS DESDE ÓRDENES ──────────────────────
def calc_financials_from_orders(orders):
    """
    Calcula gross_sales, discounts, returns directamente desde las órdenes REST.
    - gross_sales = suma de total_line_items_price (precio antes de descuentos)
    - discounts   = suma de total_discounts por orden
    - returns     = suma de transacciones de tipo refund/void
    """
    gross_sales = sum(float(o.get("total_line_items_price", 0)) for o in orders)
    discounts   = sum(float(o.get("total_discounts", 0)) for o in orders)
    returns     = 0
    for o in orders:
        for refund in o.get("refunds", []):
            for txn in refund.get("transactions", []):
                if txn.get("kind") in ("refund", "void"):
                    try: returns += float(txn.get("amount", 0))
                    except: pass
    return {
        "gross_sales": round(gross_sales, 2),
        "discounts":   round(discounts, 2),
        "returns":     round(returns, 2),
    }

# ── CALCULAR KPIs ───────────────────────────────────────────
def calc_kpis(orders, sessions=0):
    """
    Todos los financieros se calculan desde las órdenes REST (fuente confiable).
    sessions viene de ShopifyQL si está disponible.

    Fórmulas:
      net_sales    = gross_sales - discounts - returns
      pct_discount = discounts / gross_sales * 100
      pct_returns  = returns   / gross_sales * 100
      pct_gm       = (net_sales - cogs) / net_sales * 100  → cogs=0 si no hay datos
      aov          = net_sales / nb_orders
      conversion   = nb_orders / sessions * 100
    """
    if not orders:
        return {
            "gross_sales": 0, "net_sales": 0, "total_discounts": 0,
            "total_returns": 0, "cogs": 0, "nb_orders": 0, "nb_units": 0,
            "pct_discount": 0, "pct_returns": 0, "pct_gm": 0,
            "aov": 0, "units_per_order": 0,
            "sessions": 0, "unique_visitors": 0, "conversion_rate": 0,
        }

    fin = calc_financials_from_orders(orders)
    gross_sales = fin["gross_sales"]
    discounts   = fin["discounts"]
    returns     = fin["returns"]
    cogs        = 0  # COGS no disponible vía REST; columna queda en 0

    # ── Fórmulas financieras ──
    net_sales    = round(gross_sales - discounts - returns, 2)
    pct_discount = round(discounts / gross_sales * 100, 2) if gross_sales else 0
    pct_returns  = round(returns   / gross_sales * 100, 2) if gross_sales else 0
    pct_gm       = round((net_sales - cogs) / net_sales * 100, 2) if (net_sales and cogs) else 0

    # ── Operacionales ──
    nb_orders = len(orders)
    nb_units  = sum(
        sum(int(li.get("quantity", 0)) for li in o.get("line_items", []))
        for o in orders
    )
    aov = round(net_sales / nb_orders, 2) if nb_orders else 0
    upo = round(nb_units  / nb_orders, 2) if nb_orders else 0

    sessions     = int(sessions)
    uv_val       = round(sessions * 0.85) if sessions else 0
    cr_val       = round(nb_orders / sessions * 100, 2) if sessions else 0

    print(f"    ✓ Gross Sales: ${gross_sales:,.2f}")
    print(f"    ✓ Discounts:   ${discounts:,.2f}")
    print(f"    ✓ Returns:     ${returns:,.2f}")
    print(f"    ✓ Net Sales:   ${net_sales:,.2f}")
    print(f"    ✓ Orders:      {nb_orders}")
    print(f"    ✓ Sessions:    {sessions}")

    return {
        "gross_sales":     gross_sales,
        "net_sales":       net_sales,
        "total_discounts": discounts,
        "total_returns":   returns,
        "cogs":            cogs,
        "nb_orders":       nb_orders,
        "nb_units":        nb_units,
        "pct_discount":    pct_discount,
        "pct_returns":     pct_returns,
        "pct_gm":          pct_gm,
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
        amount = float(o.get("subtotal_price", 0))
        total += amount
        src  = (o.get("source_name") or "").lower()
        tags = (o.get("tags")        or "").lower()
        if src == "pos" or "wellington" in tags or "pos" in tags:
            channels["Wellington (POS)"] += amount
        elif "concierge" in tags or "concierge" in src:
            channels["Concierge"] += amount
        elif src in ("web", "shopify", "") or src is None:
            channels["Online"] += amount
        else:
            channels["Others"] += amount
    result = {}
    for k, v in channels.items():
        result[k] = {
            "amount": round(v, 2),
            "pct":    round(v / total * 100, 2) if total else 0,
        }
    return result

# ── VARIACIÓN % ──────────────────────────────────────────────
def pct_change(current, previous):
    if not previous:
        return None
    return round((current - previous) / previous * 100, 2)

# ── ESCRIBIR EN SHEETS ───────────────────────────────────────
def write_kpis(gc, sheet_id, periods_data):
    sh      = gc.open_by_key(sheet_id)
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

    # ── Pestaña kpis_daily ──────────────────────────────────
    # Columnas: incluye 'cogs' como columna propia para poder calcular gross margin
    headers = [
        "updated_at", "period", "period_start", "period_end",
        "gross_sales", "net_sales", "total_discounts", "total_returns", "cogs",
        "pct_discount", "pct_returns", "pct_gm",
        "nb_orders", "nb_units", "aov", "units_per_order",
        "sessions", "unique_visitors", "conversion_rate",
        "gross_sales_mom", "gross_sales_yoy",
        "net_sales_mom",   "net_sales_yoy",
        "nb_orders_mom",   "nb_orders_yoy",
        "aov_mom",         "aov_yoy",
    ]

    try:
        ws = sh.worksheet("kpis_daily")
    except:
        ws = sh.add_worksheet("kpis_daily", rows=500, cols=30)

    # Limpiar y reescribir headers siempre
    ws.clear()
    ws.append_row(headers)

    for period_name, data in periods_data.items():
        cur = data["current"]
        mom = data.get("mom", {})
        yoy = data.get("yoy", {})

        row = [
            now_str,
            period_name,
            str(data["start"]),
            str(data["end"]),
            cur.get("gross_sales",     0),
            cur.get("net_sales",       0),
            cur.get("total_discounts", 0),
            cur.get("total_returns",   0),
            cur.get("cogs",            0),   # ← columna COGS propia
            cur.get("pct_discount",    0),
            cur.get("pct_returns",     0),
            cur.get("pct_gm",          0),
            cur.get("nb_orders",       0),
            cur.get("nb_units",        0),
            cur.get("aov",             0),
            cur.get("units_per_order", 0),
            cur.get("sessions",        0),
            cur.get("unique_visitors", 0),
            cur.get("conversion_rate", 0),
            pct_change(cur.get("gross_sales", 0), mom.get("gross_sales")),
            pct_change(cur.get("gross_sales", 0), yoy.get("gross_sales")),
            pct_change(cur.get("net_sales",   0), mom.get("net_sales")),
            pct_change(cur.get("net_sales",   0), yoy.get("net_sales")),
            pct_change(cur.get("nb_orders",   0), mom.get("nb_orders")),
            pct_change(cur.get("nb_orders",   0), yoy.get("nb_orders")),
            pct_change(cur.get("aov",         0), mom.get("aov")),
            pct_change(cur.get("aov",         0), yoy.get("aov")),
        ]
        ws.append_row(row)

    # ── Pestaña revenue_share ───────────────────────────────
    try:
        ws_rs = sh.worksheet("revenue_share")
    except:
        ws_rs = sh.add_worksheet("revenue_share", rows=500, cols=15)

    ws_rs.clear()
    ws_rs.append_row(["updated_at", "period", "channel", "amount", "pct"])

    for period_name, data in periods_data.items():
        for channel, vals in data.get("revenue_share", {}).items():
            ws_rs.append_row([now_str, period_name, channel, vals["amount"], vals["pct"]])

    print(f"  ✓ Sheets escritas a las {now_str}")

# ── MAIN ─────────────────────────────────────────────────────
def main():
    gc      = get_gc()
    periods = get_periods()

    for brand, cfg in STORES.items():
        print(f"\n{'='*40}")
        print(f"  Procesando {brand.upper()}")
        print(f"{'='*40}")
        url   = cfg["url"]
        token = cfg["token"]

        # Órdenes REST por período
        orders_mtd     = fetch_orders(url, token, *periods["mtd"])
        orders_mtd_mom = fetch_orders(url, token, *periods["mtd_mom"])
        orders_mtd_yoy = fetch_orders(url, token, *periods["mtd_yoy"])
        orders_week    = fetch_orders(url, token, *periods["week"])
        orders_wk_prev = fetch_orders(url, token, *periods["week_prev"])
        orders_month   = fetch_orders(url, token, *periods["month"])
        orders_quarter = fetch_orders(url, token, *periods["quarter"])

        # Sessions via ShopifyQL (si falla retorna 0, no bloquea)
        print("\n  → Fetching sessions from ShopifyQL...")
        sessions_mtd     = fetch_sessions(url, token, *periods["mtd"])
        sessions_week    = fetch_sessions(url, token, *periods["week"])
        sessions_month   = fetch_sessions(url, token, *periods["month"])
        sessions_quarter = fetch_sessions(url, token, *periods["quarter"])
        print(f"    sessions MTD={sessions_mtd}, week={sessions_week}, month={sessions_month}, quarter={sessions_quarter}")

        print("\n  → Calculando KPIs...")
        periods_data = {
            "mtd": {
                "start":         periods["mtd"][0],
                "end":           periods["mtd"][1],
                "current":       calc_kpis(orders_mtd,     sessions_mtd),
                "mom":           calc_kpis(orders_mtd_mom),
                "yoy":           calc_kpis(orders_mtd_yoy),
                "revenue_share": calc_revenue_share(orders_mtd),
            },
            "week": {
                "start":         periods["week"][0],
                "end":           periods["week"][1],
                "current":       calc_kpis(orders_week,    sessions_week),
                "mom":           calc_kpis(orders_wk_prev),
                "yoy":           {},
                "revenue_share": calc_revenue_share(orders_week),
            },
            "month": {
                "start":         periods["month"][0],
                "end":           periods["month"][1],
                "current":       calc_kpis(orders_month,   sessions_month),
                "mom":           {},
                "yoy":           {},
                "revenue_share": calc_revenue_share(orders_month),
            },
            "quarter": {
                "start":         periods["quarter"][0],
                "end":           periods["quarter"][1],
                "current":       calc_kpis(orders_quarter, sessions_quarter),
                "mom":           {},
                "yoy":           {},
                "revenue_share": calc_revenue_share(orders_quarter),
            },
        }

        write_kpis(gc, cfg["sheet_id"], periods_data)
        print(f"  ✓ {brand.upper()} completado.")

if __name__ == "__main__":
    main()
