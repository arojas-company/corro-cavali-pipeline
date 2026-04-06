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

# ─────────────────────────────────────────────────────────────
# ORDERS REST — fuente de verdad financiera
# Campos completos para calcular todos los KPIs
# ─────────────────────────────────────────────────────────────
def fetch_orders(store_url, token, start_date, end_date):
    """
    Trae todas las órdenes pagadas del período con campos completos
    para calcular gross_sales, discounts, returns, net_sales, COGS.
    
    Nota sobre los campos de Shopify:
      - total_line_items_price = gross sales (precio de lista sin descuentos)
      - total_discounts        = descuentos aplicados
      - subtotal_price         = gross - discounts (antes de impuestos/envío)
      - current_subtotal_price = subtotal ajustado por devoluciones
    """
    params = {
        "status": "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min": f"{start_date}T00:00:00-05:00",
        "created_at_max": f"{end_date}T23:59:59-05:00",
        "limit": 250,
        "fields": (
            "id,total_line_items_price,total_discounts,subtotal_price,"
            "current_subtotal_price,refunds,line_items,source_name,tags"
        ),
    }
    return shopify_get(store_url, token, "orders.json", params)

def calc_financials(orders):
    """
    Calcula KPIs financieros desde órdenes REST.
    
    gross_sales   = sum(total_line_items_price)  — precio de lista sin descuentos
    discounts     = sum(total_discounts)
    returns       = suma de refunds por line items
    net_sales     = gross - discounts - returns
    cogs          = suma de cost de cada line_item × quantity
    gross_margin  = (net_sales - cogs) / net_sales * 100
    """
    gross_sales      = 0.0
    total_discounts  = 0.0
    total_returns    = 0.0
    cogs             = 0.0

    for o in orders:
        gross_sales     += float(o.get("total_line_items_price", 0) or 0)
        total_discounts += float(o.get("total_discounts", 0) or 0)

        # Devoluciones: suma de refund_line_items
        for refund in o.get("refunds", []):
            for rli in refund.get("refund_line_items", []):
                qty      = int(rli.get("quantity", 0) or 0)
                subtotal = float(rli.get("subtotal", 0) or 0)
                total_returns += subtotal

        # COGS: precio de costo × cantidad por line item
        for li in o.get("line_items", []):
            cost = float((li.get("cost") or 0))
            qty  = int(li.get("quantity", 0) or 0)
            cogs += cost * qty

    net_sales = gross_sales - total_discounts - total_returns
    pct_gm    = round((net_sales - cogs) / net_sales * 100, 2) if net_sales else 0.0

    return {
        "gross_sales":     round(gross_sales,     2),
        "total_discounts": round(total_discounts, 2),
        "total_returns":   round(total_returns,   2),
        "net_sales":       round(net_sales,        2),
        "cogs":            round(cogs,             2),
        "pct_gm":          pct_gm,
    }

def calc_units(orders):
    return sum(
        sum(int(li.get("quantity", 0) or 0) for li in o.get("line_items", []))
        for o in orders
    )

def calc_revenue_share(orders):
    channels = {"Wellington (POS)": 0, "Concierge": 0, "Online": 0, "Others": 0}
    total = 0
    for o in orders:
        # Para revenue share usamos subtotal (gross - discounts, sin envío)
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

# ─────────────────────────────────────────────────────────────
# SESSIONS — via GraphQL Analytics API (no ShopifyQL)
# ─────────────────────────────────────────────────────────────
def fetch_sessions(store_url, token, start_date, end_date):
    """
    Obtiene sesiones usando la API REST de Analytics de Shopify.
    Endpoint: /admin/api/2024-01/reports.json no da sesiones directamente,
    así que usamos el endpoint de analytics/reports con el report predefinido.
    
    Alternativa: si la tienda tiene Shopify Analytics habilitado,
    se puede consultar via el reporte 'sessions_by_device_type'.
    
    Por ahora retorna 0 con nota — las sesiones requieren Shopify Plus
    o acceso al reporte específico. Se puede configurar manualmente.
    """
    # Intentar con el endpoint de analytics
    url = f"https://{store_url}/admin/api/2024-01/analytics/reports.json"
    headers = {"X-Shopify-Access-Token": token}
    params = {
        "name": "sessions_by_landing_page",
        "date_min": str(start_date),
        "date_max": str(end_date),
    }
    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 200:
            data = r.json()
            reports = data.get("reports", [])
            if reports:
                # Sumar todas las sesiones del reporte
                total = sum(
                    sum(int(row.get("sessions", 0) or 0) for row in rep.get("result", {}).get("data", {}).get("rows", []))
                    for rep in reports
                )
                if total > 0:
                    return total
    except Exception:
        pass

    # Fallback: 0 (sin sesiones disponibles por esta ruta)
    return 0

# ─────────────────────────────────────────────────────────────
# BUILD KPIs — solo desde orders REST
# ─────────────────────────────────────────────────────────────
def build_kpis(orders, sessions=0):
    financials = calc_financials(orders)
    gross   = financials["gross_sales"]
    disc    = financials["total_discounts"]
    ret     = financials["total_returns"]
    net     = financials["net_sales"]
    cogs    = financials["cogs"]
    pct_gm  = financials["pct_gm"]
    nb_ord  = len(orders)
    units   = calc_units(orders)
    aov     = round(net / nb_ord, 2) if nb_ord else 0
    upo     = round(units / nb_ord, 2) if nb_ord else 0

    pct_disc = round(disc / gross * 100, 2) if gross else 0
    pct_ret  = round(ret  / gross * 100, 2) if gross else 0

    sessions = int(sessions or 0)
    uv       = round(sessions * 0.85) if sessions else 0
    cr       = round(nb_ord / sessions * 100, 4) if sessions else 0

    print(f"    REST → gross:{gross:,.2f} disc:{disc:,.2f} ret:{ret:,.2f} "
          f"net:{net:,.2f} cogs:{cogs:,.2f} gm:{pct_gm}% orders:{nb_ord} units:{units}")

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

        # Orders REST — fuente de verdad (ShopifyQL fue removido de la API)
        print("\n  [1/3] Fetching orders (REST)...")
        o_mtd     = fetch_orders(url, token, *periods["mtd"])
        o_mtd_mom = fetch_orders(url, token, *periods["mtd_mom"])
        o_mtd_yoy = fetch_orders(url, token, *periods["mtd_yoy"])
        o_week    = fetch_orders(url, token, *periods["week"])
        o_wk_prev = fetch_orders(url, token, *periods["week_prev"])
        o_month   = fetch_orders(url, token, *periods["month"])
        o_quarter = fetch_orders(url, token, *periods["quarter"])
        print(f"  orders mtd:{len(o_mtd)} week:{len(o_week)} month:{len(o_month)} quarter:{len(o_quarter)}")

        # Sessions
        print("\n  [2/3] Sessions...")
        s_mtd     = fetch_sessions(url, token, *periods["mtd"])
        s_week    = fetch_sessions(url, token, *periods["week"])
        s_month   = fetch_sessions(url, token, *periods["month"])
        s_quarter = fetch_sessions(url, token, *periods["quarter"])
        print(f"  sessions mtd:{s_mtd} week:{s_week} month:{s_month} quarter:{s_quarter}")

        # Build KPIs
        print("\n  [3/3] Building KPIs...")
        cur_mtd = build_kpis(o_mtd,     s_mtd)
        mom_mtd = build_kpis(o_mtd_mom)
        yoy_mtd = build_kpis(o_mtd_yoy)
        cur_wk  = build_kpis(o_week,    s_week)
        mom_wk  = build_kpis(o_wk_prev)
        cur_mo  = build_kpis(o_month,   s_month)
        cur_qtr = build_kpis(o_quarter, s_quarter)

        periods_data = {
            "mtd":     {"start": periods["mtd"][0],     "end": periods["mtd"][1],
                        "current": cur_mtd, "mom": mom_mtd, "yoy": yoy_mtd,
                        "revenue_share": calc_revenue_share(o_mtd)},
            "week":    {"start": periods["week"][0],    "end": periods["week"][1],
                        "current": cur_wk,  "mom": mom_wk,  "yoy": {},
                        "revenue_share": calc_revenue_share(o_week)},
            "month":   {"start": periods["month"][0],   "end": periods["month"][1],
                        "current": cur_mo,  "mom": {},      "yoy": {},
                        "revenue_share": calc_revenue_share(o_month)},
            "quarter": {"start": periods["quarter"][0], "end": periods["quarter"][1],
                        "current": cur_qtr, "mom": {},      "yoy": {},
                        "revenue_share": calc_revenue_share(o_quarter)},
        }

        write_kpis(gc, cfg["sheet_id"], periods_data)
        print(f"\n  ✓ {brand.upper()} done.")

if __name__ == "__main__":
    main()
