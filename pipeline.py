import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, date
import pytz

# ── CONFIGURACIÓN ──────────────────────────────────────────
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

# ── GOOGLE SHEETS ──────────────────────────────────────────
def get_gc():
    creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_json, scopes=SCOPES)
    return gspread.authorize(creds)

# ── SHOPIFY API ────────────────────────────────────────────
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

# ── PERÍODOS ───────────────────────────────────────────────
def get_periods():
    now = datetime.now(TIMEZONE)
    today = now.date()

    # MTD
    mtd_start = today.replace(day=1)
    mtd_end = today

    # Mismo período mes anterior (MOM)
    mom_end = (mtd_start - timedelta(days=1))
    mom_start = mom_end.replace(day=1)
    mom_mtd_end = mom_end.replace(day=min(today.day, mom_end.day))

    # Mismo período año anterior (YOY)
    yoy_start = mtd_start.replace(year=mtd_start.year - 1)
    yoy_end = today.replace(year=today.year - 1)

    # Week actual (lunes a hoy)
    week_start = today - timedelta(days=today.weekday())
    week_end = today

    # Week anterior
    prev_week_start = week_start - timedelta(days=7)
    prev_week_end = week_start - timedelta(days=1)

    # Month completo anterior
    full_month_end = mtd_start - timedelta(days=1)
    full_month_start = full_month_end.replace(day=1)

    # Quarter actual
    q_month = ((today.month - 1) // 3) * 3 + 1
    quarter_start = today.replace(month=q_month, day=1)
    quarter_end = today

    return {
        "mtd":          (mtd_start, mtd_end),
        "mtd_mom":      (mom_start, mom_mtd_end),
        "mtd_yoy":      (yoy_start, yoy_end),
        "week":         (week_start, week_end),
        "week_prev":    (prev_week_start, prev_week_end),
        "month":        (full_month_start, full_month_end),
        "quarter":      (quarter_start, quarter_end),
    }

# ── EXTRAER ÓRDENES ────────────────────────────────────────
def fetch_orders(store_url, token, start_date, end_date):
    params = {
        "status": "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min": f"{start_date}T00:00:00-05:00",
        "created_at_max": f"{end_date}T23:59:59-05:00",
        "limit": 250,
        "fields": "id,created_at,financial_status,total_price,subtotal_price,total_discounts,total_line_items_price,line_items,source_name,gateway,tags,refunds,cancel_reason",
    }
    return shopify_get(store_url, token, "orders.json", params)

# ── CALCULAR KPIs ──────────────────────────────────────────
def calc_kpis(orders):
    if not orders:
        return {
            "gross_sales": 0, "net_sales": 0, "total_discounts": 0,
            "total_returns": 0, "nb_orders": 0, "nb_units": 0,
            "pct_discount": 0, "pct_returns": 0,
        }

    gross = sum(float(o.get("total_line_items_price", 0)) for o in orders)
    discounts = sum(float(o.get("total_discounts", 0)) for o in orders)
    net = sum(float(o.get("subtotal_price", 0)) for o in orders)
    nb_orders = len(orders)
    nb_units = sum(
        sum(int(li.get("quantity", 0)) for li in o.get("line_items", []))
        for o in orders
    )
    returns = 0
    for o in orders:
        for refund in o.get("refunds", []):
            for txn in refund.get("transactions", []):
                if txn.get("kind") in ("refund", "void"):
                    try:
                        returns += float(txn.get("amount", 0))
                    except:
                        pass

    pct_discount = round((discounts / gross * 100), 2) if gross else 0
    pct_returns = round((returns / gross * 100), 2) if gross else 0
    aov = round(net / nb_orders, 2) if nb_orders else 0
    upo = round(nb_units / nb_orders, 2) if nb_orders else 0

    # Gross margin: sum of (price - cost) * qty per line item
    # El campo "cost" viene en line_items cuando se pide con access scope read_products
    total_cost = 0
    for o in orders:
        for li in o.get("line_items", []):
            try:
                qty = int(li.get("quantity", 0))
                # Shopify devuelve cost en line_items si tienes el scope correcto
                cost_per_unit = float(li.get("cost", 0))
                total_cost += cost_per_unit * qty
            except:
                pass
    pct_gm = round(((net - total_cost) / net * 100), 2) if net and total_cost else 0

    return {
        "gross_sales": round(gross, 2),
        "net_sales": round(net, 2),
        "total_discounts": round(discounts, 2),
        "total_returns": round(returns, 2),
        "nb_orders": nb_orders,
        "nb_units": nb_units,
        "pct_discount": pct_discount,
        "pct_returns": pct_returns,
        "aov": aov,
        "units_per_order": upo,
        "pct_gm": pct_gm,
    }

# ── REVENUE SHARE POR CANAL ────────────────────────────────
def calc_revenue_share(orders):
    channels = {"Wellington (POS)": 0, "Concierge": 0, "Online": 0, "Others": 0}
    total = 0
    for o in orders:
        amount = float(o.get("subtotal_price", 0))
        total += amount
        src = (o.get("source_name") or "").lower()
        tags = (o.get("tags") or "").lower()
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
            "pct": round((v / total * 100), 2) if total else 0,
        }
    return result

# ── VARIACIÓN % ────────────────────────────────────────────
def pct_change(current, previous):
    if not previous:
        return None
    return round(((current - previous) / previous) * 100, 2)

# ── ESCRIBIR EN SHEETS ─────────────────────────────────────
def write_kpis(gc, sheet_id, periods_data):
    sh = gc.open_by_key(sheet_id)
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

    # ── Pestaña: kpis_daily ──
    try:
        ws = sh.worksheet("kpis_daily")
    except:
        ws = sh.add_worksheet("kpis_daily", rows=500, cols=30)

    headers = [
        "updated_at", "period", "period_start", "period_end",
        "gross_sales", "net_sales", "pct_discount", "pct_returns", "pct_gm",
        "nb_orders", "nb_units", "aov", "units_per_order",
        "gross_sales_mom", "gross_sales_yoy",
        "net_sales_mom", "net_sales_yoy",
        "nb_orders_mom", "nb_orders_yoy",
        "pct_discount_mom", "pct_discount_yoy",
        "aov_mom", "aov_yoy",
    ]

    existing = ws.get_all_values()
    if not existing or existing[0] != headers:
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
            cur.get("gross_sales", 0),
            cur.get("net_sales", 0),
            cur.get("pct_discount", 0),
            cur.get("pct_returns", 0),
            cur.get("pct_gm", 0),
            cur.get("nb_orders", 0),
            cur.get("nb_units", 0),
            cur.get("aov", 0),
            cur.get("units_per_order", 0),
            pct_change(cur.get("gross_sales", 0), mom.get("gross_sales")),
            pct_change(cur.get("gross_sales", 0), yoy.get("gross_sales")),
            pct_change(cur.get("net_sales", 0), mom.get("net_sales")),
            pct_change(cur.get("net_sales", 0), yoy.get("net_sales")),
            pct_change(cur.get("nb_orders", 0), mom.get("nb_orders")),
            pct_change(cur.get("nb_orders", 0), yoy.get("nb_orders")),
            pct_change(cur.get("pct_discount", 0), mom.get("pct_discount")),
            pct_change(cur.get("pct_discount", 0), yoy.get("pct_discount")),
            pct_change(cur.get("aov", 0), mom.get("aov")),
            pct_change(cur.get("aov", 0), yoy.get("aov")),
        ]
        ws.append_row(row)

    # ── Pestaña: revenue_share ──
    try:
        ws_rs = sh.worksheet("revenue_share")
    except:
        ws_rs = sh.add_worksheet("revenue_share", rows=500, cols=15)

    rs_headers = ["updated_at", "period", "channel", "amount", "pct"]
    existing_rs = ws_rs.get_all_values()
    if not existing_rs or existing_rs[0] != rs_headers:
        ws_rs.clear()
        ws_rs.append_row(rs_headers)

    for period_name, data in periods_data.items():
        rs = data.get("revenue_share", {})
        for channel, vals in rs.items():
            ws_rs.append_row([
                now_str, period_name, channel,
                vals["amount"], vals["pct"]
            ])

    print(f"Sheets actualizadas correctamente a las {now_str}")

# ── MAIN ───────────────────────────────────────────────────
def main():
    gc = get_gc()
    periods = get_periods()

    for brand, cfg in STORES.items():
        print(f"\nProcesando {brand.upper()}...")
        url = cfg["url"]
        token = cfg["token"]

        # Fetch órdenes por período
        orders_mtd     = fetch_orders(url, token, *periods["mtd"])
        orders_mtd_mom = fetch_orders(url, token, *periods["mtd_mom"])
        orders_mtd_yoy = fetch_orders(url, token, *periods["mtd_yoy"])
        orders_week    = fetch_orders(url, token, *periods["week"])
        orders_wk_prev = fetch_orders(url, token, *periods["week_prev"])
        orders_month   = fetch_orders(url, token, *periods["month"])
        orders_quarter = fetch_orders(url, token, *periods["quarter"])

        periods_data = {
            "mtd": {
                "start": periods["mtd"][0], "end": periods["mtd"][1],
                "current": calc_kpis(orders_mtd),
                "mom": calc_kpis(orders_mtd_mom),
                "yoy": calc_kpis(orders_mtd_yoy),
                "revenue_share": calc_revenue_share(orders_mtd),
            },
            "week": {
                "start": periods["week"][0], "end": periods["week"][1],
                "current": calc_kpis(orders_week),
                "mom": calc_kpis(orders_wk_prev),
                "yoy": {},
                "revenue_share": calc_revenue_share(orders_week),
            },
            "month": {
                "start": periods["month"][0], "end": periods["month"][1],
                "current": calc_kpis(orders_month),
                "mom": {},
                "yoy": {},
                "revenue_share": calc_revenue_share(orders_month),
            },
            "quarter": {
                "start": periods["quarter"][0], "end": periods["quarter"][1],
                "current": calc_kpis(orders_quarter),
                "mom": {},
                "yoy": {},
                "revenue_share": calc_revenue_share(orders_quarter),
            },
        }

        write_kpis(gc, cfg["sheet_id"], periods_data)
        print(f"{brand.upper()} completado.")

if __name__ == "__main__":
    main()
