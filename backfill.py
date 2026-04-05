import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
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

def fetch_orders(store_url, token, start_date, end_date):
    params = {
        "status": "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min": f"{start_date}T00:00:00-05:00",
        "created_at_max": f"{end_date}T23:59:59-05:00",
        "limit": 250,
        "fields": "id,created_at,financial_status,subtotal_price,total_discounts,total_line_items_price,line_items,source_name,tags,refunds",
    }
    return shopify_get(store_url, token, "orders.json", params)

def calc_kpis(orders):
    if not orders:
        return {"gross_sales":0,"net_sales":0,"pct_discount":0,"pct_returns":0,"pct_gm":0,"nb_orders":0,"nb_units":0,"aov":0,"units_per_order":0}
    gross = sum(float(o.get("total_line_items_price",0)) for o in orders)
    discounts = sum(float(o.get("total_discounts",0)) for o in orders)
    net = sum(float(o.get("subtotal_price",0)) for o in orders)
    nb_orders = len(orders)
    nb_units = sum(sum(int(li.get("quantity",0)) for li in o.get("line_items",[])) for o in orders)
    returns = 0
    for o in orders:
        for refund in o.get("refunds",[]):
            for txn in refund.get("transactions",[]):
                if txn.get("kind") in ("refund","void"):
                    try: returns += float(txn.get("amount",0))
                    except: pass
    pct_discount = round((discounts/gross*100),2) if gross else 0
    pct_returns = round((returns/gross*100),2) if gross else 0
    aov = round(net/nb_orders,2) if nb_orders else 0
    upo = round(nb_units/nb_orders,2) if nb_orders else 0
    return {"gross_sales":round(gross,2),"net_sales":round(net,2),"pct_discount":pct_discount,"pct_returns":pct_returns,"pct_gm":0,"nb_orders":nb_orders,"nb_units":nb_units,"aov":aov,"units_per_order":upo}

def calc_revenue_share(orders):
    channels = {"Wellington (POS)":0,"Concierge":0,"Online":0,"Others":0}
    total = 0
    for o in orders:
        amount = float(o.get("subtotal_price",0))
        total += amount
        src = (o.get("source_name") or "").lower()
        tags = (o.get("tags") or "").lower()
        if src == "pos" or "wellington" in tags or "pos" in tags:
            channels["Wellington (POS)"] += amount
        elif "concierge" in tags or "concierge" in src:
            channels["Concierge"] += amount
        elif src in ("web","shopify","") or not src:
            channels["Online"] += amount
        else:
            channels["Others"] += amount
    return {k:{"amount":round(v,2),"pct":round((v/total*100),2) if total else 0} for k,v in channels.items()}

def pct_change(cur, prev):
    if not prev: return None
    return round(((cur-prev)/prev)*100,2)

def write_month(gc, sheet_id, period_label, start, end, cur, mom, yoy, rs, now_str):
    sh = gc.open_by_key(sheet_id)
    headers = ["updated_at","period","period_start","period_end","gross_sales","net_sales","pct_discount","pct_returns","pct_gm","nb_orders","nb_units","aov","units_per_order","gross_sales_mom","gross_sales_yoy","net_sales_mom","net_sales_yoy","nb_orders_mom","nb_orders_yoy","pct_discount_mom","pct_discount_yoy","aov_mom","aov_yoy"]
    try:
        ws = sh.worksheet("kpis_daily")
    except:
        ws = sh.add_worksheet("kpis_daily",rows=500,cols=30)
    existing = ws.get_all_values()
    if not existing or existing[0] != headers:
        ws.clear()
        ws.append_row(headers)
    row = [now_str,period_label,str(start),str(end),cur.get("gross_sales",0),cur.get("net_sales",0),cur.get("pct_discount",0),cur.get("pct_returns",0),cur.get("pct_gm",0),cur.get("nb_orders",0),cur.get("nb_units",0),cur.get("aov",0),cur.get("units_per_order",0),pct_change(cur.get("gross_sales",0),mom.get("gross_sales")),pct_change(cur.get("gross_sales",0),yoy.get("gross_sales")),pct_change(cur.get("net_sales",0),mom.get("net_sales")),pct_change(cur.get("net_sales",0),yoy.get("net_sales")),pct_change(cur.get("nb_orders",0),mom.get("nb_orders")),pct_change(cur.get("nb_orders",0),yoy.get("nb_orders")),pct_change(cur.get("pct_discount",0),mom.get("pct_discount")),pct_change(cur.get("pct_discount",0),yoy.get("pct_discount")),pct_change(cur.get("aov",0),mom.get("aov")),pct_change(cur.get("aov",0),yoy.get("aov"))]
    ws.append_row(row)
    try:
        ws_rs = sh.worksheet("revenue_share")
    except:
        ws_rs = sh.add_worksheet("revenue_share",rows=500,cols=10)
        ws_rs.append_row(["updated_at","period","channel","amount","pct"])
    for channel, vals in rs.items():
        ws_rs.append_row([now_str,period_label,channel,vals["amount"],vals["pct"]])

def main():
    gc = get_gc()
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

    # Meses a backfill: enero 2025 hasta marzo 2026
    months = []
    d = date(2025, 1, 1)
    while d <= date(2026, 3, 1):
        months.append(d)
        d = d + relativedelta(months=1)

    for brand, cfg in STORES.items():
        print(f"\nBackfill {brand.upper()}...")
        url = cfg["url"]
        token = cfg["token"]

        # Limpiar sheets antes de backfill
        sh = gc.open_by_key(cfg["sheet_id"])
        try:
            ws = sh.worksheet("kpis_daily")
            ws.clear()
            ws.append_row(["updated_at","period","period_start","period_end","gross_sales","net_sales","pct_discount","pct_returns","pct_gm","nb_orders","nb_units","aov","units_per_order","gross_sales_mom","gross_sales_yoy","net_sales_mom","net_sales_yoy","nb_orders_mom","nb_orders_yoy","pct_discount_mom","pct_discount_yoy","aov_mom","aov_yoy"])
        except: pass
        try:
            ws_rs = sh.worksheet("revenue_share")
            ws_rs.clear()
            ws_rs.append_row(["updated_at","period","channel","amount","pct"])
        except: pass

        prev_kpis = {}

        for month_start in months:
            month_end = month_start + relativedelta(months=1) - timedelta(days=1)
            label = month_start.strftime("%Y-%m")

            # Mes anterior
            prev_start = month_start - relativedelta(months=1)
            prev_end = prev_start + relativedelta(months=1) - timedelta(days=1)

            # Mismo mes año anterior
            yoy_start = month_start - relativedelta(years=1)
            yoy_end = yoy_start + relativedelta(months=1) - timedelta(days=1)

            print(f"  Procesando {label}...")
            orders_cur  = fetch_orders(url, token, month_start, month_end)
            orders_prev = fetch_orders(url, token, prev_start, prev_end)
            orders_yoy  = fetch_orders(url, token, yoy_start, yoy_end)

            cur  = calc_kpis(orders_cur)
            mom  = calc_kpis(orders_prev)
            yoy  = calc_kpis(orders_yoy)
            rs   = calc_revenue_share(orders_cur)

            write_month(gc, cfg["sheet_id"], label, month_start, month_end, cur, mom, yoy, rs, now_str)
            print(f"  {label} OK — {cur['nb_orders']} órdenes, ${cur['gross_sales']}")

        print(f"{brand.upper()} backfill completado.")

if __name__ == "__main__":
    main()
