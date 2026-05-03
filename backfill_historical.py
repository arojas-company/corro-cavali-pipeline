"""
BACKFILL HISTÓRICO — Shopify → Google Sheets
Jala datos desde 2024-01-01 hasta hoy para todos los meses y quarters.
Esto llena el Sheet con datos históricos para que el dashboard
pueda mostrar 2024 y 2025 correctamente.

FIX: gross_profit now fetched from ShopifyQL (gross_profit field)
     and stored as a dedicated column in kpis_daily sheet.
"""
import os, json, requests, gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, date
import pytz

TIMEZONE    = pytz.timezone("America/Bogota")
GQL_VERSION = "2025-10"

STORES = {
    "corro":  {"url":"equestrian-labs.myshopify.com","token":os.environ["SHOPIFY_TOKEN_CORRO"],"sheet_id":"1nq8xkDzowAvhD3wpMBlVK2M3FZSNS2DrAiPxz-Y2tdU"},
    "cavali": {"url":"cavali-club.myshopify.com",    "token":os.environ["SHOPIFY_TOKEN_CAVALI"],"sheet_id":"1QUdJc2EIdElIX5nlLQxWxS98aAz-TgQnSg9glJpNtig"},
}
SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]

# gross_profit added as new column after cogs
HEADERS_KPIS = [
    "updated_at","period","period_start","period_end",
    "gross_sales","net_sales","total_discounts","total_returns","cogs",
    "gross_profit",                                                        # ← NEW
    "pct_discount","pct_returns","pct_gm",
    "nb_orders","nb_units","aov","units_per_order",
    "sessions","unique_visitors","conversion_rate",
    "gross_sales_prev","gross_sales_yoy",
    "net_sales_prev","net_sales_yoy",
    "nb_orders_prev","nb_orders_yoy",
    "aov_prev","aov_yoy",
]

def get_gc():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    return gspread.authorize(creds)

def gql(store_url, token, query):
    url = f"https://{store_url}/admin/api/{GQL_VERSION}/graphql.json"
    r = requests.post(url,
        headers={"X-Shopify-Access-Token":token,"Content-Type":"application/json"},
        json={"query":query}, timeout=60)
    if r.status_code != 200: print(f"HTTP {r.status_code}"); return None
    d = r.json()
    if d.get("errors"): print(f"GQL errors: {d['errors']}"); return None
    return d.get("data")

def run_ql(store_url, token, ql_query):
    q = '{ shopifyqlQuery(query: "%s") { tableData { columns { name } rows } parseErrors } }' % ql_query.replace('"','\\"')
    data = gql(store_url, token, q)
    if not data: return None
    ql = data.get("shopifyqlQuery") or {}
    if ql.get("parseErrors"): print(f"parseErrors: {ql['parseErrors']}"); return None
    table = ql.get("tableData") or {}
    rows = table.get("rows") or []
    if not rows: return None
    return rows[-1]

def money(v):
    if v is None: return 0.0
    try: return float(str(v).replace(",","").strip())
    except: return 0.0

def gm_ratio(v):
    if v is None: return 0.0
    try:
        val = float(str(v).replace("%","").replace(",","").strip())
        return round(val*100,2) if abs(val)<=1.0 else round(val,2)
    except: return 0.0

def fetch_sales(store_url, token, start, end):
    """
    Fetch sales metrics including gross_profit directly from ShopifyQL.
    ShopifyQL field: gross_profit = net_sales - cost_of_goods_sold
    """
    row = run_ql(store_url, token,
        f"FROM sales SHOW gross_sales, discounts, returns, net_sales, cost_of_goods_sold, gross_profit, gross_margin, orders SINCE {start} UNTIL {end}")
    empty = {k:None for k in ["gross_sales","discounts","returns","net_sales","cogs","gross_profit","pct_gm","orders"]}
    if not row: return empty
    g  = round(money(row.get("gross_sales")),2)
    d  = round(abs(money(row.get("discounts"))),2)
    r  = round(abs(money(row.get("returns"))),2)
    n  = round(money(row.get("net_sales")),2)
    c  = round(money(row.get("cost_of_goods_sold")),2)
    gp = round(money(row.get("gross_profit")),2)   # ← NEW: direct from Shopify
    gm = gm_ratio(row.get("gross_margin"))
    o  = int(abs(money(row.get("orders"))))
    print(f"    gross:{g:>12,.2f} disc:{d:>9,.2f} ret:{r:>9,.2f} net:{n:>12,.2f} gp:{gp:>12,.2f} gm:{gm:>5.1f}% ord:{o}")
    return {"gross_sales":g,"discounts":d,"returns":r,"net_sales":n,"cogs":c,"gross_profit":gp,"pct_gm":gm,"orders":o}

def fetch_sessions(store_url, token, start, end):
    row = run_ql(store_url, token, f"FROM sessions SHOW sessions SINCE {start} UNTIL {end}")
    if not row: return 0
    return int(abs(money(row.get("sessions",0))))

def fetch_orders_fulfilled(store_url, token, start, end):
    row = run_ql(store_url, token, f"FROM fulfillments SHOW orders_fulfilled SINCE {start} UNTIL {end}")
    if not row: return None
    return int(abs(money(row.get("orders_fulfilled",0))))

def rest_get(store_url, token, endpoint, params):
    url = f"https://{store_url}/admin/api/2024-01/{endpoint}"
    headers = {"X-Shopify-Access-Token":token}
    results = []
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        results.extend(data[list(data.keys())[0]])
        link, url, params = r.headers.get("Link",""), None, {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part: url = part.split(";")[0].strip().strip("<>")
    return results

def fetch_orders(store_url, token, start, end):
    return rest_get(store_url, token, "orders.json", {
        "status":"any","financial_status":"paid,partially_paid,partially_refunded,refunded",
        "created_at_min":f"{start}T00:00:00-05:00","created_at_max":f"{end}T23:59:59-05:00",
        "limit":250,"fields":"id,subtotal_price,line_items,source_name,tags",
    })

def calc_units(orders):
    return sum(sum(int(li.get("quantity",0)or 0) for li in o.get("line_items",[])) for o in orders)

def calc_rs(orders):
    ch = {"Wellington (POS)":0.0,"Concierge":0.0,"Online":0.0,"Others":0.0}
    total = 0.0
    for o in orders:
        amt=float(o.get("subtotal_price",0)or 0); total+=amt
        src=(o.get("source_name")or"").lower().strip(); tags=(o.get("tags")or"").lower()
        if src=="pos" or "wellington" in tags or "pos" in tags: ch["Wellington (POS)"]+=amt
        elif "concierge" in tags or "concierge" in src: ch["Concierge"]+=amt
        elif src in ("web","shopify","","online_store") or not src: ch["Online"]+=amt
        else: ch["Others"]+=amt
    return {k:{"amount":round(v,2),"pct":round(v/total*100,2) if total else 0} for k,v in ch.items()}

def build(ql, orders, sessions=0, orders_fulfilled=None):
    if ql.get("gross_sales") is not None:
        g  = ql["gross_sales"]
        d  = ql["discounts"]
        r  = ql["returns"]
        n  = ql["net_sales"]
        c  = ql["cogs"] or 0
        gp = ql.get("gross_profit") or 0    # ← NEW
        gm = ql["pct_gm"] or 0
        nb = int(orders_fulfilled) if orders_fulfilled is not None else (ql["orders"] or len(orders))
    else:
        nb = int(orders_fulfilled) if orders_fulfilled is not None else len(orders)
        g  = sum(float(o.get("subtotal_price",0)or 0) for o in orders)
        d  = r = c = gp = gm = 0.0
        n  = g
    units  = calc_units(orders)
    aov    = round(n/nb,2) if nb else 0
    upo    = round(units/nb,2) if nb else 0
    pdisc  = round(d/g*100,2) if g else 0
    pret   = round(r/g*100,2) if g else 0
    s      = int(sessions or 0)
    return {
        "gross_sales":    round(g,2),
        "net_sales":      round(n,2),
        "total_discounts":round(d,2),
        "total_returns":  round(r,2),
        "cogs":           round(c,2),
        "gross_profit":   round(gp,2),       # ← NEW
        "pct_discount":   pdisc,
        "pct_returns":    pret,
        "pct_gm":         gm,
        "nb_orders":      nb,
        "nb_units":       units,
        "aov":            aov,
        "units_per_order":upo,
        "sessions":       s,
        "unique_visitors":round(s*0.85) if s else 0,
        "conversion_rate":round(nb/s*100,4) if s else 0,
    }

def pct(c,p):
    if not p: return None
    return round((c-p)/p*100,2)

def last_day(y,m):
    return (date(y,m+1,1)-timedelta(days=1)) if m<12 else date(y,12,31)

def monday_of(d):
    return d - timedelta(days=d.weekday())

def make_kpi_row(now_str, label, period_start, period_end, cur, prev, yoy):
    """Build a single kpis_daily row with gross_profit column included."""
    return [
        now_str, label, str(period_start), str(period_end),
        cur.get("gross_sales",0),
        cur.get("net_sales",0),
        cur.get("total_discounts",0),
        cur.get("total_returns",0),
        cur.get("cogs",0),
        cur.get("gross_profit",0),           # ← NEW column
        cur.get("pct_discount",0),
        cur.get("pct_returns",0),
        cur.get("pct_gm",0),
        cur.get("nb_orders",0),
        cur.get("nb_units",0),
        cur.get("aov",0),
        cur.get("units_per_order",0),
        cur.get("sessions",0),
        cur.get("unique_visitors",0),
        cur.get("conversion_rate",0),
        pct(cur.get("gross_sales",0), prev.get("gross_sales")),
        pct(cur.get("gross_sales",0), yoy.get("gross_sales")),
        pct(cur.get("net_sales",0),   prev.get("net_sales")),
        pct(cur.get("net_sales",0),   yoy.get("net_sales")),
        pct(cur.get("nb_orders",0),   prev.get("nb_orders")),
        pct(cur.get("nb_orders",0),   yoy.get("nb_orders")),
        pct(cur.get("aov",0),          prev.get("aov")),
        pct(cur.get("aov",0),          yoy.get("aov")),
    ]

def main():
    gc = get_gc()
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
    today   = datetime.now(TIMEZONE).date()

    # Clear existing data first
    for brand, cfg in STORES.items():
        sh = gc.open_by_key(cfg["sheet_id"])
        try:
            ws = sh.worksheet("kpis_daily"); ws.clear(); ws.append_row(HEADERS_KPIS)
            print(f"Cleared kpis_daily for {brand}")
        except: pass
        try:
            ws_rs = sh.worksheet("revenue_share"); ws_rs.clear()
            ws_rs.append_row(["updated_at","period","channel","amount","pct"])
            print(f"Cleared revenue_share for {brand}")
        except: pass

    for brand, cfg in STORES.items():
        print(f"\n{'='*55}\n  {brand.upper()} — BACKFILL 2024-01 → {today}\n{'='*55}")
        url, token, sid = cfg["url"], cfg["token"], cfg["sheet_id"]
        kpi_rows, rs_rows = [], []

        # ── MONTHLY DATA: Jan 2024 → current month ──────────
        y_start = 2024
        for y in range(y_start, today.year+1):
            m_start = 1
            m_end   = today.month if y == today.year else 12
            for m in range(m_start, m_end+1):
                mo_start    = date(y,m,1)
                mo_end      = last_day(y,m) if (y<today.year or m<today.month) else today
                period_label = f"{y}-{str(m).zfill(2)}"
                mtd_label    = f"mtd_{period_label}"

                print(f"\n  Month {period_label} ({mo_start} → {mo_end})")

                # Current
                ql_cur = fetch_sales(url, token, mo_start, mo_end)
                s_cur  = fetch_sessions(url, token, mo_start, mo_end)
                of_cur = fetch_orders_fulfilled(url, token, mo_start, mo_end)
                o_cur  = fetch_orders(url, token, mo_start, mo_end)
                cur    = build(ql_cur, o_cur, s_cur, of_cur)

                # Prev month
                pm = m-1 if m>1 else 12; py = y if m>1 else y-1
                prev_start = date(py,pm,1); prev_end = last_day(py,pm)
                ql_prev = fetch_sales(url, token, prev_start, prev_end)
                of_prev = fetch_orders_fulfilled(url, token, prev_start, prev_end)
                o_prev  = fetch_orders(url, token, prev_start, prev_end)
                prev    = build(ql_prev, o_prev, 0, of_prev)

                # YOY
                if y > 2024:
                    yoy_start = date(y-1,m,1); yoy_end = last_day(y-1,m)
                    ql_yoy  = fetch_sales(url, token, yoy_start, yoy_end)
                    of_yoy  = fetch_orders_fulfilled(url, token, yoy_start, yoy_end)
                    o_yoy   = fetch_orders(url, token, yoy_start, yoy_end)
                    yoy     = build(ql_yoy, o_yoy, 0, of_yoy)
                else:
                    yoy = {}

                # Revenue share
                rs = calc_rs(o_cur)
                for ch, v in rs.items():
                    rs_rows.append([now_str, period_label, ch, v["amount"], v["pct"]])
                    rs_rows.append([now_str, mtd_label,    ch, v["amount"], v["pct"]])

                kpi_rows.append(make_kpi_row(now_str, period_label, mo_start, mo_end, cur, prev, yoy))
                kpi_rows.append(make_kpi_row(now_str, mtd_label,    mo_start, mo_end, cur, prev, yoy))

        # ── WEEKLY DATA ──────────────────────────────────────
        wk_start = monday_of(date(y_start, 1, 1))
        while wk_start <= today:
            wk_end   = min(wk_start + timedelta(days=6), today)
            wk_label = f"week_{wk_start}"

            print(f"\n  Week {wk_label} ({wk_start} -> {wk_end})")

            ql_cur = fetch_sales(url, token, wk_start, wk_end)
            s_cur  = fetch_sessions(url, token, wk_start, wk_end)
            of_cur = fetch_orders_fulfilled(url, token, wk_start, wk_end)
            o_cur  = fetch_orders(url, token, wk_start, wk_end)
            cur    = build(ql_cur, o_cur, s_cur, of_cur)

            pws = wk_start - timedelta(days=7)
            pwe = pws + timedelta(days=(wk_end - wk_start).days)
            ql_prev = fetch_sales(url, token, pws, pwe)
            of_prev = fetch_orders_fulfilled(url, token, pws, pwe)
            prev    = build(ql_prev, [], 0, of_prev)

            yws = wk_start - timedelta(days=364)
            ywe = wk_end   - timedelta(days=364)
            ql_yoy = fetch_sales(url, token, yws, ywe)
            of_yoy = fetch_orders_fulfilled(url, token, yws, ywe)
            yoy    = build(ql_yoy, [], 0, of_yoy)

            rs = calc_rs(o_cur)
            for ch, v in rs.items():
                rs_rows.append([now_str, wk_label, ch, v["amount"], v["pct"]])

            kpi_rows.append(make_kpi_row(now_str, wk_label, wk_start, wk_end, cur, prev, yoy))
            wk_start += timedelta(days=7)

        # ── QUARTERLY DATA ───────────────────────────────────
        for y in range(y_start, today.year+1):
            max_q = ((today.month-1)//3)+1 if y == today.year else 4
            for q in range(1, max_q+1):
                q_start = date(y,(q-1)*3+1,1)
                q_end_m = q*3
                q_end   = last_day(y,q_end_m) if (y<today.year or q<max_q) else today
                q_label = f"q{q}_{y}"

                print(f"\n  Quarter {q_label} ({q_start} → {q_end})")
                ql_q  = fetch_sales(url, token, q_start, q_end)
                s_q   = fetch_sessions(url, token, q_start, q_end)
                of_q  = fetch_orders_fulfilled(url, token, q_start, q_end)
                o_q   = fetch_orders(url, token, q_start, q_end)
                cur_q = build(ql_q, o_q, s_q, of_q)

                # Prev quarter
                pq = q-1 if q>1 else 4; py = y if q>1 else y-1
                pq_start = date(py,(pq-1)*3+1,1); pq_end = last_day(py,pq*3)
                ql_pq  = fetch_sales(url, token, pq_start, pq_end)
                of_pq  = fetch_orders_fulfilled(url, token, pq_start, pq_end)
                prev_q = build(ql_pq, [], 0, of_pq)

                # YOY quarter
                if y > 2024:
                    yq_start = date(y-1,(q-1)*3+1,1); yq_end = last_day(y-1,q*3)
                    ql_yq  = fetch_sales(url, token, yq_start, yq_end)
                    of_yq  = fetch_orders_fulfilled(url, token, yq_start, yq_end)
                    yoy_q  = build(ql_yq, [], 0, of_yq)
                else:
                    yoy_q = {}

                rs_q = calc_rs(o_q)
                for ch, v in rs_q.items():
                    rs_rows.append([now_str, q_label, ch, v["amount"], v["pct"]])

                kpi_rows.append(make_kpi_row(now_str, q_label, q_start, q_end, cur_q, prev_q, yoy_q))

        # ── WRITE IN BATCHES ─────────────────────────────────
        print(f"\n  Writing {len(kpi_rows)} KPI rows to Sheets...")
        sh    = gc.open_by_key(sid)
        ws    = sh.worksheet("kpis_daily")
        ws_rs = sh.worksheet("revenue_share")
        for i in range(0, len(kpi_rows), 50):
            ws.append_rows(kpi_rows[i:i+50])
            print(f"  KPI batch {i//50+1} written")
        for i in range(0, len(rs_rows), 50):
            ws_rs.append_rows(rs_rows[i:i+50])
            print(f"  RS batch {i//50+1} written")
        print(f"\n  ✓ {brand.upper()} backfill done: {len(kpi_rows)} KPI rows + {len(rs_rows)} RS rows")

if __name__ == "__main__":
    main()
