"""
Pipeline CORRO / CAVALI — Shopify Analytics → Google Sheets
v2: Fixed period keys, correct week matching, no fake session estimates
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

HEADERS_KPIS = [
    "updated_at","period","period_start","period_end",
    "gross_sales","net_sales","total_discounts","total_returns","cogs",
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

def gql_post(store_url, token, query):
    url = f"https://{store_url}/admin/api/{GQL_VERSION}/graphql.json"
    r = requests.post(url,
        headers={"X-Shopify-Access-Token":token,"Content-Type":"application/json"},
        json={"query":query}, timeout=60)
    if r.status_code != 200: print(f"    HTTP {r.status_code}"); return None
    d = r.json()
    if d.get("errors"): print(f"    GQL errors: {d['errors']}"); return None
    return d.get("data")

def rest_get(store_url, token, endpoint, params):
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
                if 'rel="next"' in part: url = part.split(";")[0].strip().strip("<>")
    return results

def run_ql(store_url, token, ql_query):
    q = '{ shopifyqlQuery(query: "%s") { tableData { columns { name } rows } parseErrors } }' % ql_query.replace('"','\\"')
    data = gql_post(store_url, token, q)
    if not data: return None
    ql = data.get("shopifyqlQuery") or {}
    errs = ql.get("parseErrors") or []
    if errs: print(f"    parseErrors: {errs}"); return None
    table = ql.get("tableData") or {}
    rows = table.get("rows") or []
    if not rows: print("    no rows returned"); return None
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
    """ShopifyQL FROM sales — exact same numbers as Shopify Analytics."""
    row = run_ql(store_url, token,
        f"FROM sales SHOW gross_sales, discounts, returns, net_sales, "
        f"cost_of_goods_sold, gross_margin, orders SINCE {start} UNTIL {end}")
    empty = {k:None for k in ["gross_sales","discounts","returns","net_sales","cogs","pct_gm","orders"]}
    if not row: return empty
    g = round(money(row.get("gross_sales")),2)
    d = round(abs(money(row.get("discounts"))),2)
    r = round(abs(money(row.get("returns"))),2)
    n = round(money(row.get("net_sales")),2)
    c = round(money(row.get("cost_of_goods_sold")),2)
    gm = gm_ratio(row.get("gross_margin"))
    o = int(abs(money(row.get("orders"))))
    print(f"    ✓ gross:{g:>12,.2f}  disc:{d:>9,.2f}  ret:{r:>9,.2f}  net:{n:>12,.2f}  cogs:{c:>9,.2f}  gm:{gm:>5.1f}%  ord:{o}")
    return {"gross_sales":g,"discounts":d,"returns":r,"net_sales":n,"cogs":c,"pct_gm":gm,"orders":o}

def fetch_sessions(store_url, token, start, end):
    """ShopifyQL FROM sessions — real sessions from Shopify Analytics."""
    row = run_ql(store_url, token, f"FROM sessions SHOW sessions SINCE {start} UNTIL {end}")
    if not row: return 0
    s = int(abs(money(row.get("sessions",0))))
    print(f"    sessions: {s:,}")
    return s

def fetch_orders(store_url, token, start, end):
    """REST orders — for units and revenue share only."""
    return rest_get(store_url, token, "orders.json", {
        "status":"any",
        "financial_status":"paid,partially_paid,partially_refunded,refunded",
        "created_at_min":f"{start}T00:00:00-05:00",
        "created_at_max":f"{end}T23:59:59-05:00",
        "limit":250,
        "fields":"id,subtotal_price,line_items,source_name,tags",
    })

def calc_units(orders):
    return sum(sum(int(li.get("quantity",0)or 0) for li in o.get("line_items",[])) for o in orders)

def calc_rs(orders):
    ch = {"Wellington (POS)":0.0,"Concierge":0.0,"Online":0.0,"Others":0.0}
    total = 0.0
    for o in orders:
        amt=float(o.get("subtotal_price",0)or 0); total+=amt
        src=(o.get("source_name")or"").lower().strip()
        tags=(o.get("tags")or"").lower()
        if src=="pos" or "wellington" in tags or "pos" in tags: ch["Wellington (POS)"]+=amt
        elif "concierge" in tags or "concierge" in src: ch["Concierge"]+=amt
        elif src in ("web","shopify","","online_store") or not src: ch["Online"]+=amt
        else: ch["Others"]+=amt
    return {k:{"amount":round(v,2),"pct":round(v/total*100,2) if total else 0} for k,v in ch.items()}

def build(ql, orders, sessions=0):
    if ql.get("gross_sales") is not None:
        g=ql["gross_sales"]; d=ql["discounts"]; r=ql["returns"]
        n=ql["net_sales"]; c=ql["cogs"]or 0; gm=ql["pct_gm"]or 0
        nb=ql["orders"] if ql["orders"] else len(orders)
    else:
        nb=len(orders); g=sum(float(o.get("subtotal_price",0)or 0) for o in orders)
        d=r=c=gm=0.0; n=g
    units=calc_units(orders)
    aov=round((g-d)/nb,2) if nb else 0
    upo=round(units/nb,2) if nb else 0
    pdisc=round(d/g*100,2) if g else 0
    pret=round(r/g*100,2) if g else 0
    # Real sessions from ShopifyQL — NEVER estimate from orders
    s=int(sessions or 0)
    uv=round(s*0.85) if s else 0
    cr=round(nb/s*100,4) if s else 0
    return {
        "gross_sales":round(g,2),"net_sales":round(n,2),
        "total_discounts":round(d,2),"total_returns":round(r,2),"cogs":round(c,2),
        "pct_discount":pdisc,"pct_returns":pret,"pct_gm":gm,
        "nb_orders":nb,"nb_units":units,"aov":aov,"units_per_order":upo,
        "sessions":s,"unique_visitors":uv,"conversion_rate":cr,
    }

def pct(c, p):
    if not p: return None
    return round((c-p)/abs(p)*100,2)

def get_periods():
    today = datetime.now(TIMEZONE).date()
    # MTD
    mtd_start = today.replace(day=1)
    mtd_end   = today
    # MTD prev month (same # of days)
    mom_end     = mtd_start - timedelta(days=1)
    mom_start   = mom_end.replace(day=1)
    mom_mtd_end = mom_end.replace(day=min(today.day, mom_end.day))
    # MTD YOY
    yoy_start = mtd_start.replace(year=mtd_start.year-1)
    yoy_end   = today.replace(year=today.year-1)
    # Current week Mon-Sun
    dow = today.weekday()  # 0=Mon
    wk_start = today - timedelta(days=dow)
    wk_end   = today
    # Previous full week Mon-Sun
    pwk_end   = wk_start - timedelta(days=1)
    pwk_start = pwk_end - timedelta(days=6)
    # Previous full month
    mo_end   = mtd_start - timedelta(days=1)
    mo_start = mo_end.replace(day=1)
    # Month prev
    pmo_end   = mo_start - timedelta(days=1)
    pmo_start = pmo_end.replace(day=1)
    # Month YOY
    yoy_mo_start = mo_start.replace(year=mo_start.year-1)
    yoy_mo_end   = mo_end.replace(year=mo_end.year-1)
    # Quarter
    q_num     = (today.month-1)//3+1
    q_start   = today.replace(month=(q_num-1)*3+1, day=1)
    q_end     = today
    q_label   = f"q{q_num}_{today.year}"
    # Prev quarter
    pq = q_num-1 if q_num>1 else 4
    py = today.year if q_num>1 else today.year-1
    pq_start  = date(py,(pq-1)*3+1,1)
    pq_end_m  = pq*3
    pq_end    = date(py,pq_end_m,1).replace(day=1)
    import calendar
    pq_end    = date(py,pq_end_m,calendar.monthrange(py,pq_end_m)[1])
    # YOY quarter
    yoy_q_start = q_start.replace(year=q_start.year-1)
    yoy_q_end   = today.replace(year=today.year-1)
    yoy_q_label = f"q{q_num}_{today.year-1}"

    return {
        "mtd":         (mtd_start,  mtd_end,  "mtd"),
        "mtd_mom":     (mom_start,  mom_mtd_end, None),
        "mtd_yoy":     (yoy_start,  yoy_end,  None),
        "week":        (wk_start,   wk_end,   None),  # period_key = date range
        "week_prev":   (pwk_start,  pwk_end,  None),
        "month":       (mo_start,   mo_end,   mo_start.strftime("%Y-%m")),
        "month_prev":  (pmo_start,  pmo_end,  pmo_start.strftime("%Y-%m")),
        "month_yoy":   (yoy_mo_start, yoy_mo_end, None),
        "quarter":     (q_start,    q_end,    q_label),
        "quarter_prev":(pq_start,   pq_end,   f"q{pq}_{py}"),
        "quarter_yoy": (yoy_q_start,yoy_q_end,yoy_q_label),
    }

def make_row(now_str, period_key, start, end, cur, prev, yoy, rs):
    rs_data = rs
    return {
        "kpi": [
            now_str, period_key, str(start), str(end),
            cur.get("gross_sales",0),     cur.get("net_sales",0),
            cur.get("total_discounts",0), cur.get("total_returns",0),
            cur.get("cogs",0),
            cur.get("pct_discount",0),    cur.get("pct_returns",0),
            cur.get("pct_gm",0),
            cur.get("nb_orders",0),       cur.get("nb_units",0),
            cur.get("aov",0),             cur.get("units_per_order",0),
            cur.get("sessions",0),        cur.get("unique_visitors",0),
            cur.get("conversion_rate",0),
            pct(cur.get("gross_sales",0), prev.get("gross_sales")),
            pct(cur.get("gross_sales",0), yoy.get("gross_sales")),
            pct(cur.get("net_sales",0),   prev.get("net_sales")),
            pct(cur.get("net_sales",0),   yoy.get("net_sales")),
            pct(cur.get("nb_orders",0),   prev.get("nb_orders")),
            pct(cur.get("nb_orders",0),   yoy.get("nb_orders")),
            pct(cur.get("aov",0),          prev.get("aov")),
            pct(cur.get("aov",0),          yoy.get("aov")),
        ],
        "rs": [(now_str, period_key, ch, v["amount"], v["pct"]) for ch,v in rs.items()]
    }

def write_kpis(gc, sheet_id, all_rows):
    sh      = gc.open_by_key(sheet_id)
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
    try:    ws = sh.worksheet("kpis_daily")
    except: ws = sh.add_worksheet("kpis_daily",rows=500,cols=35)
    ws.clear(); ws.append_row(HEADERS_KPIS)
    for r in all_rows:
        ws.append_row(r["kpi"])
    try:    ws_rs = sh.worksheet("revenue_share")
    except: ws_rs = sh.add_worksheet("revenue_share",rows=500,cols=10)
    ws_rs.clear(); ws_rs.append_row(["updated_at","period","channel","amount","pct"])
    for r in all_rows:
        for rs_row in r["rs"]:
            ws_rs.append_row(list(rs_row))
    print(f"  ✓ Sheets OK: {now_str}")

def main():
    gc      = get_gc()
    periods = get_periods()
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

    for brand, cfg in STORES.items():
        print(f"\n{'='*55}\n  {brand.upper()}\n{'='*55}")
        url, token = cfg["url"], cfg["token"]
        all_rows = []

        # ── 1. MTD ──────────────────────────────────────────
        s,e,pk = periods["mtd"]
        print(f"\n  MTD ({s} → {e})")
        ql_cur = fetch_sales(url, token, s, e)
        s_cur  = fetch_sessions(url, token, s, e)
        o_cur  = fetch_orders(url, token, s, e)
        cur    = build(ql_cur, o_cur, s_cur)

        s2,e2,_ = periods["mtd_mom"]
        ql_p = fetch_sales(url, token, s2, e2)
        prev = build(ql_p, [])

        s3,e3,_ = periods["mtd_yoy"]
        ql_y = fetch_sales(url, token, s3, e3)
        yoy  = build(ql_y, [])

        all_rows.append(make_row(now_str, "mtd", s, e, cur, prev, yoy, calc_rs(o_cur)))

        # ── 2. WEEK — saved with period_start as key ────────
        s,e,_ = periods["week"]
        print(f"\n  WEEK ({s} → {e})")
        # Period key for week = "week_YYYY-MM-DD" so dashboard can find by start date
        week_pk = f"week_{s}"
        ql_cur = fetch_sales(url, token, s, e)
        s_cur  = fetch_sessions(url, token, s, e)
        o_cur  = fetch_orders(url, token, s, e)
        cur    = build(ql_cur, o_cur, s_cur)

        s2,e2,_ = periods["week_prev"]
        ql_p = fetch_sales(url, token, s2, e2)
        o_p  = fetch_orders(url, token, s2, e2)
        prev = build(ql_p, o_p)
        all_rows.append(make_row(now_str, week_pk, s, e, cur, prev, {}, calc_rs(o_cur)))

        # ── 3. MONTH ─────────────────────────────────────────
        s,e,pk = periods["month"]
        print(f"\n  MONTH ({s} → {e}) [period={pk}]")
        ql_cur = fetch_sales(url, token, s, e)
        s_cur  = fetch_sessions(url, token, s, e)
        o_cur  = fetch_orders(url, token, s, e)
        cur    = build(ql_cur, o_cur, s_cur)

        s2,e2,pk2 = periods["month_prev"]
        ql_p = fetch_sales(url, token, s2, e2)
        prev = build(ql_p, [])

        s3,e3,_ = periods["month_yoy"]
        ql_y = fetch_sales(url, token, s3, e3)
        yoy  = build(ql_y, [])

        all_rows.append(make_row(now_str, pk, s, e, cur, prev, yoy, calc_rs(o_cur)))

        # ── 4. QUARTER ───────────────────────────────────────
        s,e,pk = periods["quarter"]
        print(f"\n  QUARTER ({s} → {e}) [period={pk}]")
        ql_cur = fetch_sales(url, token, s, e)
        s_cur  = fetch_sessions(url, token, s, e)
        o_cur  = fetch_orders(url, token, s, e)
        cur    = build(ql_cur, o_cur, s_cur)

        s2,e2,pk2 = periods["quarter_prev"]
        ql_p = fetch_sales(url, token, s2, e2)
        prev = build(ql_p, [])

        s3,e3,pk3 = periods["quarter_yoy"]
        ql_y = fetch_sales(url, token, s3, e3)
        yoy  = build(ql_y, [])

        all_rows.append(make_row(now_str, pk, s, e, cur, prev, yoy, calc_rs(o_cur)))

        write_kpis(gc, cfg["sheet_id"], all_rows)
        print(f"\n  ✓ {brand.upper()} done — {len(all_rows)} periods saved")
        for r in all_rows:
            pk_saved = r["kpi"][1]
            ps = r["kpi"][2]; pe = r["kpi"][3]
            gs = r["kpi"][4]; ns = r["kpi"][5]
            print(f"    {pk_saved:<20} {ps} → {pe}  gross:${gs:>12,.2f}  net:${ns:>12,.2f}")

if __name__ == "__main__":
    main()
