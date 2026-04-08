"""
Pipeline CORRO / CAVALI — Shopify Analytics → Google Sheets
FIXED: Quarter now saves as q1_2026, q2_2026, etc. for precise lookup
"""
import os, json, requests, gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
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
    creds = Credentials.from_service_account_info(json.loads(os.environ["GOOGLE_CREDENTIALS"]),scopes=SCOPES)
    return gspread.authorize(creds)

def shopify_graphql(store_url, token, query):
    url = f"https://{store_url}/admin/api/{GQL_VERSION}/graphql.json"
    r = requests.post(url, headers={"X-Shopify-Access-Token":token,"Content-Type":"application/json"}, json={"query":query}, timeout=60)
    if r.status_code != 200: print(f"    HTTP {r.status_code}"); return None
    d = r.json()
    if d.get("errors"): print(f"    GQL errors: {d['errors']}"); return None
    return d.get("data")

def shopify_rest_get(store_url, token, endpoint, params):
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

def _run_ql(store_url, token, ql_query):
    gql = '{ shopifyqlQuery(query: "%s") { tableData { columns { name dataType } rows } parseErrors } }' % ql_query.replace('"','\\"')
    data = shopify_graphql(store_url, token, gql)
    if not data: return None
    ql = data.get("shopifyqlQuery") or {}
    errs = ql.get("parseErrors") or []
    if errs: print(f"    parseErrors: {errs}"); return None
    table = ql.get("tableData") or {}
    rows = table.get("rows") or []
    if not rows: print("    no rows"); return None
    return rows[-1]

def _money(v):
    if v is None: return 0.0
    try: return float(str(v).replace(",","").strip())
    except: return 0.0

def _gm(v):
    if v is None: return 0.0
    try:
        val = float(str(v).replace("%","").replace(",","").strip())
        return round(val*100,2) if abs(val)<=1.0 else round(val,2)
    except: return 0.0

def fetch_sales_ql(store_url, token, start_date, end_date):
    row = _run_ql(store_url, token,
        f"FROM sales SHOW gross_sales, discounts, returns, net_sales, cost_of_goods_sold, gross_margin, orders SINCE {start_date} UNTIL {end_date}")
    empty = {k:None for k in ["gross_sales","discounts","returns","net_sales","cogs","pct_gm","orders"]}
    if row is None: return empty
    gross = round(_money(row.get("gross_sales")),2)
    disc  = round(abs(_money(row.get("discounts"))),2)
    ret   = round(abs(_money(row.get("returns"))),2)
    net   = round(_money(row.get("net_sales")),2)
    cogs  = round(_money(row.get("cost_of_goods_sold")),2)
    gm    = _gm(row.get("gross_margin"))
    ords  = int(abs(_money(row.get("orders"))))
    print(f"    ✓ gross:{gross:>12,.2f} disc:{disc:>10,.2f} ret:{ret:>10,.2f} net:{net:>12,.2f} cogs:{cogs:>10,.2f} gm:{gm:>6.2f}% ord:{ords}")
    return {"gross_sales":gross,"discounts":disc,"returns":ret,"net_sales":net,"cogs":cogs,"pct_gm":gm,"orders":ords}

def fetch_sessions_ql(store_url, token, start_date, end_date):
    row = _run_ql(store_url, token, f"FROM sessions SHOW sessions SINCE {start_date} UNTIL {end_date}")
    if row is None: return 0
    return int(abs(_money(row.get("sessions",0))))

def fetch_orders(store_url, token, start_date, end_date):
    return shopify_rest_get(store_url, token, "orders.json", {
        "status":"any","financial_status":"paid,partially_paid,partially_refunded,refunded",
        "created_at_min":f"{start_date}T00:00:00-05:00","created_at_max":f"{end_date}T23:59:59-05:00",
        "limit":250,"fields":"id,subtotal_price,line_items,source_name,tags",
    })

def calc_units(orders):
    return sum(sum(int(li.get("quantity",0)or 0) for li in o.get("line_items",[])) for o in orders)

def calc_revenue_share(orders):
    ch = {"Wellington (POS)":0.0,"Concierge":0.0,"Online":0.0,"Others":0.0}
    total = 0.0
    for o in orders:
        amt=float(o.get("subtotal_price",0)or 0); total+=amt
        src=(o.get("source_name")or"").lower().strip(); tags=(o.get("tags")or"").lower()
        if src=="pos" or "wellington" in tags or "pos" in tags: ch["Wellington (POS)"]+=amt
        elif "concierge" in tags or "concierge" in src: ch["Concierge"]+=amt
        elif src in ("web","shopify","","online_store") or not src: ch["Online"]+=amt
        else: ch["Others"]+=amt
    return {k:{"amount":round(v,2),"pct":round(v/total*100,2) if total else 0.0} for k,v in ch.items()}

def build_kpis(ql, orders, sessions=0):
    if ql.get("gross_sales") is not None:
        gross=ql["gross_sales"]; disc=ql["discounts"]; ret=ql["returns"]
        net=ql["net_sales"]; cogs=ql["cogs"]or 0.0; pct_gm=ql["pct_gm"]or 0.0; nb_ord=ql["orders"]or len(orders)
    else:
        print("    ⚠ ShopifyQL unavailable — REST fallback"); nb_ord=len(orders)
        gross=sum(float(o.get("subtotal_price",0)or 0) for o in orders); disc=ret=net=cogs=pct_gm=0.0; net=gross
    units=calc_units(orders)
    aov=round((gross-disc)/nb_ord,2) if nb_ord else 0.0
    upo=round(units/nb_ord,2) if nb_ord else 0.0
    pct_disc=round(disc/gross*100,2) if gross else 0.0
    pct_ret=round(ret/gross*100,2) if gross else 0.0
    sessions=int(sessions or 0)
    return {"gross_sales":round(gross,2),"net_sales":round(net,2),"total_discounts":round(disc,2),
            "total_returns":round(ret,2),"cogs":round(cogs,2),"pct_discount":pct_disc,"pct_returns":pct_ret,
            "pct_gm":pct_gm,"nb_orders":nb_ord,"nb_units":units,"aov":aov,"units_per_order":upo,
            "sessions":sessions,"unique_visitors":round(sessions*0.85) if sessions else 0,
            "conversion_rate":round(nb_ord/sessions*100,4) if sessions else 0.0}

def pct_change(cur, prev):
    if not prev: return None
    return round((cur-prev)/prev*100,2)

def get_periods():
    today=datetime.now(TIMEZONE).date()
    mtd_start=today.replace(day=1); mtd_end=today
    mom_end=mtd_start-timedelta(days=1); mom_start=mom_end.replace(day=1)
    mom_mtd_end=mom_end.replace(day=min(today.day,mom_end.day))
    yoy_start=mtd_start.replace(year=mtd_start.year-1); yoy_end=today.replace(year=today.year-1)
    wk_start=today-timedelta(days=today.weekday()); pwk_start=wk_start-timedelta(days=7)
    mo_end=mtd_start-timedelta(days=1)
    q_month=((today.month-1)//3)*3+1; q_num=(today.month-1)//3+1
    q_start=today.replace(month=q_month,day=1)
    # Previous quarter
    pq_end=q_start-timedelta(days=1); pq_start=pq_end.replace(day=1)
    pq_month_start=((pq_end.month-1)//3)*3+1; pq_start=pq_end.replace(month=pq_month_start,day=1)
    # YOY quarter
    yoy_q_start=q_start.replace(year=q_start.year-1); yoy_q_end=today.replace(year=today.year-1)
    return {
        "mtd":        (mtd_start,  mtd_end),
        "mtd_mom":    (mom_start,  mom_mtd_end),
        "mtd_yoy":    (yoy_start,  yoy_end),
        "week":       (wk_start,   today),
        "week_prev":  (pwk_start,  wk_start-timedelta(days=1)),
        "week_yoy":   (wk_start-timedelta(days=364), today-timedelta(days=364)),
        "month":      (mo_end.replace(day=1), mo_end),
        "month_prev": (mom_start,  mom_end),
        "month_yoy":  (mo_end.replace(day=1,year=mo_end.year-1), mo_end.replace(year=mo_end.year-1)),
        "quarter":    (q_start, today),
        "quarter_prev":(pq_start, pq_end),
        "quarter_yoy": (yoy_q_start, yoy_q_end),
        "q_num":      q_num,
        "q_year":     today.year,
    }

def write_kpis(gc, sheet_id, periods_data):
    sh=gc.open_by_key(sheet_id); now_str=datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
    try:    ws=sh.worksheet("kpis_daily")
    except: ws=sh.add_worksheet("kpis_daily",rows=500,cols=35)
    ws.clear(); ws.append_row(HEADERS_KPIS)
    for pname,d in periods_data.items():
        cur=d["current"]; prev=d.get("prev",{}); yoy=d.get("yoy",{})
        ws.append_row([now_str,pname,str(d["start"]),str(d["end"]),
            cur.get("gross_sales",0),cur.get("net_sales",0),cur.get("total_discounts",0),cur.get("total_returns",0),cur.get("cogs",0),
            cur.get("pct_discount",0),cur.get("pct_returns",0),cur.get("pct_gm",0),
            cur.get("nb_orders",0),cur.get("nb_units",0),cur.get("aov",0),cur.get("units_per_order",0),
            cur.get("sessions",0),cur.get("unique_visitors",0),cur.get("conversion_rate",0),
            pct_change(cur.get("gross_sales",0),prev.get("gross_sales")),
            pct_change(cur.get("gross_sales",0),yoy.get("gross_sales")),
            pct_change(cur.get("net_sales",0),prev.get("net_sales")),
            pct_change(cur.get("net_sales",0),yoy.get("net_sales")),
            pct_change(cur.get("nb_orders",0),prev.get("nb_orders")),
            pct_change(cur.get("nb_orders",0),yoy.get("nb_orders")),
            pct_change(cur.get("aov",0),prev.get("aov")),
            pct_change(cur.get("aov",0),yoy.get("aov")),
        ])
    try:    ws_rs=sh.worksheet("revenue_share")
    except: ws_rs=sh.add_worksheet("revenue_share",rows=500,cols=10)
    ws_rs.clear(); ws_rs.append_row(["updated_at","period","channel","amount","pct"])
    for pname,d in periods_data.items():
        for ch,v in d.get("revenue_share",{}).items():
            ws_rs.append_row([now_str,pname,ch,v["amount"],v["pct"]])
    print(f"  ✓ Sheets OK: {now_str}")

def main():
    gc=get_gc(); periods=get_periods()
    q_num=periods["q_num"]; q_year=periods["q_year"]
    q_label=f"q{q_num}_{q_year}"  # e.g. "q1_2026"

    for brand,cfg in STORES.items():
        print(f"\n{'='*52}\n  {brand.upper()}\n{'='*52}")
        url,token=cfg["url"],cfg["token"]

        print(f"\n  [1/3] ShopifyQL (API {GQL_VERSION})...")
        ql={}
        for key in ["mtd","mtd_mom","mtd_yoy","week","week_prev","week_yoy","month","month_prev","month_yoy","quarter","quarter_prev","quarter_yoy"]:
            if key in ("q_num","q_year"): continue
            s,e=periods[key]
            print(f"  {key:<14} {s} → {e}")
            ql[key]=fetch_sales_ql(url,token,s,e)

        print("\n  [2/3] Sessions...")
        s_mtd=fetch_sessions_ql(url,token,*periods["mtd"])
        s_week=fetch_sessions_ql(url,token,*periods["week"])
        s_month=fetch_sessions_ql(url,token,*periods["month"])
        s_qtr=fetch_sessions_ql(url,token,*periods["quarter"])
        print(f"  sessions mtd:{s_mtd} week:{s_week} month:{s_month} qtr:{s_qtr}")

        print("\n  [3/3] Orders REST...")
        orders={}
        for key in ["mtd","mtd_mom","mtd_yoy","week","week_prev","month","quarter"]:
            orders[key]=fetch_orders(url,token,*periods[key])
        print(f"  orders mtd:{len(orders['mtd'])} week:{len(orders['week'])} month:{len(orders['month'])} qtr:{len(orders['quarter'])}")

        periods_data={
            "mtd":     {"start":periods["mtd"][0],     "end":periods["mtd"][1],     "current":build_kpis(ql["mtd"],orders["mtd"],s_mtd),    "prev":build_kpis(ql["mtd_mom"],orders["mtd_mom"]),  "yoy":build_kpis(ql["mtd_yoy"],orders["mtd_yoy"]),  "revenue_share":calc_revenue_share(orders["mtd"])},
            "week":    {"start":periods["week"][0],    "end":periods["week"][1],    "current":build_kpis(ql["week"],orders["week"],s_week),   "prev":build_kpis(ql["week_prev"],orders["week_prev"]), "yoy":build_kpis(ql["week_yoy"],orders.get("week_prev",[])), "revenue_share":calc_revenue_share(orders["week"])},
            "month":   {"start":periods["month"][0],   "end":periods["month"][1],   "current":build_kpis(ql["month"],orders["month"],s_month), "prev":build_kpis(ql["month_prev"],orders["mtd_mom"]), "yoy":build_kpis(ql["month_yoy"],orders["mtd_yoy"]), "revenue_share":calc_revenue_share(orders["month"])},
            q_label:   {"start":periods["quarter"][0], "end":periods["quarter"][1], "current":build_kpis(ql["quarter"],orders["quarter"],s_qtr),"prev":build_kpis(ql["quarter_prev"],[]),             "yoy":build_kpis(ql["quarter_yoy"],[]),              "revenue_share":calc_revenue_share(orders["quarter"])},
        }

        write_kpis(gc,cfg["sheet_id"],periods_data)
        print(f"\n  ✓ {brand.upper()} done. Quarter saved as '{q_label}'")

if __name__=="__main__":
    main()
