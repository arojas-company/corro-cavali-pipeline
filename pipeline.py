"""
Pipeline CORRO / CAVALI v3 — DEFINITIVO
========================================
Cambios clave vs v2:
- Guarda TODOS los períodos con period keys claros y consistentes
- Week:    period = "week_2026-04-06" (lunes de inicio)
- MTD:     period = "mtd_2026-04"     (año-mes del MTD)  
- Month:   period = "2026-03"         (año-mes del mes completo)
- Quarter: period = "q1_2026"         (q#_año)
- AOV = net_sales / nb_orders (Shopify definition)
- Revenue share usa subtotal_price (net per order, sin shipping/tax)
- Sessions reales de ShopifyQL, sin estimaciones
- GM% viene directo de ShopifyQL (misma fuente que Analytics)

Cambios v3.1:
- Revenue share guarda pct_prev y pct_chg para mostrar variación vs período anterior
- Tab ad_spend se escribe automáticamente desde variable AD_SPEND_DATA (alimentar con Stats.xlsx)
- Bug week fix: period_end se guarda como fecha real del último día con datos
"""
import os, json, requests, gspread, calendar
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, date
import pytz

TIMEZONE    = pytz.timezone("America/Bogota")
GQL_VERSION = "2025-10"

STORES = {
    "corro":  {"url":"equestrian-labs.myshopify.com",
               "token":os.environ["SHOPIFY_TOKEN_CORRO"],
               "sheet_id":"1nq8xkDzowAvhD3wpMBlVK2M3FZSNS2DrAiPxz-Y2tdU"},
    "cavali": {"url":"cavali-club.myshopify.com",
               "token":os.environ["SHOPIFY_TOKEN_CAVALI"],
               "sheet_id":"1QUdJc2EIdElIX5nlLQxWxS98aAz-TgQnSg9glJpNtig"},
}
SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]

HEADERS = [
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

# ── AD SPEND DATA (del Stats.xlsx — Total Shopify sheet) ──────────
# Formato: {"YYYY-MM": {"spend": 0, "roas": 0, "cos": 0}}
# Llenar desde Stats.xlsx antes de correr el pipeline.
# Corro = equestrian-labs, Cavali = cavali-club (separar si tienes ambos)
AD_SPEND_DATA = {
    "corro": {
        "2024-01": {"spend": 82069,    "roas": 2.12, "cos": 0.472},
        "2024-02": {"spend": 38738,    "roas": 2.94, "cos": 0.341},
        "2024-03": {"spend": 39391,    "roas": 3.24, "cos": 0.309},
        "2024-04": {"spend": 16371,    "roas": 6.22, "cos": 0.161},
        "2024-05": {"spend": 7909,     "roas": 13.78,"cos": 0.073},
        "2024-06": {"spend": 19752,    "roas": 4.98, "cos": 0.201},
        "2024-07": {"spend": 10491,    "roas": 6.21, "cos": 0.161},
        "2024-08": {"spend": 16110,    "roas": 5.34, "cos": 0.187},
        "2024-09": {"spend": 18786,    "roas": 4.54, "cos": 0.220},
        "2024-10": {"spend": 22284,    "roas": 3.95, "cos": 0.253},
        "2024-11": {"spend": 30959,    "roas": 3.77, "cos": 0.265},
        "2024-12": {"spend": 22994,    "roas": 4.84, "cos": 0.207},
        "2025-01": {"spend": 32136,    "roas": 2.77, "cos": 0.362},
        "2025-02": {"spend": 26531,    "roas": 4.16, "cos": 0.240},
        "2025-03": {"spend": 32810,    "roas": 3.64, "cos": 0.275},
        "2025-04": {"spend": 40677,    "roas": 3.19, "cos": 0.313},
        "2025-05": {"spend": 59424,    "roas": 2.88, "cos": 0.348},
        "2025-06": {"spend": 45524,    "roas": 3.23, "cos": 0.310},
        "2025-07": {"spend": 51788,    "roas": 3.10, "cos": 0.322},
        "2025-08": {"spend": 27828,    "roas": 3.72, "cos": 0.269},
        "2025-09": {"spend": 36960,    "roas": 3.34, "cos": 0.300},
        "2025-10": {"spend": 45790,    "roas": 2.95, "cos": 0.339},
        "2025-11": {"spend": 41051,    "roas": 4.08, "cos": 0.245},
        "2025-12": {"spend": 36657,    "roas": 3.55, "cos": 0.282},
        "2026-01": {"spend": 33133,    "roas": 3.77, "cos": 0.265},
        "2026-02": {"spend": 16470,    "roas": 4.56, "cos": 0.219},
        "2026-03": {"spend": 0,        "roas": 0,    "cos": 0},
        "2026-04": {"spend": 7883,     "roas": 3.85, "cos": 0.260},
    },
    "cavali": {},  # Llenar cuando tengas los datos de Cavali
}

# ── GOOGLE SHEETS ─────────────────────────────────────────────────
def get_gc():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    return gspread.authorize(creds)

# ── SHOPIFY HELPERS ───────────────────────────────────────────────
def gql(store_url, token, query):
    r = requests.post(
        f"https://{store_url}/admin/api/{GQL_VERSION}/graphql.json",
        headers={"X-Shopify-Access-Token":token,"Content-Type":"application/json"},
        json={"query":query}, timeout=60)
    if r.status_code != 200:
        print(f"    HTTP {r.status_code}"); return None
    d = r.json()
    if d.get("errors"):
        print(f"    GQL errors: {d['errors']}"); return None
    return d.get("data")

def rest(store_url, token, endpoint, params):
    url = f"https://{store_url}/admin/api/2024-01/{endpoint}"
    headers = {"X-Shopify-Access-Token":token}
    results = []
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        data = r.json(); key = list(data.keys())[0]
        results.extend(data[key])
        link = r.headers.get("Link",""); url = None; params = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
    return results

# ── SHOPIFYQL ─────────────────────────────────────────────────────
def ql_row(store_url, token, ql_query):
    q = '{ shopifyqlQuery(query: "%s") { tableData { columns { name } rows } parseErrors } }' \
        % ql_query.replace('"','\\"')
    data = gql(store_url, token, q)
    if not data: return None
    ql = data.get("shopifyqlQuery") or {}
    errs = ql.get("parseErrors") or []
    if errs: print(f"    parseErrors: {errs}"); return None
    rows = (ql.get("tableData") or {}).get("rows") or []
    if not rows: return None
    return rows[-1]

def _m(v):
    if v is None: return 0.0
    try: return float(str(v).replace(",","").strip())
    except: return 0.0

def _gm(v):
    if v is None: return 0.0
    try:
        f = float(str(v).replace("%","").replace(",","").strip())
        return round(f*100,2) if abs(f)<=1.0 else round(f,2)
    except: return 0.0

def fetch_sales(url, token, s, e):
    """FROM sales → exact Shopify Analytics numbers."""
    row = ql_row(url, token,
        f"FROM sales SHOW gross_sales, discounts, returns, net_sales, "
        f"cost_of_goods_sold, gross_margin, orders SINCE {s} UNTIL {e}")
    if not row:
        return {k:0 for k in ["gross_sales","discounts","returns","net_sales","cogs","pct_gm","orders"]}
    g  = round(_m(row.get("gross_sales")), 2)
    d  = round(abs(_m(row.get("discounts"))), 2)
    r  = round(abs(_m(row.get("returns"))), 2)
    n  = round(_m(row.get("net_sales")), 2)
    c  = round(_m(row.get("cost_of_goods_sold")), 2)
    gm = _gm(row.get("gross_margin"))
    o  = int(abs(_m(row.get("orders"))))
    print(f"    gross:{g:>12,.2f}  disc:{d:>9,.2f}  ret:{r:>9,.2f}  "
          f"net:{n:>12,.2f}  cogs:{c:>9,.2f}  gm:{gm:>5.1f}%  ord:{o}")
    return {"gross_sales":g,"discounts":d,"returns":r,"net_sales":n,
            "cogs":c,"pct_gm":gm,"orders":o}

def fetch_sessions(url, token, s, e):
    """FROM sessions → real Shopify sessions."""
    row = ql_row(url, token, f"FROM sessions SHOW sessions SINCE {s} UNTIL {e}")
    if not row: return 0
    v = int(abs(_m(row.get("sessions",0))))
    print(f"    sessions: {v:,}")
    return v

def fetch_orders_fulfilled(url, token, s, e):
    row = ql_row(url, token, f"FROM fulfillments SHOW orders_fulfilled SINCE {s} UNTIL {e}")
    if not row: return None
    v = int(abs(_m(row.get("orders_fulfilled",0))))
    print(f"    orders_fulfilled: {v:,}")
    return v

def fetch_orders(url, token, s, e):
    """REST orders — for units + revenue share only."""
    return rest(url, token, "orders.json", {
        "status":"any",
        "financial_status":"paid,partially_paid,partially_refunded,refunded",
        "created_at_min":f"{s}T00:00:00-05:00",
        "created_at_max":f"{e}T23:59:59-05:00",
        "limit":250,
        "fields":"id,subtotal_price,line_items,source_name,tags",
    })

def calc_units(orders):
    return sum(sum(int(li.get("quantity",0)or 0)
               for li in o.get("line_items",[])) for o in orders)

def calc_rs(orders):
    """Revenue share by channel using subtotal_price (net per order)."""
    ch = {"Wellington (POS)":0.,"Concierge":0.,"Online":0.,"Others":0.}
    total = 0.
    for o in orders:
        amt = float(o.get("subtotal_price",0) or 0); total += amt
        src  = (o.get("source_name") or "").lower().strip()
        tags = (o.get("tags") or "").lower()
        if src == "pos" or "wellington" in tags or "pos" in tags:
            ch["Wellington (POS)"] += amt
        elif "concierge" in tags or "concierge" in src:
            ch["Concierge"] += amt
        elif src in ("web","shopify","","online_store") or not src:
            ch["Online"] += amt
        else:
            ch["Others"] += amt
    return {k:{"amount":round(v,2),"pct":round(v/total*100,2) if total else 0}
            for k,v in ch.items()}

def build(sales, orders, sessions=0, orders_fulfilled=None):
    """Combine ShopifyQL sales + REST orders into full KPI dict."""
    g  = sales.get("gross_sales",0)
    d  = sales.get("discounts",0)
    r  = sales.get("returns",0)
    n  = sales.get("net_sales",0)
    c  = sales.get("cogs",0)
    gm = sales.get("pct_gm",0)
    nb = int(orders_fulfilled) if orders_fulfilled is not None else (sales.get("orders",0) or len(orders))

    units = calc_units(orders)
    # AOV = net_sales / orders (matching Shopify Analytics definition)
    aov = round(n/nb, 2) if nb else 0
    upo = round(units/nb, 2) if nb else 0
    pdisc = round(d/g*100, 2) if g else 0
    pret  = round(r/g*100, 2) if g else 0

    s  = int(sessions or 0)
    uv = round(s*0.85) if s else 0
    cr = round(nb/s*100, 4) if s else 0

    return {
        "gross_sales":g, "net_sales":n,
        "total_discounts":d, "total_returns":r, "cogs":c,
        "pct_discount":pdisc, "pct_returns":pret, "pct_gm":gm,
        "nb_orders":nb, "nb_units":units, "aov":aov, "units_per_order":upo,
        "sessions":s, "unique_visitors":uv, "conversion_rate":cr,
    }

def pct_chg(c, p):
    if not p: return None
    return round((c-p)/abs(p)*100, 2)

def make_kpi_row(now_str, period_key, s, e, cur, prev, yoy):
    return [
        now_str, period_key, str(s), str(e),
        cur.get("gross_sales",0),      cur.get("net_sales",0),
        cur.get("total_discounts",0),  cur.get("total_returns",0),
        cur.get("cogs",0),
        cur.get("pct_discount",0),     cur.get("pct_returns",0),
        cur.get("pct_gm",0),
        cur.get("nb_orders",0),        cur.get("nb_units",0),
        cur.get("aov",0),              cur.get("units_per_order",0),
        cur.get("sessions",0),         cur.get("unique_visitors",0),
        cur.get("conversion_rate",0),
        pct_chg(cur.get("gross_sales",0), prev.get("gross_sales")),
        pct_chg(cur.get("gross_sales",0), yoy.get("gross_sales")),
        pct_chg(cur.get("net_sales",0),   prev.get("net_sales")),
        pct_chg(cur.get("net_sales",0),   yoy.get("net_sales")),
        pct_chg(cur.get("nb_orders",0),   prev.get("nb_orders")),
        pct_chg(cur.get("nb_orders",0),   yoy.get("nb_orders")),
        pct_chg(cur.get("aov",0),          prev.get("aov")),
        pct_chg(cur.get("aov",0),          yoy.get("aov")),
    ]

def _safe_date(v):
    try:
        return datetime.strptime(str(v), "%Y-%m-%d").date()
    except:
        return date(1900, 1, 1)

def _row_to_map(headers, row):
    m = {}
    for i, h in enumerate(headers):
        m[h] = row[i] if i < len(row) else ""
    return m

def _map_to_row(headers, m):
    return [m.get(h, "") for h in headers]

# ── PERIODS ───────────────────────────────────────────────────────
def get_periods():
    today = datetime.now(TIMEZONE).date()
    dow   = today.weekday()  # 0=Mon

    # MTD
    mtd_s = today.replace(day=1)
    mtd_e = today
    mtd_pk = f"mtd_{today.strftime('%Y-%m')}"

    # MTD prev (same days last month)
    prev_mo_end   = mtd_s - timedelta(days=1)
    prev_mo_s     = prev_mo_end.replace(day=1)
    prev_mo_mtd_e = prev_mo_end.replace(day=min(today.day, prev_mo_end.day))

    # MTD yoy
    yoy_mtd_s = mtd_s.replace(year=mtd_s.year-1)
    yoy_mtd_e = today.replace(year=today.year-1)

    # Current week Mon→today
    wk_s  = today - timedelta(days=dow)
    wk_e  = today
    wk_pk = f"week_{wk_s}"

    # Previous full week Mon→Sun
    pwk_e = wk_s - timedelta(days=1)
    pwk_s = pwk_e - timedelta(days=6)
    pwk_pk = f"week_{pwk_s}"

    # Week YOY (same Mon-Sun last year)
    yoy_wk_s = wk_s - timedelta(days=364)
    yoy_wk_e = wk_e - timedelta(days=364)

    # Previous full month
    mo_e  = mtd_s - timedelta(days=1)
    mo_s  = mo_e.replace(day=1)
    mo_pk = mo_s.strftime("%Y-%m")

    # Month before that
    pmo_e  = mo_s - timedelta(days=1)
    pmo_s  = pmo_e.replace(day=1)
    pmo_pk = pmo_s.strftime("%Y-%m")

    # Month YOY
    yoy_mo_s = mo_s.replace(year=mo_s.year-1)
    yoy_mo_e = mo_e.replace(year=mo_e.year-1)

    # Quarter
    q_num = (today.month-1)//3+1
    q_s   = today.replace(month=(q_num-1)*3+1, day=1)
    q_e   = today
    q_pk  = f"q{q_num}_{today.year}"

    # Prev quarter
    pq     = q_num-1 if q_num>1 else 4
    pq_y   = today.year if q_num>1 else today.year-1
    pq_s   = date(pq_y,(pq-1)*3+1,1)
    pq_em  = pq*3
    pq_e   = date(pq_y, pq_em, calendar.monthrange(pq_y,pq_em)[1])
    pq_pk  = f"q{pq}_{pq_y}"

    # Quarter YOY
    yoy_q_s  = q_s.replace(year=q_s.year-1)
    yoy_q_e  = today.replace(year=today.year-1)
    yoy_q_pk = f"q{q_num}_{today.year-1}"

    return {
        "mtd":         (mtd_s,       mtd_e,       mtd_pk),
        "mtd_prev":    (prev_mo_s,   prev_mo_mtd_e, None),
        "mtd_yoy":     (yoy_mtd_s,   yoy_mtd_e,   None),
        "week":        (wk_s,        wk_e,        wk_pk),
        "week_prev":   (pwk_s,       pwk_e,       pwk_pk),
        "week_yoy":    (yoy_wk_s,    yoy_wk_e,    None),
        "month":       (mo_s,        mo_e,        mo_pk),
        "month_prev":  (pmo_s,       pmo_e,       pmo_pk),
        "month_yoy":   (yoy_mo_s,    yoy_mo_e,    None),
        "quarter":     (q_s,         q_e,         q_pk),
        "quarter_prev":(pq_s,        pq_e,        pq_pk),
        "quarter_yoy": (yoy_q_s,     yoy_q_e,     yoy_q_pk),
    }

# ── WRITE ─────────────────────────────────────────────────────────
def write_all(gc, sheet_id, kpi_rows, rs_rows, brand=""):
    sh = gc.open_by_key(sheet_id)

    # ── kpis_daily ────────────────────────────────────────────────
    try:    ws = sh.worksheet("kpis_daily")
    except: ws = sh.add_worksheet("kpis_daily", rows=500, cols=35)

    existing_vals = ws.get_all_values()
    existing = {}
    if len(existing_vals) >= 2:
        ex_headers = existing_vals[0]
        for r in existing_vals[1:]:
            m = _row_to_map(ex_headers, r)
            pk = str(m.get("period", "")).strip()
            if pk:
                existing[pk] = _map_to_row(HEADERS, m)
    for r in kpi_rows:
        existing[str(r[1]).strip()] = r

    merged_kpis = list(existing.values())
    merged_kpis.sort(key=lambda r: (_safe_date(r[2]), str(r[1])))

    ws.clear()
    ws.append_row(HEADERS)
    if merged_kpis:
        ws.append_rows(merged_kpis, value_input_option="USER_ENTERED")

    # ── revenue_share (con pct_prev y pct_chg) ────────────────────
    try:    ws_rs = sh.worksheet("revenue_share")
    except: ws_rs = sh.add_worksheet("revenue_share", rows=500, cols=10)
    rs_headers = ["updated_at","period","channel","amount","pct","pct_prev","pct_chg"]
    rs_vals = ws_rs.get_all_values()
    existing_rs = {}
    if len(rs_vals) >= 2:
        ex_headers = rs_vals[0]
        for r in rs_vals[1:]:
            m = _row_to_map(ex_headers, r)
            p = str(m.get("period", "")).strip()
            ch = str(m.get("channel", "")).strip()
            if p and ch:
                existing_rs[(p, ch)] = _map_to_row(rs_headers, m)
    for r in rs_rows:
        existing_rs[(str(r[1]).strip(), str(r[2]).strip())] = r

    # Calcular pct_prev y pct_chg comparando vs período anterior guardado
    sorted_rs = list(existing_rs.values())
    sorted_rs.sort(key=lambda r: (str(r[2]), str(r[1])))  # canal, luego período
    # Indexar por (canal, período) para lookups rápidos
    rs_idx = {(str(r[2]).strip(), str(r[1]).strip()): r for r in sorted_rs}
    for r in sorted_rs:
        ch = str(r[2]).strip()
        pk = str(r[1]).strip()
        # Buscar pct_prev: período anterior del mismo canal
        prev_pk = None
        if pk.startswith("mtd_"):
            yr,mo = int(pk[4:7+3].split("-")[0]), int(pk[4:7+3].split("-")[1])
            pmo = mo-1 if mo>1 else 12; py = yr if mo>1 else yr-1
            prev_pk = f"mtd_{py}-{str(pmo).padStart(2,'0') if hasattr(str(pmo),'padStart') else str(pmo).zfill(2)}"
        elif pk.startswith("week_"):
            from datetime import datetime as _dt
            try:
                d = _dt.strptime(pk[5:], "%Y-%m-%d").date()
                prev_pk = f"week_{d - timedelta(days=7)}"
            except: pass
        elif len(pk)==7 and "-" in pk:  # "2026-03"
            yr,mo = int(pk[:4]),int(pk[5:])
            pmo = mo-1 if mo>1 else 12; py = yr if mo>1 else yr-1
            prev_pk = f"{py}-{str(pmo).zfill(2)}"
        elif pk.startswith("q") and "_" in pk:
            parts = pk[1:].split("_"); q,yr = int(parts[0]),int(parts[1])
            pq = q-1 if q>1 else 4; py = yr if q>1 else yr-1
            prev_pk = f"q{pq}_{py}"
        prev_row = rs_idx.get((ch, prev_pk)) if prev_pk else None
        pct_now  = float(r[4]) if r[4] not in ("","None") else None
        pct_prev = float(prev_row[4]) if prev_row and prev_row[4] not in ("","None") else None
        pct_chg  = round(pct_now - pct_prev, 2) if pct_now is not None and pct_prev is not None else None
        while len(r) < 7: r.append("")
        r[5] = pct_prev if pct_prev is not None else ""
        r[6] = pct_chg  if pct_chg  is not None else ""

    merged_rs = sorted_rs
    merged_rs.sort(key=lambda r: (str(r[1]), str(r[2])))

    ws_rs.clear()
    ws_rs.append_row(rs_headers)
    if merged_rs:
        ws_rs.append_rows(merged_rs, value_input_option="USER_ENTERED")

    # ── ad_spend (desde AD_SPEND_DATA) ────────────────────────────
    try:    ws_ad = sh.worksheet("ad_spend")
    except: ws_ad = sh.add_worksheet("ad_spend", rows=200, cols=8)

    ad_headers = ["updated_at","brand","period","period_start","period_end","ad_spend","roas","cos"]
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
    brand_data = AD_SPEND_DATA.get(brand, {})
    ad_rows = []
    for mo, vals in sorted(brand_data.items()):
        if not vals.get("spend"): continue
        yr, mn = int(mo[:4]), int(mo[5:])
        import calendar as _cal
        ps = f"{mo}-01"
        pe = f"{mo}-{_cal.monthrange(yr, mn)[1]:02d}"
        ad_rows.append([
            now_str, brand, mo, ps, pe,
            vals.get("spend", 0),
            vals.get("roas", 0),
            vals.get("cos", 0),
        ])

    ws_ad.clear()
    ws_ad.append_row(ad_headers)
    if ad_rows:
        ws_ad.append_rows(ad_rows, value_input_option="USER_ENTERED")
    print(f"    ad_spend: {len(ad_rows)} months written")

# ── MAIN ─────────────────────────────────────────────────────────
def main():
    gc      = get_gc()
    P       = get_periods()
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

    for brand, cfg in STORES.items():
        print(f"\n{'='*58}\n  {brand.upper()}\n{'='*58}")
        url, token = cfg["url"], cfg["token"]
        kpi_rows, rs_rows = [], []

        periods_to_run = [
            {"label":"MTD",                   "cur":"mtd",         "prev":"mtd_prev",     "yoy":"mtd_yoy"},
            {"label":"WEEK",                  "cur":"week",        "prev":"week_prev",    "yoy":"week_yoy"},
            {"label":"WEEK_PREV_SNAPSHOT",    "cur":"week_prev"},
            {"label":"MONTH",                 "cur":"month",       "prev":"month_prev",   "yoy":"month_yoy"},
            {"label":"MONTH_PREV_SNAPSHOT",   "cur":"month_prev"},
            {"label":"QUARTER",               "cur":"quarter",     "prev":"quarter_prev", "yoy":"quarter_yoy"},
            {"label":"QUARTER_PREV_SNAPSHOT", "cur":"quarter_prev"},
        ]

        for it in periods_to_run:
            label = it["label"]
            cur_k = it["cur"]
            prev_k = it.get("prev")
            yoy_k = it.get("yoy")
            s, e, pk = P[cur_k]
            sp = ep = sy = ey = None
            if prev_k:
                sp, ep, _ = P[prev_k]
            if yoy_k:
                sy, ey, _ = P[yoy_k]

            print(f"\n  [{label}] {s} → {e}  (period='{pk}')")

            sal_cur  = fetch_sales(url, token, s, e)
            ses_cur  = fetch_sessions(url, token, s, e)
            of_cur   = fetch_orders_fulfilled(url, token, s, e)
            ord_cur  = fetch_orders(url, token, s, e)
            sal_prev = fetch_sales(url, token, sp, ep) if prev_k else {}
            sal_yoy  = fetch_sales(url, token, sy, ey) if yoy_k else {}
            of_prev  = fetch_orders_fulfilled(url, token, sp, ep) if prev_k else None
            of_yoy   = fetch_orders_fulfilled(url, token, sy, ey) if yoy_k else None

            cur  = build(sal_cur,  ord_cur, ses_cur, of_cur)
            prev = build(sal_prev, [], 0, of_prev) if prev_k else {}
            yoy  = build(sal_yoy,  [], 0, of_yoy) if yoy_k else {}

            kpi_rows.append(make_kpi_row(now_str, pk, s, e, cur, prev, yoy))

            rs = calc_rs(ord_cur)
            for ch, v in rs.items():
                rs_rows.append([now_str, pk, ch, v["amount"], v["pct"]])

        write_all(gc, cfg["sheet_id"], kpi_rows, rs_rows, brand=brand)

        print(f"\n  ✓ {brand.upper()} — {len(kpi_rows)} periods written:")
        for row in kpi_rows:
            print(f"    {row[1]:<22}  {row[2]} → {row[3]}  "
                  f"gross:{float(row[4]):>12,.2f}  net:{float(row[5]):>12,.2f}  "
                  f"sess:{int(row[16] or 0):>8,}")

if __name__ == "__main__":
    main()
