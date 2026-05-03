"""
BACKFILL HISTÓRICO — Shopify → Google Sheets  (v4 — con gross_profit)
Jala datos desde 2024-01-01 hasta hoy para todos los meses y quarters.
FIXES v4:
  - run_ql: compatible con API 2025-10 (rows.cells en lugar de rows array plano)
  - fetch_sales: agrega gross_profit al query ShopifyQL
  - HEADERS_KPIS: alineados con pipeline.py v4 (sin columnas _prev/_yoy)
  - kpi_rows: columna gross_profit incluida en posición correcta
  - revenue_share: incluye gross_profit y gross_margin por canal
"""
import os, json, requests, gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta, date
import pytz

TIMEZONE    = pytz.timezone("America/Bogota")
GQL_VERSION = "2025-10"

STORES = {
    "corro":  {
        "url":      "equestrian-labs.myshopify.com",
        "token":    os.environ["SHOPIFY_TOKEN_CORRO"],
        "sheet_id": "1nq8xkDzowAvhD3wpMBlVK2M3FZSNS2DrAiPxz-Y2tdU",
    },
    "cavali": {
        "url":      "cavali-club.myshopify.com",
        "token":    os.environ["SHOPIFY_TOKEN_CAVALI"],
        "sheet_id": "1QUdJc2EIdElIX5nlLQxWxS98aAz-TgQnSg9glJpNtig",
    },
}
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Headers alineados con pipeline.py v4 — SIN columnas _prev/_yoy
HEADERS_KPIS = [
    "updated_at", "period", "period_start", "period_end",
    "gross_sales", "net_sales", "gross_profit",
    "total_discounts", "total_returns", "cogs",
    "pct_discount", "pct_returns", "pct_gm",
    "nb_orders", "nb_units", "aov", "units_per_order",
    "sessions", "unique_visitors", "conversion_rate",
    "new_customers", "returning_customers",
    "new_revenue", "returning_revenue",
    "new_gross_profit", "returning_gross_profit",
]

HEADERS_RS = [
    "updated_at", "period", "channel",
    "amount", "pct",
    "gross_profit", "gross_margin",
    "pct_prev", "pct_chg",
    "gp_is_estimate",
]

# ─────────────────────────────────────────────────────────────────
def get_gc():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    return gspread.authorize(creds)

# ─────────────────────────────────────────────────────────────────
# GQL / ShopifyQL
# ─────────────────────────────────────────────────────────────────
def gql_raw(store_url, token, query):
    r = requests.post(
        f"https://{store_url}/admin/api/{GQL_VERSION}/graphql.json",
        headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
        json={"query": query}, timeout=60,
    )
    if r.status_code != 200:
        print(f"    HTTP {r.status_code}"); return None
    d = r.json()
    if d.get("errors"):
        print(f"    GQL errors: {d['errors']}"); return None
    return d.get("data")

def run_ql(store_url, token, ql_query):
    """
    Ejecuta ShopifyQL. Compatible con API 2025-10 donde rows viene como
    lista de {cells: [{value: ...}]} en lugar de lista de listas.
    """
    q = ('{ shopifyqlQuery(query: "%s") '
         '{ tableData { columns { name } rows { cells } } parseErrors } }'
         % ql_query.replace('"', '\\"'))
    data = gql_raw(store_url, token, q)
    if not data: return None
    ql   = data.get("shopifyqlQuery") or {}
    errs = ql.get("parseErrors") or []
    if errs:
        print(f"    parseErrors: {errs}"); return None
    td   = ql.get("tableData") or {}
    cols = [c["name"] for c in (td.get("columns") or [])]
    rows = td.get("rows") or []
    if not rows: return None

    parsed = []
    for row in rows:
        if isinstance(row, dict):
            cells = row.get("cells") or []
            vals  = [c.get("value") if isinstance(c, dict) else c for c in cells]
        elif isinstance(row, list):
            vals = row
        else:
            continue
        parsed.append({cols[i]: (vals[i] if i < len(vals) else "") for i in range(len(cols))})

    return parsed[-1] if parsed else None

# ─────────────────────────────────────────────────────────────────
# REST helpers
# ─────────────────────────────────────────────────────────────────
def rest_get(store_url, token, endpoint, params):
    url = f"https://{store_url}/admin/api/2024-01/{endpoint}"
    headers = {"X-Shopify-Access-Token": token}
    results = []
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        results.extend(data[list(data.keys())[0]])
        link, url, params = r.headers.get("Link", ""), None, {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
    return results

def fetch_orders(store_url, token, start, end):
    return rest_get(store_url, token, "orders.json", {
        "status":           "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min":   f"{start}T00:00:00-05:00",
        "created_at_max":   f"{end}T23:59:59-05:00",
        "limit":            250,
        "fields":           "id,subtotal_price,line_items,source_name,tags",
    })

# ─────────────────────────────────────────────────────────────────
# Conversión de valores
# ─────────────────────────────────────────────────────────────────
def money(v):
    if v is None: return 0.0
    try:    return float(str(v).replace(",", "").strip())
    except: return 0.0

def gm_ratio(v):
    if v is None: return 0.0
    try:
        val = float(str(v).replace("%", "").replace(",", "").strip())
        return round(val * 100, 2) if abs(val) <= 1.0 else round(val, 2)
    except: return 0.0

# ─────────────────────────────────────────────────────────────────
# Fetch ShopifyQL
# ─────────────────────────────────────────────────────────────────
def fetch_sales(store_url, token, start, end):
    """Incluye gross_profit real desde ShopifyQL."""
    row = run_ql(store_url, token,
        f"FROM sales SHOW gross_sales, discounts, returns, net_sales, "
        f"cost_of_goods_sold, gross_profit, gross_margin, orders "
        f"SINCE {start} UNTIL {end}")
    empty = {k: 0 for k in
             ["gross_sales","discounts","returns","net_sales","cogs",
              "gross_profit","pct_gm","orders"]}
    if not row: return empty
    g  = round(money(row.get("gross_sales")),   2)
    d  = round(abs(money(row.get("discounts"))), 2)
    r  = round(abs(money(row.get("returns"))),   2)
    n  = round(money(row.get("net_sales")),      2)
    c  = round(money(row.get("cost_of_goods_sold")), 2)
    gp = round(money(row.get("gross_profit")),   2)
    gm = gm_ratio(row.get("gross_margin"))
    o  = int(abs(money(row.get("orders"))))
    print(f"    gross:{g:>12,.2f}  net:{n:>12,.2f}  gp:{gp:>10,.2f}  gm:{gm:>5.1f}%  ord:{o}")
    return {"gross_sales": g, "discounts": d, "returns": r, "net_sales": n,
            "cogs": c, "gross_profit": gp, "pct_gm": gm, "orders": o}

def fetch_sessions(store_url, token, start, end):
    row = run_ql(store_url, token,
        f"FROM sessions SHOW sessions SINCE {start} UNTIL {end}")
    if not row: return 0
    return int(abs(money(row.get("sessions", 0))))

def fetch_orders_fulfilled(store_url, token, start, end):
    row = run_ql(store_url, token,
        f"FROM fulfillments SHOW orders_fulfilled SINCE {start} UNTIL {end}")
    if not row: return None
    return int(abs(money(row.get("orders_fulfilled", 0))))

def fetch_new_vs_returning(store_url, token, start, end):
    """New vs returning customers — real desde ShopifyQL GROUP BY customer_type."""
    rows_raw = []
    q = ('{ shopifyqlQuery(query: "%s") '
         '{ tableData { columns { name } rows { cells } } parseErrors } }'
         % (f"FROM sales SHOW customer_type, net_sales, orders, gross_profit "
            f"SINCE {start} UNTIL {end} GROUP BY customer_type").replace('"', '\\"'))
    data = gql_raw(store_url, token, q)
    if data:
        ql   = data.get("shopifyqlQuery") or {}
        td   = (ql.get("tableData") or {})
        cols = [c["name"] for c in (td.get("columns") or [])]
        for row in (td.get("rows") or []):
            if isinstance(row, dict):
                cells = row.get("cells") or []
                vals  = [c.get("value") if isinstance(c, dict) else c for c in cells]
            elif isinstance(row, list):
                vals = row
            else:
                continue
            rows_raw.append({cols[i]: (vals[i] if i < len(vals) else "") for i in range(len(cols))})

    result = {"new_customers": 0, "returning_customers": 0,
              "new_revenue": 0.0, "returning_revenue": 0.0,
              "new_gross_profit": 0.0, "returning_gross_profit": 0.0}
    for row in rows_raw:
        ctype  = str(row.get("customer_type") or "").lower().strip()
        rev    = round(money(row.get("net_sales")), 2)
        orders = int(abs(money(row.get("orders", 0))))
        gp     = round(money(row.get("gross_profit", 0)), 2)
        if ctype in ("new", "first_time"):
            result["new_revenue"]      += rev
            result["new_customers"]    += orders
            result["new_gross_profit"] += gp
        elif ctype in ("returning", "repeat"):
            result["returning_revenue"]      += rev
            result["returning_customers"]    += orders
            result["returning_gross_profit"] += gp
    return result

# ─────────────────────────────────────────────────────────────────
# Build KPI dict
# ─────────────────────────────────────────────────────────────────
def calc_units(orders):
    return sum(
        sum(int(li.get("quantity", 0) or 0) for li in o.get("line_items", []))
        for o in orders
    )

def build(sal, orders, nvr, sessions=0, orders_fulfilled=None):
    g   = sal.get("gross_sales", 0)
    d   = sal.get("discounts",   0)
    r   = sal.get("returns",     0)
    n   = sal.get("net_sales",   0)
    c   = sal.get("cogs",        0)
    gp  = sal.get("gross_profit",0)
    gm  = sal.get("pct_gm",      0)
    nb  = int(orders_fulfilled) if orders_fulfilled is not None \
          else (sal.get("orders", 0) or len(orders))
    units = calc_units(orders)
    aov   = round(n / nb,    2) if nb    else 0
    upo   = round(units / nb, 2) if nb   else 0
    pdisc = round(d / g * 100, 2) if g   else 0
    pret  = round(r / g * 100, 2) if g   else 0
    s     = int(sessions or 0)
    uv    = round(s * 0.85) if s else 0
    cr    = round(nb / s * 100, 4) if s  else 0
    return {
        "gross_sales":            round(g,  2),
        "net_sales":              round(n,  2),
        "gross_profit":           round(gp, 2),
        "total_discounts":        round(d,  2),
        "total_returns":          round(r,  2),
        "cogs":                   round(c,  2),
        "pct_discount":           pdisc,
        "pct_returns":            pret,
        "pct_gm":                 gm,
        "nb_orders":              nb,
        "nb_units":               units,
        "aov":                    aov,
        "units_per_order":        upo,
        "sessions":               s,
        "unique_visitors":        uv,
        "conversion_rate":        cr,
        "new_customers":          nvr.get("new_customers",          0),
        "returning_customers":    nvr.get("returning_customers",    0),
        "new_revenue":            nvr.get("new_revenue",            0),
        "returning_revenue":      nvr.get("returning_revenue",      0),
        "new_gross_profit":       nvr.get("new_gross_profit",       0),
        "returning_gross_profit": nvr.get("returning_gross_profit", 0),
    }

def make_kpi_row(now_str, period_key, s, e, cur):
    return [
        now_str, period_key, str(s), str(e),
        cur.get("gross_sales",            0),
        cur.get("net_sales",              0),
        cur.get("gross_profit",           0),
        cur.get("total_discounts",        0),
        cur.get("total_returns",          0),
        cur.get("cogs",                   0),
        cur.get("pct_discount",           0),
        cur.get("pct_returns",            0),
        cur.get("pct_gm",                 0),
        cur.get("nb_orders",              0),
        cur.get("nb_units",               0),
        cur.get("aov",                    0),
        cur.get("units_per_order",        0),
        cur.get("sessions",               0),
        cur.get("unique_visitors",        0),
        cur.get("conversion_rate",        0),
        cur.get("new_customers",          0),
        cur.get("returning_customers",    0),
        cur.get("new_revenue",            0),
        cur.get("returning_revenue",      0),
        cur.get("new_gross_profit",       0),
        cur.get("returning_gross_profit", 0),
    ]

# ─────────────────────────────────────────────────────────────────
# Revenue share por canal (con GP estimado)
# ─────────────────────────────────────────────────────────────────
def calc_rs(orders, overall_gm_pct, now_str, period_key):
    ch = {"Wellington (POS)": 0., "Concierge": 0., "Online": 0., "Others": 0.}
    total = 0.
    for o in orders:
        amt   = float(o.get("subtotal_price", 0) or 0)
        total += amt
        src   = (o.get("source_name") or "").lower().strip()
        tags  = (o.get("tags") or "").lower()
        if src == "pos" or "wellington" in tags or "pos" in tags:
            ch["Wellington (POS)"] += amt
        elif "concierge" in tags or "concierge" in src:
            ch["Concierge"] += amt
        elif src in ("web", "shopify", "", "online_store") or not src:
            ch["Online"] += amt
        else:
            ch["Others"] += amt

    rows = []
    for k, v in ch.items():
        pct    = round(v / total * 100, 2) if total else 0
        est_gp = round(v * overall_gm_pct / 100, 2)
        rows.append([
            now_str, period_key, k,
            round(v, 2), pct,
            est_gp, round(overall_gm_pct, 2),
            "", "",          # pct_prev, pct_chg — vacío, el pipeline principal los calcula
            "True",          # gp_is_estimate
        ])
    return rows

# ─────────────────────────────────────────────────────────────────
# Date helpers
# ─────────────────────────────────────────────────────────────────
def last_day(y, m):
    return (date(y, m + 1, 1) - timedelta(days=1)) if m < 12 else date(y, 12, 31)

def monday_of(d):
    return d - timedelta(days=d.weekday())

def _safe_date(v):
    try:    return datetime.strptime(str(v), "%Y-%m-%d").date()
    except: return date(1900, 1, 1)

# ─────────────────────────────────────────────────────────────────
# Write to Sheets (upsert por period key)
# ─────────────────────────────────────────────────────────────────
def write_all(gc, sheet_id, kpi_rows, rs_rows):
    sh = gc.open_by_key(sheet_id)

    # ── kpis_daily ──────────────────────────────────────────────
    try:    ws = sh.worksheet("kpis_daily")
    except: ws = sh.add_worksheet("kpis_daily", rows=2000, cols=35)

    existing_vals = ws.get_all_values()
    existing = {}
    if len(existing_vals) >= 2:
        ex_h = existing_vals[0]
        for r in existing_vals[1:]:
            m  = {ex_h[i]: (r[i] if i < len(r) else "") for i in range(len(ex_h))}
            pk = str(m.get("period", "")).strip()
            if pk:
                existing[pk] = [m.get(h, "") for h in HEADERS_KPIS]

    for r in kpi_rows:
        existing[str(r[1]).strip()] = r

    merged = sorted(existing.values(),
                    key=lambda r: (_safe_date(r[2]), str(r[1])))
    ws.clear()
    ws.append_row(HEADERS_KPIS)
    for i in range(0, len(merged), 50):
        ws.append_rows(merged[i:i+50], value_input_option="USER_ENTERED")
        print(f"    KPI batch {i//50+1} written ({min(i+50,len(merged))}/{len(merged)})")

    # ── revenue_share ────────────────────────────────────────────
    try:    ws_rs = sh.worksheet("revenue_share")
    except: ws_rs = sh.add_worksheet("revenue_share", rows=2000, cols=12)

    existing_rs_vals = ws_rs.get_all_values()
    existing_rs = {}
    if len(existing_rs_vals) >= 2:
        ex_h = existing_rs_vals[0]
        for r in existing_rs_vals[1:]:
            m  = {ex_h[i]: (r[i] if i < len(r) else "") for i in range(len(ex_h))}
            pk = str(m.get("period",  "")).strip()
            ch = str(m.get("channel", "")).strip()
            if pk and ch:
                existing_rs[(pk, ch)] = [m.get(h, "") for h in HEADERS_RS]

    for r in rs_rows:
        existing_rs[(str(r[1]).strip(), str(r[2]).strip())] = r

    merged_rs = sorted(existing_rs.values(),
                       key=lambda r: (str(r[1]), str(r[2])))
    ws_rs.clear()
    ws_rs.append_row(HEADERS_RS)
    for i in range(0, len(merged_rs), 50):
        ws_rs.append_rows(merged_rs[i:i+50], value_input_option="USER_ENTERED")
        print(f"    RS batch {i//50+1} written ({min(i+50,len(merged_rs))}/{len(merged_rs)})")

    print(f"    ✓ kpis_daily: {len(merged)} rows | revenue_share: {len(merged_rs)} rows")

# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    gc      = get_gc()
    today   = datetime.now(TIMEZONE).date()
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
    y_start = 2024

    for brand, cfg in STORES.items():
        print(f"\n{'='*60}\n  {brand.upper()} — BACKFILL {y_start}-01 → {today}\n{'='*60}")
        url, token, sid = cfg["url"], cfg["token"], cfg["sheet_id"]
        kpi_rows, rs_rows = [], []

        # ── MESES: Jan 2024 → mes actual ────────────────────────
        for y in range(y_start, today.year + 1):
            m_end_range = today.month if y == today.year else 12
            for m in range(1, m_end_range + 1):
                mo_s = date(y, m, 1)
                mo_e = last_day(y, m) if (y < today.year or m < today.month) else today
                pk   = f"{y}-{str(m).zfill(2)}"
                mpk  = f"mtd_{pk}"

                print(f"\n  [MONTH] {pk}  ({mo_s} → {mo_e})")
                sal  = fetch_sales(url, token, mo_s, mo_e)
                sess = fetch_sessions(url, token, mo_s, mo_e)
                of   = fetch_orders_fulfilled(url, token, mo_s, mo_e)
                ords = fetch_orders(url, token, mo_s, mo_e)
                nvr  = fetch_new_vs_returning(url, token, mo_s, mo_e)
                cur  = build(sal, ords, nvr, sess, of)

                kpi_rows.append(make_kpi_row(now_str, pk,  mo_s, mo_e, cur))
                kpi_rows.append(make_kpi_row(now_str, mpk, mo_s, mo_e, cur))

                rs_rows.extend(calc_rs(ords, sal.get("pct_gm", 0), now_str, pk))
                rs_rows.extend(calc_rs(ords, sal.get("pct_gm", 0), now_str, mpk))

        # ── SEMANAS: lunes 2024-01-01 → hoy ─────────────────────
        wk_s = monday_of(date(y_start, 1, 1))
        while wk_s <= today:
            wk_e  = min(wk_s + timedelta(days=6), today)
            wk_pk = f"week_{wk_s}"

            print(f"\n  [WEEK]  {wk_pk}  ({wk_s} → {wk_e})")
            sal  = fetch_sales(url, token, wk_s, wk_e)
            sess = fetch_sessions(url, token, wk_s, wk_e)
            of   = fetch_orders_fulfilled(url, token, wk_s, wk_e)
            ords = fetch_orders(url, token, wk_s, wk_e)
            nvr  = fetch_new_vs_returning(url, token, wk_s, wk_e)
            cur  = build(sal, ords, nvr, sess, of)

            kpi_rows.append(make_kpi_row(now_str, wk_pk, wk_s, wk_e, cur))
            rs_rows.extend(calc_rs(ords, sal.get("pct_gm", 0), now_str, wk_pk))
            wk_s += timedelta(days=7)

        # ── QUARTERS: Q1 2024 → Q actual ────────────────────────
        for y in range(y_start, today.year + 1):
            max_q = ((today.month - 1) // 3) + 1 if y == today.year else 4
            for q in range(1, max_q + 1):
                q_s  = date(y, (q - 1) * 3 + 1, 1)
                q_em = q * 3
                q_e  = last_day(y, q_em) if (y < today.year or q < max_q) else today
                q_pk = f"q{q}_{y}"

                print(f"\n  [QTR]   {q_pk}  ({q_s} → {q_e})")
                sal  = fetch_sales(url, token, q_s, q_e)
                sess = fetch_sessions(url, token, q_s, q_e)
                of   = fetch_orders_fulfilled(url, token, q_s, q_e)
                ords = fetch_orders(url, token, q_s, q_e)
                nvr  = fetch_new_vs_returning(url, token, q_s, q_e)
                cur  = build(sal, ords, nvr, sess, of)

                kpi_rows.append(make_kpi_row(now_str, q_pk, q_s, q_e, cur))
                rs_rows.extend(calc_rs(ords, sal.get("pct_gm", 0), now_str, q_pk))

        # ── WRITE ────────────────────────────────────────────────
        print(f"\n  Writing {len(kpi_rows)} KPI rows + {len(rs_rows)} RS rows to Sheets...")
        write_all(gc, sid, kpi_rows, rs_rows)
        print(f"\n  ✓ {brand.upper()} backfill completo.")

if __name__ == "__main__":
    main()
