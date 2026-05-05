"""
BACKFILL HISTÓRICO v2.3 — Shopify → Google Sheets
===================================================
Jala datos desde 2024-01-01 hasta hoy para todos los meses, semanas y quarters.

FIXES v2.3:
- ql_run corregido para API 2025-10: `rows` devuelve OBJETOS JSON (dicts),
  NO rowData (eliminado) NI arrays de listas.
  Estructura real: tableData.rows = [{"col_name": "value", ...}, ...]
  Solo se pide `rows` en el query GQL, sin `rowData`.
- UNTIL date es e+1 cuando e == hoy para incluir el día actual en ShopifyQL
- new_vs_returning usa REST orders con customer.orders_count
- gross_profit viene directo de ShopifyQL (campo gross_profit)

EJECUCIÓN:
  python backfill.py
  (requiere env vars: SHOPIFY_TOKEN_CORRO, SHOPIFY_TOKEN_CAVALI, GOOGLE_CREDENTIALS)

COMPORTAMIENTO EN SHEETS:
  ⚠️  BORRA y reescribe completamente los tabs:
      kpis_daily, revenue_share, new_vs_returning
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

HEADERS_KPIS = [
    "updated_at", "period", "period_start", "period_end",
    "gross_sales", "net_sales", "total_discounts", "total_returns", "cogs",
    "gross_profit",
    "pct_discount", "pct_returns", "pct_gm",
    "nb_orders", "nb_units", "aov", "units_per_order",
    "sessions", "unique_visitors", "conversion_rate",
    "gross_sales_prev", "gross_sales_yoy",
    "net_sales_prev", "net_sales_yoy",
    "nb_orders_prev", "nb_orders_yoy",
    "aov_prev", "aov_yoy",
    "new_customers", "returning_customers",
    "new_revenue", "returning_revenue",
    "new_gross_profit", "returning_gross_profit",
]

# ─────────────────────────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────
def get_gc():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    return gspread.authorize(creds)

# ─────────────────────────────────────────────────────────────────
# FIX: UNTIL date helper
# ─────────────────────────────────────────────────────────────────
def _until(e):
    today = datetime.now(TIMEZONE).date()
    if e >= today:
        return e + timedelta(days=1)
    return e

# ─────────────────────────────────────────────────────────────────
# SHOPIFY GQL — raw request
# ─────────────────────────────────────────────────────────────────
def gql(store_url, token, query):
    url = f"https://{store_url}/admin/api/{GQL_VERSION}/graphql.json"
    r = requests.post(url,
        headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
        json={"query": query}, timeout=60)
    if r.status_code != 200:
        print(f"HTTP {r.status_code} — {r.text[:200]}")
        return None
    d = r.json()
    if d.get("errors"):
        print(f"GQL errors: {d['errors']}")
        return None
    return d.get("data")

# ─────────────────────────────────────────────────────────────────
# ql_run — FIX v2.3
# API 2025-10: tableData.rows = lista de OBJETOS {col: val, ...}
# Se pide solo `rows` (NO rowData, que fue eliminado de la API).
# ─────────────────────────────────────────────────────────────────
def ql_run(store_url, token, ql_query):
    """
    Ejecuta una ShopifyQL query.
    API 2025-10+: tableData.rows devuelve lista de objetos JSON.
    Devuelve lista de {columna: valor}.
    """
    escaped = ql_query.replace("\\", "\\\\").replace('"', '\\"')

    q = (
        f'{{ shopifyqlQuery(query: "{escaped}") {{ '
        f'tableData {{ columns {{ name }} rows }} '
        f'parseErrors {{ code message }} }} }}'
    )
    data = gql(store_url, token, q)

    if not data:
        return []

    ql_obj = data.get("shopifyqlQuery") or {}
    errs   = ql_obj.get("parseErrors") or []
    if errs:
        print(f"parseErrors: {errs}")
        return []

    td   = ql_obj.get("tableData") or {}
    rows = td.get("rows") or []

    if not rows:
        return []

    # API 2025-10: cada row ES un objeto dict {col_name: value}
    # Validamos que sea dict; si fuera lista (API antigua) hacemos fallback.
    if isinstance(rows[0], dict):
        # Formato correcto 2025-10 — devolver directo
        return rows

    # Fallback para API antigua donde rows era lista de listas
    cols = [c["name"] for c in (td.get("columns") or [])]
    if not cols:
        return []

    result = []
    for row in rows:
        if isinstance(row, list):
            result.append({cols[i]: (row[i] if i < len(row) else "") for i in range(len(cols))})
        elif isinstance(row, str):
            try:
                parsed = json.loads(row)
                if isinstance(parsed, list):
                    result.append({cols[i]: (parsed[i] if i < len(parsed) else "") for i in range(len(cols))})
                elif isinstance(parsed, dict):
                    result.append(parsed)
            except Exception:
                pass
    return result


def run_ql(store_url, token, ql_query):
    rows = ql_run(store_url, token, ql_query)
    return rows[-1] if rows else None


def rest_get(store_url, token, endpoint, params):
    url     = f"https://{store_url}/admin/api/2024-01/{endpoint}"
    headers = {"X-Shopify-Access-Token": token}
    results = []
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        results.extend(data[list(data.keys())[0]])
        link   = r.headers.get("Link", "")
        url    = None
        params = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
    return results


def money(v):
    if v is None:
        return 0.0
    try:
        return float(str(v).replace(",", "").strip())
    except Exception:
        return 0.0


def gm_ratio(v):
    if v is None:
        return 0.0
    try:
        val = float(str(v).replace("%", "").replace(",", "").strip())
        return round(val * 100, 2) if abs(val) <= 1.0 else round(val, 2)
    except Exception:
        return 0.0

# ─────────────────────────────────────────────────────────────────
# FETCH: SALES
# ─────────────────────────────────────────────────────────────────
def fetch_sales(store_url, token, start, end):
    e_ql = _until(end)
    row  = run_ql(store_url, token,
        f"FROM sales SHOW gross_sales, discounts, returns, net_sales, "
        f"cost_of_goods_sold, gross_profit, gross_margin, orders "
        f"SINCE {start} UNTIL {e_ql}")

    empty = {k: None for k in ["gross_sales","discounts","returns","net_sales",
                                "cogs","gross_profit","pct_gm","orders"]}
    if not row:
        print(f"    ⚠ fetch_sales: sin datos para {start} → {e_ql}")
        return empty

    g  = round(money(row.get("gross_sales")),        2)
    d  = round(abs(money(row.get("discounts"))),      2)
    r  = round(abs(money(row.get("returns"))),        2)
    n  = round(money(row.get("net_sales")),           2)
    c  = round(money(row.get("cost_of_goods_sold")),  2)
    gp = round(money(row.get("gross_profit")),        2)
    gm = gm_ratio(row.get("gross_margin"))
    o  = int(abs(money(row.get("orders"))))

    print(f"    gross:{g:>12,.2f}  net:{n:>12,.2f}  gp:{gp:>10,.2f}  "
          f"gm:{gm:>5.1f}%  orders:{o}  [UNTIL {e_ql}]")

    return {"gross_sales": g, "discounts": d, "returns": r, "net_sales": n,
            "cogs": c, "gross_profit": gp, "pct_gm": gm, "orders": o}

# ─────────────────────────────────────────────────────────────────
# FETCH: SESSIONS
# ─────────────────────────────────────────────────────────────────
def fetch_sessions(store_url, token, start, end):
    e_ql = _until(end)
    row  = run_ql(store_url, token,
        f"FROM sessions SHOW sessions SINCE {start} UNTIL {e_ql}")
    if not row:
        return 0
    return int(abs(money(row.get("sessions", 0))))

# ─────────────────────────────────────────────────────────────────
# FETCH: ORDERS FULFILLED
# ─────────────────────────────────────────────────────────────────
def fetch_orders_fulfilled(store_url, token, start, end):
    e_ql = _until(end)
    row  = run_ql(store_url, token,
        f"FROM fulfillments SHOW orders_fulfilled SINCE {start} UNTIL {e_ql}")
    if not row:
        return None
    return int(abs(money(row.get("orders_fulfilled", 0))))

# ─────────────────────────────────────────────────────────────────
# FETCH: REST ORDERS
# ─────────────────────────────────────────────────────────────────
def fetch_orders(store_url, token, start, end):
    return rest_get(store_url, token, "orders.json", {
        "status":           "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min":   f"{start}T00:00:00-05:00",
        "created_at_max":   f"{end}T23:59:59-05:00",
        "limit":            250,
        "fields":           "id,subtotal_price,line_items,source_name,tags,customer",
    })

# ─────────────────────────────────────────────────────────────────
# NEW vs RETURNING via REST
# ─────────────────────────────────────────────────────────────────
def calc_new_returning(orders):
    new_rev = ret_rev = 0.0
    new_nc  = ret_nc  = 0
    for o in orders:
        amt      = float(o.get("subtotal_price", 0) or 0)
        customer = o.get("customer") or {}
        count    = int(customer.get("orders_count", 1) or 1)
        if count <= 1:
            new_nc  += 1
            new_rev += amt
        else:
            ret_nc  += 1
            ret_rev += amt
    return {
        "new_customers":       new_nc,
        "returning_customers": ret_nc,
        "new_revenue":         round(new_rev, 2),
        "returning_revenue":   round(ret_rev, 2),
    }


def calc_units(orders):
    return sum(
        sum(int(li.get("quantity", 0) or 0) for li in o.get("line_items", []))
        for o in orders
    )


def calc_rs(orders):
    ch    = {"Wellington (POS)": 0., "Concierge": 0., "Online": 0., "Others": 0.}
    total = 0.
    for o in orders:
        amt    = float(o.get("subtotal_price", 0) or 0)
        total += amt
        src    = (o.get("source_name") or "").lower().strip()
        tags   = (o.get("tags") or "").lower()
        if src == "pos" or "wellington" in tags or "pos" in tags:
            ch["Wellington (POS)"] += amt
        elif "concierge" in tags or "concierge" in src:
            ch["Concierge"] += amt
        elif src in ("web", "shopify", "", "online_store") or not src:
            ch["Online"] += amt
        else:
            ch["Others"] += amt
    return {k: {"amount": round(v, 2), "pct": round(v / total * 100, 2) if total else 0}
            for k, v in ch.items()}

# ─────────────────────────────────────────────────────────────────
# BUILD KPI DICT
# ─────────────────────────────────────────────────────────────────
def build(ql, orders, sessions=0, orders_fulfilled=None):
    if ql.get("gross_sales") is not None:
        g  = ql["gross_sales"]
        d  = ql["discounts"]
        r  = ql["returns"]
        n  = ql["net_sales"]
        c  = ql["cogs"] or 0
        gp = ql.get("gross_profit") or 0
        gm = ql["pct_gm"] or 0
        nb = int(orders_fulfilled) if orders_fulfilled is not None else (ql["orders"] or len(orders))
    else:
        nb = int(orders_fulfilled) if orders_fulfilled is not None else len(orders)
        g  = sum(float(o.get("subtotal_price", 0) or 0) for o in orders)
        d  = r = c = gp = gm = 0.0
        n  = g

    nvr   = calc_new_returning(orders)
    units = calc_units(orders)
    aov   = round(n / nb,     2) if nb   else 0
    upo   = round(units / nb, 2) if nb   else 0
    pdisc = round(d / g * 100, 2) if g   else 0
    pret  = round(r / g * 100, 2) if g   else 0
    sess  = int(sessions or 0)

    gm_rate = gm / 100 if gm > 0 else (gp / n if n > 0 else 0)
    new_gp  = round(nvr["new_revenue"]       * gm_rate, 2)
    ret_gp  = round(nvr["returning_revenue"] * gm_rate, 2)

    return {
        "gross_sales":            round(g,  2),
        "net_sales":              round(n,  2),
        "total_discounts":        round(d,  2),
        "total_returns":          round(r,  2),
        "cogs":                   round(c,  2),
        "gross_profit":           round(gp, 2),
        "pct_discount":           pdisc,
        "pct_returns":            pret,
        "pct_gm":                 gm,
        "nb_orders":              nb,
        "nb_units":               units,
        "aov":                    aov,
        "units_per_order":        upo,
        "sessions":               sess,
        "unique_visitors":        round(sess * 0.85) if sess else 0,
        "conversion_rate":        round(nb / sess * 100, 4) if sess else 0,
        "new_customers":          nvr["new_customers"],
        "returning_customers":    nvr["returning_customers"],
        "new_revenue":            nvr["new_revenue"],
        "returning_revenue":      nvr["returning_revenue"],
        "new_gross_profit":       new_gp,
        "returning_gross_profit": ret_gp,
    }


def pct(c, p):
    if not p:
        return None
    return round((c - p) / p * 100, 2)


def last_day(y, m):
    return (date(y, m + 1, 1) - timedelta(days=1)) if m < 12 else date(y, 12, 31)


def monday_of(d):
    return d - timedelta(days=d.weekday())

# ─────────────────────────────────────────────────────────────────
# BUILD KPI ROW
# ─────────────────────────────────────────────────────────────────
def make_kpi_row(now_str, label, period_start, period_end, cur, prev, yoy):
    return [
        now_str, label, str(period_start), str(period_end),
        cur.get("gross_sales",            0),
        cur.get("net_sales",              0),
        cur.get("total_discounts",        0),
        cur.get("total_returns",          0),
        cur.get("cogs",                   0),
        cur.get("gross_profit",           0),
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
        pct(cur.get("gross_sales", 0), prev.get("gross_sales")),
        pct(cur.get("gross_sales", 0), yoy.get("gross_sales")),
        pct(cur.get("net_sales",   0), prev.get("net_sales")),
        pct(cur.get("net_sales",   0), yoy.get("net_sales")),
        pct(cur.get("nb_orders",   0), prev.get("nb_orders")),
        pct(cur.get("nb_orders",   0), yoy.get("nb_orders")),
        pct(cur.get("aov",         0), prev.get("aov")),
        pct(cur.get("aov",         0), yoy.get("aov")),
        cur.get("new_customers",          0),
        cur.get("returning_customers",    0),
        cur.get("new_revenue",            0),
        cur.get("returning_revenue",      0),
        cur.get("new_gross_profit",       0),
        cur.get("returning_gross_profit", 0),
    ]

# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    gc      = get_gc()
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")
    today   = datetime.now(TIMEZONE).date()

    # ── Limpiar tabs antes del backfill completo ──────────────────
    print("Limpiando tabs existentes...")
    for brand, cfg in STORES.items():
        sh = gc.open_by_key(cfg["sheet_id"])
        for tab, headers in [
            ("kpis_daily",      HEADERS_KPIS),
            ("revenue_share",   ["updated_at","period","channel","amount","pct"]),
            ("new_vs_returning",["updated_at","period","period_start","period_end",
                                  "new_customers","returning_customers",
                                  "new_revenue","returning_revenue",
                                  "new_gross_profit","returning_gross_profit"]),
        ]:
            try:
                ws = sh.worksheet(tab)
                ws.clear()
                ws.append_row(headers)
                print(f"  ✓ Limpiado {tab} para {brand}")
            except Exception as ex:
                print(f"  ✗ No se pudo limpiar {tab} para {brand}: {ex}")

    for brand, cfg in STORES.items():
        print(f"\n{'='*55}\n  {brand.upper()} — BACKFILL 2024-01 → {today}\n{'='*55}")
        url, token, sid = cfg["url"], cfg["token"], cfg["sheet_id"]
        kpi_rows, rs_rows, nvr_rows = [], [], []

        # ── MONTHLY ──────────────────────────────────────────────
        for y in range(2024, today.year + 1):
            m_start = 1
            m_end   = today.month if y == today.year else 12
            for m in range(m_start, m_end + 1):
                mo_start     = date(y, m, 1)
                mo_end       = last_day(y, m) if (y < today.year or m < today.month) else today
                period_label = f"{y}-{str(m).zfill(2)}"
                mtd_label    = f"mtd_{period_label}"

                print(f"\n  Month {period_label} ({mo_start} → {mo_end})")

                ql_cur = fetch_sales(url, token, mo_start, mo_end)
                s_cur  = fetch_sessions(url, token, mo_start, mo_end)
                of_cur = fetch_orders_fulfilled(url, token, mo_start, mo_end)
                o_cur  = fetch_orders(url, token, mo_start, mo_end)
                cur    = build(ql_cur, o_cur, s_cur, of_cur)

                # Prev month
                pm = m - 1 if m > 1 else 12
                py = y if m > 1 else y - 1
                prev_start = date(py, pm, 1)
                prev_end   = last_day(py, pm)
                ql_prev    = fetch_sales(url, token, prev_start, prev_end)
                of_prev    = fetch_orders_fulfilled(url, token, prev_start, prev_end)
                o_prev     = fetch_orders(url, token, prev_start, prev_end)
                prev       = build(ql_prev, o_prev, 0, of_prev)

                # YOY
                if y > 2024:
                    yoy_start = date(y - 1, m, 1)
                    yoy_end   = last_day(y - 1, m)
                    ql_yoy    = fetch_sales(url, token, yoy_start, yoy_end)
                    of_yoy    = fetch_orders_fulfilled(url, token, yoy_start, yoy_end)
                    o_yoy     = fetch_orders(url, token, yoy_start, yoy_end)
                    yoy       = build(ql_yoy, o_yoy, 0, of_yoy)
                else:
                    yoy = {}

                # Revenue share
                rs = calc_rs(o_cur)
                for ch, v in rs.items():
                    rs_rows.append([now_str, period_label, ch, v["amount"], v["pct"]])
                    rs_rows.append([now_str, mtd_label,    ch, v["amount"], v["pct"]])

                # New vs returning
                nvr     = calc_new_returning(o_cur)
                gm_rate = (cur.get("pct_gm", 0) or 0) / 100
                for lbl in [period_label, mtd_label]:
                    nvr_rows.append([
                        now_str, lbl, str(mo_start), str(mo_end),
                        nvr["new_customers"],
                        nvr["returning_customers"],
                        nvr["new_revenue"],
                        nvr["returning_revenue"],
                        round(nvr["new_revenue"]       * gm_rate, 2),
                        round(nvr["returning_revenue"] * gm_rate, 2),
                    ])

                kpi_rows.append(make_kpi_row(now_str, period_label, mo_start, mo_end, cur, prev, yoy))
                kpi_rows.append(make_kpi_row(now_str, mtd_label,    mo_start, mo_end, cur, prev, yoy))

        # ── WEEKLY ───────────────────────────────────────────────
        wk_start = monday_of(date(2024, 1, 1))
        while wk_start <= today:
            wk_end   = min(wk_start + timedelta(days=6), today)
            wk_label = f"week_{wk_start}"

            print(f"\n  Week {wk_label} ({wk_start} → {wk_end})")

            ql_cur = fetch_sales(url, token, wk_start, wk_end)
            s_cur  = fetch_sessions(url, token, wk_start, wk_end)
            of_cur = fetch_orders_fulfilled(url, token, wk_start, wk_end)
            o_cur  = fetch_orders(url, token, wk_start, wk_end)
            cur    = build(ql_cur, o_cur, s_cur, of_cur)

            pws    = wk_start - timedelta(days=7)
            pwe    = pws + timedelta(days=(wk_end - wk_start).days)
            ql_prev = fetch_sales(url, token, pws, pwe)
            of_prev = fetch_orders_fulfilled(url, token, pws, pwe)
            o_prev  = fetch_orders(url, token, pws, pwe)
            prev    = build(ql_prev, o_prev, 0, of_prev)

            yws    = wk_start - timedelta(days=364)
            ywe    = wk_end   - timedelta(days=364)
            ql_yoy = fetch_sales(url, token, yws, ywe)
            of_yoy = fetch_orders_fulfilled(url, token, yws, ywe)
            o_yoy  = fetch_orders(url, token, yws, ywe)
            yoy    = build(ql_yoy, o_yoy, 0, of_yoy)

            rs  = calc_rs(o_cur)
            nvr = calc_new_returning(o_cur)
            gm_rate = (cur.get("pct_gm", 0) or 0) / 100

            for ch, v in rs.items():
                rs_rows.append([now_str, wk_label, ch, v["amount"], v["pct"]])

            nvr_rows.append([
                now_str, wk_label, str(wk_start), str(wk_end),
                nvr["new_customers"],
                nvr["returning_customers"],
                nvr["new_revenue"],
                nvr["returning_revenue"],
                round(nvr["new_revenue"]       * gm_rate, 2),
                round(nvr["returning_revenue"] * gm_rate, 2),
            ])

            kpi_rows.append(make_kpi_row(now_str, wk_label, wk_start, wk_end, cur, prev, yoy))
            wk_start += timedelta(days=7)

        # ── QUARTERLY ────────────────────────────────────────────
        for y in range(2024, today.year + 1):
            max_q = ((today.month - 1) // 3) + 1 if y == today.year else 4
            for q in range(1, max_q + 1):
                q_start = date(y, (q - 1) * 3 + 1, 1)
                q_end_m = q * 3
                q_end   = last_day(y, q_end_m) if (y < today.year or q < max_q) else today
                q_label = f"q{q}_{y}"

                print(f"\n  Quarter {q_label} ({q_start} → {q_end})")

                ql_q   = fetch_sales(url, token, q_start, q_end)
                s_q    = fetch_sessions(url, token, q_start, q_end)
                of_q   = fetch_orders_fulfilled(url, token, q_start, q_end)
                o_q    = fetch_orders(url, token, q_start, q_end)
                cur_q  = build(ql_q, o_q, s_q, of_q)

                pq_    = q - 1 if q > 1 else 4
                py_    = y if q > 1 else y - 1
                pq_start = date(py_, (pq_ - 1) * 3 + 1, 1)
                pq_end   = last_day(py_, pq_ * 3)
                ql_pq    = fetch_sales(url, token, pq_start, pq_end)
                of_pq    = fetch_orders_fulfilled(url, token, pq_start, pq_end)
                o_pq     = fetch_orders(url, token, pq_start, pq_end)
                prev_q   = build(ql_pq, o_pq, 0, of_pq)

                if y > 2024:
                    yq_start = date(y - 1, (q - 1) * 3 + 1, 1)
                    yq_end   = last_day(y - 1, q * 3)
                    ql_yq    = fetch_sales(url, token, yq_start, yq_end)
                    of_yq    = fetch_orders_fulfilled(url, token, yq_start, yq_end)
                    o_yq     = fetch_orders(url, token, yq_start, yq_end)
                    yoy_q    = build(ql_yq, o_yq, 0, of_yq)
                else:
                    yoy_q = {}

                rs_q  = calc_rs(o_q)
                nvr_q = calc_new_returning(o_q)
                gm_rate = (cur_q.get("pct_gm", 0) or 0) / 100

                for ch, v in rs_q.items():
                    rs_rows.append([now_str, q_label, ch, v["amount"], v["pct"]])

                nvr_rows.append([
                    now_str, q_label, str(q_start), str(q_end),
                    nvr_q["new_customers"],
                    nvr_q["returning_customers"],
                    nvr_q["new_revenue"],
                    nvr_q["returning_revenue"],
                    round(nvr_q["new_revenue"]       * gm_rate, 2),
                    round(nvr_q["returning_revenue"] * gm_rate, 2),
                ])

                kpi_rows.append(make_kpi_row(now_str, q_label, q_start, q_end, cur_q, prev_q, yoy_q))

        # ── WRITE ────────────────────────────────────────────────
        print(f"\n  Escribiendo {len(kpi_rows)} filas KPI...")
        sh    = gc.open_by_key(sid)
        ws    = sh.worksheet("kpis_daily")
        ws_rs = sh.worksheet("revenue_share")

        try:
            ws_nvr = sh.worksheet("new_vs_returning")
        except Exception:
            ws_nvr = sh.add_worksheet("new_vs_returning", rows=600, cols=12)
            ws_nvr.append_row([
                "updated_at","period","period_start","period_end",
                "new_customers","returning_customers",
                "new_revenue","returning_revenue",
                "new_gross_profit","returning_gross_profit",
            ])

        for i in range(0, len(kpi_rows), 50):
            ws.append_rows(kpi_rows[i:i+50])
            print(f"  KPI batch {i//50+1} escrito")

        for i in range(0, len(rs_rows), 50):
            ws_rs.append_rows(rs_rows[i:i+50])
            print(f"  RS batch {i//50+1} escrito")

        for i in range(0, len(nvr_rows), 50):
            ws_nvr.append_rows(nvr_rows[i:i+50])
            print(f"  NVR batch {i//50+1} escrito")

        print(f"\n  ✓ {brand.upper()} completado: "
              f"{len(kpi_rows)} KPI + {len(rs_rows)} RS + {len(nvr_rows)} NVR filas")


if __name__ == "__main__":
    main()
