"""
Pipeline CORRO / CAVALI — Shopify Analytics → Google Sheets
============================================================
Fuente de verdad: ShopifyQL via GraphQL Admin API 2025-10
  • gross_sales, net_sales, discounts, returns, cogs, gross_margin
  • sessions
Complemento REST (orders 2024-01):
  • nb_units (suma de quantities)
  • revenue_share por canal (POS / Concierge / Online / Others)

Definiciones Shopify (igual que Shopify Analytics):
  gross_sales  = precio de lista de line items (sin descuentos, sin devoluciones)
  discounts    = descuentos aplicados (valor negativo en Shopify → lo guardamos positivo)
  returns      = valor de devoluciones (valor negativo → lo guardamos positivo)
  net_sales    = gross_sales − discounts − returns
  cogs         = cost_of_goods_sold (requiere costo cargado en productos)
  gross_margin = (net_sales − cogs) / net_sales × 100
  AOV Shopify  = (gross_sales − discounts) / orders  [sin returns, sin post-edits]
"""

import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
import pytz

TIMEZONE = pytz.timezone("America/Bogota")

# ── Versiones de API ──────────────────────────────────────────
# shopifyqlQuery fue lanzado en la version 2025-10 del GraphQL Admin API.
# El error "Field doesn't exist on type QueryRoot" ocurre cuando se usa
# cualquier version anterior a 2025-10.
GQL_VERSION  = "2025-10"
REST_VERSION = "2024-01"

STORES = {
    "corro": {
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
    "pct_discount", "pct_returns", "pct_gm",
    "nb_orders", "nb_units", "aov", "units_per_order",
    "sessions", "unique_visitors", "conversion_rate",
    "gross_sales_mom", "gross_sales_yoy",
    "net_sales_mom",   "net_sales_yoy",
    "nb_orders_mom",   "nb_orders_yoy",
    "aov_mom",         "aov_yoy",
]


# ══════════════════════════════════════════════════════════════
# Google Sheets
# ══════════════════════════════════════════════════════════════
def get_gc():
    creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    creds = Credentials.from_service_account_info(creds_json, scopes=SCOPES)
    return gspread.authorize(creds)


# ══════════════════════════════════════════════════════════════
# HTTP helpers
# ══════════════════════════════════════════════════════════════
def shopify_graphql(store_url, token, query):
    """POST al GraphQL Admin API 2025-10. Retorna data dict o None."""
    url = f"https://{store_url}/admin/api/{GQL_VERSION}/graphql.json"
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
    }
    r = requests.post(url, headers=headers, json={"query": query}, timeout=60)
    if r.status_code != 200:
        print(f"    HTTP {r.status_code}: {r.text[:300]}")
        return None
    d = r.json()
    if d.get("errors"):
        print(f"    GraphQL errors: {d['errors']}")
        return None
    return d.get("data")


def shopify_rest_paginate(store_url, token, endpoint, params):
    """Pagina automaticamente el REST Admin API."""
    url = f"https://{store_url}/admin/api/{REST_VERSION}/{endpoint}"
    headers = {"X-Shopify-Access-Token": token}
    results = []
    while url:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        key = list(data.keys())[0]
        results.extend(data[key])
        link   = r.headers.get("Link", "")
        url    = None
        params = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
    return results


# ══════════════════════════════════════════════════════════════
# ShopifyQL helpers
# ══════════════════════════════════════════════════════════════
def _parse_ql_totals(data, query_key="shopifyqlQuery"):
    """
    Extrae la fila de totales (ultima fila con WITH TOTALS) de una respuesta
    ShopifyQL 2025-10.
    Estructura: tableData.columns[{name,dataType}] + tableData.rows[{col:val}]
    Retorna dict {col_name: value_str} o None.
    """
    if not data:
        return None
    ql = data.get(query_key, {})
    if not ql:
        return None
    # En 2025-10 parseErrors es un String scalar (no objeto)
    # Viene vacío "" o null si no hay errores
    parse_errors = ql.get("parseErrors") or ""
    if parse_errors:
        print(f"    ShopifyQL parseErrors: {parse_errors}")
        return None
    table = ql.get("tableData")
    if not table:
        return None
    rows = table.get("rows", [])
    if not rows:
        print("    ShopifyQL: sin filas devueltas")
        return None
    # WITH TOTALS -> ultimo row es la fila de totales globales
    return rows[-1]


def _money(v):
    """String de dinero Shopify -> float. Ej: '1234.50' -> 1234.5"""
    if v is None:
        return 0.0
    return float(str(v).replace(",", "").strip() or 0)


def _pct(v):
    """
    Porcentaje Shopify -> float escala 0-100.
    ShopifyQL devuelve gross_margin en escala 0-1 (ej: 0.3250 = 32.50%).
    """
    if v is None:
        return 0.0
    val = float(str(v).replace("%", "").strip() or 0)
    return round(val * 100, 2) if abs(val) < 1.0 else round(val, 2)


# ══════════════════════════════════════════════════════════════
# ShopifyQL — metricas financieras
# ══════════════════════════════════════════════════════════════
def fetch_sales_ql(store_url, token, start_date, end_date):
    """
    ShopifyQL FROM sales: metricas financieras exactas de Shopify Analytics.

    Campos devueltos:
      gross_sales        = precio de lista (sin descuentos ni returns)
      discounts          = descuentos (Shopify los devuelve negativos -> abs)
      returns            = devoluciones (negativo -> abs)
      net_sales          = gross − discounts − returns
      cost_of_goods_sold = COGS
      gross_margin       = % margen bruto (escala 0-1 en API)
      orders             = cantidad de ordenes

    Sintaxis ShopifyQL 2025-10: SINCE <fecha> UNTIL <fecha>
    """
    query = """
    {
      shopifyqlQuery(query: "FROM sales SHOW gross_sales, discounts, returns, net_sales, cost_of_goods_sold, gross_margin, orders SINCE %s UNTIL %s WITH TOTALS") {
        tableData {
          columns { name dataType }
          rows
        }
        parseErrors
      }
    }
    """ % (start_date, end_date)

    empty = {
        "gross_sales": None, "discounts": None, "returns": None,
        "net_sales": None,   "cogs": None,       "pct_gm": None,
        "orders": None,
    }

    data = shopify_graphql(store_url, token, query)
    row  = _parse_ql_totals(data)
    if row is None:
        return empty

    gross  = round(_money(row.get("gross_sales")),        2)
    disc   = round(abs(_money(row.get("discounts"))),     2)
    ret    = round(abs(_money(row.get("returns"))),       2)
    net    = round(_money(row.get("net_sales")),          2)
    cogs   = round(_money(row.get("cost_of_goods_sold")), 2)
    pct_gm = _pct(row.get("gross_margin"))
    orders = int(_money(row.get("orders")))

    print(
        f"    ✓ gross:{gross:,.2f}  disc:{disc:,.2f}  ret:{ret:,.2f}  "
        f"net:{net:,.2f}  cogs:{cogs:,.2f}  gm:{pct_gm}%  orders:{orders}"
    )
    return {
        "gross_sales": gross, "discounts": disc, "returns": ret,
        "net_sales":   net,   "cogs": cogs,       "pct_gm": pct_gm,
        "orders":      orders,
    }


def fetch_sessions_ql(store_url, token, start_date, end_date):
    """ShopifyQL FROM sessions: total de sesiones del periodo."""
    query = """
    {
      shopifyqlQuery(query: "FROM sessions SHOW sessions SINCE %s UNTIL %s WITH TOTALS") {
        tableData {
          columns { name dataType }
          rows
        }
        parseErrors
      }
    }
    """ % (start_date, end_date)

    data = shopify_graphql(store_url, token, query)
    row  = _parse_ql_totals(data)
    if row is None:
        return 0
    return int(_money(row.get("sessions", 0)))


# ══════════════════════════════════════════════════════════════
# REST orders — solo para units + revenue share
# ══════════════════════════════════════════════════════════════
def fetch_orders_rest(store_url, token, start_date, end_date):
    """
    Orders REST: campos minimos para calcular units y revenue share.
      subtotal_price = gross − discounts (base para revenue share por canal)
      line_items     = para contar unidades
      source_name, tags = para clasificar canal
    """
    params = {
        "status":           "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min":   f"{start_date}T00:00:00-05:00",
        "created_at_max":   f"{end_date}T23:59:59-05:00",
        "limit":            250,
        "fields":           "id,subtotal_price,line_items,source_name,tags",
    }
    return shopify_rest_paginate(store_url, token, "orders.json", params)


def calc_units(orders):
    """Suma total de unidades (quantities) en todas las ordenes."""
    return sum(
        sum(int(li.get("quantity", 0) or 0) for li in o.get("line_items", []))
        for o in orders
    )


def calc_revenue_share(orders):
    """
    Distribuye subtotal_price por canal de venta.
    subtotal_price = precio despues de descuentos, antes de impuestos/envio.
    Es la mejor base para revenue share porque excluye costos operativos.
    """
    channels = {
        "Wellington (POS)": 0.0,
        "Concierge":        0.0,
        "Online":           0.0,
        "Others":           0.0,
    }
    total = 0.0
    for o in orders:
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
        k: {"amount": round(v, 2), "pct": round(v / total * 100, 2) if total else 0.0}
        for k, v in channels.items()
    }


# ══════════════════════════════════════════════════════════════
# Build KPIs
# ══════════════════════════════════════════════════════════════
def build_kpis(ql, orders, sessions=0):
    """
    Combina ShopifyQL (financiero exacto) + REST orders (units).

    AOV segun Shopify = (gross_sales − discounts) / orders
    (Shopify excluye returns y post-edits del AOV)
    """
    if ql.get("gross_sales") is not None:
        gross  = ql["gross_sales"]
        disc   = ql["discounts"]
        ret    = ql["returns"]
        net    = ql["net_sales"]
        cogs   = ql["cogs"]  or 0.0
        pct_gm = ql["pct_gm"] or 0.0
        nb_ord = ql["orders"] or len(orders)
    else:
        # Fallback: ShopifyQL no disponible. Sin COGS ni GM reales.
        print("    ⚠ ShopifyQL no disponible — fallback REST parcial")
        nb_ord = len(orders)
        gross  = 0.0
        disc   = 0.0
        for o in orders:
            sub  = float(o.get("subtotal_price",  0) or 0)
            # subtotal_price ya tiene descuentos restados.
            # No podemos reconstruir gross_sales sin total_line_items_price.
            # Usamos subtotal como net aproximado.
            gross += sub
        disc   = 0.0
        ret    = 0.0
        net    = gross
        cogs   = 0.0
        pct_gm = 0.0

    units   = calc_units(orders)
    # AOV Shopify: (gross - discounts) / orders, sin returns
    aov     = round((gross - disc) / nb_ord, 2) if nb_ord else 0.0
    upo     = round(units / nb_ord, 2)           if nb_ord else 0.0

    pct_disc = round(disc / gross * 100, 2) if gross else 0.0
    pct_ret  = round(ret  / gross * 100, 2) if gross else 0.0

    sessions = int(sessions or 0)
    uv       = round(sessions * 0.85) if sessions else 0
    cr       = round(nb_ord / sessions * 100, 4) if sessions else 0.0

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


# ══════════════════════════════════════════════════════════════
# Periodos
# ══════════════════════════════════════════════════════════════
def get_periods():
    today = datetime.now(TIMEZONE).date()

    mtd_start   = today.replace(day=1)
    mtd_end     = today
    mom_end     = mtd_start - timedelta(days=1)
    mom_start   = mom_end.replace(day=1)
    mom_mtd_end = mom_end.replace(day=min(today.day, mom_end.day))
    yoy_start   = mtd_start.replace(year=mtd_start.year - 1)
    yoy_end     = today.replace(year=today.year - 1)
    wk_start    = today - timedelta(days=today.weekday())
    wk_end      = today
    pwk_start   = wk_start - timedelta(days=7)
    pwk_end     = wk_start - timedelta(days=1)
    mo_end      = mtd_start - timedelta(days=1)
    mo_start    = mo_end.replace(day=1)
    q_month     = ((today.month - 1) // 3) * 3 + 1
    q_start     = today.replace(month=q_month, day=1)
    q_end       = today

    return {
        "mtd":       (mtd_start, mtd_end),
        "mtd_mom":   (mom_start, mom_mtd_end),
        "mtd_yoy":   (yoy_start, yoy_end),
        "week":      (wk_start,  wk_end),
        "week_prev": (pwk_start, pwk_end),
        "month":     (mo_start,  mo_end),
        "quarter":   (q_start,   q_end),
    }


# ══════════════════════════════════════════════════════════════
# Google Sheets writer
# ══════════════════════════════════════════════════════════════
def pct_change(cur, prev):
    if not prev:
        return None
    return round((cur - prev) / prev * 100, 2)


def write_kpis(gc, sheet_id, periods_data):
    sh      = gc.open_by_key(sheet_id)
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

    # kpis_daily
    try:
        ws = sh.worksheet("kpis_daily")
    except Exception:
        ws = sh.add_worksheet("kpis_daily", rows=500, cols=35)
    ws.clear()
    ws.append_row(HEADERS_KPIS)

    for pname, d in periods_data.items():
        cur = d["current"]
        mom = d.get("mom", {})
        yoy = d.get("yoy", {})
        ws.append_row([
            now_str, pname, str(d["start"]), str(d["end"]),
            cur.get("gross_sales",     0),
            cur.get("net_sales",       0),
            cur.get("total_discounts", 0),
            cur.get("total_returns",   0),
            cur.get("cogs",            0),
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
        ])

    # revenue_share
    try:
        ws_rs = sh.worksheet("revenue_share")
    except Exception:
        ws_rs = sh.add_worksheet("revenue_share", rows=500, cols=10)
    ws_rs.clear()
    ws_rs.append_row(["updated_at", "period", "channel", "amount", "pct"])
    for pname, d in periods_data.items():
        for ch, v in d.get("revenue_share", {}).items():
            ws_rs.append_row([now_str, pname, ch, v["amount"], v["pct"]])

    print(f"  ✓ Sheets OK: {now_str}")


# ══════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════
def main():
    gc      = get_gc()
    periods = get_periods()

    for brand, cfg in STORES.items():
        print(f"\n{'='*52}\n  {brand.upper()}\n{'='*52}")
        url   = cfg["url"]
        token = cfg["token"]

        # [1/3] ShopifyQL financiero
        print(f"\n  [1/3] ShopifyQL sales (API {GQL_VERSION})...")
        print(f"  MTD        {periods['mtd'][0]} → {periods['mtd'][1]}")
        ql_mtd     = fetch_sales_ql(url, token, *periods["mtd"])
        print(f"  MTD MOM    {periods['mtd_mom'][0]} → {periods['mtd_mom'][1]}")
        ql_mtd_mom = fetch_sales_ql(url, token, *periods["mtd_mom"])
        print(f"  MTD YOY    {periods['mtd_yoy'][0]} → {periods['mtd_yoy'][1]}")
        ql_mtd_yoy = fetch_sales_ql(url, token, *periods["mtd_yoy"])
        print(f"  WEEK       {periods['week'][0]} → {periods['week'][1]}")
        ql_week    = fetch_sales_ql(url, token, *periods["week"])
        print(f"  WEEK PREV  {periods['week_prev'][0]} → {periods['week_prev'][1]}")
        ql_wk_prev = fetch_sales_ql(url, token, *periods["week_prev"])
        print(f"  MONTH      {periods['month'][0]} → {periods['month'][1]}")
        ql_month   = fetch_sales_ql(url, token, *periods["month"])
        print(f"  QUARTER    {periods['quarter'][0]} → {periods['quarter'][1]}")
        ql_quarter = fetch_sales_ql(url, token, *periods["quarter"])

        # [2/3] Sessions
        print("\n  [2/3] Sessions...")
        s_mtd     = fetch_sessions_ql(url, token, *periods["mtd"])
        s_week    = fetch_sessions_ql(url, token, *periods["week"])
        s_month   = fetch_sessions_ql(url, token, *periods["month"])
        s_quarter = fetch_sessions_ql(url, token, *periods["quarter"])
        print(f"  sessions → mtd:{s_mtd}  week:{s_week}  month:{s_month}  qtr:{s_quarter}")

        # [3/3] Orders REST
        print("\n  [3/3] Orders REST (units + revenue share)...")
        o_mtd     = fetch_orders_rest(url, token, *periods["mtd"])
        o_mtd_mom = fetch_orders_rest(url, token, *periods["mtd_mom"])
        o_mtd_yoy = fetch_orders_rest(url, token, *periods["mtd_yoy"])
        o_week    = fetch_orders_rest(url, token, *periods["week"])
        o_wk_prev = fetch_orders_rest(url, token, *periods["week_prev"])
        o_month   = fetch_orders_rest(url, token, *periods["month"])
        o_quarter = fetch_orders_rest(url, token, *periods["quarter"])
        print(
            f"  orders → mtd:{len(o_mtd)}  week:{len(o_week)}  "
            f"month:{len(o_month)}  qtr:{len(o_quarter)}"
        )

        # Build KPIs
        cur_mtd = build_kpis(ql_mtd,     o_mtd,     s_mtd)
        mom_mtd = build_kpis(ql_mtd_mom, o_mtd_mom)
        yoy_mtd = build_kpis(ql_mtd_yoy, o_mtd_yoy)
        cur_wk  = build_kpis(ql_week,    o_week,    s_week)
        mom_wk  = build_kpis(ql_wk_prev, o_wk_prev)
        cur_mo  = build_kpis(ql_month,   o_month,   s_month)
        cur_qtr = build_kpis(ql_quarter, o_quarter, s_quarter)

        periods_data = {
            "mtd": {
                "start": periods["mtd"][0],  "end": periods["mtd"][1],
                "current": cur_mtd, "mom": mom_mtd, "yoy": yoy_mtd,
                "revenue_share": calc_revenue_share(o_mtd),
            },
            "week": {
                "start": periods["week"][0], "end": periods["week"][1],
                "current": cur_wk, "mom": mom_wk, "yoy": {},
                "revenue_share": calc_revenue_share(o_week),
            },
            "month": {
                "start": periods["month"][0], "end": periods["month"][1],
                "current": cur_mo, "mom": {}, "yoy": {},
                "revenue_share": calc_revenue_share(o_month),
            },
            "quarter": {
                "start": periods["quarter"][0], "end": periods["quarter"][1],
                "current": cur_qtr, "mom": {}, "yoy": {},
                "revenue_share": calc_revenue_share(o_quarter),
            },
        }

        write_kpis(gc, cfg["sheet_id"], periods_data)
        print(f"\n  ✓ {brand.upper()} done.")


if __name__ == "__main__":
    main()
