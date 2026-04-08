"""
Top 100 Products Pipeline — Shopify → Google Sheets
Extrae productos con net sales, units, gross, dropship flag
para cualquier período definido.
"""
import os, json, requests, gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz

TIMEZONE = pytz.timezone("America/Bogota")

STORES = {
    "corro":  {"url":"equestrian-labs.myshopify.com","token":os.environ["SHOPIFY_TOKEN_CORRO"],"sheet_id":"1nq8xkDzowAvhD3wpMBlVK2M3FZSNS2DrAiPxz-Y2tdU"},
    "cavali": {"url":"cavali-club.myshopify.com",    "token":os.environ["SHOPIFY_TOKEN_CAVALI"],"sheet_id":"1QUdJc2EIdElIX5nlLQxWxS98aAz-TgQnSg9glJpNtig"},
}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]

DROPSHIP_TAG = "drop_ship"

# Periods to extract
PERIODS = {
    "q1_2026": ("2026-01-01","2026-03-31"),
    "q4_2025": ("2025-10-01","2025-12-31"),
    "q3_2025": ("2025-07-01","2025-09-30"),
    "q2_2025": ("2025-04-01","2025-06-30"),
    "q1_2025": ("2025-01-01","2025-03-31"),
    "q4_2024": ("2024-10-01","2024-12-31"),
    "q3_2024": ("2024-07-01","2024-09-30"),
    "q2_2024": ("2024-04-01","2024-06-30"),
    "q1_2024": ("2024-01-01","2024-03-31"),
}

def get_gc():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    return gspread.authorize(creds)

def shopify_rest_get(store_url, token, endpoint, params):
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
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
    return results

def fetch_orders_with_lines(store_url, token, start_date, end_date):
    """Fetch all paid orders with line items for the period."""
    return shopify_rest_get(store_url, token, "orders.json", {
        "status":           "any",
        "financial_status": "paid,partially_paid,partially_refunded,refunded",
        "created_at_min":   f"{start_date}T00:00:00-05:00",
        "created_at_max":   f"{end_date}T23:59:59-05:00",
        "limit":            250,
        "fields":           "id,line_items,total_discounts,refunds",
    })

def fetch_product_tags(store_url, token, product_ids):
    """Batch fetch product tags to identify dropship items."""
    tags = {}
    # Fetch in batches of 100
    ids_list = list(product_ids)
    for i in range(0, len(ids_list), 100):
        batch = ids_list[i:i+100]
        products = shopify_rest_get(store_url, token, "products.json", {
            "ids": ",".join(str(x) for x in batch),
            "limit": 100,
            "fields": "id,tags",
        })
        for p in products:
            tags[str(p["id"])] = p.get("tags","")
    return tags

def calc_refunds_by_line(orders):
    """Calculate refunded amount per line item id."""
    refunds = {}
    for order in orders:
        for refund in order.get("refunds", []):
            for rli in refund.get("refund_line_items", []):
                lid = str(rli.get("line_item_id",""))
                amt = float(rli.get("subtotal", 0) or 0)
                refunds[lid] = refunds.get(lid, 0) + amt
    return refunds

def aggregate_products(orders, product_tags):
    """Aggregate sales by product from order line items."""
    products = {}

    for order in orders:
        order_discount_rate = 0
        gross_total = sum(float(li.get("price",0))*int(li.get("quantity",0)) for li in order.get("line_items",[]))
        if gross_total > 0:
            order_discount_rate = float(order.get("total_discounts",0)) / gross_total

    # Calculate refunds per line item
    refunds_by_line = calc_refunds_by_line(orders)

    for order in orders:
        gross_order = sum(float(li.get("price",0))*int(li.get("quantity",0)) for li in order.get("line_items",[]))
        disc_rate = float(order.get("total_discounts",0)) / gross_order if gross_order else 0

        for li in order.get("line_items", []):
            pid       = str(li.get("product_id",""))
            lid       = str(li.get("id",""))
            title     = li.get("title","") or ""
            variant   = li.get("variant_title","") or ""
            sku       = li.get("sku","") or ""
            qty       = int(li.get("quantity", 0) or 0)
            price     = float(li.get("price", 0) or 0)
            gross     = price * qty
            disc      = round(gross * disc_rate, 2)
            refund    = refunds_by_line.get(lid, 0)
            net       = round(gross - disc - refund, 2)
            tags      = product_tags.get(pid, "")
            is_ds     = DROPSHIP_TAG in tags.lower()

            key = f"{pid}__{sku}" if sku else f"{pid}__{title}"
            if key not in products:
                products[key] = {
                    "product_id":    pid,
                    "product_title": title,
                    "variant_title": variant,
                    "sku":           sku,
                    "tags":          tags,
                    "is_dropship":   is_ds,
                    "gross_sales":   0.0,
                    "total_discounts":0.0,
                    "total_returns": 0.0,
                    "net_sales":     0.0,
                    "units_sold":    0,
                    "orders_count":  0,
                }
            products[key]["gross_sales"]    += gross
            products[key]["total_discounts"] += disc
            products[key]["total_returns"]   += refund
            products[key]["net_sales"]       += net
            products[key]["units_sold"]      += qty
            products[key]["orders_count"]    += 1

    return list(products.values())

def write_products_sheet(gc, sheet_id, period_key, products, now_str):
    sh = gc.open_by_key(sheet_id)
    tab_name = f"products_{period_key}"

    # Delete if exists, recreate
    try:
        ws = sh.worksheet(tab_name)
        sh.del_worksheet(ws)
    except:
        pass
    ws = sh.add_worksheet(tab_name, rows=120, cols=15)

    headers = [
        "updated_at","period","rank",
        "product_title","variant_title","sku","product_id",
        "is_dropship","tags",
        "gross_sales","total_discounts","total_returns","net_sales",
        "units_sold","orders_count","pct_of_total"
    ]
    ws.append_row(headers)

    # Sort by net_sales desc, take top 100
    sorted_products = sorted(products, key=lambda x: x["net_sales"], reverse=True)[:100]
    total_net = sum(p["net_sales"] for p in sorted_products) or 1

    rows = []
    for i, p in enumerate(sorted_products, 1):
        pct = round(p["net_sales"] / total_net * 100, 2)
        rows.append([
            now_str, period_key, i,
            p["product_title"], p["variant_title"], p["sku"], p["product_id"],
            "Yes" if p["is_dropship"] else "No",
            p["tags"][:100] if p["tags"] else "",
            round(p["gross_sales"],2), round(p["total_discounts"],2),
            round(p["total_returns"],2), round(p["net_sales"],2),
            p["units_sold"], p["orders_count"], pct
        ])

    # Batch write
   # Truly batch write all rows at once
    if rows:
        ws.append_rows(rows)

    print(f"  ✓ Written {len(rows)} products to tab '{tab_name}'")
    return sorted_products

def main():
    gc = get_gc()
    now_str = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M")

    # Check if specific period requested via env var
    target_period = os.environ.get("TARGET_PERIOD", "all")

    for brand, cfg in STORES.items():
        print(f"\n{'='*50}\n  {brand.upper()} — Products Pipeline\n{'='*50}")
        url, token = cfg["url"], cfg["token"]

        periods_to_run = PERIODS if target_period == "all" else {
            k:v for k,v in PERIODS.items() if k==target_period
        }
        if not periods_to_run:
            print(f"  Period '{target_period}' not found. Available: {list(PERIODS.keys())}")
            continue

        for period_key, (start, end) in periods_to_run.items():
            print(f"\n  Period: {period_key} ({start} → {end})")

            print(f"  Fetching orders...")
            orders = fetch_orders_with_lines(url, token, start, end)
            print(f"  Orders: {len(orders)}")

            if not orders:
                print(f"  No orders for this period, skipping.")
                continue

            # Get unique product IDs
            product_ids = set()
            for order in orders:
                for li in order.get("line_items",[]):
                    if li.get("product_id"):
                        product_ids.add(str(li["product_id"]))
            print(f"  Unique products: {len(product_ids)}")

            print(f"  Fetching product tags (dropship detection)...")
            product_tags = fetch_product_tags(url, token, product_ids)

            print(f"  Aggregating by product...")
            products = aggregate_products(orders, product_tags)

            ds_count = sum(1 for p in products if p["is_dropship"])
            total_net = sum(p["net_sales"] for p in products)
            print(f"  Products found: {len(products)} ({ds_count} dropship)")
            print(f"  Total net sales: ${total_net:,.2f}")

            write_products_sheet(gc, cfg["sheet_id"], period_key, products, now_str)

        print(f"\n  ✓ {brand.upper()} products done.")

if __name__ == "__main__":
    main()
