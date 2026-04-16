"""
Script de diagnóstico — lee el Sheet y muestra exactamente qué hay guardado
para entender por qué el dashboard no encuentra los datos.
"""
import os, json, requests
from google.oauth2.service_account import Credentials
import gspread

SCOPES = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]

STORES = {
    "corro":  {"sheet_id":"1nq8xkDzowAvhD3wpMBlVK2M3FZSNS2DrAiPxz-Y2tdU"},
    "cavali": {"sheet_id":"1QUdJc2EIdElIX5nlLQxWxS98aAz-TgQnSg9glJpNtig"},
}

def get_gc():
    creds = Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]), scopes=SCOPES)
    return gspread.authorize(creds)

def main():
    gc = get_gc()
    for brand, cfg in STORES.items():
        print(f"\n{'='*60}")
        print(f"  {brand.upper()} — Sheet: {cfg['sheet_id']}")
        print(f"{'='*60}")
        sh = gc.open_by_key(cfg["sheet_id"])

        # kpis_daily
        try:
            ws = sh.worksheet("kpis_daily")
            rows = ws.get_all_records()
            print(f"\n  kpis_daily: {len(rows)} rows")
            print(f"  {'period':<25} {'period_start':<14} {'period_end':<14} {'gross_sales':>12} {'net_sales':>12} {'sessions':>10}")
            print(f"  {'-'*95}")
            for r in rows:
                print(f"  {str(r.get('period','')):<25} {str(r.get('period_start','')):<14} {str(r.get('period_end','')):<14} {str(r.get('gross_sales','0')):>12} {str(r.get('net_sales','0')):>12} {str(r.get('sessions','0')):>10}")
        except Exception as e:
            print(f"  kpis_daily error: {e}")

        # revenue_share
        try:
            ws_rs = sh.worksheet("revenue_share")
            rows_rs = ws_rs.get_all_records()
            print(f"\n  revenue_share: {len(rows_rs)} rows")
            periods_rs = list(set(r.get('period','') for r in rows_rs))
            print(f"  periods in RS: {periods_rs}")
        except Exception as e:
            print(f"  revenue_share error: {e}")

if __name__ == "__main__":
    main()
