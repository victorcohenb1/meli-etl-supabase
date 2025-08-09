import os, time, requests
from datetime import datetime, timezone

# ====== SECRETS (GitHub Actions) ======
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
MELI_CLIENT_ID = os.environ["MELI_CLIENT_ID"]
MELI_CLIENT_SECRET = os.environ["MELI_CLIENT_SECRET"]
MELI_REFRESH_TOKEN = os.environ["MELI_REFRESH_TOKEN"]
SELLER_ID = os.environ["SELLER_ID"]

# cuántos meses hacia atrás quieres traer (incluye el mes actual)
MONTHS_BACK = 12  # cámbialo si quieres menos

# ====== Helpers de fechas ======
def _ymd(dt):  # ISO con milisegundos y offset -00:00 (lo que Meli acepta bien)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "-00:00")

def _utc_now():
    return datetime.now(timezone.utc)

def _month_start(dt):         # primer día del mes a las 00:00 UTC
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

def _add_months(year, month, k):
    m = month + k
    y = year + (m - 1)//12
    m2 = ((m - 1) % 12) + 1
    return y, m2

def month_ranges(months_back=12):
    """
    Regresa una lista de (from_dt, to_dt) en UTC:
    - índice 0: mes actual desde el 1 a 00:00 hasta ahora
    - siguientes: meses completos anteriores
    """
    now = _utc_now()
    start_cur = _month_start(now)
    ranges = [(start_cur, now)]  # mes actual (a la fecha)

    y, m = start_cur.year, start_cur.month
    for i in range(1, months_back):  # meses completos anteriores
        y_i, m_i = _add_months(y, m, -i)             # inicio del mes i atrás
        y_next, m_next = _add_months(y_i, m_i, 1)    # inicio del mes siguiente
        start_i = datetime(y_i, m_i, 1, tzinfo=timezone.utc)
        end_i   = datetime(y_next, m_next, 1, tzinfo=timezone.utc)
        ranges.append((start_i, end_i))
    return ranges

# ====== API Meli / Supabase ======
def refresh_access_token():
    r = requests.post(
        "https://api.mercadolibre.com/oauth/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "refresh_token",
            "client_id": MELI_CLIENT_ID,
            "client_secret": MELI_CLIENT_SECRET,
            "refresh_token": MELI_REFRESH_TOKEN,
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def sb_insert_raw(table, payload_json):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={
            "apikey": SUPABASE_SERVICE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
            "Content-Type": "application/json",
        },
        params={"prefer": "return=minimal"},
        json=[{"payload": payload_json}],
        timeout=120,
    )
    r.raise_for_status()

def get_json_with_retry(url, headers, retries=3, backoff=0.7):
    for i in range(retries):
        resp = requests.get(url, headers=headers, timeout=120)
        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(backoff * (2**i))
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()

def pull_orders_month(token, dt_from, dt_to):
    """
    Trae TODAS las órdenes del rango [dt_from, dt_to) paginando y
    guarda cada página en raw_orders.
    """
    headers = {"Authorization": f"Bearer {token}"}
    frm = _ymd(dt_from)
    to  = _ymd(dt_to)

    limit = 50
    offset = 0
    pages = 0

    while True:
        url = (
            "https://api.mercadolibre.com/orders/search"
            f"?seller={SELLER_ID}"
            f"&order.date_created.from={frm}"
            f"&order.date_created.to={to}"
            f"&limit={limit}&offset={offset}"
        )
        data = get_json_with_retry(url, headers)
        results = data.get("results", [])
        if not results:
            break

        # Guardamos la página cruda + metadatos del rango
        data["_bandana_range"] = {"from": frm, "to": to, "limit": limit, "offset": offset}
        sb_insert_raw("raw_orders", data)

        pages += 1
        offset += limit
        time.sleep(0.2)  # cuida rate limits

    return pages

def main():
    token = refresh_access_token()
    total_pages = 0
    for idx, (start_dt, end_dt) in enumerate(month_ranges(MONTHS_BACK)):
        pages = pull_orders_month(token, start_dt, end_dt)
        print(f"[{idx}] {start_dt:%Y-%m} -> páginas insertadas: {pages}")
        total_pages += pages
    print({"total_pages_inserted": total_pages})

if __name__ == "__main__":
    main()
