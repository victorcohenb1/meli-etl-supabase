
import os, requests, time
from datetime import datetime, timedelta, timezone

# Secrets from GitHub Actions
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]
MELI_CLIENT_ID = os.environ["MELI_CLIENT_ID"]
MELI_CLIENT_SECRET = os.environ["MELI_CLIENT_SECRET"]
MELI_REFRESH_TOKEN = os.environ["MELI_REFRESH_TOKEN"]
SELLER_ID = os.environ.get("SELLER_ID", "381183837")

def refresh_access_token():
    r = requests.post(
        "https://api.mercadolibre.com/oauth/token",
        headers={"Content-Type":"application/x-www-form-urlencoded"},
        data={
            "grant_type":"refresh_token",
            "client_id": MELI_CLIENT_ID,
            "client_secret": MELI_CLIENT_SECRET,
            "refresh_token": MELI_REFRESH_TOKEN,
        },
        timeout=60,
    )
    r.raise_for_status()
    return r.json()["access_token"]

def sb_insert(table, payload_json):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={
            "apikey": SUPABASE_SERVICE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
            "Content-Type": "application/json",
        },
        params={"prefer":"return=minimal"},
        json=[{"payload": payload_json}],
        timeout=60,
    )
    r.raise_for_status()

def get_json(url, headers, retries=3, backoff=0.5):
    for i in range(retries):
        r = requests.get(url, headers=headers, timeout=120)
        if r.status_code in (429,500,502,503,504):
            time.sleep(backoff * (2**i))
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()

def pull_orders(token):
    headers = {"Authorization": f"Bearer {token}"}
    # últimos 7 días, en ISO UTC
    now = datetime.now(timezone.utc)
    frm = now - timedelta(days=7)
    from_iso, to_iso = frm.isoformat(), now.isoformat()

    pages = 0
    offset = 0
    while True:
        url = (
            "https://api.mercadolibre.com/orders/search"
            f"?seller={SELLER_ID}"
            f"&order.date_created.from={from_iso}"
            f"&order.date_created.to={to_iso}"
            f"&limit=50&offset={offset}"
        )
        data = get_json(url, headers)
        results = data.get("results", [])
        if not results:
            break
        sb_insert("raw_orders", data)
        pages += 1
        offset += 50
        time.sleep(0.2)
    return pages

def main():
    token = refresh_access_token()
    pages = pull_orders(token)
    print({"orders_pages_inserted": pages})

if __name__ == "__main__":
    main()
