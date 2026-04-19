import paramiko
import csv
import io
import requests
import base64
import uuid
import time
import logging
import os
from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# --- Config (set these as env vars in Railway) ---
SFTP_HOST     = os.getenv('SFTP_HOST', 'edi.cwrdistribution.com')
SFTP_PORT     = int(os.getenv('SFTP_PORT', '22'))
SFTP_USER     = os.getenv('SFTP_USER', 'hashshop')
SFTP_PASS     = os.getenv('SFTP_PASS', 'bvsPN7ge')
INVENTORY_PATH = '/out/inventory.csv'

WM_CLIENT_ID     = os.getenv('WM_CLIENT_ID', '47f10f25-5746-41a6-be4a-db812f949894')
WM_CLIENT_SECRET = os.getenv('WM_CLIENT_SECRET', 'C11fEdT35CMZfGxND0q54-LgDsjKXsHVpQojyeYcGE0ulVWiqdTsZ-hvneSJpInP3cOWwmq_-8aKsigRUwDNRQ')
WM_BASE_URL      = 'https://marketplace.walmartapis.com'

SYNC_INTERVAL_MINUTES = int(os.getenv('SYNC_INTERVAL_MINUTES', '15'))


def get_walmart_token():
    creds = base64.b64encode(f"{WM_CLIENT_ID}:{WM_CLIENT_SECRET}".encode()).decode()
    resp = requests.post(
        f'{WM_BASE_URL}/v3/token',
        headers={
            'Authorization': f'Basic {creds}',
            'WM_SVC.NAME': 'Walmart Marketplace',
            'WM_QOS.CORRELATION_ID': str(uuid.uuid4()),
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json',
        },
        data='grant_type=client_credentials',
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()['access_token']


def fetch_cwr_inventory():
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        with sftp.open(INVENTORY_PATH, 'r') as f:
            content = f.read().decode('utf-8')
        return list(csv.DictReader(io.StringIO(content)))
    finally:
        sftp.close()
        transport.close()


def update_inventory(token, sku, qty):
    resp = requests.put(
        f'{WM_BASE_URL}/v3/inventory',
        params={'sku': sku},
        headers={
            'Authorization': f'Bearer {token}',
            'WM_SEC.ACCESS_TOKEN': token,
            'WM_SVC.NAME': 'Walmart Marketplace',
            'WM_QOS.CORRELATION_ID': str(uuid.uuid4()),
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        },
        json={'sku': str(sku), 'quantity': {'unit': 'EACH', 'amount': int(qty)}},
        timeout=15,
    )
    return resp


def sync():
    logger.info("=== Starting CWR → Walmart sync ===")
    start = time.time()

    # 1. Fetch inventory from CWR SFTP
    try:
        records = fetch_cwr_inventory()
        logger.info(f"Fetched {len(records)} SKUs from CWR")
    except Exception as e:
        logger.error(f"SFTP fetch failed: {e}")
        return

    # 2. Get Walmart token
    try:
        token = get_walmart_token()
    except Exception as e:
        logger.error(f"Walmart auth failed: {e}")
        return

    # 3. Push each SKU to Walmart
    updated = skipped = errors = 0
    for row in records:
        sku = str(row.get('sku') or row.get('SKU') or '').strip()
        qty = row.get('qty') or row.get('QTY') or '0'

        if not sku:
            skipped += 1
            continue

        try:
            qty_int = int(float(str(qty).strip() or '0'))
        except ValueError:
            qty_int = 0

        try:
            resp = update_inventory(token, sku, qty_int)
            if resp.status_code == 200:
                updated += 1
            else:
                logger.warning(f"SKU {sku} → {resp.status_code}: {resp.text[:120]}")
                errors += 1
        except Exception as e:
            logger.error(f"SKU {sku} failed: {e}")
            errors += 1

        time.sleep(0.05)  # ~20 req/s, well within Walmart rate limits

    elapsed = round(time.time() - start, 1)
    logger.info(f"Done in {elapsed}s — updated: {updated}, skipped: {skipped}, errors: {errors}")


if __name__ == '__main__':
    sync()  # run immediately on startup

    scheduler = BlockingScheduler(timezone='UTC')
    scheduler.add_job(sync, 'interval', minutes=SYNC_INTERVAL_MINUTES)
    logger.info(f"Scheduler running — syncing every {SYNC_INTERVAL_MINUTES} min")
    scheduler.start()
