import csv
import io
import requests
import base64
import uuid
import time
import logging
import os
import sqlite3
import threading
import xml.etree.ElementTree as ET
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# --- Config ---
CWR_FEED_URL = 'https://cwrdistribution.com/feeds/productdownload.php'
CWR_FEED_ID  = 'MPB_NDI2NzUwNDI2NzUwMTE1NQ=='

WM_CLIENT_ID     = os.getenv('WM_CLIENT_ID', '47f10f25-5746-41a6-be4a-db812f949894')
WM_CLIENT_SECRET = os.getenv('WM_CLIENT_SECRET', 'C11fEdT35CMZfGxND0q54-LgDsjKXsHVpQojyeYcGE0ulVWiqdTsZ-hvneSJpInP3cOWwmq_-8aKsigRUwDNRQ')
WM_BASE_URL      = 'https://marketplace.walmartapis.com'

SYNC_INTERVAL_MINUTES = int(os.getenv('SYNC_INTERVAL_MINUTES', '360'))
DB_PATH               = os.getenv('DB_PATH', 'dropmate.db')
PORT                  = int(os.getenv('PORT', '8080'))

# --- Database ---
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS sync_runs (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at   TEXT DEFAULT (datetime('now')),
                completed_at TEXT,
                status       TEXT DEFAULT 'running',
                total        INTEGER DEFAULT 0,
                updated      INTEGER DEFAULT 0,
                errors       INTEGER DEFAULT 0,
                duration_s   REAL
            );
        """)

# --- CWR ---
def fetch_cwr_inventory():
    # Use ohtime=0 to get all SKUs and their current quantities
    resp = requests.get(CWR_FEED_URL, params={
        'id':      CWR_FEED_ID,
        'version': 3,
        'format':  'csv',
        'fields':  'sku,qty',
        'ohtime':  0,
    }, timeout=120)
    resp.raise_for_status()
    return list(csv.DictReader(io.StringIO(resp.text)))

# --- Walmart ---
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

def update_inventory(token, sku, qty):
    return requests.put(
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

# --- Sync ---
def sync():
    logger.info("=== Starting CWR → Walmart sync ===")
    start = time.time()

    with get_db() as conn:
        cur = conn.execute("INSERT INTO sync_runs (status) VALUES ('running')")
        run_id = cur.lastrowid

    try:
        records = fetch_cwr_inventory()
        logger.info(f"Fetched {len(records)} SKUs from CWR")
    except Exception as e:
        logger.error(f"SFTP fetch failed: {e}")
        with get_db() as conn:
            conn.execute("UPDATE sync_runs SET status='error', completed_at=datetime('now') WHERE id=?", (run_id,))
        return

    try:
        token = get_walmart_token()
    except Exception as e:
        logger.error(f"Walmart auth failed: {e}")
        with get_db() as conn:
            conn.execute("UPDATE sync_runs SET status='error', completed_at=datetime('now') WHERE id=?", (run_id,))
        return

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
        time.sleep(0.05)

    duration = round(time.time() - start, 1)
    with get_db() as conn:
        conn.execute("""
            UPDATE sync_runs SET status='success', completed_at=datetime('now'),
            total=?, updated=?, errors=?, duration_s=? WHERE id=?
        """, (len(records), updated, errors, duration, run_id))

    logger.info(f"Done in {duration}s — updated: {updated}, skipped: {skipped}, errors: {errors}")

# --- Flask ---
app = Flask(__name__)
CORS(app)

@app.route('/health')
def health():
    return jsonify({'status': 'ok'})

@app.route('/api/status')
def status():
    with get_db() as conn:
        last = conn.execute(
            "SELECT * FROM sync_runs WHERE status != 'running' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        is_running = conn.execute(
            "SELECT COUNT(*) as c FROM sync_runs WHERE status='running'"
        ).fetchone()['c'] > 0
    return jsonify({
        'is_running': is_running,
        'last_run': dict(last) if last else None,
        'interval_minutes': SYNC_INTERVAL_MINUTES,
    })

@app.route('/api/runs')
def runs():
    limit = request.args.get('limit', 20, type=int)
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM sync_runs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/sync', methods=['POST'])
def trigger_sync():
    threading.Thread(target=sync, daemon=True).start()
    return jsonify({'status': 'started'})

if __name__ == '__main__':
    init_db()
    sync()

    scheduler = BackgroundScheduler(timezone='UTC')
    scheduler.add_job(sync, 'interval', minutes=SYNC_INTERVAL_MINUTES)
    scheduler.start()
    logger.info(f"Scheduler started — syncing every {SYNC_INTERVAL_MINUTES} min")

    app.run(host='0.0.0.0', port=PORT)
