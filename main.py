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
from datetime import datetime, timezone, timedelta
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
                feed_id      TEXT,
                duration_s   REAL
            );
        """)

# --- CWR ---
def fetch_cwr_inventory():
    resp = requests.get(CWR_FEED_URL, params={
        'id':      CWR_FEED_ID,
        'version': 3,
        'format':  'csv',
        'fields':  'sku,qty',
        'ohtime':  0,
    }, timeout=120)
    resp.raise_for_status()
    return list(csv.DictReader(io.StringIO(resp.text), fieldnames=['sku', 'qty']))

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

def build_inventory_xml(records):
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<InventoryFeed xmlns="http://walmart.com/">',
        '  <InventoryHeader><version>1.4</version></InventoryHeader>',
    ]
    for row in records:
        sku = str(row.get('sku') or '').strip()
        qty_raw = str(row.get('qty') or '0').strip()
        if not sku:
            continue
        try:
            qty = int(float(qty_raw))
        except ValueError:
            qty = 0
        lines.append(
            f'  <inventory>'
            f'<sku>{sku}</sku>'
            f'<quantity><unit>EACH</unit><amount>{qty}</amount></quantity>'
            f'</inventory>'
        )
    lines.append('</InventoryFeed>')
    return '\n'.join(lines)

def upload_bulk_feed(token, xml_data, feed_name):
    resp = requests.post(
        f'{WM_BASE_URL}/v3/feeds',
        params={'feedType': 'inventory'},
        headers={
            'Authorization':       f'Bearer {token}',
            'WM_SEC.ACCESS_TOKEN': token,
            'WM_SVC.NAME':         'Walmart Marketplace',
            'WM_QOS.CORRELATION_ID': str(uuid.uuid4()),
            'Accept':              'application/json',
        },
        files={'file': (feed_name, xml_data.encode('utf-8'), 'application/xml')},
        timeout=120,
    )
    return resp

# --- Sync ---
def sync():
    eastern = timezone(timedelta(hours=-4))  # EDT (UTC-4)
    now = datetime.now(eastern)
    feed_name = f"CWR-DMv2-{now.strftime('%m%d')}-{now.strftime('%I%p').lower()}.xml"
    logger.info(f"=== Starting CWR → Walmart sync | feed: {feed_name} ===")
    start = time.time()

    with get_db() as conn:
        cur = conn.execute("INSERT INTO sync_runs (status) VALUES ('running')")
        run_id = cur.lastrowid

    # 1. Fetch from CWR
    try:
        records = fetch_cwr_inventory()
        logger.info(f"Fetched {len(records)} SKUs from CWR")
    except Exception as e:
        logger.error(f"CWR fetch failed: {e}")
        with get_db() as conn:
            conn.execute("UPDATE sync_runs SET status='error', completed_at=datetime('now') WHERE id=?", (run_id,))
        return

    # 2. Get Walmart token
    try:
        token = get_walmart_token()
    except Exception as e:
        logger.error(f"Walmart auth failed: {e}")
        with get_db() as conn:
            conn.execute("UPDATE sync_runs SET status='error', completed_at=datetime('now') WHERE id=?", (run_id,))
        return

    # 3. Build XML and upload as one bulk feed
    try:
        xml_data = build_inventory_xml(records)
        resp = upload_bulk_feed(token, xml_data, feed_name)
        resp.raise_for_status()
        feed_id = resp.json().get('feedId', '')
        logger.info(f"Bulk feed submitted — feedId: {feed_id} | SKUs: {len(records)}")
    except Exception as e:
        logger.error(f"Bulk feed upload failed: {e}")
        with get_db() as conn:
            conn.execute("UPDATE sync_runs SET status='error', completed_at=datetime('now') WHERE id=?", (run_id,))
        return

    duration = round(time.time() - start, 1)
    with get_db() as conn:
        conn.execute("""
            UPDATE sync_runs SET status='success', completed_at=datetime('now'),
            total=?, feed_id=?, duration_s=? WHERE id=?
        """, (len(records), feed_id, duration, run_id))

    logger.info(f"Done in {duration}s — feed: {feed_name}")

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

    scheduler = BackgroundScheduler(timezone='America/New_York')
    scheduler.add_job(sync, 'cron', hour='0,6,12,18', minute=0)
    scheduler.start()
    logger.info("Scheduler started — syncing at 12am, 6am, 12pm, 6pm Eastern")

    app.run(host='0.0.0.0', port=PORT)
