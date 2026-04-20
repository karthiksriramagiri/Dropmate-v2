import csv
import ftplib
import io
import requests
import base64
import uuid
import time
import logging
import os
import sqlite3
import ssl
import socket
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

KS_FTP_HOST = 'ftp.ekeystone.com'
KS_FTP_PORT = 990
KS_FTP_USER = 'S173312'
KS_FTP_PASS = 'sia33yct'
KS_FTP_FILE = 'Inventory.csv'

TWH_FTP_HOST = 'ftp.twhouse.com'
TWH_FTP_USER = 'HASHSHOPINC'
TWH_FTP_PASS = 'v4E6EsGNvIBKpKu'
TWH_FTP_FILE = 'invenfeed_6930.csv'

WM_CLIENT_ID     = os.getenv('WM_CLIENT_ID', '47f10f25-5746-41a6-be4a-db812f949894')
WM_CLIENT_SECRET = os.getenv('WM_CLIENT_SECRET', 'C11fEdT35CMZfGxND0q54-LgDsjKXsHVpQojyeYcGE0ulVWiqdTsZ-hvneSJpInP3cOWwmq_-8aKsigRUwDNRQ')
WM_BASE_URL      = 'https://marketplace.walmartapis.com'

DB_PATH = os.getenv('DB_PATH', 'dropmate.db')
PORT    = int(os.getenv('PORT', '8080'))

EASTERN = timezone(timedelta(hours=-4))  # EDT — auto-DST handled by scheduler

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
                source       TEXT DEFAULT 'cwr',
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
    rows = list(csv.DictReader(io.StringIO(resp.text), fieldnames=['sku', 'qty']))
    return [{'sku': r['sku'], 'qty': r['qty']} for r in rows if r.get('sku')]

# --- Keystone ---
class ImplicitFTPS(ftplib.FTP_TLS):
    def connect(self, host, port=990, timeout=30):
        self.host = host
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        sock = socket.create_connection((host, port), timeout)
        self.sock = ctx.wrap_socket(sock, server_hostname=host)
        self.af = sock.family
        self.file = self.sock.makefile('r', encoding='utf-8', errors='replace')
        self.welcome = self.getresp()
        return self.welcome

def fetch_keystone_inventory():
    ftp = ImplicitFTPS()
    ftp.connect(KS_FTP_HOST, KS_FTP_PORT)
    ftp.login(KS_FTP_USER, KS_FTP_PASS)
    ftp.set_pasv(True)
    ftp.prot_c()
    buf = io.BytesIO()
    ftp.retrbinary(f'RETR {KS_FTP_FILE}', buf.write)
    ftp.quit()
    content = buf.getvalue().decode('utf-8', errors='replace')
    reader = csv.DictReader(io.StringIO(content))
    records = []
    for row in reader:
        sku = str(row.get('VCPN') or '').strip()
        qty_raw = str(row.get('TotalQty') or '0').strip()
        if not sku:
            continue
        try:
            qty = int(float(qty_raw))
        except ValueError:
            qty = 0
        records.append({'sku': sku, 'qty': qty})
    return records

# --- TWH ---
def fetch_twh_inventory():
    ftp = ftplib.FTP()
    ftp.connect(TWH_FTP_HOST, 21, timeout=30)
    ftp.login(TWH_FTP_USER, TWH_FTP_PASS)
    ftp.set_pasv(True)
    buf = io.BytesIO()
    ftp.retrbinary(f'RETR {TWH_FTP_FILE}', buf.write)
    ftp.quit()
    content = buf.getvalue().decode('utf-8', errors='replace')
    reader = csv.DictReader(io.StringIO(content))
    records = []
    for row in reader:
        sku = str(row.get('itemcode') or '').strip()
        qty_raw = str(row.get('totalQty') or '0').strip()
        if not sku:
            continue
        try:
            qty = int(float(qty_raw))
        except ValueError:
            qty = 0
        records.append({'sku': f'TWH-{sku}', 'qty': qty})  # prefix for Walmart
    return records

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
        if not sku:
            continue
        try:
            qty = int(float(str(row.get('qty') or '0')))
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
            'Authorization':         f'Bearer {token}',
            'WM_SEC.ACCESS_TOKEN':   token,
            'WM_SVC.NAME':           'Walmart Marketplace',
            'WM_QOS.CORRELATION_ID': str(uuid.uuid4()),
            'Accept':                'application/json',
        },
        files={'file': (feed_name, xml_data.encode('utf-8'), 'application/xml')},
        timeout=120,
    )
    return resp

# --- Sync runners ---
CHUNK_SIZE = 25000  # SKUs per feed upload

def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def run_sync(source, fetch_fn, feed_prefix):
    now = datetime.now(EASTERN)
    ts = f"{now.strftime('%m%d')}-{now.strftime('%I%p').lower()}"
    logger.info(f"=== {source.upper()} sync starting | {feed_prefix}-{ts} ===")
    start = time.time()

    with get_db() as conn:
        cur = conn.execute("INSERT INTO sync_runs (source, status) VALUES (?, 'running')", (source,))
        run_id = cur.lastrowid

    try:
        records = fetch_fn()
        logger.info(f"[{source}] Fetched {len(records)} SKUs")
    except Exception as e:
        logger.error(f"[{source}] Fetch failed: {e}")
        with get_db() as conn:
            conn.execute("UPDATE sync_runs SET status='error', completed_at=datetime('now') WHERE id=?", (run_id,))
        return

    try:
        token = get_walmart_token()
    except Exception as e:
        logger.error(f"[{source}] Walmart auth failed: {e}")
        with get_db() as conn:
            conn.execute("UPDATE sync_runs SET status='error', completed_at=datetime('now') WHERE id=?", (run_id,))
        return

    chunks = list(chunked(records, CHUNK_SIZE))
    feed_ids = []
    failed = 0
    for i, chunk in enumerate(chunks, 1):
        feed_name = f"{feed_prefix}-{ts}-p{i}.xml"
        xml_data = build_inventory_xml(chunk)
        # Retry up to 3 times on 429 rate limit
        for attempt in range(3):
            try:
                resp = upload_bulk_feed(token, xml_data, feed_name)
                if resp.status_code == 429:
                    wait = 30 * (attempt + 1)
                    logger.warning(f"[{source}] Rate limited, waiting {wait}s before retry...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                feed_id = resp.json().get('feedId', '')
                feed_ids.append(feed_id)
                logger.info(f"[{source}] Chunk {i}/{len(chunks)} submitted — feedId: {feed_id} | SKUs: {len(chunk)}")
                break
            except Exception as e:
                if attempt == 2:
                    logger.error(f"[{source}] Chunk {i}/{len(chunks)} failed after 3 attempts: {e}")
                    failed += 1
                else:
                    time.sleep(10)
        time.sleep(5)  # pause between chunks

    status = 'error' if failed == len(chunks) else 'success'
    duration = round(time.time() - start, 1)
    with get_db() as conn:
        conn.execute("""
            UPDATE sync_runs SET status=?, completed_at=datetime('now'),
            total=?, feed_id=?, duration_s=? WHERE id=?
        """, (status, len(records), feed_ids[0] if feed_ids else '', duration, run_id))

    logger.info(f"[{source}] Done in {duration}s — {len(chunks) - failed}/{len(chunks)} chunks OK")

def sync_all():
    run_sync('cwr',      fetch_cwr_inventory,      'CWR-DMv2')
    time.sleep(60)  # let Walmart breathe between distributors
    run_sync('keystone', fetch_keystone_inventory,  'KS-DMv2')
    time.sleep(60)
    run_sync('twh',      fetch_twh_inventory,       'TWH-DMv2')

# --- Flask ---
app = Flask(__name__)
CORS(app)

@app.route('/health')
def health():
    return jsonify({'status': 'ok'})

@app.route('/api/status')
def status():
    with get_db() as conn:
        last_cwr = conn.execute(
            "SELECT * FROM sync_runs WHERE source='cwr' AND status!='running' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        last_ks = conn.execute(
            "SELECT * FROM sync_runs WHERE source='keystone' AND status!='running' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        last_twh = conn.execute(
            "SELECT * FROM sync_runs WHERE source='twh' AND status!='running' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        is_running = conn.execute(
            "SELECT COUNT(*) as c FROM sync_runs WHERE status='running'"
        ).fetchone()['c'] > 0
    return jsonify({
        'is_running':    is_running,
        'last_cwr':      dict(last_cwr)  if last_cwr  else None,
        'last_keystone': dict(last_ks)   if last_ks   else None,
        'last_twh':      dict(last_twh)  if last_twh  else None,
    })

@app.route('/api/runs')
def runs():
    source = request.args.get('source')
    limit  = request.args.get('limit', 20, type=int)
    with get_db() as conn:
        if source:
            rows = conn.execute(
                "SELECT * FROM sync_runs WHERE source=? ORDER BY id DESC LIMIT ?", (source, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM sync_runs ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
    return jsonify([dict(r) for r in rows])

@app.route('/api/sync', methods=['POST'])
def trigger_sync():
    source = request.json.get('source', 'all') if request.json else 'all'
    if source == 'cwr':
        threading.Thread(target=run_sync, args=('cwr', fetch_cwr_inventory, 'CWR-DMv2'), daemon=True).start()
    elif source == 'keystone':
        threading.Thread(target=run_sync, args=('keystone', fetch_keystone_inventory, 'KS-DMv2'), daemon=True).start()
    elif source == 'twh':
        threading.Thread(target=run_sync, args=('twh', fetch_twh_inventory, 'TWH-DMv2'), daemon=True).start()
    else:
        threading.Thread(target=sync_all, daemon=True).start()
    return jsonify({'status': 'started', 'source': source})

if __name__ == '__main__':
    init_db()
    sync_all()

    scheduler = BackgroundScheduler(timezone='America/New_York')
    scheduler.add_job(sync_all, 'cron', hour='0,6,12,18', minute=0)
    scheduler.start()
    logger.info("Scheduler started — syncing at 12am, 6am, 12pm, 6pm Eastern")

    app.run(host='0.0.0.0', port=PORT)
