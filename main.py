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

DH_FTP_HOST  = 'ftp.dandh.com'
DH_FTP_USER  = '3280530000FTP'
DH_FTP_PASS  = '3rUnzEvfj86g'
DH_ITEMLIST  = 'ITEMLIST'
DH_DISCOITEM = 'DISCOITEM'

WM_CLIENT_ID     = os.getenv('WM_CLIENT_ID', '47f10f25-5746-41a6-be4a-db812f949894')
WM_CLIENT_SECRET = os.getenv('WM_CLIENT_SECRET', 'C11fEdT35CMZfGxND0q54-LgDsjKXsHVpQojyeYcGE0ulVWiqdTsZ-hvneSJpInP3cOWwmq_-8aKsigRUwDNRQ')
WM_BASE_URL      = 'https://marketplace.walmartapis.com'

DB_PATH    = os.getenv('DB_PATH', 'dropmate.db')
PORT       = int(os.getenv('PORT', '8080'))
CHUNK_SIZE = 25000
EASTERN    = timezone(timedelta(hours=-4))

# --- In-memory progress tracking ---
_progress = {}  # keyed by source: cwr / keystone / twh
_progress_lock = threading.Lock()

def set_progress(source, **kwargs):
    with _progress_lock:
        _progress[source] = {**_progress.get(source, {}), **kwargs}

def clear_progress(source):
    with _progress_lock:
        _progress.pop(source, None)

def get_progress():
    with _progress_lock:
        return dict(_progress)

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
                disc_count   INTEGER DEFAULT 0,
                feed_id      TEXT,
                duration_s   REAL
            );
        """)
        # Add disc_count to existing DBs that predate this column
        try:
            conn.execute("ALTER TABLE sync_runs ADD COLUMN disc_count INTEGER DEFAULT 0")
        except Exception:
            pass

# --- CWR ---
def get_last_cwr_sync_timestamp():
    """Return unix timestamp of last successful CWR sync, or 30 days ago if none."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT started_at FROM sync_runs WHERE source='cwr' AND status='success' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if row:
        try:
            dt = datetime.strptime(row['started_at'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except Exception:
            pass
    return int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp())

def fetch_cwr_inventory():
    set_progress('cwr', step='Fetching active inventory from CWR…', pct=5)
    resp = requests.get(CWR_FEED_URL, params={
        'id': CWR_FEED_ID, 'version': 3, 'format': 'csv',
        'fields': 'sku,qty', 'ohtime': 0,
    }, timeout=120)
    resp.raise_for_status()
    rows = list(csv.DictReader(io.StringIO(resp.text), fieldnames=['sku', 'qty']))
    records = {r['sku']: {'sku': r['sku'], 'qty': r['qty']} for r in rows if r.get('sku')}

    # Fetch discontinued SKUs since last sync and zero them out on Walmart
    set_progress('cwr', step='Checking for discontinued SKUs…', pct=12)
    try:
        disctime = get_last_cwr_sync_timestamp()
        dresp = requests.get(CWR_FEED_URL, params={
            'id': CWR_FEED_ID, 'version': 3, 'format': 'csv',
            'fields': 'sku', 'disctime': disctime,
        }, timeout=60)
        dresp.raise_for_status()
        disc_count = 0
        for line in dresp.text.splitlines():
            sku = line.strip()
            if sku and sku not in records:
                records[sku] = {'sku': sku, 'qty': 0}
                disc_count += 1
        if disc_count:
            logger.info(f"[cwr] Zeroing out {disc_count} discontinued SKUs")
            set_progress('cwr', step=f'Found {disc_count} discontinued SKUs — zeroing out…', pct=18, disc_count=disc_count)
        else:
            logger.info(f"[cwr] No newly discontinued SKUs since last sync")
            set_progress('cwr', disc_count=0)
    except Exception as e:
        logger.warning(f"[cwr] Could not fetch discontinued SKUs: {e}")
        set_progress('cwr', disc_count=0)

    return list(records.values()), _progress.get('cwr', {}).get('disc_count', 0)

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
    set_progress('keystone', step='Connecting to Keystone FTP…', pct=5)
    ftp = ImplicitFTPS()
    ftp.connect(KS_FTP_HOST, KS_FTP_PORT)
    ftp.login(KS_FTP_USER, KS_FTP_PASS)
    ftp.set_pasv(True)
    ftp.prot_c()
    set_progress('keystone', step='Downloading Inventory.csv…', pct=15)
    buf = io.BytesIO()
    ftp.retrbinary(f'RETR {KS_FTP_FILE}', buf.write)
    ftp.quit()
    set_progress('keystone', step='Parsing CSV…', pct=25)
    content = buf.getvalue().decode('utf-8', errors='replace')
    reader = csv.DictReader(io.StringIO(content))
    records = []
    for row in reader:
        sku = str(row.get('VCPN') or '').strip()
        if not sku:
            continue
        try:
            qty = int(float(str(row.get('TotalQty') or '0')))
        except ValueError:
            qty = 0
        records.append({'sku': sku, 'qty': qty})
    return records

# --- TWH ---
def fetch_twh_inventory():
    set_progress('twh', step='Connecting to TWH FTP…', pct=5)
    ftp = ftplib.FTP()
    ftp.connect(TWH_FTP_HOST, 21, timeout=30)
    ftp.login(TWH_FTP_USER, TWH_FTP_PASS)
    ftp.set_pasv(True)
    set_progress('twh', step='Downloading inventory file…', pct=15)
    buf = io.BytesIO()
    ftp.retrbinary(f'RETR {TWH_FTP_FILE}', buf.write)
    ftp.quit()
    set_progress('twh', step='Parsing CSV…', pct=25)
    content = buf.getvalue().decode('utf-8', errors='replace')
    reader = csv.DictReader(io.StringIO(content))
    records = []
    for row in reader:
        sku = str(row.get('itemcode') or '').strip()
        if not sku:
            continue
        try:
            qty = int(float(str(row.get('totalQty') or '0')))
        except ValueError:
            qty = 0
        records.append({'sku': f'TWH-{sku}', 'qty': qty})
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
            f'  <inventory><sku>{sku}</sku>'
            f'<quantity><unit>EACH</unit><amount>{qty}</amount></quantity></inventory>'
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

def chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

# --- Sync ---
def run_sync(source, fetch_fn, feed_prefix):
    now = datetime.now(EASTERN)
    ts  = f"{now.strftime('%m%d')}-{now.strftime('%I%p').lower()}"
    logger.info(f"=== {source.upper()} sync starting ===")

    set_progress(source, step='Starting…', pct=0, started=now.isoformat(), total=0, chunks_done=0, chunks_total=0)

    start = time.time()
    with get_db() as conn:
        cur = conn.execute("INSERT INTO sync_runs (source, status) VALUES (?, 'running')", (source,))
        run_id = cur.lastrowid

    # Fetch
    disc_count = 0
    try:
        result = fetch_fn()
        # CWR returns (records, disc_count); others return just records
        if isinstance(result, tuple):
            records, disc_count = result
        else:
            records = result
        logger.info(f"[{source}] Fetched {len(records)} SKUs")
        set_progress(source, step='Fetched — authenticating with Walmart…', pct=30, total=len(records))
    except Exception as e:
        logger.error(f"[{source}] Fetch failed: {e}")
        set_progress(source, step=f'Fetch failed: {e}', pct=0, error=True)
        with get_db() as conn:
            conn.execute("UPDATE sync_runs SET status='error', completed_at=datetime('now') WHERE id=?", (run_id,))
        clear_progress(source)
        return

    # Auth
    try:
        token = get_walmart_token()
    except Exception as e:
        logger.error(f"[{source}] Walmart auth failed: {e}")
        set_progress(source, step=f'Walmart auth failed: {e}', pct=0, error=True)
        with get_db() as conn:
            conn.execute("UPDATE sync_runs SET status='error', completed_at=datetime('now') WHERE id=?", (run_id,))
        clear_progress(source)
        return

    # Upload chunks
    chunks = list(chunked(records, CHUNK_SIZE))
    set_progress(source, chunks_total=len(chunks), step='Uploading to Walmart…', pct=35)
    feed_ids = []
    failed   = 0

    for i, chunk in enumerate(chunks, 1):
        feed_name = f"{feed_prefix}-{ts}-p{i}.xml"
        pct = 35 + int((i / len(chunks)) * 60)
        set_progress(source, step=f'Uploading chunk {i}/{len(chunks)} ({len(chunk):,} SKUs)…', pct=pct, chunks_done=i-1)
        xml_data = build_inventory_xml(chunk)

        rate_waits = [60, 120, 180, 240, 300]
        for attempt in range(5):
            try:
                resp = upload_bulk_feed(token, xml_data, feed_name)
                if resp.status_code == 429:
                    wait = rate_waits[min(attempt, len(rate_waits)-1)]
                    logger.warning(f"[{source}] Rate limited, waiting {wait}s…")
                    set_progress(source, step=f'Rate limited — waiting {wait}s before retry (attempt {attempt+1}/5)…')
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                feed_id = resp.json().get('feedId', '')
                feed_ids.append(feed_id)
                logger.info(f"[{source}] Chunk {i}/{len(chunks)} ✓ feedId: {feed_id}")
                set_progress(source, chunks_done=i)
                break
            except Exception as e:
                if attempt == 4:
                    logger.error(f"[{source}] Chunk {i} failed after 5 attempts: {e}")
                    failed += 1
                else:
                    time.sleep(15)
        time.sleep(5)

    status   = 'error' if failed == len(chunks) else 'success'
    duration = round(time.time() - start, 1)

    with get_db() as conn:
        conn.execute("""
            UPDATE sync_runs SET status=?, completed_at=datetime('now'),
            total=?, disc_count=?, feed_id=?, duration_s=? WHERE id=?
        """, (status, len(records), disc_count, feed_ids[0] if feed_ids else '', duration, run_id))

    set_progress(source, step='Done ✓', pct=100, chunks_done=len(chunks))
    logger.info(f"[{source}] Done in {duration}s — {len(chunks)-failed}/{len(chunks)} chunks OK")
    time.sleep(3)
    clear_progress(source)

def sync_cwr_keystone():
    run_sync('cwr',      fetch_cwr_inventory,     'CWR-DMv2')
    time.sleep(60)
    run_sync('keystone', fetch_keystone_inventory, 'KS-DMv2')

def sync_twh():
    run_sync('twh', fetch_twh_inventory, 'TWH-DMv2')

# --- D&H ---
def fetch_dandh_inventory():
    set_progress('dandh', step='Connecting to D&H FTP…', pct=5)
    ftp = ftplib.FTP()
    ftp.connect(DH_FTP_HOST, 21, timeout=30)
    ftp.login(DH_FTP_USER, DH_FTP_PASS)
    ftp.set_pasv(True)

    set_progress('dandh', step='Downloading ITEMLIST…', pct=15)
    buf = io.BytesIO()
    ftp.retrbinary(f'RETR {DH_ITEMLIST}', buf.write)

    set_progress('dandh', step='Downloading DISCOITEM…', pct=25)
    disc_buf = io.BytesIO()
    ftp.retrbinary(f'RETR {DH_DISCOITEM}', disc_buf.write)
    ftp.quit()

    set_progress('dandh', step='Parsing inventory…', pct=30)
    records = {}
    for line in buf.getvalue().decode('utf-8', errors='replace').splitlines():
        cols = line.split('|')
        if len(cols) < 5:
            continue
        sku = cols[4].strip()
        if not sku:
            continue
        try:
            qty = int(float(cols[1].strip() or '0'))
        except ValueError:
            qty = 0
        records[sku] = {'sku': f'DANDH-{sku}', 'qty': qty}

    # Zero out discontinued items from DISCOITEM (col 0 = SKU, space-padded)
    disc_count = 0
    for line in disc_buf.getvalue().decode('utf-8', errors='replace').splitlines():
        cols = line.split('|')
        if not cols:
            continue
        sku = cols[0].strip()
        if sku and sku not in records:
            records[sku] = {'sku': f'DANDH-{sku}', 'qty': 0}
            disc_count += 1

    if disc_count:
        logger.info(f"[dandh] Zeroing out {disc_count} discontinued SKUs")
        set_progress('dandh', step=f'Found {disc_count} discontinued SKUs — zeroing out…', pct=35, disc_count=disc_count)
    else:
        set_progress('dandh', disc_count=0)

    return list(records.values()), disc_count

def sync_dandh():
    run_sync('dandh', fetch_dandh_inventory, 'DH-DMv2')

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
        last_dh = conn.execute(
            "SELECT * FROM sync_runs WHERE source='dandh' AND status!='running' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        is_running = conn.execute(
            "SELECT COUNT(*) as c FROM sync_runs WHERE status='running'"
        ).fetchone()['c'] > 0
    return jsonify({
        'is_running':    is_running,
        'last_cwr':      dict(last_cwr)  if last_cwr  else None,
        'last_keystone': dict(last_ks)   if last_ks   else None,
        'last_twh':      dict(last_twh)  if last_twh  else None,
        'last_dandh':    dict(last_dh)   if last_dh   else None,
    })

@app.route('/api/progress')
def progress():
    return jsonify(get_progress())

@app.route('/api/runs')
def runs():
    source = request.args.get('source')
    limit  = request.args.get('limit', 40, type=int)
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
        threading.Thread(target=sync_twh, daemon=True).start()
    elif source == 'dandh':
        return jsonify({'status': 'disabled', 'source': 'dandh'}), 200
    else:
        threading.Thread(target=sync_cwr_keystone, daemon=True).start()
        threading.Thread(target=sync_twh, daemon=True).start()
        threading.Thread(target=sync_dandh, daemon=True).start()
    return jsonify({'status': 'started', 'source': source})

if __name__ == '__main__':
    init_db()
    # No startup sync — scheduler handles it at fixed times
    scheduler = BackgroundScheduler(timezone='America/New_York')
    scheduler.add_job(sync_cwr_keystone, 'cron', hour='0,6,12,18', minute=0)
    scheduler.add_job(sync_twh,          'cron', hour='3,9,15,21', minute=0)
    # D&H disabled
    scheduler.start()
    logger.info("Scheduler ready — CWR/Keystone: 12am/6am/12pm/6pm | TWH: 3am/9am/3pm/9pm Eastern")
    app.run(host='0.0.0.0', port=PORT)
