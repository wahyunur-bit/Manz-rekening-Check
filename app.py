from flask import Flask, request, Response, render_template, send_file, jsonify, stream_with_context
import pandas as pd
import json
import io
import requests
import concurrent.futures
import os
import re
import threading
import time

app = Flask(__name__)

# Ambil API Key dari Railway Variables
API_KEY = os.getenv("APICOID_API_KEY")
BASE_URL = "https://use.api.co.id/validation/bank"

import redis
REDIS_URL = os.getenv("REDIS_URL")
r = None
if REDIS_URL:
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        r.ping()
        print("[DB] Connected to Redis")
    except Exception as e:
        print(f"[DB ERROR] Redis failed: {e}")

WHITESPACE = re.compile(r'\s+')
CODES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'codes.json')
codes_lock = threading.Lock()

def load_codes():
    try:
        if os.path.exists(CODES_FILE):
            with open(CODES_FILE, 'r') as f:
                return json.load(f)
    except Exception: pass
    return {}

def save_codes(codes):
    with open(CODES_FILE, 'w') as f:
        json.dump(codes, f, indent=2)

def get_quota(code):
    if r:
        try:
            val = r.get(f"code:{code}")
            if val is not None: return int(val)
        except Exception: pass
    codes = load_codes()
    q = codes.get(code)
    if isinstance(q, bool): q = 0 if q is True else 100
    return q

def deduct_quota(code):
    if r:
        try: return r.decr(f"code:{code}")
        except Exception: pass
    with codes_lock:
        codes = load_codes()
        current = get_quota(code)
        if current is None: return 0
        new_q = max(0, current - 1)
        codes[code] = new_q
        save_codes(codes)
        return new_q

def migrate_to_redis():
    if not r: return
    try:
        if r.dbsize() == 0 and os.path.exists(CODES_FILE):
            data = load_codes()
            for k, v in data.items():
                if isinstance(v, bool): v = 0 if v is True else 100
                r.set(f"code:{k}", v)
    except Exception: pass

def clean_rekening(val):
    s = str(val).strip()
    try:
        if '.' in s: s = str(int(float(s)))
    except Exception: pass
    return s

def normalize_bank_code(bank_input):
    code = str(bank_input).strip().lower()
    code = WHITESPACE.sub('', code)
    if code.startswith('bank_'): code = code.replace('bank_', '', 1)
    return code

def cek_rekening(rekening, bank_code_raw, nama_pengirim):
    if not API_KEY: return None
    bank_clean = normalize_bank_code(bank_code_raw)
    formats = [bank_clean, f"bank_{bank_clean}"]
    headers = {"x-api-co-id": API_KEY, "Accept": "application/json"}

    for fmt in formats:
        params = {"bank_code": fmt, "account_number": str(rekening).strip(), "account_name": str(nama_pengirim).strip()}
        try:
            res = requests.get(BASE_URL, params=params, headers=headers, timeout=5)
            if res.status_code == 429:
                time.sleep(1)
                continue
            if res.status_code in [401, 402]: return None
            if res.status_code >= 500: continue
            
            data = res.json()
            inner = data.get("data")
            if data.get("is_success") and inner:
                return {"nama_bank": inner.get("name"), "is_valid": inner.get("is_valid", False), "score": inner.get("score", 0)}
        except Exception: continue
    return None

def proses_satu(args):
    i, row = args
    nama = str(row.get('nama', '')).strip()
    rekening = clean_rekening(row.get('rekening', ''))
    bank = str(row.get('bank', '')).strip()
    res = cek_rekening(rekening, bank, nama)
    
    if res is None: hasil, nb, sc = "TIDAK VALID", "-", 0
    elif res["is_valid"]: hasil, nb, sc = "MATCH", nama.upper(), res["score"]
    elif res["nama_bank"]: hasil, nb, sc = "TIDAK SAMA", res["nama_bank"], res["score"]
    else: hasil, nb, sc = "TIDAK VALID", "-", 0

    return {"type":"result", "index":i+1, "nama":nama, "rekening":rekening, "bank":bank, "nama_bank":nb, "hasil":hasil, "score":sc}

def generate_stream(records, code, start_quota):
    try:
        total = len(records)
        yield f"data: {json.dumps({'type':'start','total':total})}\n\n"

        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
            futures = {executor.submit(proses_satu, (i, row)): i for i, row in enumerate(records)}
            for future in concurrent.futures.as_completed(futures):
                try:
                    data = future.result()
                    data['sisa_kuota'] = deduct_quota(code)
                    yield f"data: {json.dumps(data)}\n\n"
                except Exception as e:
                    yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"

        yield f"data: {json.dumps({'type':'done','total':total})}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"

@app.route('/')
def index(): return render_template('index.html')

@app.route('/verify-code', methods=['POST'])
def verify_code():
    body = request.json or {}
    code = str(body.get('code', '')).strip()
    q = get_quota(code)
    if q is None: return jsonify({"valid":False, "message":"Kode tidak ditemukan"}), 403
    if q <= 0: return jsonify({"valid":False, "message":"Kuota habis"}), 403
    return jsonify({"valid":True, "quota":q})

@app.route('/stream', methods=['POST'])
def stream():
    code = request.form.get('code', '')
    if 'file' not in request.files: return jsonify({"error":"No file"}), 400
    file = request.files['file']
    try:
        df = pd.read_excel(io.BytesIO(file.read()))
        df.columns = [str(c).strip().lower() for c in df.columns]
        records = df.to_dict('records')
        q = get_quota(code)
        if q is None or q <= 0: return jsonify({"error":"Kuota habis"}), 403
        
        return Response(stream_with_context(generate_stream(records, code, q)), 
                        mimetype='text/event-stream',
                        headers={'Cache-Control':'no-cache','X-Accel-Buffering':'no','Connection':'keep-alive'})
    except Exception as e: return jsonify({"error":str(e)}), 400

@app.route('/template')
def download_template():
    df = pd.DataFrame({"nama":["BUDI","SITI"],"rekening":["12345","67890"],"bank":["BCA","BNI"]})
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name='template.xlsx')

@app.route('/download', methods=['POST'])
def download():
    raw = (request.json or {}).get('data', [])
    df = pd.DataFrame(raw, columns=['No','Nama','Rekening','Bank','Nama Bank','Score','Hasil'])
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name='hasil.xlsx')

if __name__ == '__main__':
    migrate_to_redis()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
