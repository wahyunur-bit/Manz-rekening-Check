from flask import Flask, request, Response, render_template, send_file, jsonify
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
        print(f"[DB ERROR] Redis failed to connect: {e}")

WHITESPACE = re.compile(r'\s+')

CODES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'codes.json')
codes_lock = threading.Lock()


def load_codes():
    """Fallback function for local file storage if Redis is not available."""
    try:
        if os.path.exists(CODES_FILE):
            with open(CODES_FILE, 'r') as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def save_codes(codes):
    """Fallback function for local file storage."""
    with open(CODES_FILE, 'w') as f:
        json.dump(codes, f, indent=2)


def get_quota(code):
    """Ambil sisa kuota dari Redis atau local file."""
    if r:
        try:
            val = r.get(f"code:{code}")
            if val is not None:
                return int(val)
        except Exception as e:
            print(f"[DB ERROR] get_quota: {e}")

    # Fallback to local
    codes = load_codes()
    quota = codes.get(code)
    if isinstance(quota, bool):
        quota = 0 if quota is True else 100
    return quota


def update_quota(code, new_q):
    """Update kuota ke Redis atau local file."""
    if r:
        try:
            r.set(f"code:{code}", new_q)
            return
        except Exception as e:
            print(f"[DB ERROR] update_quota: {e}")

    with codes_lock:
        codes = load_codes()
        codes[code] = new_q
        save_codes(codes)


def deduct_quota(code):
    """Potong kuota 1 secara atomic menggunakan Redis DECR."""
    if r:
        try:
            new_q = r.decr(f"code:{code}")
            return new_q
        except Exception as e:
            print(f"[DB ERROR] deduct_quota: {e}")

    with codes_lock:
        current = get_quota(code)
        if current is None: return 0
        new_q = max(0, current - 1)
        update_quota(code, new_q)
        return new_q


def migrate_to_redis():
    """Pindahkan data dari codes.json ke Redis jika Redis masih kosong."""
    if not r: return
    try:
        if r.dbsize() == 0 and os.path.exists(CODES_FILE):
            print("[DB] Migrating codes.json data to Redis...")
            data = load_codes()
            for code, quota in data.items():
                if isinstance(quota, bool):
                    quota = 0 if quota is True else 100
                r.set(f"code:{code}", quota)
            print(f"[DB] Migration success: {len(data)} codes moved.")
    except Exception as e:
        print(f"[DB ERROR] Migration failed: {e}")

def clean(s):
    return WHITESPACE.sub('', str(s).upper())


def clean_rekening(val):
    """Bersihkan nomor rekening — hilangkan .0 dari float pandas."""
    s = str(val).strip()
    try:
        if '.' in s:
            s = str(int(float(s)))
    except (ValueError, OverflowError):
        pass
    return s


def normalize_bank_code(bank_input):
    """Bersihkan input bank dari user agar siap diolah di cek_rekening."""
    code = str(bank_input).strip().lower()
    code = WHITESPACE.sub('', code)
    if code.startswith('bank_'):
        code = code.replace('bank_', '', 1)
    return code


def cek_rekening(rekening, bank_code_raw, nama_pengirim):
    if not API_KEY:
        print("[ERROR] API_KEY tidak terkonfigurasi (None)")
        return None

    bank_clean = normalize_bank_code(bank_code_raw)
    formats_to_try = [bank_clean, f"bank_{bank_clean}"]

    headers = {
        "x-api-co-id": API_KEY,
        "Accept": "application/json"
    }

    for attempt, current_bank_code in enumerate(formats_to_try):
        params = {
            "bank_code": current_bank_code,
            "account_number": str(rekening).strip(),
            "account_name": str(nama_pengirim).strip()
        }

        try:
            print(f"[API REQ] {rekening} | Try: {current_bank_code}")
            res = requests.get(BASE_URL, params=params, headers=headers, timeout=4)

            if res.status_code == 429:
                time.sleep(1)
                continue
            elif res.status_code in [500, 502, 503, 504]:
                continue

            if res.status_code == 401 or res.status_code == 402:
                print(f"[AUTH ERROR] API Key bermasalah atau Saldo api.co.id Habis (HTTP {res.status_code})")
                return None

            data = res.json()
            inner = data.get("data")

            if data.get("is_success") and inner:
                print(f"[API OK] Found: {inner.get('name')} | Score: {inner.get('score')}")
                return {
                    "nama_bank": inner.get("name"),
                    "is_valid": inner.get("is_valid", False),
                    "score": inner.get("score", 0)
                }

            msg = data.get("message", "Fail")
            print(f"[API FAIL] {current_bank_code}: {msg}")

        except Exception as e:
            print(f"[API ERROR] {e}")

    print(f"[DONE] Semua format gagal untuk: {rekening}")
    return None


def proses_satu(args):
    i, row = args
    nama     = str(row.get('nama', '')).strip()
    rekening = clean_rekening(row.get('rekening', ''))
    bank     = str(row.get('bank', '')).strip()

    result = cek_rekening(rekening, bank, nama)

    if result is None:
        hasil = "TIDAK VALID"
        nama_bank = "-"
    elif result["is_valid"]:
        hasil = "MATCH"
        nama_bank = nama.upper()
    elif result["nama_bank"]:
        hasil = "TIDAK SAMA"
        nama_bank = result["nama_bank"]
    else:
        hasil = "TIDAK VALID"
        nama_bank = "-"

    return {
        "type": "result",
        "index": i + 1,
        "nama": nama,
        "rekening": rekening,
        "bank": bank,
        "nama_bank": nama_bank,
        "hasil": hasil,
        "score": result["score"] if result else 0
    }


def generate_stream(records, code, start_quota):
    try:
        total = len(records)
        yield f"data: {json.dumps({'type':'start','total':total})}\n\n"

        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = {
                executor.submit(proses_satu, (i, row)): i
                for i, row in enumerate(records)
            }

            processed_count = 0
            for future in concurrent.futures.as_completed(futures):
                try:
                    data = future.result()
                    processed_count += 1
                    new_q = deduct_quota(code)
                    data['sisa_kuota'] = new_q
                    yield f"data: {json.dumps(data)}\n\n"
                    if new_q <= 0:
                        yield f"data: {json.dumps({'type':'error','message':'Kuota telah habis di tengah proses.'})}\n\n"
                        break
                except Exception as e:
                    yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"

        yield f"data: {json.dumps({'type':'done','total':total, 'sisa_kuota': start_quota - total})}\n\n"

    except Exception as e:
        yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/verify-code', methods=['POST'])
def verify_code():
    body = request.json or {}
    code = str(body.get('code', '')).strip()
    if not code:
        return jsonify({"valid": False, "message": "Kode tidak boleh kosong"}), 400
    quota = get_quota(code)
    if quota is None:
        return jsonify({"valid": False, "message": "Kode tidak ditemukan"}), 403
    if quota <= 0:
        return jsonify({"valid": False, "message": "Kuota kode aktivasi ini sudah habis (0)"}), 403
    return jsonify({"valid": True, "quota": quota, "message": "Kode valid, selamat menggunakan!"})


@app.route('/stream', methods=['POST'])
def stream():
    code = request.form.get('code', '')
    if 'file' not in request.files:
        return jsonify({"error": "Tidak ada file"}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "File kosong"}), 400
    try:
        file_data = io.BytesIO(file.read())
        df = pd.read_excel(file_data)
        df.columns = [str(c).strip().lower() for c in df.columns]
        records = df.to_dict('records')
        total = len(records)
    except Exception as e:
        return jsonify({"error": "Gagal membaca Excel: " + str(e)}), 400

    quota = get_quota(code)
    if quota is None:
        return jsonify({"error": "Kode lisensi tidak valid / sesi kadaluarsa"}), 403
    if quota <= 0:
        return jsonify({"error": "Kuota sudah habis (0). Silakan isi ulang kuota Anda."}), 403
    start_quota = quota
    return Response(
        generate_stream(records, code, start_quota),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive'
        }
    )


@app.route('/template')
def download_template():
    df = pd.DataFrame({
        "nama": ["TEGUH HASYA", "BAMBANG SUGITO", "WAHYU NUR IMAN", "SITI RAHAYU"],
        "rekening": ["2840446855", "7330699393", "1330024362634", "0987654321"],
        "bank": ["BCA", "BCA", "MANDIRI", "BRI"]
    })
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name='template.xlsx',
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@app.route('/supported-banks')
def supported_banks():
    try:
        res = requests.get("https://use.api.co.id/validation/bank/available", headers={"x-api-co-id": API_KEY}, timeout=15)
        if res.status_code == 200 and res.json().get("is_success"):
            banks = res.json()["data"]["banks"]
            df = pd.DataFrame(banks)
            df.index = df.index + 1
            df.columns = ["Nama Bank Resmi", "Kode Asli API"]
            df["Ketikan di Excel (Acuan)"] = df["Kode Asli API"].str.replace("bank_", "", n=1).str.upper()
            buf = io.BytesIO()
            df.to_excel(buf, index=False)
            buf.seek(0)
            return send_file(buf, as_attachment=True, download_name='Daftar_Bank_Support.xlsx',
                             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        print("Error fetching supported banks:", e)
    return "Gagal mengambil daftar bank dari API. Pastikan API key valid.", 500


@app.route('/download', methods=['POST'])
def download():
    body = request.json or {}
    raw  = body.get('data', [])
    fmt  = body.get('format', 'xlsx')
    cols = ['No', 'Nama', 'Rekening', 'Bank', 'Nama Bank', 'Score', 'Hasil']
    df = pd.DataFrame(raw, columns=cols)
    buf = io.BytesIO()
    if fmt == 'csv':
        df.to_csv(buf, index=False, sep=',', encoding='utf-8-sig')
        buf.seek(0)
        return send_file(buf, as_attachment=True, download_name='hasil.csv', mimetype='text/csv')
    else:
        df.to_excel(buf, index=False)
        buf.seek(0)
        return send_file(buf, as_attachment=True, download_name='hasil.xlsx',
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


if __name__ == '__main__':
    migrate_to_redis()
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
