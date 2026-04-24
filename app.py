from flask import Flask, request, Response, render_template, send_file, jsonify
import pandas as pd
import json
import io
import requests
import concurrent.futures
import os
import re
import threading

app = Flask(__name__)

API_KEY = os.getenv("APICOID_API_KEY")
BASE_URL = "https://api.api.co.id/v1/bank/account"

WHITESPACE = re.compile(r'\s+')

# --- Activation Code System ---
CODES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'codes.json')
codes_lock = threading.Lock()


def load_codes():
    try:
        with open(CODES_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return {}


def save_codes(codes):
    with open(CODES_FILE, 'w') as f:
        json.dump(codes, f, indent=2)


# --- Helpers ---
def clean(s):
    return WHITESPACE.sub('', str(s).upper())


def clean_rekening(val):
    """Bersihkan nomor rekening — hilangkan .0 dari float pandas."""
    s = str(val).strip()
    # Jika pandas baca sebagai float misal "1234567890.0"
    try:
        if '.' in s:
            s = str(int(float(s)))
    except (ValueError, OverflowError):
        pass
    return s


def cek_rekening(rekening, bank):
    try:
        headers = {"x-api-co-id": API_KEY}
        payload = {
            "bank": bank.lower().strip(),
            "account_number": str(rekening).strip()
        }
        res = requests.post(BASE_URL, json=payload, headers=headers, timeout=10)

        if res.status_code != 200:
            print(f"API HTTP {res.status_code}: {res.text[:200]}")
            return None

        data = res.json()

        if not data.get("is_success"):
            print(f"API not success: {json.dumps(data)[:200]}")
            return None

        inner = data.get("data", {})

        nama = inner.get("name")
        if nama:
            return str(nama)

        print(f"API no name field: {json.dumps(inner)[:200]}")
        return None

    except Exception as e:
        print("ERROR cek_rekening:", e)
        return None


def proses_satu(args):
    i, row = args

    nama     = str(row.get('nama', '')).strip()
    rekening = clean_rekening(row.get('rekening', ''))
    bank     = str(row.get('bank', '')).strip()

    nama_bank = cek_rekening(rekening, bank)

    if not nama_bank:
        hasil = "TIDAK VALID"
    elif clean(nama) == clean(nama_bank):
        hasil = "MATCH"
    else:
        hasil = "TIDAK SAMA"

    return {
        "type": "result",
        "index": i + 1,
        "nama": nama,
        "rekening": rekening,
        "bank": bank,
        "nama_bank": nama_bank or "-",
        "hasil": hasil
    }


def generate_stream(file_data):
    try:
        df = pd.read_excel(file_data)
        df.columns = [str(c).strip().lower() for c in df.columns]
        records = df.to_dict('records')
        total = len(records)

        yield f"data: {json.dumps({'type':'start','total':total})}\n\n"

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(proses_satu, (i, row)): i
                for i, row in enumerate(records)
            }

            for future in concurrent.futures.as_completed(futures):
                try:
                    data = future.result()
                    yield f"data: {json.dumps(data)}\n\n"
                except Exception as e:
                    yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"

        yield f"data: {json.dumps({'type':'done','total':total})}\n\n"

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

    with codes_lock:
        codes = load_codes()

        if code not in codes:
            return jsonify({"valid": False, "message": "Kode tidak ditemukan"}), 403

        if codes[code] is True:
            return jsonify({"valid": False, "message": "Kode sudah pernah digunakan"}), 403

        # Tandai kode sebagai sudah dipakai
        codes[code] = True
        save_codes(codes)

    return jsonify({"valid": True, "message": "Kode valid, selamat menggunakan!"})


@app.route('/stream', methods=['POST'])
def stream():
    file = request.files.get('file')
    if not file:
        return Response("data: {\"type\":\"error\",\"message\":\"No file\"}\n\n", mimetype='text/event-stream')

    file_data = io.BytesIO(file.read())

    return Response(
        generate_stream(file_data),
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
        "nama": ["Budi Santoso", "Siti Rahayu"],
        "rekening": ["1234567890", "0987654321"],
        "bank": ["bca", "mandiri"]
    })
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name='template.xlsx',
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@app.route('/download', methods=['POST'])
def download():
    body = request.json or {}
    raw  = body.get('data', [])
    fmt  = body.get('format', 'xlsx')

    cols = ['No', 'Nama', 'Rekening', 'Bank', 'Nama Bank', 'Hasil']
    df = pd.DataFrame(raw, columns=cols)

    buf = io.BytesIO()

    if fmt == 'csv':
        df.to_csv(buf, index=False, sep=',', encoding='utf-8-sig')
        buf.seek(0)
        return send_file(buf, as_attachment=True, download_name='hasil.csv',
                         mimetype='text/csv')
    else:
        df.to_excel(buf, index=False)
        buf.seek(0)
        return send_file(buf, as_attachment=True, download_name='hasil.xlsx',
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


if __name__ == '__main__':
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
