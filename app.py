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
BASE_URL = "https://use.api.co.id/validation/bank"

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


def normalize_bank_code(bank_input):
    """Normalisasi kode bank — API menerima short (bca) atau full (bank_bca)."""
    code = str(bank_input).strip().lower()
    # Hilangkan spasi dan karakter aneh
    code = WHITESPACE.sub('', code)
    # Ensure bank_ prefix is present to be extremely valid
    if not code.startswith('bank_'):
        code = 'bank_' + code
    return code


def cek_rekening(rekening, bank, nama_pengirim):
    """
    Cek rekening via api.co.id — POST /validation/bank
    Parameters:
      - bank_code: kode bank (short/full format)
      - account_number: nomor rekening
      - account_name: nama yang akan dicocokkan
    Returns dict: {nama_bank, is_valid, score} atau None jika gagal
    """
    try:
        headers = {
            "x-api-co-id": API_KEY,
            "Content-Type": "application/json"
        }
        bank_code = normalize_bank_code(bank)

        payload = {
            "bank_code": bank_code,
            "account_number": str(rekening).strip(),
            "account_name": str(nama_pengirim).strip()
        }

        print(f"[API REQ] bank_code={bank_code}, account={rekening}, name={nama_pengirim}")

        res = requests.post(BASE_URL, json=payload, headers=headers, timeout=15)

        print(f"[API RES] HTTP {res.status_code}: {res.text[:500]}")

        if res.status_code != 200:
            print(f"API HTTP Error {res.status_code}: {res.text[:300]}")
            return None

        data = res.json()

        if not data.get("is_success"):
            print(f"API not success: {json.dumps(data)[:300]}")
            return None

        inner = data.get("data", {})

        # API mengembalikan:
        # - is_valid: true/false (score >= 7.0 = valid)
        # - score: 0.0 - 10.0 (tingkat kecocokan nama)
        # - name: nama rekening ter-mask (contoh: "Rif**** Eln****"), null jika invalid
        # - message: pesan status
        # - note: catatan tambahan

        nama_bank = inner.get("name")  # Nama ter-mask dari bank, bisa null
        is_valid = inner.get("is_valid", False)
        score = inner.get("score", 0)

        return {
            "nama_bank": nama_bank if nama_bank else None,
            "is_valid": is_valid,
            "score": score,
            "message": inner.get("message", ""),
            "note": inner.get("note", "")
        }

    except requests.exceptions.Timeout:
        print(f"TIMEOUT cek_rekening: bank={bank}, rek={rekening}")
        return None
    except requests.exceptions.ConnectionError:
        print(f"CONNECTION ERROR cek_rekening: bank={bank}, rek={rekening}")
        return None
    except Exception as e:
        print(f"ERROR cek_rekening: {e}")
        return None


def proses_satu(args):
    i, row = args

    nama     = str(row.get('nama', '')).strip()
    rekening = clean_rekening(row.get('rekening', ''))
    bank     = str(row.get('bank', '')).strip()

    result = cek_rekening(rekening, bank, nama)

    if result is None:
        # API call gagal total (timeout, connection error, dsb.)
        hasil = "TIDAK VALID"
        nama_bank = "-"
    elif result["is_valid"]:
        # API bilang valid (score >= 7.0) — nama cocok
        hasil = "MATCH"
        nama_bank = result["nama_bank"] or "-"
    elif result["nama_bank"]:
        # API berhasil tapi nama tidak cocok (score < 7.0)
        # Rekening ditemukan, tapi nama beda
        hasil = "TIDAK SAMA"
        nama_bank = result["nama_bank"]
    else:
        # Rekening tidak ditemukan di bank (name=null, is_valid=false)
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

    cols = ['No', 'Nama', 'Rekening', 'Bank', 'Nama Bank', 'Score', 'Hasil']
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
