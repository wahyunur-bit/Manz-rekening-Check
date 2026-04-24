from flask import Flask, request, Response, render_template, send_file, jsonify
import pandas as pd
import json
import io
import requests
import concurrent.futures
import os
import re

app = Flask(__name__)

API_KEY = os.getenv("APICOID_API_KEY")
BASE_URL = "https://api.api.co.id/v1/bank/account"

WHITESPACE = re.compile(r'\s+')

# =============================
# LICENSE SYSTEM (1x USE CODE)
# =============================

VALID_CODES = {
    "MANZ-001",
    "MANZ-002",
    "VIP-ACCESS-01"
}

USED_CODES = set()


def clean(s):
    s = str(s).upper().strip()
    s = re.sub(r'[^A-Z0-9]', '', s)
    return s


def cek_rekening(rekening, bank):
    try:
        headers = {"Authorization": f"Bearer {API_KEY}"}
        payload = {
            "bank": bank.lower().strip(),
            "account_number": str(rekening).strip()
        }

        res = requests.post(BASE_URL, json=payload, headers=headers, timeout=5)

        if res.status_code != 200:
            return None

        data = res.json()
        print(data)
        if not data.get("success"):
            return None

        return data["data"]["account_name"]

    except Exception as e:
        print("ERROR cek_rekening:", e)
        return None


def proses_satu(args):
    i, row = args

    nama     = str(row.get('nama', '')).strip()
    rekening = str(row.get('rekening', '')).strip()
    bank     = str(row.get('bank', '')).strip()

    nama_bank = cek_rekening(rekening, bank)

    if not nama_bank:
        hasil = "TIDAK VALID"
    else:
        nama1 = clean(nama)
        nama2 = clean(nama_bank)

        if nama1 == nama2 or nama1 in nama2 or nama2 in nama1:
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


# =============================
# VERIFY LICENSE
# =============================

@app.route('/verify-code', methods=['POST'])
def verify_code():
    data = request.json or {}
    code = str(data.get("code", "")).strip().upper()

    if not code:
        return jsonify({"success": False, "message": "Kode wajib diisi"})

    if code not in VALID_CODES:
        return jsonify({"success": False, "message": "Kode tidak valid"})

    if code in USED_CODES:
        return jsonify({"success": False, "message": "Kode sudah digunakan"})

    USED_CODES.add(code)

    return jsonify({"success": True})


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
