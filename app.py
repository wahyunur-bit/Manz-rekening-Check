from flask import Flask, request, Response, render_template, send_file
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

def clean(s):
    return WHITESPACE.sub('', str(s).upper())


# 🔍 CEK REKENING
def cek_rekening(nama, rekening, bank):
    try:
        headers = {"Authorization": f"Bearer {API_KEY}"}
        payload = {
            "bank": bank.lower(),
            "account_number": rekening
        }

        res = requests.post(BASE_URL, json=payload, headers=headers, timeout=3)

        if res.status_code != 200:
            print("API ERROR:", res.text)
            return None

        data = res.json()

        if not data.get("success"):
            return None

        return data["data"]["account_name"]

    except Exception as e:
        print("ERROR:", e)
        return None


# 🔄 PROSES 1 BARIS
def proses_satu(args):
    i, row = args

    nama     = str(row.get('nama', '')).strip()
    rekening = str(row.get('rekening', '')).strip()
    bank     = str(row.get('bank', '')).strip()

    nama_bank = cek_rekening(nama, rekening, bank)

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


# ⚡ STREAM GENERATOR (REALTIME FIX)
def generate_stream(file):
    try:
        df = pd.read_excel(file)
        df.columns = [str(c).strip().lower() for c in df.columns]
        records = df.to_dict('records')
        total = len(records)

        yield f"data: {json.dumps({'type':'start','total':total})}\n\n"

        # ⚡ IMPORTANT: realtime streaming
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [
                executor.submit(proses_satu, (i, row))
                for i, row in enumerate(records)
            ]

            for future in concurrent.futures.as_completed(futures):
                data = future.result()
                yield f"data: {json.dumps(data)}\n\n"

        yield f"data: {json.dumps({'type':'done','total':total})}\n\n"

    except Exception as e:
        yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/stream', methods=['POST'])
def stream():
    file = request.files['file']

    return Response(
        generate_stream(file),
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

    return send_file(buf, as_attachment=True, download_name='template.xlsx')


@app.route('/download', methods=['POST'])
def download():
    data = request.json.get('data', [])
    fmt = request.json.get('format', 'xlsx')

    df = pd.DataFrame(data)
    buf = io.BytesIO()

    if fmt == 'csv':
        df.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(buf, as_attachment=True, download_name='hasil.csv')
    else:
        df.to_excel(buf, index=False)
        buf.seek(0)
        return send_file(buf, as_attachment=True, download_name='hasil.xlsx')


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080)
