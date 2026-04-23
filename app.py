from flask import Flask, request, Response, render_template, send_file
import pandas as pd
import json, io, os, time, re, requests
import concurrent.futures

app = Flask(__name__)

# 🔑 API KEY dari Railway
API_KEY = os.getenv("APICOID_API_KEY")

# 🔥 Normalisasi nama
WHITESPACE = re.compile(r'\s+')
def clean(s):
    return WHITESPACE.sub('', str(s).upper())

# 🔥 Mapping bank biar aman
BANK_MAP = {
    "bca": "bca",
    "bri": "bri",
    "bni": "bni",
    "mandiri": "mandiri",
    "cimb": "cimb",
    "permata": "permata"
}

# 🔥 REAL API (sesuai docs kamu)
def cek_rekening(nama, rekening, bank):
    url = "https://use.api.co.id/validation/bank"

    bank = BANK_MAP.get(bank.lower(), bank.lower())

    payload = {
        "account_number": rekening,
        "bank_code": bank
    }

    headers = {
        "x-api-key": API_KEY,
        "Content-Type": "application/json"
    }

    for _ in range(3):  # retry
        try:
            res = requests.post(url, json=payload, headers=headers, timeout=10)

            if res.status_code != 200:
                time.sleep(0.5)
                continue

            data = res.json()

            if not data.get("is_valid"):
                return None

            return data.get("name")

        except Exception as e:
            print("API ERROR:", e)
            time.sleep(1)

    return None


def proses_satu(args):
    i, row = args

    nama = str(row.get('nama', '')).strip()
    rekening = str(row.get('rekening', '')).strip()
    bank = str(row.get('bank', '')).strip()

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


def generate_stream(file):
    df = pd.read_excel(file)

    df.columns = [str(c).strip().lower() for c in df.columns]
    records = df.to_dict('records')
    total = len(records)

    yield f"data: {json.dumps({'type':'start','total':total})}\n\n"

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        for data in executor.map(proses_satu, enumerate(records)):
            yield f"data: {json.dumps(data)}\n\n"
            time.sleep(0.01)

    yield f"data: {json.dumps({'type':'done','total':total})}\n\n"


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
def template():
    df = pd.DataFrame({
        "nama": ["Budi Santoso", "Siti Rahayu"],
        "rekening": ["1234567890", "0987654321"],
        "bank": ["bca", "mandiri"]
    })

    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)

    return send_file(buf, as_attachment=True, download_name="template.xlsx")


@app.route('/download', methods=['POST'])
def download():
    data = request.json.get("data", [])
    fmt = request.json.get("format", "xlsx")

    df = pd.DataFrame(data)

    buf = io.BytesIO()

    if fmt == "csv":
        df.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(buf, as_attachment=True, download_name="hasil.csv")

    df.to_excel(buf, index=False)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name="hasil.xlsx")


if __name__ == '__main__':
    app.run(debug=True, threaded=True)
