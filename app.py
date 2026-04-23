from flask import Flask, render_template, request, Response, stream_with_context
import pandas as pd
import requests
import os
import json
import time
from io import BytesIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__)

API_KEY = os.environ.get("APICOID_API_KEY")

# =========================
# SESSION (biar stabil)
# =========================
def create_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1,
                    status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    return session

session = create_session()

# =========================
# CLEAN TEXT
# =========================
def clean_text(s):
    return " ".join(str(s).upper().strip().split())

# =========================
# API CALL
# =========================
def cek_rekening(bank, rekening, nama):
    if not API_KEY:
        return {"error": "API KEY tidak ada"}

    try:
        url = "https://use.api.co.id/validation/bank"

        res = session.get(
            url,
            headers={"x-api-co-id": API_KEY},
            params={
                "bank_code": bank.lower(),
                "account_number": rekening,
                "account_name": nama
            },
            timeout=15
        )

        if res.status_code != 200:
            return {"error": f"HTTP {res.status_code}"}

        return res.json()

    except Exception as e:
        return {"error": str(e)}

# =========================
# ROUTES
# =========================
@app.route("/")
def index():
    return render_template("index.html")

# =========================
# STREAM REALTIME
# =========================
@app.route("/stream", methods=["POST"])
def stream():

    file = request.files.get("file")
    if not file:
        return {"error": "File tidak ada"}, 400

    file_bytes = file.read()

    def generate():
        try:
            df = pd.read_excel(BytesIO(file_bytes))
            df.columns = [c.lower().strip() for c in df.columns]

            yield f"data: {json.dumps({'type':'start','total': len(df)})}\n\n"

            index = 0

            for _, row in df.iterrows():

                nama = str(row.get("nama", "")).strip()
                rekening = str(row.get("rekening", "")).strip()
                bank = str(row.get("bank", "")).strip()

                # skip kosong
                if not nama or not rekening or not bank:
                    continue

                index += 1

                res = cek_rekening(bank, rekening, nama)

                if "error" in res:
                    yield f"data: {json.dumps({
                        'type':'result',
                        'index': index,
                        'nama': nama,
                        'rekening': rekening,
                        'bank': bank,
                        'nama_di_bank': '-',
                        'status': 'ERROR'
                    })}\n\n"
                    continue

                is_valid = res.get("is_valid", False)
                nama_api = res.get("name")

                if not is_valid:
                    hasil = "TIDAK VALID"
                    nama_bank = "-"
                else:
                    nama_bank = nama_api if nama_api else nama

                    if clean_text(nama) == clean_text(nama_bank):
                        hasil = "MATCH"
                    else:
                        hasil = "TIDAK SAMA"

                yield f"data: {json.dumps({
                    'type':'result',
                    'index': index,
                    'nama': nama,
                    'rekening': rekening,
                    'bank': bank,
                    'nama_di_bank': nama_bank,
                    'status': hasil
                })}\n\n"

                time.sleep(0.15)

            yield f"data: {json.dumps({'type':'done'})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'type':'fatal','error': str(e)})}\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream")


# =========================
# TEMPLATE EXCEL
# =========================
@app.route("/template")
def template():
    df = pd.DataFrame([
        {"nama": "WAHYU NUR IMAN", "rekening": "1234567890", "bank": "MANDIRI"}
    ])
    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    return Response(
        output,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=template.xlsx"}
    )


if __name__ == "__main__":
    app.run(debug=True)
