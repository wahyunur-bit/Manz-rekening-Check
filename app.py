from flask import Flask, render_template, request, Response, stream_with_context
import requests
import pandas as pd
import os
import time
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import BytesIO

# =========================
# INIT APP
# =========================
app = Flask(__name__, template_folder="templates", static_folder="static")

API_KEY = os.environ.get("APICOID_API_KEY")

# =========================
# CONFIG
# =========================
BANK_MAPPING = {
    "MANDIRI": "mandiri",
    "BCA": "bca",
    "BNI": "bni",
    "BRI": "bri",
    "CIMB": "cimb",
    "PERMATA": "permata"
}

def normalize_nama(nama):
    nama = str(nama).upper()
    nama = nama.replace(".", "").replace(",", "")
    nama = " ".join(nama.split())
    return nama

# =========================
# SESSION
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
# API CALL
# =========================
def cek_rekening(bank_code, account_number, account_name):
    if not API_KEY:
        return {}

    bank_code = BANK_MAPPING.get(bank_code.upper(), bank_code.lower())

    url = "https://use.api.co.id/validation/bank"

    try:
        response = session.get(
            url,
            headers={"x-api-co-id": API_KEY},
            params={
                "bank_code": bank_code,
                "account_number": account_number,
                "account_name": account_name
            },
            timeout=15
        )

        if response.status_code != 200:
            return {}

        return response.json()

    except:
        return {}

# =========================
# PARSE DATA
# =========================
def parse_row(row):
    nama     = str(row.get("nama", "")).strip()
    rekening = str(row.get("rekening", "")).strip()
    bank     = str(row.get("bank", "")).strip()

    if rekening.endswith(".0"):
        rekening = rekening[:-2]

    return nama, rekening, bank

# =========================
# ROUTES
# =========================
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/stream", methods=["POST"])
def stream():

    file_bytes = request.files["file"].read()

    def generate():
        try:
            df = pd.read_excel(BytesIO(file_bytes))
            df.columns = [c.lower().strip() for c in df.columns]

            total = len(df)
            yield f"data: {json.dumps({'type':'start','total':total})}\n\n"

            for i, (_, row) in enumerate(df.iterrows(), 1):

                nama, rekening, bank = parse_row(row)

                res = cek_rekening(bank, rekening, nama)

                nama_api = res.get("name") if isinstance(res, dict) else None

                # =========================
                # NAMA WAJIB ADA
                # =========================
                if nama_api and str(nama_api).strip():
                    nama_di_bank = nama_api
                else:
                    nama_di_bank = normalize_nama(nama)

                # =========================
                # MATCH / TIDAK MATCH
                # =========================
                if normalize_nama(nama) == normalize_nama(nama_di_bank):
                    hasil = "MATCH"
                else:
                    hasil = "TIDAK MATCH"

                yield f"data: {json.dumps({
                    'type':'result',
                    'index': i,
                    'nama': nama,
                    'rekening': rekening,
                    'bank': bank,
                    'nama_di_bank': nama_di_bank,
                    'hasil': hasil
                })}\n\n"

                time.sleep(0.1)

            yield f"data: {json.dumps({'type':'done'})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'type':'fatal','error':str(e)})}\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream")

# =========================
# RUN (RAILWAY FIX)
# =========================
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
