from flask import Flask, render_template, request, jsonify, Response, stream_with_context
import requests
import pandas as pd
import os
import time
import json
import traceback
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from io import BytesIO
from rapidfuzz import fuzz

app = Flask(__name__)

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
    nama = nama.upper()
    nama = nama.replace(".", "").replace(",", "")
    nama = " ".join(nama.split())
    return nama

def hitung_kemiripan(nama_input, nama_bank):
    if not nama_bank:
        return 0
    return fuzz.token_sort_ratio(
        normalize_nama(nama_input),
        normalize_nama(nama_bank)
    )

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
        return {"error": "API key tidak ada"}

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

        print("[DEBUG]", response.text)

        if response.status_code != 200:
            return {"error": f"HTTP {response.status_code}"}

        return response.json()

    except Exception as e:
        return {"error": str(e)}

# =========================
# PARSE
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

                if "error" in res:
                    yield f"data: {json.dumps({'status':'ERROR','keterangan':res['error']})}\n\n"
                    continue

                is_valid = res.get("is_valid", False)
                nama_api = res.get("name")
                note = res.get("note", "")

                # =========================
                # ALWAYS ADA NAMA
                # =========================
                if nama_api and nama_api.strip():
                    nama_di_bank = nama_api
                    sumber = "API"
                else:
                    nama_di_bank = normalize_nama(nama)
                    sumber = "INPUT"

                similarity = hitung_kemiripan(nama, nama_di_bank)

                # =========================
                # FINAL LOGIC
                # =========================
                if is_valid:

                    if sumber == "API":

                        if similarity >= 85:
                            status = "MATCH"
                            ket = f"Sangat cocok ({similarity}%)"

                        elif similarity >= 60:
                            status = "MIRIP"
                            ket = f"Cukup mirip ({similarity}%)"

                        else:
                            status = "VALID REKENING"
                            ket = f"Nama beda ({similarity}%)"

                    else:
                        status = "VALID TANPA NAMA API"
                        ket = "Rekening valid, nama dari input"

                else:
                    status = "TIDAK COCOK"
                    ket = "Rekening tidak valid"

                yield f"data: {json.dumps({
                    'type':'result',
                    'index': i,
                    'nama': nama,
                    'rekening': rekening,
                    'bank': bank,
                    'nama_di_bank': nama_di_bank,
                    'sumber_nama': sumber,
                    'similarity': similarity,
                    'status': status,
                    'keterangan': ket
                })}\n\n"

                time.sleep(0.2)

            yield f"data: {json.dumps({'type':'done'})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'type':'fatal','error':str(e)})}\n\n"

    return Response(stream_with_context(generate()), mimetype="text/event-stream")

# =========================
# RUN
# =========================

if __name__ == "__main__":
    app.run(debug=True, port=5000)
