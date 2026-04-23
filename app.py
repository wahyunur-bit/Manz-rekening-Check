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

app = Flask(__name__)

API_KEY = os.environ.get("APICOID_API_KEY")
print("=" * 40)
print("API KEY:", "ADA" if API_KEY else "TIDAK ADA — set env APICOID_API_KEY")
print("=" * 40)


def create_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    return session

session = create_session()


def validasi_format_rekening(rekening):
    if not rekening.isdigit():
        return False, "Nomor rekening harus angka"
    if len(rekening) < 6 or len(rekening) > 20:
        return False, "Panjang nomor rekening tidak wajar"
    return True, ""


def cek_rekening(bank_code, account_number, account_name):
    if not API_KEY:
        return {"error": "API key tidak ada. Set env variable APICOID_API_KEY"}

    # ✅ URL YANG BENAR sesuai dokumentasi
    url = "https://use.api.co.id/validation/bank"

    try:
        response = session.get(
            url,
            headers={"x-api-co-id": API_KEY},
            params={
                "bank_code": bank_code.lower(),
                "account_number": account_number,
                "account_name": account_name
            },
            timeout=15
        )

        print(f"[API] {bank_code} {account_number} -> HTTP {response.status_code} | {response.text[:120]}")

        if response.status_code == 401:
            return {"error": "API Key tidak valid (401)"}
        if response.status_code == 402:
            return {"error": "Saldo points habis, top up di dashboard.api.co.id (402)"}
        if response.status_code == 403:
            return {"error": "Akses ditolak, paket perlu upgrade (403)"}
        if response.status_code == 422:
            return {"error": f"Parameter tidak valid (422): {response.text[:200]}"}
        if response.status_code != 200:
            return {"error": f"HTTP {response.status_code}: {response.text[:200]}"}

        return response.json()

    except requests.exceptions.Timeout:
        return {"error": "Timeout — api.co.id tidak merespons dalam 15 detik"}
    except requests.exceptions.ConnectionError as e:
        return {"error": f"Koneksi gagal: {str(e)[:100]}"}
    except Exception as e:
        return {"error": str(e)}


def parse_row(row):
    nama     = str(row.get("nama", "")).strip()
    rekening = str(row.get("rekening", "")).strip()
    bank     = str(row.get("bank", "")).strip()
    if rekening.endswith(".0"):
        rekening = rekening[:-2]
    return nama, rekening, bank


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/health")
def health():
    return jsonify({"status": "ok", "api_key": "ADA" if API_KEY else "TIDAK ADA"})


@app.route("/stream", methods=["POST"])
def stream():
    if not API_KEY:
        def err():
            yield f"data: {json.dumps({'type':'fatal','error':'API KEY belum diset di server'})}\n\n"
        return Response(stream_with_context(err()), mimetype="text/event-stream")

    if "file" not in request.files:
        def err():
            yield f"data: {json.dumps({'type':'fatal','error':'File tidak ada'})}\n\n"
        return Response(stream_with_context(err()), mimetype="text/event-stream")

    file_bytes = request.files["file"].read()

    def generate():
        try:
            try:
                df = pd.read_excel(BytesIO(file_bytes))
            except Exception as e:
                yield f"data: {json.dumps({'type':'fatal','error':f'File Excel tidak bisa dibaca: {str(e)}'})}\n\n"
                return

            df.columns = [str(c).lower().strip() for c in df.columns]
            missing = [c for c in ["nama", "rekening", "bank"] if c not in df.columns]
            if missing:
                yield f"data: {json.dumps({'type':'fatal','error':f'Kolom tidak ditemukan: {missing}. Kolom ada: {list(df.columns)}'})}\n\n"
                return

            df = df.dropna(subset=["nama", "rekening"])
            total = len(df)
            yield f"data: {json.dumps({'type':'start','total':total})}\n\n"

            for idx, (_, row) in enumerate(df.iterrows(), 1):
                nama, rekening, bank = parse_row(row)
                if not nama or nama.lower() in ["nan", "none", ""]:
                    continue

                base = {"type":"result","index":idx,"total":total,"nama":nama,"rekening":rekening,"bank":bank.upper()}

                valid, msg = validasi_format_rekening(rekening)
                if not valid:
                    yield f"data: {json.dumps({**base,'nama_di_bank':'-','score':0,'status':'FORMAT SALAH','keterangan':msg})}\n\n"
                    continue

                res = cek_rekening(bank, rekening, nama)

                if "error" in res:
                    yield f"data: {json.dumps({**base,'nama_di_bank':'-','score':0,'status':'ERROR','keterangan':res['error']})}\n\n"
                    time.sleep(0.3)
                    continue

                is_valid     = res.get("is_valid", False)
                score        = res.get("score", 0) or 0
                nama_di_bank = res.get("name") or "-"
                note         = res.get("note", "")

                if is_valid and score >= 9:
                    status, ket = "MATCH",       f"Nama cocok (score {score})"
                elif is_valid and score >= 7:
                    status, ket = "MIRIP",       f"Nama mirip, cek manual (score {score})"
                elif note == "Name was not returned":
                    status, ket = "CEK MANUAL",  "Rekening valid, bank tidak kirim nama"
                else:
                    status, ket = "TIDAK COCOK", f"Nama tidak cocok (score {score})"

                yield f"data: {json.dumps({**base,'nama_di_bank':nama_di_bank,'score':score,'status':status,'keterangan':ket})}\n\n"
                time.sleep(0.3)

            yield f"data: {json.dumps({'type':'done','total':total})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'type':'fatal','error':f'Server error: {str(e)}','trace':traceback.format_exc()})}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"}
    )


if __name__ == "__main__":
    if not API_KEY:
        print("\nSet API key dulu:")
        print("  Windows CMD : set APICOID_API_KEY=xxx")
        print("  PowerShell  : $env:APICOID_API_KEY='xxx'")
        print("  Linux/Mac   : export APICOID_API_KEY=xxx\n")
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)
