from flask import Flask, render_template, request, jsonify
import requests
import pandas as pd
import os
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__)

API_KEY = os.environ.get("APICOID_API_KEY")
print("API KEY:", "ADA" if API_KEY else "TIDAK ADA")


# 🔥 SESSION + RETRY (ANTI CONNECTION RESET)
def create_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    return session


session = create_session()


def validasi_format_rekening(rekening):
    if not rekening.isdigit():
        return False, "Nomor rekening harus angka"
    if len(rekening) < 6 or len(rekening) > 20:
        return False, "Panjang tidak wajar"
    return True, ""


def cek_rekening(bank_code, account_number, account_name):
    if not API_KEY:
        return {"error": "API key tidak ada"}

    url = "https://api.co.id/v1/validation/bank"

    try:
        response = session.get(
            url,
            headers={
                "Authorization": f"Bearer {API_KEY}"
            },
            params={
                "bank_code": bank_code.lower(),
                "account_number": account_number
                # ⛔ sengaja ga kirim nama dulu biar aman
            },
            timeout=15
        )

        print("====== DEBUG API ======")
        print("STATUS:", response.status_code)
        print("TEXT:", response.text)

        if response.status_code != 200:
            return {"error": response.text}

        try:
            return response.json()
        except:
            return {"error": "Response bukan JSON"}

    except Exception as e:
        print("ERROR REQUEST:", e)
        return {"error": str(e)}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    try:
        if not API_KEY:
            return jsonify({"error": "API KEY BELUM DISET"}), 500

        if "file" not in request.files:
            return jsonify({"error": "File tidak ada"}), 400

        file = request.files["file"]

        try:
            df = pd.read_excel(file)
        except:
            return jsonify({"error": "File Excel rusak"}), 400

        df.columns = [c.lower().strip() for c in df.columns]

        for col in ["nama", "rekening", "bank"]:
            if col not in df.columns:
                return jsonify({"error": f"Kolom {col} tidak ada"}), 400

        results = []

        for _, row in df.iterrows():
            nama = str(row["nama"]).strip()
            rekening = str(row["rekening"]).strip()
            bank = str(row["bank"]).strip()

            # FIX angka jadi string
            if rekening.endswith(".0"):
                rekening = rekening[:-2]

            valid, msg = validasi_format_rekening(rekening)

            if not valid:
                results.append({
                    "nama": nama,
                    "rekening": rekening,
                    "bank": bank.upper(),
                    "nama_bank": "-",
                    "score": 0,
                    "status": "FORMAT SALAH",
                    "keterangan": msg
                })
                continue

            res = cek_rekening(bank, rekening, nama)

            if "error" in res:
                results.append({
                    "nama": nama,
                    "rekening": rekening,
                    "bank": bank.upper(),
                    "nama_bank": "-",
                    "score": 0,
                    "status": "ERROR",
                    "keterangan": res["error"]
                })
                continue

            is_valid = res.get("is_valid", False)
            score = res.get("score", 0)
            nama_bank = res.get("name", "-")
            note = res.get("note", "")

            if is_valid and score >= 9:
                status = "MATCH"
                ket = f"Match (score {score})"
            elif is_valid and score >= 7:
                status = "MIRIP"
                ket = f"Mirip (score {score})"
            elif note == "Name was not returned":
                status = "INVALID"
                ket = "Bank tidak kirim nama"
            else:
                status = "SALAH"
                ket = f"Tidak cocok (score {score})"

            results.append({
                "nama": nama,
                "rekening": rekening,
                "bank": bank.upper(),
                "nama_bank": nama_bank,
                "score": score,
                "status": status,
                "keterangan": ket
            })

            time.sleep(0.3)  # biar ga kena rate limit

        return jsonify(results)

    except Exception as e:
        print("ERROR FATAL:", e)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
