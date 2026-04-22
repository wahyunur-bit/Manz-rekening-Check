from flask import Flask, render_template, request, jsonify
import requests
import pandas as pd
import os
import time
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

API_KEY = os.environ.get("APICOID_API_KEY")

print("API KEY TERBACA:", "ADA" if API_KEY else "TIDAK ADA")

def validasi_format_rekening(rekening):
    if not rekening.isdigit():
        return False, "Nomor rekening harus berupa angka"
    if len(rekening) < 6 or len(rekening) > 20:
        return False, "Panjang nomor rekening tidak wajar"
    return True, ""

def cek_rekening(bank_code, account_number, account_name):
    if not API_KEY:
        return {"error": "API key tidak ditemukan"}

    url = "https://api.api.co.id/v1/validation/bank"

    try:
        response = requests.get(
            url,
            headers={
                "x-api-co-id": API_KEY
            },
            params={
                "bank_code": bank_code.lower(),   # API.co.id pakai lowercase
                "account_number": account_number,
                "account_name": account_name
            },
            timeout=10
        )

        print("STATUS:", response.status_code)
        print("RESPONSE:", response.text)

        if response.status_code != 200:
            return {}

        return response.json()

    except Exception as e:
        print("ERROR API:", e)
        return {}

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload():
    try:
        print("UPLOAD HIT")

        if not API_KEY:
            return jsonify({"error": "APICOID_API_KEY belum diset"}), 500

        if "file" not in request.files:
            return jsonify({"error": "File tidak ditemukan"}), 400

        file = request.files["file"]

        try:
            df = pd.read_excel(file)
        except Exception:
            return jsonify({"error": "File Excel tidak valid"}), 400

        df.columns = [col.lower().strip() for col in df.columns]

        required_cols = ["nama", "rekening", "bank"]
        for col in required_cols:
            if col not in df.columns:
                return jsonify({"error": f"Kolom '{col}' tidak ditemukan"}), 400

        results = []

        for _, row in df.iterrows():
            nama = str(row["nama"]).strip()
            rekening = str(row["rekening"]).strip()

            if rekening.endswith(".0"):
                rekening = rekening[:-2]

            bank = str(row["bank"]).strip()

            valid, pesan = validasi_format_rekening(rekening)
            if not valid:
                results.append({
                    "nama": nama,
                    "rekening": rekening,
                    "bank": bank.upper(),
                    "nama_bank": "",
                    "score": "",
                    "status": "FORMAT SALAH",
                    "keterangan": pesan
                })
                continue

            # Kirim nama ke API untuk fuzzy matching
            res = cek_rekening(bank, rekening, nama)

            is_valid = res.get("is_valid", False)
            score = res.get("score", 0)
            nama_termasked = res.get("name", "")
            note = res.get("note", "")

            if not res or "error" in res:
                status = "ERROR"
                keterangan = "Gagal menghubungi API"
            elif is_valid and score >= 9.0:
                status = "MATCH"
                keterangan = f"Nama sesuai (score: {score})"
            elif is_valid and score >= 7.0:
                status = "MIRIP"
                keterangan = f"Nama mirip (score: {score})"
            elif note == "Name was not returned":
                status = "INVALID"
                keterangan = "Bank tidak mengembalikan nama rekening"
            else:
                status = "SALAH"
                keterangan = f"Nama tidak cocok (score: {score})"

            results.append({
                "nama": nama,
                "rekening": rekening,
                "bank": bank.upper(),
                "nama_bank": nama_termasked or "-",
                "score": score,
                "status": status,
                "keterangan": keterangan
            })

            time.sleep(0.3)

        return jsonify(results)

    except Exception as e:
        print("ERROR FATAL:", e)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    print("APP STARTED")
    app.run(host="0.0.0.0", port=5000)
