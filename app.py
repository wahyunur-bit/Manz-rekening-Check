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
        return {"error": "API key tidak ada di environment variable APICOID_API_KEY"}

    url = "https://api.co.id/v1/validation/bank"

    try:
        response = session.get(
            url,
            headers={
                # ✅ FIX: Header yang benar untuk api.co.id
                "x-api-co-id": API_KEY
            },
            params={
                "bank_code": bank_code.lower(),
                "account_number": account_number,
                "account_name": account_name  # ✅ kirim nama biar scoring jalan
            },
            timeout=15
        )

        print("====== DEBUG API ======")
        print("STATUS:", response.status_code)
        print("TEXT:", response.text[:300])

        if response.status_code == 401:
            return {"error": "API Key salah atau tidak valid"}
        if response.status_code == 402:
            return {"error": "Saldo points habis, top up dulu di dashboard.api.co.id"}
        if response.status_code == 403:
            return {"error": "Akses ditolak - cek paket subscription kamu"}
        if response.status_code != 200:
            return {"error": f"HTTP {response.status_code}: {response.text[:200]}"}

        try:
            return response.json()
        except Exception:
            return {"error": "Response bukan JSON: " + response.text[:100]}

    except requests.exceptions.ConnectionError as e:
        return {"error": f"Tidak bisa konek ke api.co.id: {str(e)}"}
    except requests.exceptions.Timeout:
        return {"error": "Request timeout (>15 detik)"}
    except Exception as e:
        return {"error": str(e)}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/health")
def health():
    """Endpoint untuk cek apakah server jalan"""
    return jsonify({
        "status": "ok",
        "api_key": "ADA" if API_KEY else "TIDAK ADA - set env APICOID_API_KEY"
    })


@app.route("/upload", methods=["POST"])
def upload():
    try:
        if not API_KEY:
            return jsonify({"error": "API KEY BELUM DISET. Jalankan: export APICOID_API_KEY=your_key"}), 500

        if "file" not in request.files:
            return jsonify({"error": "File tidak ada"}), 400

        file = request.files["file"]

        try:
            df = pd.read_excel(file)
        except Exception as e:
            return jsonify({"error": f"File Excel tidak bisa dibaca: {str(e)}"}), 400

        # Normalize kolom
        df.columns = [c.lower().strip() for c in df.columns]

        for col in ["nama", "rekening", "bank"]:
            if col not in df.columns:
                return jsonify({
                    "error": f"Kolom '{col}' tidak ditemukan. Kolom yang ada: {list(df.columns)}"
                }), 400

        results = []

        for idx, row in df.iterrows():
            nama = str(row["nama"]).strip()
            rekening = str(row["rekening"]).strip()
            bank = str(row["bank"]).strip()

            # Fix: angka float jadi string (misal 1234567890.0 -> 1234567890)
            if rekening.endswith(".0"):
                rekening = rekening[:-2]

            # Skip baris kosong
            if not nama or nama.lower() in ["nan", "none", ""]:
                continue

            valid, msg = validasi_format_rekening(rekening)

            if not valid:
                results.append({
                    "nama": nama,
                    "rekening": rekening,
                    "bank": bank.upper(),
                    "nama_di_bank": "-",
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
                    "nama_di_bank": "-",
                    "score": 0,
                    "status": "ERROR",
                    "keterangan": res["error"]
                })
                continue

            is_valid = res.get("is_valid", False)
            score = res.get("score", 0)
            nama_di_bank = res.get("name", "-") or "-"
            note = res.get("note", "")

            if is_valid and score >= 9:
                status = "✅ MATCH"
                ket = f"Nama cocok (score {score})"
            elif is_valid and score >= 7:
                status = "⚠️ MIRIP"
                ket = f"Nama mirip, cek manual (score {score})"
            elif note == "Name was not returned":
                status = "ℹ️ CEK MANUAL"
                ket = "Rekening valid tapi bank tidak kirim nama"
            else:
                status = "❌ TIDAK COCOK"
                ket = f"Nama tidak cocok (score {score})"

            results.append({
                "nama": nama,
                "rekening": rekening,
                "bank": bank.upper(),
                "nama_di_bank": nama_di_bank,
                "score": score,
                "status": status,
                "keterangan": ket
            })

            time.sleep(0.3)  # anti rate limit

        return jsonify(results)

    except Exception as e:
        import traceback
        print("ERROR FATAL:", traceback.format_exc())
        return jsonify({"error": f"Server error: {str(e)}"}), 500


if __name__ == "__main__":
    if not API_KEY:
        print("⚠️  WARNING: APICOID_API_KEY belum diset!")
        print("   Jalankan: export APICOID_API_KEY=your_api_key_here")
    app.run(host="0.0.0.0", port=5000, debug=True)
