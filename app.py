import os
import re
import json
import pandas as pd
import requests
from flask import Flask, request, jsonify

# Inisialisasi Flask (Mengatur folder statis agar index.html dan script.js bisa diakses)
app = Flask(__name__, static_folder='.', static_url_path='')

# ==========================================
# 1. INIT & CONFIGURATION
# ==========================================
# Muat codes.json saat aplikasi pertama kali berjalan
try:
    with open('codes.json', 'r') as f:
        BANK_CODES = json.load(f)
except FileNotFoundError:
    BANK_CODES = {}
    print("[WARNING] File codes.json tidak ditemukan. Mapping bank otomatis tidak akan berjalan sempurna.")

# ==========================================
# 2. DATA SANITIZATION ENGINE
# ==========================================
def clean_account_number(raw_account):
    """ Mencegah float trap dari Excel (misal: 1234.0) dan membuang spasi """
    if pd.isna(raw_account) or raw_account is None:
        return ""
    # Ubah ke string, pecah berdasarkan titik, ambil bagian depannya saja
    str_acc = str(raw_account).split('.')[0]
    # Hapus semua karakter yang bukan angka
    return re.sub(r'\D', '', str_acc)

def normalize_bank_code(raw_bank_name):
    """ Normalisasi nama bank agar cocok dengan spesifikasi API """
    if pd.isna(raw_bank_name) or not raw_bank_name:
        return None
    clean_name = str(raw_bank_name).strip().upper()
    # Jika codes.json memiliki mapping, gunakan itu. Jika tidak, jadikan lowercase.
    return BANK_CODES.get(clean_name, clean_name.lower())

# ==========================================
# 3. EXTERNAL API INTEGRATION
# ==========================================
def validate_via_provider(bank_code, account_number):
    """ Fungsi khusus menangani koneksi ke API Pihak Ketiga """
    
    # ---------------------------------------------------------
    # [CRITICAL ZONE] UBAH BAGIAN INI SESUAI API PROVIDER ANDA
    # ---------------------------------------------------------
    API_URL = "https://api.domain-provider-anda.com/v1/bank-account/validate" # Ganti URL aslinya
    API_KEY = os.environ.get('API_SECRET_KEY', 'KUNCI_API_ANDA_DISINI') # Set di Railway Variables
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_KEY}" # Sesuaikan dengan skema Auth provider Anda
    }
    
    payload = {
        "bank_code": bank_code,
        "account_number": account_number
    }
    # ---------------------------------------------------------

    try:
        # Timeout 15 detik agar antrean batch tidak hang
        response = requests.post(API_URL, json=payload, headers=headers, timeout=15)
        data = response.json() if response.status_code == 200 else {}

        if response.status_code == 200:
            # Sesuaikan kunci 'is_valid' dan 'account_name' dengan respons JSON asli provider API Anda
            if data.get('is_valid') == True or data.get('status') == 'SUCCESS':
                return {"status": "VALID", "name": data.get('account_name', '-'), "keterangan": "Sukses"}
            else:
                return {"status": "TIDAK VALID", "name": "-", "keterangan": "Rekening tidak dikenali bank"}
        else:
            # Mencetak pesan error presisi ke log server
            print(f"[API ERROR] {bank_code}-{account_number} | HTTP {response.status_code}: {response.text}")
            return {"status": "ERROR", "name": "-", "keterangan": f"Ditolak Provider (HTTP {response.status_code})"}

    except requests.exceptions.Timeout:
        print(f"[TIMEOUT] Request ke {bank_code}-{account_number} terlalu lama.")
        return {"status": "ERROR", "name": "-", "keterangan": "Koneksi Timeout"}
    except requests.exceptions.RequestException as e:
        print(f"[NETWORK ERROR] {str(e)}")
        return {"status": "ERROR", "name": "-", "keterangan": "Gagal terhubung ke Provider"}

# ==========================================
# 4. CONTROLLERS & ROUTING
# ==========================================
@app.route('/')
def serve_frontend():
    """ Menyajikan file index.html sebagai tampilan utama """
    return app.send_static_file('index.html')

@app.route('/api/check', methods=['POST']) # Pastikan URL endpoint ini sama dengan fetch() di script.js Anda
def process_batch():
    if 'file' not in request.files:
        return jsonify({"error": "Tidak ada file yang diunggah"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "File kosong"}), 400

    try:
        # Deteksi format file dan gunakan Pandas untuk akurasi tinggi
        if file.filename.endswith('.csv'):
            df = pd.read_csv(file)
        elif file.filename.endswith(('.xls', '.xlsx')):
            df = pd.read_excel(file)
        else:
            return jsonify({"error": "Format file tidak didukung. Gunakan .csv atau .xlsx"}), 400

        # Normalisasi nama kolom Excel menjadi UPPERCASE agar tidak case-sensitive
        df.columns = [str(c).strip().upper() for c in df.columns]
        
        # Asumsi nama kolom di Excel Anda. Sesuaikan jika di Excel namanya "NAMA BANK" atau "NO REKENING"
        COL_BANK = 'BANK'
        COL_REK = 'REKENING'

        if COL_BANK not in df.columns or COL_REK not in df.columns:
            return jsonify({"error": f"File harus memiliki kolom bernama '{COL_BANK}' dan '{COL_REK}'"}), 400

        results = []
        for index, row in df.iterrows():
            raw_bank = row.get(COL_BANK, "")
            raw_rek = row.get(COL_REK, "")

            clean_bank = normalize_bank_code(raw_bank)
            clean_rek = clean_account_number(raw_rek)

            # Jika data baris ini kosong
            if not clean_bank or not clean_rek:
                results.append({
                    "bank": raw_bank,
                    "rekening": raw_rek,
                    "status": "TIDAK VALID",
                    "nama": "-",
                    "keterangan": "Data kosong atau format salah"
                })
                continue

            # Panggil pengecekan API
            api_result = validate_via_provider(clean_bank, clean_rek)

            results.append({
                "bank": clean_bank.upper(),
                "rekening": clean_rek,
                "status": api_result['status'],
                "nama": api_result['name'],
                "keterangan": api_result['keterangan']
            })

        return jsonify({
            "message": "Pemrosesan selesai",
            "total_data": len(results),
            "data": results
        })

    except Exception as e:
        print(f"[SYSTEM CRASH] {str(e)}")
        return jsonify({"error": f"Kesalahan internal server: {str(e)}"}), 500

# ==========================================
# 5. SERVER EXECUTION
# ==========================================
if __name__ == '__main__':
    # PORT otomatis dari Railway, fallback ke 8080 untuk localhost
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
