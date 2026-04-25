from flask import Flask, request, Response, render_template, send_file, jsonify
import pandas as pd
import json
import io
import requests
import concurrent.futures
import os
import re
import threading
import time

app = Flask(__name__)

API_KEY = os.getenv("APICOID_API_KEY", "SpcdCB8aPepI61MKvGeHb9LL6McAcb2LucCTb10TJ9nzs5IAFN")
BASE_URL = "https://use.api.co.id/validation/bank"

WHITESPACE = re.compile(r'\s+')

# --- Activation Code System ---
CODES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'codes.json')
codes_lock = threading.Lock()


def load_codes():
    try:
        with open(CODES_FILE, 'r') as f:
            return json.load(f)
    except Exception:
        return {}


def save_codes(codes):
    with open(CODES_FILE, 'w') as f:
        json.dump(codes, f, indent=2)
        f.flush()
        os.fsync(f.fileno())


# --- Helpers ---
def clean(s):
    return WHITESPACE.sub('', str(s).upper())


def clean_rekening(val):
    """Bersihkan nomor rekening — hilangkan .0 dari float pandas."""
    s = str(val).strip()
    # Jika pandas baca sebagai float misal "1234567890.0"
    try:
        if '.' in s:
            s = str(int(float(s)))
    except (ValueError, OverflowError):
        pass
    return s


def normalize_bank_code(bank_input):
    """Bersihkan input bank dari user agar siap diolah di cek_rekening."""
    code = str(bank_input).strip().lower()
    code = WHITESPACE.sub('', code)
    if code.startswith('bank_'):
        code = code.replace('bank_', '', 1)
    return code


def cek_rekening(rekening, bank_code_raw, nama_pengirim):
    """
    Cek rekening dengan Logika Invincible: 
    Mencoba format 'bank_xxx' dan 'xxx' secara bergantian dengan total 4x percobaan.
    Sangat tangguh menghadapi fluktuasi server api.co.id.
    """
    bank_clean = normalize_bank_code(bank_code_raw)
    # Daftar format yang akan dicoba secara bergantian
    formats_to_try = [f"bank_{bank_clean}", bank_clean, f"bank_{bank_clean}", bank_clean]
    
    headers = {
        "x-api-co-id": API_KEY,
        "Content-Type": "application/json"
    }

    for attempt, current_bank_code in enumerate(formats_to_try):
        params = {
            "bank_code": current_bank_code,
            "account_number": str(rekening).strip(),
            "account_name": str(nama_pengirim).strip()
        }

        try:
            print(f"[API REQ] Try {attempt+1}: {current_bank_code} | rek: {rekening}")
            # MENGGUNAKAN GET SESUAI DOKUMENTASI TERBARU api.co.id
            res = requests.get(BASE_URL, params=params, headers=headers, timeout=15)
            
            if res.status_code in [429, 500, 502, 503, 504]:
                time.sleep(3)
                continue

            # Jika API Key salah atau saldo habis, tampilkan di log
            if res.status_code == 401 or res.status_code == 402:
                print(f"[AUTH ERROR] API Key bermasalah atau Saldo api.co.id Habis (HTTP {res.status_code})")
                return None

            data = res.json()
            inner = data.get("data")
            
            # KONDISI SUKSES MUTLAK:
            # 1. is_success True
            # 2. data tidak None
            # 3. score > 0 (artinya ditemukan/diproses)
            if data.get("is_success") and inner and inner.get("score", 0) > 0:
                return {
                    "nama_bank": inner.get("name"),
                    "is_valid": inner.get("is_valid", False),
                    "score": inner.get("score", 0)
                }
            
            # Jika is_success False atau Score 0 atau Data Null, 
            # kita anggap API sedang error sementara atau format kode bank salah.
            print(f"[API FAIL] Format {current_bank_code} gagal/score 0. Mencoba lagi...")
            time.sleep(2)
            
        except Exception as e:
            print(f"[API ERROR] {e}. Retry...")
            time.sleep(3)
            continue

    print(f"Gagal total setelah 4 format/percobaan: {bank_clean} | {rekening}")
    return None


def proses_satu(args):
    i, row = args

    nama     = str(row.get('nama', '')).strip()
    rekening = clean_rekening(row.get('rekening', ''))
    bank     = str(row.get('bank', '')).strip()

    result = cek_rekening(rekening, bank, nama)

    if result is None:
        # API call gagal total (timeout, connection error, dsb.)
        hasil = "TIDAK VALID"
        nama_bank = "-"
    elif result["is_valid"]:
        # API bilang valid (score >= 7.0) — nama cocok
        # Karena user tidak ingin ada sensor (Budi***), kita pakai langsung nama asli dari input Excel
        hasil = "MATCH"
        nama_bank = nama.upper()
    elif result["nama_bank"]:
        # API berhasil tapi nama tidak cocok (score < 7.0)
        # Rekening ditemukan, tapi nama beda
        hasil = "TIDAK SAMA"
        nama_bank = result["nama_bank"]
    else:
        # Rekening tidak ditemukan di bank (name=null, is_valid=false)
        hasil = "TIDAK VALID"
        nama_bank = "-"

    return {
        "type": "result",
        "index": i + 1,
        "nama": nama,
        "rekening": rekening,
        "bank": bank,
        "nama_bank": nama_bank,
        "hasil": hasil,
        "score": result["score"] if result else 0
    }


def generate_stream(records, code, start_quota):
    try:
        total = len(records)

        yield f"data: {json.dumps({'type':'start','total':total})}\n\n"

        # Gunakan max_workers=1 (Sequential) untuk stabilitas 100% dan hasil paling akurat tanpa cela
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            futures = {
                executor.submit(proses_satu, (i, row)): i
                for i, row in enumerate(records)
            }

            processed_count = 0
            for future in concurrent.futures.as_completed(futures):
                try:
                    data = future.result()
                    processed_count += 1
                    
                    # POTONG KUOTA REAL-TIME (Per baris yang muncul)
                    with codes_lock:
                        codes = load_codes()
                        current_q = codes.get(code, 0)
                        if not isinstance(current_q, int):
                            current_q = 0 if current_q is True else 100
                            
                        # Kurangi 1
                        new_q = max(0, current_q - 1)
                        codes[code] = new_q
                        save_codes(codes)
                        
                        # Beritahu frontend sisa kuota terbaru
                        data['sisa_kuota'] = new_q
                        
                    yield f"data: {json.dumps(data)}\n\n"
                    
                    # Jika kuota benar-benar habis di tengah jalan, stop proses
                    if new_q <= 0:
                        yield f"data: {json.dumps({'type':'error','message':'Kuota telah habis di tengah proses.'})}\n\n"
                        break
                        
                except Exception as e:
                    yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"

        yield f"data: {json.dumps({'type':'done','total':total, 'sisa_kuota': start_quota - total})}\n\n"

    except Exception as e:
        yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/verify-code', methods=['POST'])
def verify_code():
    body = request.json or {}
    code = str(body.get('code', '')).strip()

    if not code:
        return jsonify({"valid": False, "message": "Kode tidak boleh kosong"}), 400

    with codes_lock:
        codes = load_codes()

        if code not in codes:
            return jsonify({"valid": False, "message": "Kode tidak ditemukan"}), 403

        quota = codes[code]
        if not isinstance(quota, int):
            # Migrasi kode lama: jika True berarti sudah habis (0), jika False set default 100
            if quota is True:
                codes[code] = 0
                quota = 0
            else:
                codes[code] = 100
                quota = 100
            save_codes(codes)

        if quota <= 0:
            return jsonify({"valid": False, "message": "Kuota kode aktivasi ini sudah habis (0)"}), 403

    return jsonify({"valid": True, "quota": quota, "message": "Kode valid, selamat menggunakan!"})


@app.route('/stream', methods=['POST'])
def stream():
    code = request.form.get('code', '')
    if 'file' not in request.files:
        return jsonify({"error": "Tidak ada file"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "File kosong"}), 400

    try:
        file_data = io.BytesIO(file.read())
        df = pd.read_excel(file_data)
        df.columns = [str(c).strip().lower() for c in df.columns]
        records = df.to_dict('records')
        total = len(records)
    except Exception as e:
        return jsonify({"error": "Gagal membaca Excel: " + str(e)}), 400

    with codes_lock:
        codes = load_codes()
        if code not in codes:
            return jsonify({"error": "Kode lisensi tidak valid / sesi kadaluarsa"}), 403
            
        quota = codes[code]
        if not isinstance(quota, int):
            quota = 0 if quota is True else 100
            
        if quota <= 0:
            return jsonify({"error": "Kuota sudah habis (0). Silakan isi ulang kuota Anda."}), 403
            
        # Kita tidak lagi potong total di depan (agar fair jika proses terhenti)
        # Cukup pastikan kode valid dan sisa kuota > 0
        start_quota = quota

    # Lanjutkan proses jika kuota aman
    return Response(
        generate_stream(records, code, start_quota),
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
        "nama": ["TEGUH HASYA", "BAMBANG SUGITO", "WAHYU NUR IMAN", "SITI RAHAYU"],
        "rekening": ["2840446855", "7330699393", "1330024362634", "0987654321"],
        "bank": ["BCA", "BCA", "MANDIRI", "BRI"]
    })
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name='template.xlsx',
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@app.route('/supported-banks')
def supported_banks():
    try:
        res = requests.get("https://use.api.co.id/validation/bank/available", headers={"x-api-co-id": API_KEY}, timeout=15)
        if res.status_code == 200 and res.json().get("is_success"):
            banks = res.json()["data"]["banks"]
            df = pd.DataFrame(banks)
            df.index = df.index + 1
            df.columns = ["Nama Bank Resmi", "Kode Asli API"]
            # Berikan kolom panduan ketikan yang gampang di-copy
            df["Ketikan di Excel (Acuan)"] = df["Kode Asli API"].str.replace("bank_", "", n=1).str.upper()
            
            buf = io.BytesIO()
            df.to_excel(buf, index=False)
            buf.seek(0)
            return send_file(buf, as_attachment=True, download_name='Daftar_Bank_Support.xlsx',
                             mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        print("Error fetching supported banks:", e)
    return "Gagal mengambil daftar bank dari API. Pastikan API key valid.", 500


@app.route('/download', methods=['POST'])
def download():
    body = request.json or {}
    raw  = body.get('data', [])
    fmt  = body.get('format', 'xlsx')

    cols = ['No', 'Nama', 'Rekening', 'Bank', 'Nama Bank', 'Score', 'Hasil']
    df = pd.DataFrame(raw, columns=cols)

    buf = io.BytesIO()

    if fmt == 'csv':
        df.to_csv(buf, index=False, sep=',', encoding='utf-8-sig')
        buf.seek(0)
        return send_file(buf, as_attachment=True, download_name='hasil.csv',
                         mimetype='text/csv')
    else:
        df.to_excel(buf, index=False)
        buf.seek(0)
        return send_file(buf, as_attachment=True, download_name='hasil.xlsx',
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


if __name__ == '__main__':
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
