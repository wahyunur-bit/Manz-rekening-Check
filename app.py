from flask import Flask, request, Response, render_template, send_file, jsonify
import pandas as pd
import json
import time
import io
import concurrent.futures
from threading import Lock

app = Flask(__name__)

# 🔥 Normalisasi nama
def clean(s: str) -> str:
    return ''.join(str(s).upper().split())

# ⚠️ SIMULASI API BANK — GANTI DENGAN API ASLI
def cek_rekening(nama: str, rekening: str, bank: str) -> str:
    """
    Ganti fungsi ini dengan API bank asli.
    Return: nama_bank (string) atau None jika gagal
    """
    time.sleep(0.3)  # simulasi latency API
    return nama  # simulasi: nama_bank = nama (anggap valid)

def proses_satu(args):
    i, row = args
    nama = str(row.get('nama', '')).strip()
    rekening = str(row.get('rekening', '')).strip()
    bank = str(row.get('bank', '')).strip()

    try:
        nama_bank = cek_rekening(nama, rekening, bank)
    except Exception as e:
        return {
            "type": "result",
            "index": i + 1,
            "nama": nama,
            "rekening": rekening,
            "bank": bank,
            "nama_bank": "-", 
            "hasil": f"ERROR: {str(e)}"
        }

    if not nama_bank or nama_bank == '-':
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

# 🔥 STREAMING — concurrent, ordered emit
def generate_stream(file) -> str:
    df = pd.read_excel(file)

    # Normalisasi kolom ke lowercase
    df.columns = [c.strip().lower() for c in df.columns]

    total = len(df)
    yield f"data: {json.dumps({'type':'start','total':total})}\n\n"

    rows = list(df.iterrows())

    # Proses concurrent dengan max 5 worker (sesuaikan limit API bank)
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(proses_satu, row): idx for idx, row in enumerate(rows)}
        results = [None] * len(rows)

        for future in concurrent.futures.as_completed(futures):
            idx = futures[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                results[idx] = {
                    "type": "result",
                    "index": idx + 1,
                    "nama": "-", "rekening": "-", "bank": "-",
                    "nama_bank": "-", "hasil": f"ERROR: {str(e)}"
                }

        # Emit hasil dalam urutan
        for data in results:
            yield f"data: {json.dumps(data)}\n\n"

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
            'X-Accel-Buffering': 'no'
        }
    )

@app.route('/template')
def download_template():
    df = pd.DataFrame(columns=['nama', 'rekening', 'bank'])
    # Contoh data
    df.loc[0] = ['Budi Santoso', '1234567890', 'BCA']
    df.loc[1] = ['Siti Rahayu', '0987654321', 'Mandiri']

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    buf.seek(0)
    return send_file(
        buf,
        as_attachment=True,
        download_name='template_rekening.xlsx',
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

@app.route('/download', methods=['POST'])
def download_hasil():
    data = request.json.get('data', [])
    fmt = request.json.get('format', 'xlsx')  # 'xlsx' or 'csv'
    df = pd.DataFrame(data, columns=['No', 'Nama', 'Rekening', 'Bank', 'Nama Bank', 'Hasil'])

    buf = io.BytesIO()
    if fmt == 'csv':
        csv_str = df.to_csv(index=False)
        buf.write(csv_str.encode('utf-8-sig'))
        buf.seek(0)
        return send_file(buf, as_attachment=True,
                         download_name='hasil_validasi.csv',
                         mimetype='text/csv')
    else:
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        buf.seek(0)
        return send_file(buf, as_attachment=True,
                         download_name='hasil_validasi.xlsx',
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

if __name__ == '__main__':
    app.run(debug=True)
