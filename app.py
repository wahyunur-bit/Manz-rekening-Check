from flask import Flask, request, Response, render_template, send_file
import pandas as pd
import json
import time
import io
import concurrent.futures
from threading import Lock
import re

app = Flask(__name__)

# 🔥 OPTIMASI 1: Normalisasi nama menggunakan Compiled Regex (Lebih Cepat)
WHITESPACE = re.compile(r'\s+')
def clean(s):
    return WHITESPACE.sub('', str(s).upper())

# ⚠️ SIMULASI API BANK — GANTI DENGAN API ASLI
def cek_rekening(nama, rekening, bank):
    """
    Ganti fungsi ini dengan API bank asli.
    Return: nama_bank (string) atau None jika gagal
    """
    time.sleep(0.3)  # simulasi latency API
    return nama     # simulasi: nama_bank = nama (anggap valid)

def proses_satu(args):
    i, row = args
    # Dictionary '.get()' jauh lebih cepat daripada menggunakan iterrows Pandas
    nama     = str(row.get('nama', '')).strip()
    rekening = str(row.get('rekening', '')).strip()
    bank     = str(row.get('bank', '')).strip()

    try:
        nama_bank = cek_rekening(nama, rekening, bank)
    except Exception as e:
        nama_bank = None

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

# Error handler jika worker gagal agar stream tidak crash
def proses_satu_safe(args):
    try:
        return proses_satu(args)
    except Exception as e:
        i, _ = args
        return {
            "type": "result",
            "index": i + 1,
            "nama": "-", "rekening": "-", "bank": "-",
            "nama_bank": "-", "hasil": "ERROR"
        }

# 🔥 STREAMING — concurrent, ordered emit
def generate_stream(file):
    df = pd.read_excel(file)

    # Normalisasi kolom lowercase & buang spasi
    df.columns = [str(c).strip().lower() for c in df.columns]

    # 🔥 OPTIMASI 2: iterrows() sangat lambat. Gunakan to_dict('records')
    records = df.to_dict('records')
    total = len(records)
    
    yield f"data: {json.dumps({'type':'start','total':total})}\n\n"

    # Proses concurrent
    # 🔥 OPTIMASI 3: Menggunakan executor.map untuk TRUE STREAMING + ORDERED
    # map() menjaga urutan index asli dan kita bisa langsung yield (stream) hasilnya 
    # satu per satu tanpa menahan/memblokir antrean hasil lainnya di memori!
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        for data in executor.map(proses_satu_safe, enumerate(records)):
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
            'X-Accel-Buffering': 'no',
            'Connection': 'keep-alive'  # 🔥 OPTIMASI 4: Cegah timeout koneksi oleh browser
        }
    )


@app.route('/template')
def download_template():
    df = pd.DataFrame(columns=['nama', 'rekening', 'bank'])
    df.loc[0] = ['Budi Santoso', '1234567890', 'BCA']
    df.loc[1] = ['Siti Rahayu',  '0987654321', 'Mandiri']

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
    data    = request.json.get('data', [])
    fmt     = request.json.get('format', 'xlsx')
    
    df = pd.DataFrame(data, columns=['No','Nama','Rekening','Bank','Nama Bank','Hasil'])
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
    # Pastikan threaded=True untuk mendukung multiple requests (meski default pada Flask modern)
    app.run(debug=True, threaded=True)
