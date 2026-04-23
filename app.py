from flask import Flask, request, Response, render_template
import pandas as pd
import json
import time

app = Flask(__name__)

# 🔥 Normalisasi nama
def clean(s):
    return ''.join(str(s).upper().split())

# 🔥 STREAMING FUNCTION
def generate_stream(file):
    df = pd.read_excel(file)

    total = len(df)

    # start event
    yield f"data: {json.dumps({'type':'start','total':total})}\n\n"

    for i, row in df.iterrows():
        nama = str(row.get('nama', '')).strip()
        rekening = str(row.get('rekening', '')).strip()
        bank = str(row.get('bank', '')).strip()

        # ⚠️ SIMULASI API BANK (GANTI DENGAN API KAMU)
        time.sleep(0.5)
        nama_bank = nama  # <- anggap valid

        # 🔥 LOGIC VALIDASI
        if not nama_bank or nama_bank == '-':
            hasil = "TIDAK VALID"
        elif clean(nama) == clean(nama_bank):
            hasil = "MATCH"
        else:
            hasil = "TIDAK SAMA"

        data = {
            "type": "result",
            "index": i + 1,
            "nama": nama,
            "rekening": rekening,
            "bank": bank,
            "nama_bank": nama_bank,
            "hasil": hasil
        }

        yield f"data: {json.dumps(data)}\n\n"

    # done event
    yield f"data: {json.dumps({'type':'done','total':total})}\n\n"


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/stream', methods=['POST'])
def stream():
    file = request.files['file']
    return Response(generate_stream(file), mimetype='text/event-stream')


if __name__ == '__main__':
    app.run(debug=True)
