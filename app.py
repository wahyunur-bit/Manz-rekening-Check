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

# ─── ENV ───────────────────────────────────────────────
API_KEY   = os.getenv("APICOID_API_KEY", "zcrnNzDDvBzehZluxLQFQJmG2LHgdK75Ayhl4FtCtenKPw04cH")
ADMIN_PWD = os.getenv("ADMIN_SECRET", "admin123")

# ─── ENDPOINT API ──────────────────────────────────────
BASE_URL_OFFICIAL = "https://use.api.co.id/validation/bank"

# ─── LICENSE / QUOTA SYSTEM ────────────────────────────
# Simpan di Redis jika ada, fallback ke file JSON lokal
CODES_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "codes.json")
codes_lock  = threading.RLock()

try:
    import redis as _redis
    REDIS_URL = os.getenv("REDIS_URL", "")
    _r = _redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None
    if _r: _r.ping()
except Exception:
    _r = None

print(f"[STORE] {'Redis' if _r else 'Local JSON'}")


def _load_codes() -> dict:
    try:
        if os.path.exists(CODES_FILE):
            with open(CODES_FILE) as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def _save_codes(data: dict):
    with open(CODES_FILE, "w") as f:
        json.dump(data, f, indent=2)


def quota_get(code: str):
    """Return kuota int, atau None jika kode tidak ada."""
    code = code.strip().upper()
    if _r:
        v = _r.get(f"code:{code}")
        if v is None:
            return None
        return int(v)
    d = _load_codes()
    if code not in d:
        return None
    return int(d[code])


def quota_set(code: str, val: int):
    code = code.strip().upper()
    if _r:
        _r.set(f"code:{code}", val)
    else:
        with codes_lock:
            d = _load_codes()
            d[code] = val
            _save_codes(d)


def quota_decr(code: str) -> int:
    """Potong 1 kuota, return sisa. Thread-safe."""
    code = code.strip().upper()
    if _r:
        new = _r.decr(f"code:{code}")
        if new < 0:
            _r.set(f"code:{code}", 0)
            new = 0
        return new
    with codes_lock:
        d = _load_codes()
        cur = int(d.get(code, 0))
        new = max(0, cur - 1)
        d[code] = new
        _save_codes(d)
        return new


def quota_add(code: str, amount: int):
    code = code.strip().upper()
    cur = quota_get(code) or 0
    quota_set(code, cur + amount)


def quota_list() -> list:
    if _r:
        keys = _r.keys("code:*")
        result = []
        for k in sorted(keys):
            code = k.replace("code:", "")
            result.append({"code": code, "quota": int(_r.get(k) or 0)})
        return result
    d = _load_codes()
    return [{"code": k, "quota": int(v)} for k, v in sorted(d.items())]


# ─── HELPER ────────────────────────────────────────────
WHITESPACE = re.compile(r'\s+')


def clean_str(s: str) -> str:
    """Hapus spasi, uppercase untuk perbandingan nama."""
    return WHITESPACE.sub('', str(s).upper())


def clean_rekening(val) -> str:
    """Pastikan nomor rekening jadi string digit bersih (handle scientific notation Excel)."""
    s = str(val).strip()
    if not s or s.lower() == 'nan':
        return ""
    # Handle scientific notation: 1.23E+10
    try:
        if 'e' in s.lower():
            from decimal import Decimal
            s = format(Decimal(s), 'f').split('.')[0]
    except Exception:
        pass
    # Hapus titik desimal jika ada (misal: 123456789.0)
    if '.' in s:
        s = s.split('.')[0]
    # Hanya digit
    s = re.sub(r'\D', '', s)
    return s


# ─── BANK MAPPING ──────────────────────────────────────
# Beberapa API lama butuh kode numerik
NUMERIC_BANKS = {
    "bca": "014", "mandiri": "008", "bni": "009", "bri": "002",
    "btpn": "213", "cimb": "022", "danamon": "011", "ocbc": "028",
    "permata": "013", "panin": "019", "hana": "484", "seabank": "535",
    "jago": "542", "allo": "561"
}

# ─── CORE API CALL ─────────────────────────────────────

def cek_rekening(rekening: str, bank: str, nama_input: str = "", session=None) -> dict:
    """
    Triple-endpoint strategy (urutan prioritas):
    1. POST api.api.co.id/v1/bank/account  → nama FULL tanpa sensor (script lama)
    2. GET  use.api.co.id/validation/bank  → bank_bca format + score
    3. GET  use.api.co.id/validation/bank  → bca format (short) + score
    Return: {"account_name", "score", "is_valid"} atau {"error"}
    """
    if not API_KEY:
        return {"error": "API KEY KOSONG"}

    caller     = session or requests
    bank_raw   = str(bank).strip().lower()
    rekening   = str(rekening).strip()
    nama_input = str(nama_input).strip()

    bank_clean = re.sub(r'[^a-z0-9_]', '', bank_raw)
    if bank_clean.startswith('bank_'):
        short = bank_clean[5:]
        full  = bank_clean
    else:
        short = bank_clean
        full  = f"bank_{bank_clean}"

    headers_base = {"x-api-co-id": API_KEY, "Accept": "application/json"}

    # ── Strategi 1: POST (endpoint lama, nama FULL) ──────
    def try_post() -> dict | None:
        try:
            payload = {"bank": short, "account_number": rekening}
            print(f"[POST] {BASE_URL_POST} | bank={short} | rek={rekening}")
            res = caller.post(BASE_URL_POST, json=payload,
                              headers={**headers_base, "Content-Type": "application/json"},
                              timeout=15)
            print(f"[POST-RESP] {res.status_code} | {res.text[:300]}")
            if res.status_code == 200:
                data = res.json()
                if data.get("is_success"):
                    inner = data.get("data", {})
                    nama = inner.get("name") or inner.get("account_name")
                    if nama:
                        return {"account_name": str(nama).strip(), "score": 10.0, "is_valid": True}
            return None
        except Exception:
            return None

    # ── Strategi 2 & 3: GET (endpoint resmi, score-based) ─
    def try_get(bank_code: str) -> dict | None:
        try:
            params = {"bank_code": bank_code, "account_number": rekening, "account_name": nama_input}
            print(f"[GET] {BASE_URL_GET} | bank={bank_code} | rek={rekening}")
            res = caller.get(BASE_URL_GET, params=params, headers=headers_base, timeout=20)
            print(f"[GET-RESP] {res.status_code} | {res.text[:300]}")
            if res.status_code == 200:
                data = res.json()
                if data.get("is_success"):
                    inner    = data.get("data", {})
                    nama     = inner.get("name") or inner.get("account_name")
                    is_valid = inner.get("is_valid", False)
                    score    = float(inner.get("score") or 0)
                    if nama or is_valid:
                        return {"account_name": str(nama).strip() if nama else "",
                                "is_valid": is_valid, "score": score}
                    return {"error": inner.get("message") or "Bank account was not found"}
                else:
                    msg = data.get("message", "")
                    if "api key" in msg.lower() or "unauthorized" in msg.lower():
                        return {"error": msg}
                    return None
        except Exception:
            return None

    # ── Eksekusi urutan prioritas ─────────────────────────
    result = try_post()
    if result and "error" not in result:
        return result

    result = try_get(full)
    if result is not None:
        if "error" in result and "not found" in result["error"].lower():
            return result
        if "error" not in result:
            return result

    if short != full:
        result = try_get(short)
        if result is not None:
            return result

    return {"error": "Timeout - server lambat"}


# ─── PROSES 1 BARIS ────────────────────────────────────

def proses_satu(args):
    i, row, session = args

    nama     = str(row.get('nama', '')).strip()
    rekening = clean_rekening(row.get('rekening', ''))
    bank     = str(row.get('bank', '')).strip()

    if not rekening:
        return {
            "type": "result", "index": i + 1,
            "nama": nama, "rekening": "-", "bank": bank,
            "nama_bank": "-", "hasil": "TIDAK VALID"
        }

    result = cek_rekening(rekening, bank, nama, session)

    if "error" in result:
        err_msg = result["error"]
        # Tentukan apakah ini error jaringan/sistem atau benar tidak valid
        network_errors = ["timeout", "koneksi", "server lambat", "gagal menghubungi"]
        auth_errors    = ["api key", "saldo", "unauthorized", "http 4"]

        if any(x in err_msg.lower() for x in auth_errors):
            hasil     = "ERROR"
            nama_bank = f"[!] {err_msg}"
        elif any(x in err_msg.lower() for x in network_errors):
            hasil     = "ERROR"
            nama_bank = f"[!] {err_msg}"
        else:
            # "Bank account was not found" → memang tidak valid
            hasil     = "TIDAK VALID"
            nama_bank = f"({err_msg})"
    else:
        nama_bank_api = result.get("account_name", "")
        is_valid      = result.get("is_valid", False)
        score         = result.get("score", 0)

        # Gunakan score >= 7 OR is_valid=true sebagai patokan MATCH (dari script lama)
        if is_valid or float(score) >= 7.0:
            hasil     = "MATCH"
            nama_bank = nama.upper()  # Tampil nama full dari Excel (bukan masked API)
        elif nama_bank_api:
            # Rekening ditemukan tapi nama beda
            hasil     = "TIDAK SAMA"
            nama_bank = nama_bank_api  # Nama masked dari API
        else:
            hasil     = "TIDAK VALID"
            nama_bank = "-"

    return {
        "type":      "result",
        "index":     i + 1,
        "nama":      nama,
        "rekening":  rekening,
        "bank":      bank,
        "nama_bank": nama_bank,
        "hasil":     hasil
    }


# ─── STREAM GENERATOR ──────────────────────────────────

def generate_stream(records: list, code: str):
    total = len(records)
    yield f"data: {json.dumps({'type':'start','total':total})}\n\n"

    processed = 0
    try:
        with requests.Session() as session:
            # 4 workers: lebih cepat dari 2, aman dari rate-limit vs 10 lama
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as exe:
                futures = {
                    exe.submit(proses_satu, (i, row, session)): i
                    for i, row in enumerate(records)
                }
                for future in concurrent.futures.as_completed(futures):
                    try:
                        data = future.result()
                    except Exception as e:
                        data = {
                            "type": "result",
                            "index": futures[future] + 1,
                            "nama": "-", "rekening": "-", "bank": "-",
                            "nama_bank": str(e), "hasil": "ERROR"
                        }

                    # Kuota: hanya potong MATCH/BEDA/TIDAK VALID — ERROR tidak dipotong
                    hasil = data.get("hasil", "")
                    if hasil != "ERROR":
                        sisa = quota_decr(code)
                        data["sisa_kuota"] = sisa
                    else:
                        data["sisa_kuota"] = quota_get(code)
                    processed += 1

                    yield f"data: {json.dumps(data)}\n\n"

    except Exception as e:
        yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"

    yield f"data: {json.dumps({'type':'done','total':total,'processed':processed})}\n\n"



# ─── ROUTES ────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/verify-code', methods=['POST'])
def verify_code():
    body = request.json or {}
    code = str(body.get('code', '')).strip()
    if not code:
        return jsonify({"valid": False, "message": "Kode tidak boleh kosong"}), 400

    q = quota_get(code)
    if q is None:
        return jsonify({"valid": False, "message": "Kode tidak ditemukan"}), 403
    if q <= 0:
        return jsonify({"valid": False, "message": "Kuota habis. Kode tidak dapat digunakan lagi."}), 403

    return jsonify({"valid": True, "quota": q, "message": "Kode valid!"})


@app.route('/stream', methods=['POST'])
def stream():
    code = str(request.form.get('code', '')).strip().upper()

    # Validasi kode + kuota
    q = quota_get(code)
    if q is None:
        return jsonify({"error": "Kode tidak ditemukan"}), 403
    if q <= 0:
        return jsonify({"error": "Kuota habis. Kode tidak bisa digunakan lagi."}), 403

    file = request.files.get('file')
    if not file:
        return jsonify({"error": "File tidak ada"}), 400

    try:
        file_data = io.BytesIO(file.read())
        df = pd.read_excel(file_data)
        df.columns = [str(c).strip().lower() for c in df.columns]
        records = df.to_dict('records')
    except Exception as e:
        return jsonify({"error": f"Gagal baca Excel: {e}"}), 400

    if len(records) > q:
        return jsonify({"error": f"Kuota tidak cukup. Kuota: {q}, Data: {len(records)}"}), 403

    return Response(
        generate_stream(records, code),
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
        "nama":     ["TEGUH HASYA", "BAMBANG SUGITO", "WAHYU NUR IMAN", "SITI RAHAYU"],
        "rekening": ["2840446855", "7330699393", "1330024362634", "0987654321"],
        "bank":     ["bca", "bca", "mandiri", "bri"]
    })
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return send_file(buf, as_attachment=True, download_name='template.xlsx',
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@app.route('/download', methods=['POST'])
def download():
    body = request.json or {}
    raw  = body.get('data', [])
    fmt  = body.get('format', 'xlsx')

    cols = ['No', 'Nama', 'Rekening', 'Bank', 'Nama Bank', 'Hasil']
    df   = pd.DataFrame(raw, columns=cols)
    buf  = io.BytesIO()

    if fmt == 'csv':
        df.to_csv(buf, index=False, sep=',', encoding='utf-8-sig')
        buf.seek(0)
        return send_file(buf, as_attachment=True, download_name='hasil.csv', mimetype='text/csv')
    else:
        df.to_excel(buf, index=False)
        buf.seek(0)
        return send_file(buf, as_attachment=True, download_name='hasil.xlsx',
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@app.route('/supported-banks')
def supported_banks():
    """Download daftar semua bank yang didukung oleh API sebagai file Excel."""
    try:
        res = requests.get(
            "https://use.api.co.id/validation/bank/available",
            headers={"x-api-co-id": API_KEY},
            timeout=15
        )
        if res.status_code == 200:
            data = res.json()
            if data.get("is_success") and data.get("data", {}).get("banks"):
                banks = data["data"]["banks"]
                rows = []
                for i, b in enumerate(banks, 1):
                    nama = b.get("bank_name", "")
                    kode = b.get("bank_code", "")
                    # Buat ketikan acuan: hapus prefix bank_ dan uppercase
                    acuan = kode.replace("bank_", "", 1).upper() if kode.startswith("bank_") else kode.upper()
                    rows.append({"No": i, "Nama Bank": nama.title(), "Kode API": kode, "NAMA BANK (GUNAKAN INI)": acuan})
                df = pd.DataFrame(rows)
                buf = io.BytesIO()
                df.to_excel(buf, index=False)
                buf.seek(0)
                return send_file(buf, as_attachment=True, download_name='daftar_bank_support.xlsx',
                                 mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        return "Gagal mengambil daftar bank dari API.", 500
    except Exception as e:
        return f"Error: {e}", 500


# ─── ADMIN API ─────────────────────────────────────────

def _admin_check():
    return request.headers.get('X-Admin-Secret', '') == ADMIN_PWD


@app.route('/admin/add', methods=['POST'])
def admin_add():
    if not _admin_check():
        return jsonify({"ok": False, "msg": "Unauthorized"}), 401
    body    = request.json or {}
    code    = str(body.get('code', '')).strip().upper()
    amount  = int(body.get('quota', 100))
    if not code:
        return jsonify({"ok": False, "msg": "Kode kosong"}), 400
    quota_add(code, amount)
    return jsonify({"ok": True, "code": code, "quota": quota_get(code)})


@app.route('/admin/list', methods=['GET'])
def admin_list():
    if not _admin_check():
        return jsonify({"ok": False, "msg": "Unauthorized"}), 401
    data = quota_list()
    return jsonify({"ok": True, "codes": data, "total": len(data)})


@app.route('/admin/set', methods=['POST'])
def admin_set():
    """Set kuota spesifik (override)."""
    if not _admin_check():
        return jsonify({"ok": False, "msg": "Unauthorized"}), 401
    body   = request.json or {}
    code   = str(body.get('code', '')).strip().upper()
    amount = int(body.get('quota', 0))
    if not code:
        return jsonify({"ok": False, "msg": "Kode kosong"}), 400
    quota_set(code, amount)
    return jsonify({"ok": True, "code": code, "quota": amount})


@app.route('/admin/delete', methods=['DELETE'])
def admin_delete():
    if not _admin_check():
        return jsonify({"ok": False, "msg": "Unauthorized"}), 401
    body = request.json or {}
    code = str(body.get('code', '')).strip().upper()
    if _r:
        _r.delete(f"code:{code}")
    else:
        with codes_lock:
            d = _load_codes()
            d.pop(code, None)
            _save_codes(d)
    return jsonify({"ok": True, "msg": f"Kode {code} dihapus"})


# ─── ADMIN PANEL (UI) ──────────────────────────────────
@app.route('/admin')
def admin_panel():
    return '''<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Admin Panel – License Manager</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #02040a; --surface: #0d1117; --surface2: #111827;
  --border: rgba(0,229,255,0.12); --border-h: rgba(0,229,255,0.35);
  --accent: #00e5ff; --green: #00ffa3; --red: #ff4d6d; --yellow: #fbbf24;
  --text: #e2e8f0; --muted: #64748b; --mono: 'JetBrains Mono',monospace;
}
* { box-sizing:border-box; margin:0; padding:0; }
body { background:var(--bg); color:var(--text); font-family:'Syne',sans-serif;
  min-height:100vh; padding:30px 20px; }
.wrap { max-width:860px; margin:0 auto; }
h1 { font-size:24px; font-weight:700; background:linear-gradient(135deg,#fff,var(--accent));
  -webkit-background-clip:text; -webkit-text-fill-color:transparent; margin-bottom:4px; }
.sub { color:var(--muted); font-size:12px; letter-spacing:2px; margin-bottom:30px; }
.card { background:var(--surface); border:1px solid var(--border); border-radius:16px;
  padding:24px; margin-bottom:20px; }
.card-title { color:var(--accent); font-size:11px; letter-spacing:2px;
  text-transform:uppercase; font-weight:700; margin-bottom:16px; }
input { background:var(--surface2); border:1px solid var(--border); color:var(--text);
  padding:10px 14px; border-radius:8px; font-size:14px; font-family:var(--mono);
  width:100%; transition:.2s; outline:none; }
input:focus { border-color:var(--accent); }
.row { display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
.row input { flex:1; }
.btn { padding:10px 20px; border:none; border-radius:8px; font-size:13px;
  font-weight:600; cursor:pointer; transition:.2s; white-space:nowrap; }
.btn-cyan { background:var(--accent); color:#000; }
.btn-cyan:hover { opacity:.85; }
.btn-green { background:rgba(0,255,163,.1); color:var(--green);
  border:1px solid rgba(0,255,163,.25); }
.btn-green:hover { background:rgba(0,255,163,.2); }
.btn-red { background:rgba(255,77,109,.1); color:var(--red);
  border:1px solid rgba(255,77,109,.25); padding:6px 14px; font-size:11px; }
.btn-red:hover { background:rgba(255,77,109,.2); }
.btn-yellow { background:rgba(251,191,36,.1); color:var(--yellow);
  border:1px solid rgba(251,191,36,.25); padding:6px 14px; font-size:11px; }
.btn-yellow:hover { background:rgba(251,191,36,.2); }
table { width:100%; border-collapse:collapse; font-size:13px; }
th { background:var(--surface2); color:var(--accent); padding:10px 14px;
  text-align:left; font-size:10px; letter-spacing:1.5px; font-weight:600; }
td { padding:10px 14px; border-bottom:1px solid var(--border); font-family:var(--mono); }
.badge { padding:3px 10px; border-radius:4px; font-size:10px; font-weight:700; letter-spacing:1px; }
.bgreen { background:rgba(0,255,163,.12); color:var(--green); }
.bred   { background:rgba(255,77,109,.12); color:var(--red); }
#msg { margin-top:12px; font-size:13px; min-height:20px; font-family:var(--mono); }
.edit-input { width:90px; padding:5px 8px; font-size:12px; }
.tdact { display:flex; gap:6px; align-items:center; }
.stat-grid { display:grid; grid-template-columns:repeat(3,1fr); gap:12px; margin-bottom:0; }
.stat-box { background:var(--surface2); border:1px solid var(--border);
  border-radius:10px; padding:16px; text-align:center; }
.stat-num { font-size:28px; font-weight:700; color:var(--accent); font-family:var(--mono); }
.stat-lbl { font-size:11px; color:var(--muted); margin-top:4px; }
</style>
</head>
<body>
<div class="wrap">
  <h1>🔐 License Manager</h1>
  <div class="sub">// ADMIN PANEL — REKENING VALIDATOR PRO</div>

  <!-- LOGIN -->
  <div class="card">
    <div class="card-title">Authentication</div>
    <div class="row">
      <input type="password" id="secret" placeholder="Masukkan Admin Secret Password"
        onkeydown="if(event.key==='Enter') loadAll()">
      <button class="btn btn-cyan" onclick="loadAll()">🔓 Login & Refresh</button>
    </div>
    <div id="msg"></div>
  </div>

  <!-- STATS -->
  <div class="card" id="statsCard" style="display:none">
    <div class="card-title">Statistik</div>
    <div class="stat-grid">
      <div class="stat-box"><div class="stat-num" id="stTotal">0</div><div class="stat-lbl">Total Kode</div></div>
      <div class="stat-box"><div class="stat-num" id="stAktif">0</div><div class="stat-lbl">Kode Aktif</div></div>
      <div class="stat-box"><div class="stat-num" id="stKuota">0</div><div class="stat-lbl">Total Kuota</div></div>
    </div>
  </div>

  <!-- TAMBAH KODE -->
  <div class="card">
    <div class="card-title">➕ Tambah / Top-up Kode</div>
    <div class="row">
      <input type="text" id="newCode" placeholder="Kode Lisensi (contoh: MANZ-VIP01)"
        style="text-transform:uppercase">
      <input type="number" id="newQuota" placeholder="Kuota" value="100" style="max-width:130px">
      <button class="btn btn-green" onclick="addCode()">Tambah / Top-up</button>
    </div>
  </div>

  <!-- DAFTAR KODE -->
  <div class="card">
    <div class="card-title">📋 Daftar Kode Aktif</div>
    <table>
      <thead><tr>
        <th>#</th><th>KODE LISENSI</th><th>KUOTA SISA</th>
        <th>STATUS</th><th>AKSI</th>
      </tr></thead>
      <tbody id="tbl"><tr><td colspan="5" style="text-align:center;color:var(--muted);padding:30px">
        Login untuk melihat daftar kode
      </td></tr></tbody>
    </table>
  </div>
</div>

<script>
const $ = id => document.getElementById(id);
const secret = () => $('secret').value;

function msg(t, ok) {
  $('msg').textContent = t;
  $('msg').style.color = ok ? 'var(--green)' : 'var(--red)';
}

async function loadAll() {
  const res = await fetch('/admin/list', { headers: { 'X-Admin-Secret': secret() } });
  const data = await res.json();
  if (!data.ok) { msg('❌ ' + (data.msg || 'Unauthorized'), false); return; }

  // Stats
  const aktif = data.codes.filter(c => c.quota > 0).length;
  const totalKuota = data.codes.reduce((a,c) => a + c.quota, 0);
  $('stTotal').textContent = data.total;
  $('stAktif').textContent = aktif;
  $('stKuota').textContent = totalKuota.toLocaleString();
  $('statsCard').style.display = '';

  // Table
  const tbody = $('tbl');
  tbody.innerHTML = '';
  if (data.codes.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--muted);padding:30px">Belum ada kode</td></tr>';
    return;
  }
  data.codes.forEach((c, i) => {
    const ok = c.quota > 0;
    tbody.innerHTML += `<tr>
      <td style="color:var(--muted)">${i+1}</td>
      <td><b>${c.code}</b></td>
      <td>${c.quota.toLocaleString()}</td>
      <td><span class="badge ${ok?'bgreen':'bred'}">${ok?'AKTIF':'HABIS'}</span></td>
      <td><div class="tdact">
        <input class="edit-input" id="eq_${c.code}" type="number" value="${c.quota}" min="0">
        <button class="btn btn-yellow" onclick="setKuota('${c.code}')">Set</button>
        <button class="btn btn-red" onclick="delCode('${c.code}')">Hapus</button>
      </div></td>
    </tr>`;
  });
  msg(`✓ ${data.total} kode berhasil dimuat`, true);
}

async function addCode() {
  const code = $('newCode').value.trim().toUpperCase();
  const quota = parseInt($('newQuota').value);
  if (!code) { msg('⚠ Isi kode terlebih dahulu!', false); return; }
  if (!quota || quota < 1) { msg('⚠ Kuota minimal 1', false); return; }
  const res = await fetch('/admin/add', {
    method: 'POST',
    headers: { 'Content-Type':'application/json', 'X-Admin-Secret': secret() },
    body: JSON.stringify({ code, quota })
  });
  const data = await res.json();
  if (data.ok) {
    msg(`✓ Kode ${data.code} | Sisa kuota: ${data.quota}`, true);
    $('newCode').value = '';
    loadAll();
  } else {
    msg('❌ ' + (data.msg || 'Gagal'), false);
  }
}

async function setKuota(code) {
  const quota = parseInt(document.getElementById('eq_'+code).value);
  if (isNaN(quota) || quota < 0) { msg('⚠ Kuota tidak valid', false); return; }
  const res = await fetch('/admin/set', {
    method: 'POST',
    headers: { 'Content-Type':'application/json', 'X-Admin-Secret': secret() },
    body: JSON.stringify({ code, quota })
  });
  const data = await res.json();
  msg(data.ok ? `✓ Kuota ${code} → ${quota}` : '❌ Gagal', data.ok);
  if (data.ok) loadAll();
}

async function delCode(code) {
  if (!confirm(`Hapus kode "${code}"?\nAksi ini tidak bisa dibatalkan.`)) return;
  const res = await fetch('/admin/delete', {
    method: 'DELETE',
    headers: { 'Content-Type':'application/json', 'X-Admin-Secret': secret() },
    body: JSON.stringify({ code })
  });
  const data = await res.json();
  msg(data.ok ? `✓ ${code} dihapus` : '❌ ' + data.msg, data.ok);
  if (data.ok) loadAll();
}
</script>
</body>
</html>'''


if __name__ == '__main__':
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
