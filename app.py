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
    Sesuai Dokumentasi Resmi api.co.id:
    GET https://use.api.co.id/validation/bank
    Header: x-api-co-id
    """
    if not API_KEY:
        return {"error": "API KEY KOSONG"}

    caller = session or requests
    bank_raw = str(bank).strip().lower()
    rekening = str(rekening).strip()
    nama_input = str(nama_input).strip()

    # Normalisasi: user ketik "BCA" -> "bank_bca" (format resmi API)
    bank_clean = re.sub(r'[^a-z0-9_]', '', bank_raw)
    if not bank_clean.startswith('bank_'):
        bank_code = f"bank_{bank_clean}"
    else:
        bank_code = bank_clean

    headers = {"x-api-co-id": API_KEY, "Accept": "application/json"}
    params = {
        "bank_code": bank_code,
        "account_number": rekening,
        "account_name": nama_input
    }

    # Coba 2x (1 normal + 1 retry jika timeout)
    for attempt in range(2):
        try:
            print(f"[CEK] {bank_code} | {rekening} | try={attempt+1}")
            res = caller.get(BASE_URL_OFFICIAL, params=params, headers=headers, timeout=45)
            
            if res.status_code == 200:
                data = res.json()
                
                if data.get("is_success"):
                    inner = data.get("data", {})
                    if inner:
                        res_name = inner.get("name") or inner.get("account_name")
                        score = inner.get("score", 0)

                        if res_name:
                            return {"account_name": str(res_name).strip(), "score": score}
                        
                        if not inner.get("is_valid", False):
                            return {"error": inner.get("message") or "Bank account was not found"}
                else:
                    msg = data.get("message", "Unknown error")
                    if "api key" in msg.lower():
                        return {"error": msg}
                    return {"error": msg}
            
            elif res.status_code == 401:
                return {"error": "API Key tidak valid"}
            elif res.status_code == 403:
                return {"error": "Saldo habis"}
            else:
                return {"error": f"HTTP {res.status_code}"}
                
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt == 0:
                time.sleep(2)
                continue
            return {"error": "Timeout - server lambat"}
        except Exception as e:
            return {"error": str(e)}

    return {"error": "Gagal menghubungi server"}


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
        system_errors = ["api key", "saldo", "aktif", "http 4", "timeout", "limit", "auth"]
        
        if any(x in err_msg.lower() for x in system_errors):
            hasil     = "ERROR"
            nama_bank = f"[!] {err_msg}"
        else:
            # Tampilkan pesan error asli dari API di kolom nama agar user tahu masalahnya
            hasil     = "TIDAK VALID"
            nama_bank = f"({err_msg})"
    else:
        nama_bank = result["account_name"]
        score     = result.get("score")
        
        c_nama      = clean_str(nama)
        c_nama_bank = clean_str(nama_bank)

        # Logika Fuzzy Match untuk Masked Names (contoh: 'MUH**** FIO**')
        is_match = False
        if c_nama == c_nama_bank:
            is_match = True
        elif '*' in nama_bank:
            # Jika ada masking, cek apakah 3 huruf pertama sama
            prefix = c_nama_bank.split('*')[0]
            if prefix and c_nama.startswith(prefix):
                is_match = True
        
        # Gunakan score jika ada (score >= 7 biasanya dianggap match)
        if score is not None:
            try:
                if float(score) >= 7.0: is_match = True
            except: pass

        if is_match:
            hasil = "MATCH"
            # Tampilkan nama FULL dari input Excel (tanpa sensor/masking dari API)
            nama_bank = nama.upper()
        else:
            hasil = "TIDAK SAMA"

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
            # 2 workers paralel: cepat tapi aman dari rate-limit
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as exe:
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

                    # Kuota: hanya potong untuk MATCH/BEDA/TIDAK VALID, bukan ERROR
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
<style>
* { box-sizing:border-box; margin:0; padding:0; }
body { background:#0b0e1a; color:#e2e8f0; font-family:system-ui,sans-serif; padding:30px; }
h1 { color:#00e5ff; font-size:22px; margin-bottom:6px; }
.sub { color:#64748b; font-size:12px; margin-bottom:30px; }
.card { background:#0d1117; border:1px solid #1e2d45; border-radius:12px; padding:24px; margin-bottom:20px; }
h3 { color:#00e5ff; font-size:13px; letter-spacing:1px; text-transform:uppercase; margin-bottom:16px; }
input, select { background:#111827; border:1px solid #1e2d45; color:#fff; padding:10px 14px;
  border-radius:8px; font-size:14px; width:100%; margin-bottom:10px; }
button { padding:10px 20px; border:none; border-radius:8px; font-size:13px; font-weight:600; cursor:pointer; }
.btn-cyan { background:#00e5ff; color:#000; }
.btn-red  { background:#ff4d6d; color:#fff; }
.btn-gray { background:#1e2d45; color:#e2e8f0; }
.row { display:flex; gap:10px; align-items:flex-end; flex-wrap:wrap; }
.row input { flex:1; margin-bottom:0; }
table { width:100%; border-collapse:collapse; font-size:13px; }
th { background:#111827; color:#00e5ff; padding:10px 14px; text-align:left; font-size:11px; letter-spacing:1px; }
td { padding:10px 14px; border-bottom:1px solid #1e2d45; font-family:monospace; }
.badge { padding:3px 10px; border-radius:4px; font-size:11px; font-weight:700; }
.bgreen { background:rgba(0,255,163,.15); color:#00ffa3; }
.bred   { background:rgba(255,77,109,.15); color:#ff4d6d; }
#msg { margin-top:10px; font-size:13px; min-height:20px; }
#secret { margin-bottom:20px; }
</style>
</head>
<body>
<h1>🔐 License Manager</h1>
<div class="sub">Panel admin untuk kelola kode lisensi</div>

<div class="card">
  <h3>Authentication</h3>
  <div class="row">
    <input type="password" id="secret" placeholder="Admin Secret / Password">
    <button class="btn-cyan" onclick="loadList()">Login & Lihat Kode</button>
  </div>
</div>

<div class="card">
  <h3>Tambah / Top-up Kode</h3>
  <div class="row">
    <input type="text" id="newCode" placeholder="Kode (contoh: MANZ-ABC1)" style="text-transform:uppercase">
    <input type="number" id="newQuota" placeholder="Jumlah kuota" value="100" style="max-width:160px">
    <button class="btn-cyan" onclick="addCode()">Tambah / Top-up</button>
  </div>
  <div id="msg"></div>
</div>

<div class="card">
  <h3>Daftar Kode Aktif</h3>
  <button class="btn-gray" onclick="loadList()" style="margin-bottom:16px; font-size:12px;">↻ Refresh</button>
  <table>
    <thead><tr><th>#</th><th>Kode</th><th>Kuota Sisa</th><th>Status</th><th>Aksi</th></tr></thead>
    <tbody id="tbl"></tbody>
  </table>
</div>

<script>
function secret() { return document.getElementById('secret').value; }
function msg(t, ok) {
  const el = document.getElementById('msg');
  el.textContent = t;
  el.style.color = ok ? '#00ffa3' : '#ff4d6d';
}

async function loadList() {
  const res = await fetch('/admin/list', { headers: { 'X-Admin-Secret': secret() } });
  const data = await res.json();
  if (!data.ok) return msg(data.msg || 'Unauthorized', false);
  const tbody = document.getElementById('tbl');
  tbody.innerHTML = '';
  data.codes.forEach((c, i) => {
    const ok = c.quota > 0;
    tbody.innerHTML += `<tr>
      <td>${i+1}</td>
      <td><b>${c.code}</b></td>
      <td>${c.quota}</td>
      <td><span class="badge ${ok?'bgreen':'bred'}">${ok?'AKTIF':'HABIS'}</span></td>
      <td><button class="btn-red" style="padding:4px 12px;font-size:11px" onclick="delCode('${c.code}')">Hapus</button></td>
    </tr>`;
  });
  msg(`${data.total} kode ditemukan`, true);
}

async function addCode() {
  const code = document.getElementById('newCode').value.trim().toUpperCase();
  const quota = parseInt(document.getElementById('newQuota').value);
  if (!code || !quota) return msg('Isi kode dan kuota!', false);
  const res = await fetch('/admin/add', {
    method: 'POST',
    headers: { 'Content-Type':'application/json', 'X-Admin-Secret': secret() },
    body: JSON.stringify({ code, quota })
  });
  const data = await res.json();
  if (data.ok) {
    msg(`✓ Kode ${data.code} | Sisa kuota: ${data.quota}`, true);
    loadList();
  } else {
    msg(data.msg || 'Gagal', false);
  }
}

async function delCode(code) {
  if (!confirm('Hapus kode ' + code + '?')) return;
  const res = await fetch('/admin/delete', {
    method: 'DELETE',
    headers: { 'Content-Type':'application/json', 'X-Admin-Secret': secret() },
    body: JSON.stringify({ code })
  });
  const data = await res.json();
  msg(data.ok ? `✓ ${code} dihapus` : data.msg, data.ok);
  if (data.ok) loadList();
}
</script>
</body>
</html>'''


if __name__ == '__main__':
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
