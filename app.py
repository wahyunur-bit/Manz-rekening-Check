"""
Rekening Validator Pro — Production Backend
Author: Senior Engineering Standard
Stack: Flask + SSE + ThreadPoolExecutor + Redis/JSON fallback
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import threading
import time
from decimal import Decimal, InvalidOperation
from typing import Optional

import pandas as pd
import requests
from flask import Flask, Response, jsonify, render_template, request, send_file, session, redirect, url_for
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from functools import wraps

# ─────────────────────────────────────────────────────────────────────────────
# BOOTSTRAP
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("validator")

app = Flask(__name__, template_folder="templates")
app.secret_key = os.getenv("FLASK_SECRET_KEY", "manz-validator-pro-2024")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION (Railway Environment Variables)
# ─────────────────────────────────────────────────────────────────────────────
API_KEY    = os.getenv("APICOID_API_KEY", "")
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PWD  = os.getenv("ADMIN_SECRET", "admin123")  # Mengambil 'ADMIN_SECRET' dari Railway

# Endpoint resmi api.co.id (GET + header x-api-co-id)
API_ENDPOINT = "https://use.api.co.id/validation/bank"

MAX_WORKERS     = 4
REQUEST_TIMEOUT = 30
CODES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "codes.json")

# ─────────────────────────────────────────────────────────────────────────────
# QUOTA STORE  (Redis jika tersedia, fallback JSON lokal)
# ─────────────────────────────────────────────────────────────────────────────
_redis = None
_lock  = threading.RLock()

try:
    import redis as _redis_lib  # type: ignore
    _ru = os.getenv("REDIS_URL", "")
    if _ru:
        _redis = _redis_lib.from_url(_ru, decode_responses=True, socket_timeout=3)
        _redis.ping()
        log.info("Quota store: Redis ✓")
except Exception as exc:
    log.warning("Quota store: Redis tidak tersedia (%s) → pakai JSON lokal", exc)


def _codes_read() -> dict:
    try:
        if os.path.exists(CODES_FILE):
            with open(CODES_FILE, encoding="utf-8") as fh:
                return json.load(fh)
    except Exception:
        pass
    return {}


def _codes_write(data: dict) -> None:
    with open(CODES_FILE, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)


def quota_get(code: str) -> Optional[int]:
    code = code.strip().upper()
    if _redis:
        v = _redis.get(f"q:{code}")
        return int(v) if v is not None else None
    d = _codes_read()
    return int(d[code]) if code in d else None


def quota_set(code: str, val: int) -> None:
    code = code.strip().upper()
    if _redis:
        _redis.set(f"q:{code}", val)
        return
    with _lock:
        d = _codes_read()
        d[code] = val
        _codes_write(d)


def quota_decr(code: str) -> int:
    """Atomic decrement, floor 0. Returns new value."""
    code = code.strip().upper()
    if _redis:
        new = _redis.decr(f"q:{code}")
        if new < 0:
            _redis.set(f"q:{code}", 0)
            return 0
        return new
    with _lock:
        d = _codes_read()
        cur = int(d.get(code, 0))
        new = max(0, cur - 1)
        d[code] = new
        _codes_write(d)
        return new


def quota_add(code: str, amount: int) -> int:
    code = code.strip().upper()
    cur = quota_get(code) or 0
    quota_set(code, cur + amount)
    return cur + amount


def quota_list() -> list[dict]:
    if _redis:
        keys = sorted(_redis.keys("q:*"))
        return [{"code": k[2:], "quota": int(_redis.get(k) or 0)} for k in keys]
    d = _codes_read()
    return [{"code": k, "quota": int(v)} for k, v in sorted(d.items())]


# Health check: verifikasi API key bisa benar-benar validasi rekening
_api_health_ok: Optional[bool] = None
_api_health_msg: str = ""
_api_health_lock = threading.Lock()


def api_health_check(force: bool = False) -> tuple[bool, str]:
    """Cek konektivitas API dan validitas key. Tidak memblokir berdasarkan hasil validasi."""
    global _api_health_ok, _api_health_msg
    if not force and _api_health_ok is not None:
        return _api_health_ok, _api_health_msg

    with _api_health_lock:
        if not force and _api_health_ok is not None:
            return _api_health_ok, _api_health_msg

        if not API_KEY:
            _api_health_ok = False
            _api_health_msg = "APICOID_API_KEY belum di-set"
            return False, _api_health_msg

        sess = get_session()
        try:
            # Cek koneksi + validitas key via endpoint available banks
            resp = sess.get(
                API_ENDPOINT + "/available",
                headers={"x-api-co-id": API_KEY, "Accept": "application/json"},
                timeout=15,
            )
            log.info("[HEALTH] HTTP %d | %s", resp.status_code, resp.text[:150])

            if resp.status_code == 401:
                _api_health_ok = False
                _api_health_msg = "API Key tidak valid (401). Cek di dashboard api.co.id"
            elif resp.status_code == 402:
                _api_health_ok = False
                _api_health_msg = "Saldo api.co.id habis (402). Top-up di dashboard api.co.id"
            elif resp.status_code == 200:
                body = resp.json()
                if body.get("is_success"):
                    total = body.get("data", {}).get("total", 0) if isinstance(body.get("data"), dict) else len(body.get("data", []))
                    _api_health_ok = True
                    _api_health_msg = f"API aktif, {total} bank tersedia"
                else:
                    _api_health_ok = False
                    _api_health_msg = f"API Error: {body.get('message', 'Unknown')}"
            else:
                _api_health_ok = False
                _api_health_msg = f"HTTP {resp.status_code}"
        except Exception as exc:
            _api_health_ok = False
            _api_health_msg = f"Gagal koneksi ke api.co.id: {exc}"

    return _api_health_ok, _api_health_msg

# ─────────────────────────────────────────────────────────────────────────────
# HTTP SESSION (shared, connection-pooled)
# ─────────────────────────────────────────────────────────────────────────────
def _make_session() -> requests.Session:
    sess = requests.Session()
    retry = Retry(total=0)          # Kita handle retry manual agar lebih kontrol
    adapter = HTTPAdapter(
        pool_connections=MAX_WORKERS + 2,
        pool_maxsize=MAX_WORKERS + 2,
        max_retries=retry,
    )
    sess.mount("https://", adapter)
    sess.mount("http://",  adapter)
    return sess


# Session global — di-share antar thread (thread-safe untuk requests)
_SESSION: Optional[requests.Session] = None
_SESSION_LOCK = threading.Lock()


def get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        with _SESSION_LOCK:
            if _SESSION is None:
                _SESSION = _make_session()
    return _SESSION


# ─────────────────────────────────────────────────────────────────────────────
# TEXT UTILITIES
# ─────────────────────────────────────────────────────────────────────────────
_WS = re.compile(r"\s+")


def normalize(s: str) -> str:
    """Hapus semua spasi & uppercase — untuk perbandingan nama."""
    return _WS.sub("", str(s)).upper().strip()


def sanitize_account(val) -> str:
    """
    Konversi nilai rekening dari Excel ke string digit bersih.
    Handle: float (12345.0), scientific (1.23E+10), string campur huruf.
    """
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return ""
    # Scientific notation
    if "e" in s.lower():
        try:
            s = format(Decimal(s), "f")
        except InvalidOperation:
            pass
    # Strip desimal trailing (.0)
    if "." in s:
        s = s.split(".")[0]
    # Hanya digit
    return re.sub(r"\D", "", s)


def sanitize_bank(raw: str) -> tuple[str, str]:
    """
    Return (short, full) e.g. ("bca", "bank_bca")
    Menerima input: "BCA", "bca", "bank_bca", "BANK_BCA"
    """
    b = re.sub(r"\s+", "", raw.strip().lower())
    b = re.sub(r"[^a-z0-9_]", "", b)
    if b.startswith("bank_"):
        return b[5:], b
    return b, f"bank_{b}"


# ─────────────────────────────────────────────────────────────────────────────
# CORE API CALL
# ─────────────────────────────────────────────────────────────────────────────

class APIResult:
    def __init__(self, ok=False, name="", is_valid=False, score=0.0, error="", is_system_error=False):
        self.ok = ok
        self.account_name = name
        self.is_valid = is_valid
        self.score = score
        self.error = error
        self.is_system_error = is_system_error

def _call_api(sess: requests.Session, bank_code: str, account_no: str, account_name: str = "") -> Optional[APIResult]:
    """Internal caller ke api.co.id"""
    try:
        # Jika account_name kosong, JANGAN dikirimkan parameternya.
        params = {"bank_code": bank_code, "account_number": account_no}
        if account_name:
            params["account_name"] = account_name

        resp = sess.get(
            API_ENDPOINT,
            params=params,
            headers={"x-api-co-id": API_KEY, "Accept": "application/json"},
            timeout=REQUEST_TIMEOUT
        )
        
        if resp.status_code == 401: return APIResult(error="API Key Invalid", is_system_error=True)
        if resp.status_code == 402: return APIResult(error="Saldo API Habis", is_system_error=True)
        if resp.status_code == 429: return None
        if resp.status_code != 200: return None

        body = resp.json()
        if not body.get("is_success"):
            msg = body.get("message", "Unknown error")
            # Jika error soal bank_code ATAU rekening tidak ditemukan, 
            # jangan menyerah dulu, coba format bank lain (misal dari bank_bca ke bca)
            retry_keywords = ("bank_code", "invalid", "not supported", "tidak ditemukan", "not found")
            if any(kw in msg.lower() for kw in retry_keywords):
                return None
            return APIResult(error=msg)

        data = body.get("data") or {}
        name = data.get("name") or data.get("account_name")
        
        if not name:
            return APIResult(error="Rekening tidak ditemukan")

        return APIResult(
            ok=True, 
            name=name, 
            is_valid=bool(data.get("is_valid")), 
            score=float(data.get("score") or 0)
        )

    except Exception as e:
        log.error(f"API Error: {e}")
        return None

def check_account(account_no: str, bank_raw: str, account_name: str = "") -> APIResult:
    """
    Strategi Final:
    1. Ambil data MURNI (Tanpa Nama Hint) -> Ini kunci agar tidak 'Validation failed'.
    2. Jika TAHAP 1 gagal, baru coba pakai Nama Hint sebagai cadangan.
    """
    if not API_KEY: return APIResult(error="API Key belum di-set", is_system_error=True)

    sess = get_session()
    short, full = sanitize_bank(bank_raw)
    
    # Coba variasi format: bank_bca, bca, dan BCA (Capital)
    formats = []
    if full not in formats: formats.append(full)
    if short not in formats: formats.append(short)
    if short.upper() not in formats: formats.append(short.upper())

    last_error = "Rekening tidak ditemukan"
    
    # TAHAP 1: Cek tanpa nama hint (Paling Ampuh)
    for fmt in formats:
        res = _call_api(sess, fmt, account_no, "")
        if res and res.ok: return res
        if res and res.is_system_error: return res
        if res: last_error = res.error

    # TAHAP 2: Jika Tahap 1 gagal, coba dengan nama hint
    if account_name:
        for fmt in formats:
            res = _call_api(sess, fmt, account_no, account_name)
            if res and res.ok: return res
            if res: last_error = res.error

    return APIResult(error=last_error)


# ─────────────────────────────────────────────────────────────────────────────
# ROW PROCESSOR
# ─────────────────────────────────────────────────────────────────────────────

def process_row(index: int, row: dict) -> dict:
    nama     = str(row.get("nama", "")).strip()
    rekening = sanitize_account(row.get("rekening", ""))
    bank     = str(row.get("bank", "")).strip()

    base = {
        "type":     "result",
        "index":    index + 1,
        "nama":     nama,
        "rekening": rekening,
        "bank":     bank,
    }

    if not rekening:
        return {**base, "nama_bank": "-", "hasil": "TIDAK VALID"}

    result = check_account(rekening, bank, nama)

    # ── Tentukan hasil ──────────────────────────────────────────────────────
    if not result.ok:
        if result.is_system_error:
            return {**base, "nama_bank": f"⚠ {result.error}", "hasil": "ERROR"}
        # Rekening definitif tidak ada, tampilkan pesan dari API agar transparan
        api_err = result.error if result.error else "Tidak Ditemukan"
        return {**base, "nama_bank": f"API: {api_err}", "hasil": "TIDAK VALID"}

    nama_bank = result.account_name

    # Kriteria MATCH:
    #   • is_valid=True dari API baru  ATAU
    #   • score ≥ 7.0                  ATAU
    #   • nama bersih identik          ATAU
    #   • nama input adalah bagian dari nama bank (Fuzzy/Partial Match)
    n_input = normalize(nama)
    n_bank  = normalize(nama_bank)

    if result.is_valid or result.score >= 7.0 or n_input == n_bank or (n_input and n_input in n_bank):
        return {**base, "nama_bank": nama_bank or nama.upper(), "hasil": "MATCH"}

    if nama_bank:
        return {**base, "nama_bank": nama_bank, "hasil": "TIDAK SAMA"}

    return {**base, "nama_bank": "-", "hasil": "TIDAK VALID"}


# ─────────────────────────────────────────────────────────────────────────────
# SSE STREAM GENERATOR
# ─────────────────────────────────────────────────────────────────────────────

def stream_generator(records: list[dict], code: str):
    """
    Server-Sent Events generator.
    Kirim 'start' → N×'result' → 'done'.
    Kuota dipotong per baris hasil valid (ERROR tidak dipotong).
    """
    total = len(records)
    yield f"data: {json.dumps({'type': 'start', 'total': total})}\n\n"

    processed = 0

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            future_map = {
                pool.submit(process_row, i, row): i
                for i, row in enumerate(records)
            }

            for future in as_completed(future_map):
                try:
                    payload = future.result()
                except Exception as exc:
                    idx = future_map[future]
                    payload = {
                        "type": "result", "index": idx + 1,
                        "nama": "-", "rekening": "-", "bank": "-",
                        "nama_bank": f"Exception: {exc}", "hasil": "ERROR",
                    }

                # Potong kuota hanya untuk hasil non-ERROR
                if payload.get("hasil") != "ERROR":
                    sisa = quota_decr(code)
                else:
                    sisa = quota_get(code) or 0

                payload["sisa_kuota"] = sisa
                processed += 1

                yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

    except Exception as exc:
        yield f"data: {json.dumps({'type': 'error', 'message': str(exc)})}\n\n"
        return

    yield f"data: {json.dumps({'type': 'done', 'total': total, 'processed': processed})}\n\n"


# ─────────────────────────────────────────────────────────────────────────────
# FLASK ROUTES — Public
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/verify-code", methods=["POST"])
def verify_code():
    body = request.get_json(silent=True) or {}
    code = str(body.get("code", "")).strip().upper()

    if not code:
        return jsonify({"valid": False, "message": "Kode tidak boleh kosong"}), 400

    q = quota_get(code)
    if q is None:
        return jsonify({"valid": False, "message": "Kode tidak ditemukan"}), 403
    if q <= 0:
        return jsonify({"valid": False, "message": "Kuota habis. Hubungi admin untuk top-up."}), 403

    return jsonify({"valid": True, "quota": q})


@app.route("/stream", methods=["POST"])
def stream():
    code = str(request.form.get("code", "")).strip().upper()

    # Guard: validasi kode & kuota sebelum baca file
    q = quota_get(code)
    if q is None:
        return jsonify({"error": "Kode tidak valid"}), 403
    if q <= 0:
        return jsonify({"error": "Kuota habis. Hubungi admin untuk top-up."}), 403

    # Pre-flight: cek apakah API key bisa validasi rekening
    healthy, health_msg = api_health_check(force=True)
    if not healthy:
        return jsonify({"error": f"API tidak siap: {health_msg}"}), 503

    file = request.files.get("file")
    if not file:
        return jsonify({"error": "File tidak ditemukan"}), 400

    # Baca Excel — validasi kolom wajib
    try:
        raw_bytes = file.read()
        df = pd.read_excel(io.BytesIO(raw_bytes), dtype=str)
        df.columns = [c.strip().lower() for c in df.columns]

        required = {"nama", "rekening", "bank"}
        missing  = required - set(df.columns)
        if missing:
            return jsonify({"error": f"Kolom tidak ditemukan: {', '.join(sorted(missing))}"}), 400

        records = df[["nama", "rekening", "bank"]].to_dict("records")
    except Exception as exc:
        return jsonify({"error": f"Gagal membaca Excel: {exc}"}), 400

    if not records:
        return jsonify({"error": "File kosong atau tidak ada data"}), 400

    if len(records) > q:
        return jsonify({
            "error": f"Data ({len(records)} baris) melebihi sisa kuota ({q}). Hubungi admin."
        }), 403

    return Response(
        stream_generator(records, code),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":       "keep-alive",
        },
    )


@app.route("/template")
def download_template():
    # Sesuai permintaan: Hanya berisi rekening WAHYU NUR IMAN yang asli
    df = pd.DataFrame({
        "nama":     ["WAHYU NUR IMAN"],
        "rekening": ["1330024362634"],
        "bank":     ["mandiri"],
    })
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    buf.seek(0)
    return send_file(
        buf, as_attachment=True, download_name="template_rekening.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/download", methods=["POST"])
def download():
    body   = request.get_json(silent=True) or {}
    rows   = body.get("data", [])
    fmt    = body.get("format", "xlsx").lower()

    cols = ["No", "Nama", "Rekening", "Bank", "Nama Bank", "Hasil"]
    df   = pd.DataFrame(rows, columns=cols)
    buf  = io.BytesIO()

    if fmt == "csv":
        df.to_csv(buf, index=False, sep=",", encoding="utf-8-sig")
        buf.seek(0)
        return send_file(buf, as_attachment=True, download_name="hasil_validasi.csv", mimetype="text/csv")

    df.to_excel(buf, index=False)
    buf.seek(0)
    return send_file(
        buf, as_attachment=True, download_name="hasil_validasi.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/supported-banks")
def supported_banks():
    """Download daftar bank yang didukung API sebagai Excel."""
    try:
        resp = requests.get(
            "https://use.api.co.id/validation/bank/available",
            headers={"x-api-co-id": API_KEY, "Accept": "application/json"},
            timeout=15,
        )
        if resp.status_code == 200:
            body = resp.json()
            if body.get("is_success"):
                banks = body.get("data", {}).get("banks", [])
                rows = [
                    {
                        "No":         i + 1,
                        "Nama Bank":  b.get("bank_name", "").title(),
                        "Kode API":   b.get("bank_code", ""),
                        "Input Excel": re.sub(r"^bank_", "", b.get("bank_code", "")).upper(),
                    }
                    for i, b in enumerate(banks)
                ]
                df  = pd.DataFrame(rows)
                buf = io.BytesIO()
                df.to_excel(buf, index=False)
                buf.seek(0)
                return send_file(
                    buf, as_attachment=True, download_name="daftar_bank_support.xlsx",
                    mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
    except Exception as exc:
        log.error("supported_banks: %s", exc)
    return jsonify({"error": "Gagal mengambil daftar bank"}), 500


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN AUTHENTICATION
# ─────────────────────────────────────────────────────────────────────────────

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return decorated_function


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        user = request.form.get("username")
        pwd  = request.form.get("password")
        if user == ADMIN_USER and pwd == ADMIN_PWD:
            session["logged_in"] = True
            return redirect(url_for("admin_panel"))
        return render_template("login.html", error="Username atau Password salah")
    return render_template("login.html")


@app.route("/admin/logout")
def admin_logout():
    session.pop("logged_in", None)
    return redirect(url_for("admin_login"))


def _require_admin():
    """Support untuk API call lama (header) atau Session."""
    return session.get("logged_in") or request.headers.get("X-Admin-Secret", "") == ADMIN_PWD


@app.route("/admin/add", methods=["POST"])
def admin_add():
    if not _require_admin():
        return jsonify({"ok": False, "msg": "Unauthorized"}), 401
    body   = request.get_json(silent=True) or {}
    code   = str(body.get("code", "")).strip().upper()
    amount = max(1, int(body.get("quota", 100)))
    if not code:
        return jsonify({"ok": False, "msg": "Kode tidak boleh kosong"}), 400
    new_q = quota_add(code, amount)
    log.info("ADMIN add %s +%d → total %d", code, amount, new_q)
    return jsonify({"ok": True, "code": code, "quota": new_q})


@app.route("/admin/set", methods=["POST"])
def admin_set():
    if not _require_admin():
        return jsonify({"ok": False, "msg": "Unauthorized"}), 401
    body   = request.get_json(silent=True) or {}
    code   = str(body.get("code", "")).strip().upper()
    amount = max(0, int(body.get("quota", 0)))
    if not code:
        return jsonify({"ok": False, "msg": "Kode tidak boleh kosong"}), 400
    quota_set(code, amount)
    log.info("ADMIN set %s → %d", code, amount)
    return jsonify({"ok": True, "code": code, "quota": amount})


@app.route("/admin/delete", methods=["DELETE"])
def admin_delete():
    if not _require_admin():
        return jsonify({"ok": False, "msg": "Unauthorized"}), 401
    body = request.get_json(silent=True) or {}
    code = str(body.get("code", "")).strip().upper()
    if not code:
        return jsonify({"ok": False, "msg": "Kode tidak boleh kosong"}), 400
    if _redis:
        _redis.delete(f"q:{code}")
    else:
        with _lock:
            d = _codes_read()
            d.pop(code, None)
            _codes_write(d)
    log.info("ADMIN delete %s", code)
    return jsonify({"ok": True, "msg": f"Kode {code} berhasil dihapus"})


@app.route("/admin/list", methods=["GET"])
def admin_list():
    if not _require_admin():
        return jsonify({"ok": False, "msg": "Unauthorized"}), 401
    codes = quota_list()
    return jsonify({
        "ok":    True,
        "codes": codes,
        "total": len(codes),
        "active": sum(1 for c in codes if c["quota"] > 0),
        "total_quota": sum(c["quota"] for c in codes),
    })


@app.route("/admin/debug", methods=["GET"])
def admin_debug():
    """Test konektivitas & format API — berguna saat trouble-shoot."""
    if not _require_admin():
        return jsonify({"ok": False, "msg": "Unauthorized"}), 401

    # Jalankan health check dulu
    healthy, health_msg = api_health_check(force=True)

    tests: dict = {
        "api_key_set":    bool(API_KEY),
        "api_key_prefix": (API_KEY[:12] + "…") if API_KEY else "—",
        "redis":          _redis is not None,
        "health_check":   {"ok": healthy, "message": health_msg},
    }

    sess = get_session()

    # Test endpoint
    for bank_code in ("bank_bca", "bca"):
        try:
            r = sess.get(
                API_ENDPOINT,
                params={"bank_code": bank_code, "account_number": "0201245750", "account_name": "TEST"},
                headers={"x-api-co-id": API_KEY, "Accept": "application/json"},
                timeout=15,
            )
            tests[f"endpoint_{bank_code}"] = {
                "status": r.status_code, "body": r.text[:300]
            }
        except Exception as exc:
            tests[f"endpoint_{bank_code}"] = {"error": str(exc)}

    return jsonify(tests)


# ─────────────────────────────────────────────────────────────────────────────
# ADMIN PANEL UI
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/admin")
@login_required
def admin_panel():
    return render_template("admin.html")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
