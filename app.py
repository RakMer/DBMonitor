import hmac
import os
import sqlite3
import subprocess
import sys
import time
import threading
import pyodbc
from flask import Flask, Response, jsonify, render_template, request
from dotenv import dotenv_values, load_dotenv, set_key




app = Flask(__name__)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.getenv("FLASK_COOKIE_SECURE", "1") == "1",
)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dbmonitor.sqlite3")
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_SCRIPT_PATH = os.path.join(BASE_DIR, "Test.py")
RUN_TEST_LOCK = threading.Lock()
RUN_CHECK_STATE_LOCK = threading.Lock()
RUN_CHECK_STATE = {
    "status": "idle",
    "message": "",
    "error": "",
    "started_at": None,
    "finished_at": None,
}

# Load .env so security credentials can be read from environment
load_dotenv(ENV_PATH)

DEFAULT_SETTINGS = {
    "TELEGRAM_THRESHOLD": "70",
    "DISK_WARN_PCT": "80",
    "DISK_CRIT_PCT": "90",
    "LOG_USED_CRIT_PCT": "70",
    "FAILED_LOGIN_ALERT": "10",
    "FAILED_LOGIN_WINDOW_HOURS": "24",
    "BACKUP_MAX_AGE_HOURS": "24",
    "SYSADMIN_MAX_COUNT": "2",
    "LONG_QUERY_SEC": "30",
    "LARGE_QUERY_LOGICAL_READS": "1000000",
}

DASHBOARD_USER = os.getenv("DASHBOARD_USER")
DASHBOARD_PASS = os.getenv("DASHBOARD_PASS")
DB_SERVER = os.getenv("DB_SERVER")
DB_NAME = os.getenv("DB_NAME", "master")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DRIVER = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")


def constant_time_compare(val: str | None, expected: str | None) -> bool:
    if val is None or expected is None:
        return False
    return hmac.compare_digest(str(val), str(expected))


def require_basic_auth():
    if not DASHBOARD_USER or not DASHBOARD_PASS:
        return None
    auth = request.authorization
    if auth and constant_time_compare(auth.username, DASHBOARD_USER) and constant_time_compare(auth.password, DASHBOARD_PASS):
        return None
    return Response(
        "Kimlik doğrulaması gerekli",
        401,
        {"WWW-Authenticate": 'Basic realm="DB Monitor"'},
    )


def enforce_auth():
    guard = require_basic_auth()
    if guard:
        return guard
    return None


@app.after_request
def add_security_headers(resp):
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    resp.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    resp.headers.setdefault("Cache-Control", "no-store")
    return resp


def load_settings():
    cfg = dotenv_values(ENV_PATH)
    telegram_threshold = (
        cfg.get("TELEGRAM_THRESHOLD")
        or cfg.get("TELEGRAM_ALERT_THRESHOLD")
        or DEFAULT_SETTINGS["TELEGRAM_THRESHOLD"]
    )
    return {
        "TELEGRAM_THRESHOLD": telegram_threshold,
        "DISK_WARN_PCT": cfg.get("DISK_WARN_PCT", DEFAULT_SETTINGS["DISK_WARN_PCT"]),
        "DISK_CRIT_PCT": cfg.get("DISK_CRIT_PCT", DEFAULT_SETTINGS["DISK_CRIT_PCT"]),
        "LOG_USED_CRIT_PCT": cfg.get("LOG_USED_CRIT_PCT", DEFAULT_SETTINGS["LOG_USED_CRIT_PCT"]),
        "FAILED_LOGIN_ALERT": cfg.get("FAILED_LOGIN_ALERT", DEFAULT_SETTINGS["FAILED_LOGIN_ALERT"]),
        "FAILED_LOGIN_WINDOW_HOURS": cfg.get("FAILED_LOGIN_WINDOW_HOURS", DEFAULT_SETTINGS["FAILED_LOGIN_WINDOW_HOURS"]),
        "BACKUP_MAX_AGE_HOURS": cfg.get("BACKUP_MAX_AGE_HOURS", DEFAULT_SETTINGS["BACKUP_MAX_AGE_HOURS"]),
        "SYSADMIN_MAX_COUNT": cfg.get("SYSADMIN_MAX_COUNT", DEFAULT_SETTINGS["SYSADMIN_MAX_COUNT"]),
        "LONG_QUERY_SEC": cfg.get("LONG_QUERY_SEC", DEFAULT_SETTINGS["LONG_QUERY_SEC"]),
        "LARGE_QUERY_LOGICAL_READS": cfg.get("LARGE_QUERY_LOGICAL_READS", DEFAULT_SETTINGS["LARGE_QUERY_LOGICAL_READS"]),
    }


def persist_setting(key: str, value: str):
    set_key(ENV_PATH, key, value)
    os.environ[key] = value


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_monitoring_table(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS MonitoringConfig (
            db_name TEXT PRIMARY KEY,
            is_monitored INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT
        )
        """
    )


def get_mssql_connection():
    if not all([DB_SERVER, DB_USER, DB_PASSWORD]):
        raise ValueError("MSSQL baglanti bilgileri eksik")

    conn_str = (
        f"DRIVER={{{DB_DRIVER}}};"
        f"SERVER={DB_SERVER};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, timeout=8)


def set_run_check_state(status: str, message: str = "", error: str = ""):
    now_ts = int(time.time())
    with RUN_CHECK_STATE_LOCK:
        RUN_CHECK_STATE["status"] = status
        RUN_CHECK_STATE["message"] = message
        RUN_CHECK_STATE["error"] = error
        if status == "running":
            RUN_CHECK_STATE["started_at"] = now_ts
            RUN_CHECK_STATE["finished_at"] = None
        elif status in {"completed", "failed"}:
            RUN_CHECK_STATE["finished_at"] = now_ts


def get_run_check_state():
    with RUN_CHECK_STATE_LOCK:
        return dict(RUN_CHECK_STATE)


def run_check_worker():
    try:
        child_env = os.environ.copy()
        child_env["PYTHONIOENCODING"] = "utf-8"
        child_env["PYTHONUTF8"] = "1"

        proc = subprocess.run(
            [sys.executable, TEST_SCRIPT_PATH],
            cwd=BASE_DIR,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=child_env,
            timeout=180,
        )
        if proc.returncode != 0:
            err_text = (proc.stderr or proc.stdout or "Bilinmeyen hata").strip()
            set_run_check_state("failed", error=f"Test calismadi: {err_text}")
            return

        out_text = (proc.stdout or "Kontrol tamamlandi").strip()
        set_run_check_state("completed", message=out_text[-600:])
    except subprocess.TimeoutExpired:
        set_run_check_state("failed", error="Test zaman asimina ugradi (180 sn)")
    except Exception as e:
        set_run_check_state("failed", error=f"Beklenmeyen hata: {e}")
    finally:
        RUN_TEST_LOCK.release()


@app.route("/")
def dashboard():
    guard = enforce_auth()
    if guard:
        return guard

    conn = get_db()
    cursor = conn.cursor()

    # --- 1) En güncel skor kaydı ---
    cursor.execute(
        "SELECT id, check_date, score FROM HealthHistory ORDER BY id DESC LIMIT 1"
    )
    latest_row = cursor.fetchone()

    latest = None
    penalties = []
    if latest_row:
        latest = {
            "id": latest_row["id"],
            "check_date": latest_row["check_date"],
            "score": latest_row["score"],
        }

        # --- 2) En güncel kayda ait ceza logları ---
        cursor.execute(
            "SELECT penalty_desc FROM PenaltyLog WHERE history_id = ?",
            (latest_row["id"],),
        )
        penalties = [row["penalty_desc"] for row in cursor.fetchall()]

    # --- 3) Son 20 kontrol (grafik + tablo) ---
    cursor.execute(
        "SELECT id, check_date, score FROM HealthHistory ORDER BY id DESC LIMIT 200"
    )
    history_rows = cursor.fetchall()

    # Her kayda ait cezaları da çek (tablo için)
    history = []
    for row in history_rows:
        cursor.execute(
            "SELECT penalty_desc FROM PenaltyLog WHERE history_id = ?", (row["id"],)
        )
        row_penalties = [p["penalty_desc"] for p in cursor.fetchall()]
        history.append(
            {
                "id": row["id"],
                "check_date": row["check_date"],
                "score": row["score"],
                "penalties": row_penalties,
            }
        )

    conn.close()

    # Grafik için kronolojik sıra (eskiden yeniye)
    chart_labels = [h["check_date"] for h in reversed(history)]
    chart_scores = [h["score"] for h in reversed(history)]

    return render_template(
        "index.html",
        latest=latest,
        penalties=penalties,
        history=history,
        chart_labels=chart_labels,
        chart_scores=chart_scores,
    )


@app.route("/api/settings", methods=["GET", "POST"])
def api_settings():
    guard = enforce_auth()
    if guard:
        return guard

    allowed_keys = {
        "TELEGRAM_THRESHOLD": int,
        "DISK_WARN_PCT": float,
        "DISK_CRIT_PCT": float,
        "LOG_USED_CRIT_PCT": float,
        "FAILED_LOGIN_ALERT": int,
        "FAILED_LOGIN_WINDOW_HOURS": int,
        "BACKUP_MAX_AGE_HOURS": int,
        "SYSADMIN_MAX_COUNT": int,
        "LONG_QUERY_SEC": float,
        "LARGE_QUERY_LOGICAL_READS": int,
    }

    alias_map = {"TELEGRAM_ALERT_THRESHOLD": "TELEGRAM_THRESHOLD"}

    if request.method == "GET":
        if request.args.get("defaults"):
            return jsonify(DEFAULT_SETTINGS)
        return jsonify(load_settings())

    data = request.get_json(silent=True) or {}
    updated = {}
    for raw_key, raw_val in data.items():
        key = alias_map.get(raw_key, raw_key)
        if key not in allowed_keys:
            continue
        caster = allowed_keys[key]
        try:
            val = caster(raw_val)
        except Exception:
            return jsonify({"error": f"{key} geçersiz değer"}), 400
        persist_setting(key, str(val))
        if key == "TELEGRAM_THRESHOLD":
            persist_setting("TELEGRAM_ALERT_THRESHOLD", str(val))
        updated[key] = str(val)

    if not updated:
        return jsonify({"error": "Güncellenecek anahtar yok"}), 400

    return jsonify({"status": "ok", "updated": updated})


@app.route("/api/monitoring-databases", methods=["GET", "POST"])
def api_monitoring_databases():
    guard = enforce_auth()
    if guard:
        return guard

    conn = get_db()
    ensure_monitoring_table(conn)
    cursor = conn.cursor()

    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        all_dbs = data.get("all_databases") or []
        selected_dbs = set(data.get("selected_databases") or [])

        if not all_dbs:
            conn.close()
            return jsonify({"error": "Kaydedilecek veritabani listesi bos"}), 400

        for db_name in all_dbs:
            is_monitored = 1 if db_name in selected_dbs else 0
            cursor.execute(
                """
                INSERT INTO MonitoringConfig (db_name, is_monitored, updated_at)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(db_name) DO UPDATE SET
                    is_monitored = excluded.is_monitored,
                    updated_at = excluded.updated_at
                """,
                (db_name, is_monitored),
            )

        conn.commit()
        conn.close()
        return jsonify({"status": "ok"})

    cursor.execute("SELECT db_name, is_monitored FROM MonitoringConfig")
    rows = cursor.fetchall()
    monitored_map = {row["db_name"]: bool(row["is_monitored"]) for row in rows}

    warning = None
    database_names = []
    try:
        sql_conn = get_mssql_connection()
        sql_cursor = sql_conn.cursor()
        sql_cursor.execute("SELECT name FROM sys.databases ORDER BY name")
        database_names = [r[0] for r in sql_cursor.fetchall()]
        sql_conn.close()
    except Exception as e:
        warning = f"MSSQL listesi alinamadi: {e}"
        database_names = sorted(monitored_map.keys())

    payload = []
    for db_name in database_names:
        payload.append({
            "name": db_name,
            "is_monitored": monitored_map.get(db_name, True),
        })

    conn.close()
    return jsonify({"databases": payload, "warning": warning})


@app.route("/api/run-check", methods=["POST"])
def api_run_check():
    guard = enforce_auth()
    if guard:
        return guard

    if not os.path.exists(TEST_SCRIPT_PATH):
        return jsonify({"error": "Test.py bulunamadi"}), 404

    if not RUN_TEST_LOCK.acquire(blocking=False):
        state = get_run_check_state()
        return jsonify({"status": "running", "state": state}), 202

    set_run_check_state("running", message="Test calistirildi")
    thread = threading.Thread(target=run_check_worker, daemon=True)
    thread.start()
    return jsonify({"status": "started", "state": get_run_check_state()}), 202


@app.route("/api/run-check-status", methods=["GET"])
def api_run_check_status():
    guard = enforce_auth()
    if guard:
        return guard
    return jsonify(get_run_check_state())


if __name__ == "__main__":
    debug_mode = os.getenv("FLASK_DEBUG", "0") == "1"
    host = os.getenv("FLASK_HOST", "127.0.0.1")
    port = int(os.getenv("FLASK_PORT", "5050"))
    app.run(debug=debug_mode, host=host, port=port)
