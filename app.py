import hmac
import os
import sqlite3
from flask import Flask, Response, jsonify, render_template, request
from dotenv import dotenv_values, load_dotenv, set_key

current_message_statu = 0
alert_sent_time = None      # Mesajın atıldığı zamanı tutar
alert_resend_after = None   # Bir sonraki gönderim için belirlenen zamanı tutar


app = Flask(__name__)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.getenv("FLASK_COOKIE_SECURE", "1") == "1",
)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dbmonitor.sqlite3")
ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

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
}

DASHBOARD_USER = os.getenv("DASHBOARD_USER")
DASHBOARD_PASS = os.getenv("DASHBOARD_PASS")


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
    }


def persist_setting(key: str, value: str):
    set_key(ENV_PATH, key, value)
    os.environ[key] = value


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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


if __name__ == "__main__":
    debug_mode = os.getenv("FLASK_DEBUG", "0") == "1"
    host = os.getenv("FLASK_HOST", "127.0.0.1")
    port = int(os.getenv("FLASK_PORT", "5050"))
    app.run(debug=debug_mode, host=host, port=port)
