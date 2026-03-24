import hmac
import os
import sqlite3
import subprocess
import sys
import time
import threading
from datetime import datetime
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


def ensure_resource_tables(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ResourceSnapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_time TEXT NOT NULL,
            cpu_pct REAL,
            ram_used_pct REAL,
            sql_mem_used_mb REAL,
            disk_read_bytes_total INTEGER,
            disk_write_bytes_total INTEGER,
            net_sent_bytes_per_sec REAL,
            net_recv_bytes_per_sec REAL
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS DatabaseResourceSnapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_time TEXT NOT NULL,
            db_name TEXT NOT NULL,
            read_bytes_total INTEGER,
            write_bytes_total INTEGER,
            io_stall_ms_total INTEGER,
            UNIQUE(snapshot_time, db_name)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS WaitSnapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_time TEXT NOT NULL,
            wait_type TEXT NOT NULL,
            wait_time_ms_total INTEGER,
            signal_wait_ms_total INTEGER,
            waiting_tasks_count_total INTEGER,
            category TEXT,
            UNIQUE(snapshot_time, wait_type)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ActiveWaitSnapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_time TEXT NOT NULL,
            session_id INTEGER,
            db_name TEXT,
            wait_type TEXT,
            wait_time_ms INTEGER,
            blocking_session_id INTEGER,
            category TEXT
        )
        """
    )
    conn.commit()


def parse_snapshot_time(value: str):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def safe_delta(current_value, previous_value):
    try:
        curr = float(current_value or 0)
        prev = float(previous_value or 0)
        return max(0.0, curr - prev)
    except Exception:
        return 0.0


def avg(values):
    vals = [float(v) for v in values if v is not None]
    if not vals:
        return 0.0
    return sum(vals) / len(vals)


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
    ensure_resource_tables(conn)
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
    ensure_resource_tables(conn)
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


@app.route("/api/resource-metrics", methods=["GET"])
def api_resource_metrics():
    guard = enforce_auth()
    if guard:
        return guard

    conn = get_db()
    ensure_resource_tables(conn)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT snapshot_time, cpu_pct, ram_used_pct, sql_mem_used_mb,
               disk_read_bytes_total, disk_write_bytes_total,
               net_sent_bytes_per_sec, net_recv_bytes_per_sec
        FROM ResourceSnapshots
        ORDER BY snapshot_time DESC
        LIMIT 120
        """
    )
    rows = cursor.fetchall()
    snapshots = list(reversed(rows))

    trend_labels = []
    cpu_trend = []
    ram_trend = []
    disk_io_trend = []
    net_trend = []

    prev_row = None
    prev_dt = None
    for row in snapshots:
        row_dt = parse_snapshot_time(row["snapshot_time"])
        trend_labels.append(row["snapshot_time"])
        cpu_trend.append(round(float(row["cpu_pct"] or 0), 2))
        ram_trend.append(round(float(row["ram_used_pct"] or 0), 2))

        net_kb_s = (float(row["net_sent_bytes_per_sec"] or 0) + float(row["net_recv_bytes_per_sec"] or 0)) / 1024.0
        net_trend.append(round(net_kb_s, 2))

        disk_mb_s = 0.0
        if prev_row is not None and prev_dt is not None and row_dt is not None:
            seconds = max(1.0, (row_dt - prev_dt).total_seconds())
            read_delta = safe_delta(row["disk_read_bytes_total"], prev_row["disk_read_bytes_total"])
            write_delta = safe_delta(row["disk_write_bytes_total"], prev_row["disk_write_bytes_total"])
            disk_mb_s = (read_delta + write_delta) / (1024.0 * 1024.0) / seconds
        disk_io_trend.append(round(disk_mb_s, 3))

        prev_row = row
        prev_dt = row_dt

    latest_ts = trend_labels[-1] if trend_labels else None
    previous_ts = trend_labels[-2] if len(trend_labels) >= 2 else None

    db_distribution = []
    if latest_ts:
        if previous_ts:
            cursor.execute(
                """
                SELECT cur.db_name,
                       MAX(0, CAST(cur.read_bytes_total AS INTEGER) - CAST(COALESCE(prev.read_bytes_total, 0) AS INTEGER)) AS read_delta,
                       MAX(0, CAST(cur.write_bytes_total AS INTEGER) - CAST(COALESCE(prev.write_bytes_total, 0) AS INTEGER)) AS write_delta
                FROM DatabaseResourceSnapshots cur
                LEFT JOIN DatabaseResourceSnapshots prev
                       ON prev.snapshot_time = ? AND prev.db_name = cur.db_name
                WHERE cur.snapshot_time = ?
                """,
                (previous_ts, latest_ts),
            )
        else:
            cursor.execute(
                """
                SELECT db_name,
                       CAST(read_bytes_total AS INTEGER) AS read_delta,
                       CAST(write_bytes_total AS INTEGER) AS write_delta
                FROM DatabaseResourceSnapshots
                WHERE snapshot_time = ?
                """,
                (latest_ts,),
            )

        db_rows = cursor.fetchall()
        total_bytes = sum((int(r["read_delta"] or 0) + int(r["write_delta"] or 0)) for r in db_rows)
        for r in db_rows:
            bytes_total = int(r["read_delta"] or 0) + int(r["write_delta"] or 0)
            if bytes_total <= 0:
                continue
            ratio = (bytes_total / total_bytes * 100.0) if total_bytes > 0 else 0.0
            db_distribution.append(
                {
                    "db_name": r["db_name"],
                    "io_mb": round(bytes_total / (1024.0 * 1024.0), 3),
                    "share_pct": round(ratio, 2),
                }
            )
        db_distribution.sort(key=lambda x: x["io_mb"], reverse=True)
        db_distribution = db_distribution[:10]

    spike_summary = []
    if len(cpu_trend) >= 8:
        n = max(3, min(10, len(cpu_trend) // 2))
        metrics = [
            ("CPU", cpu_trend, "%", 8.0),
            ("RAM", ram_trend, "%", 5.0),
            ("Disk I/O", disk_io_trend, "MB/s", 1.0),
            ("Ag", net_trend, "KB/s", 10.0),
        ]
        for name, data, unit, min_abs in metrics:
            current = avg(data[-n:])
            previous = avg(data[-(2 * n):-n])
            delta = current - previous
            delta_pct = (delta / previous * 100.0) if previous > 0 else (100.0 if current > 0 else 0.0)
            is_spike = delta > min_abs and delta_pct >= 35.0
            spike_summary.append(
                {
                    "metric": name,
                    "previous_avg": round(previous, 2),
                    "current_avg": round(current, 2),
                    "delta_pct": round(delta_pct, 2),
                    "unit": unit,
                    "is_spike": bool(is_spike),
                }
            )

    conn.close()
    return jsonify(
        {
            "trend": {
                "labels": trend_labels,
                "cpu_pct": cpu_trend,
                "ram_pct": ram_trend,
                "disk_io_mb_s": disk_io_trend,
                "net_kb_s": net_trend,
            },
            "distribution": db_distribution,
            "spikes": spike_summary,
            "latest_snapshot": latest_ts,
        }
    )


@app.route("/api/wait-analysis", methods=["GET"])
def api_wait_analysis():
    guard = enforce_auth()
    if guard:
        return guard

    conn = get_db()
    ensure_resource_tables(conn)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT DISTINCT snapshot_time
        FROM WaitSnapshots
        ORDER BY snapshot_time DESC
        LIMIT 40
        """
    )
    snapshot_rows = cursor.fetchall()
    snapshot_times_desc = [r["snapshot_time"] for r in snapshot_rows]

    if not snapshot_times_desc:
        conn.close()
        return jsonify({
            "latest_snapshot": None,
            "top_waits": [],
            "category_breakdown": [],
            "wait_trend": {"labels": [], "delta_wait_ms": []},
            "blocking_summary": {"blocked_sessions": 0, "blocking_sessions": 0, "top_blocking_waits": []},
        })

    latest_ts = snapshot_times_desc[0]
    previous_ts = snapshot_times_desc[1] if len(snapshot_times_desc) >= 2 else None

    cursor.execute(
        """
        SELECT wait_type, wait_time_ms_total, signal_wait_ms_total, waiting_tasks_count_total, category
        FROM WaitSnapshots
        WHERE snapshot_time = ?
        """,
        (latest_ts,),
    )
    latest_waits = cursor.fetchall()

    previous_map = {}
    if previous_ts:
        cursor.execute(
            """
            SELECT wait_type, wait_time_ms_total, signal_wait_ms_total, waiting_tasks_count_total
            FROM WaitSnapshots
            WHERE snapshot_time = ?
            """,
            (previous_ts,),
        )
        for row in cursor.fetchall():
            previous_map[row["wait_type"]] = row

    top_waits = []
    category_totals = {}
    for row in latest_waits:
        wait_type = row["wait_type"]
        prev = previous_map.get(wait_type)
        wait_delta = safe_delta(row["wait_time_ms_total"], prev["wait_time_ms_total"] if prev else 0)
        signal_delta = safe_delta(row["signal_wait_ms_total"], prev["signal_wait_ms_total"] if prev else 0)
        tasks_delta = safe_delta(row["waiting_tasks_count_total"], prev["waiting_tasks_count_total"] if prev else 0)

        if wait_delta <= 0:
            continue

        category = row["category"] or "Other"
        category_totals[category] = category_totals.get(category, 0.0) + wait_delta

        top_waits.append(
            {
                "wait_type": wait_type,
                "category": category,
                "wait_ms": round(wait_delta, 2),
                "signal_ms": round(signal_delta, 2),
                "tasks": int(tasks_delta),
            }
        )

    top_waits.sort(key=lambda x: x["wait_ms"], reverse=True)
    top_waits = top_waits[:12]

    category_breakdown = []
    total_wait_ms = sum(category_totals.values())
    for category, value in sorted(category_totals.items(), key=lambda kv: kv[1], reverse=True):
        ratio = (value / total_wait_ms * 100.0) if total_wait_ms > 0 else 0.0
        category_breakdown.append(
            {
                "category": category,
                "wait_ms": round(value, 2),
                "share_pct": round(ratio, 2),
            }
        )

    trend_labels = []
    trend_delta = []
    snapshot_times = list(reversed(snapshot_times_desc[:20]))
    total_by_snapshot = {}
    cursor.execute(
        """
        SELECT snapshot_time, SUM(wait_time_ms_total) AS total_wait
        FROM WaitSnapshots
        WHERE snapshot_time IN ({})
        GROUP BY snapshot_time
        """.format(
            ",".join(["?"] * len(snapshot_times))
        ),
        snapshot_times,
    )
    for row in cursor.fetchall():
        total_by_snapshot[row["snapshot_time"]] = float(row["total_wait"] or 0)

    prev_total = None
    for ts in snapshot_times:
        curr_total = total_by_snapshot.get(ts, 0.0)
        delta = safe_delta(curr_total, prev_total if prev_total is not None else curr_total)
        trend_labels.append(ts)
        trend_delta.append(round(delta, 2))
        prev_total = curr_total

    cursor.execute(
        """
        SELECT session_id, db_name, wait_type, wait_time_ms, blocking_session_id, category
        FROM ActiveWaitSnapshots
        WHERE snapshot_time = ?
        """,
        (latest_ts,),
    )
    active_rows = cursor.fetchall()
    blocked_sessions = [r for r in active_rows if int(r["blocking_session_id"] or 0) > 0]
    blocking_session_ids = sorted({int(r["blocking_session_id"]) for r in blocked_sessions if int(r["blocking_session_id"]) > 0})

    wait_group = {}
    for r in blocked_sessions:
        wt = r["wait_type"] or "UNKNOWN"
        wait_group[wt] = wait_group.get(wt, 0) + 1
    top_blocking_waits = [
        {"wait_type": wt, "count": cnt}
        for wt, cnt in sorted(wait_group.items(), key=lambda kv: kv[1], reverse=True)[:5]
    ]

    conn.close()
    return jsonify(
        {
            "latest_snapshot": latest_ts,
            "top_waits": top_waits,
            "category_breakdown": category_breakdown,
            "wait_trend": {
                "labels": trend_labels,
                "delta_wait_ms": trend_delta,
            },
            "blocking_summary": {
                "blocked_sessions": len(blocked_sessions),
                "blocking_sessions": len(blocking_session_ids),
                "top_blocking_waits": top_blocking_waits,
            },
        }
    )


if __name__ == "__main__":
    debug_mode = os.getenv("FLASK_DEBUG", "0") == "1"
    host = os.getenv("FLASK_HOST", "127.0.0.1")
    port = int(os.getenv("FLASK_PORT", "5050"))
    app.run(debug=debug_mode, host=host, port=port)
