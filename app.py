import hmac
import os
import sqlite3
import subprocess
import sys
import time
import threading
from datetime import datetime
from flask import Flask, Response, jsonify, render_template, request
from dotenv import dotenv_values, load_dotenv, set_key
from db_adapters import MSSQLAdapter, PostgresAdapter, get_db_adapter




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

# Shared alert throttle state used by Test.py
current_message_statu = 0
alert_sent_time = None
alert_resend_after = None

# Load .env so security credentials can be read from environment
load_dotenv(ENV_PATH)

DEFAULT_SETTINGS = {
    "TELEGRAM_THRESHOLD": "70",
    "DISK_WARN_PCT": "80",
    "DISK_CRIT_PCT": "90",
    "LOG_USED_CRIT_PCT": "70",
    "INDEX_FRAGMENTATION_PCT": "30",
    "FAILED_LOGIN_ALERT": "10",
    "FAILED_LOGIN_WINDOW_HOURS": "24",
    "BACKUP_MAX_AGE_HOURS": "24",
    "BACKUP_CHECK_REQUIRED": "1",
    "BACKUP_UNSUPPORTED_PENALTY": "15",
    "POSTGRES_BACKUP_MODE": "auto",
    "SYSADMIN_MAX_COUNT": "2",
    "LONG_QUERY_SEC": "30",
    "LARGE_QUERY_LOGICAL_READS": "1000000",
    "QUERY_ANALYSIS_TOP_N": "5",
    "QUERY_MIN_CALLS": "2",
    "QUERY_AVG_SEC": "30",
    "QUERY_TOTAL_SEC": "60",
    "POSTGRES_JOB_LOOKBACK_HOURS": "24",
    "POSTGRES_JOB_CONNECT_TIMEOUT_SEC": "5",
    "POSTGRES_JOB_DB": "postgres",
    "AUTOGROWTH_MIN_FREE_PCT": "15",
    "SQL_AGENT_PENALTY": "30",
    "OFFLINE_DB_PENALTY": "20",
    "MISSING_BACKUP_PENALTY": "50",
    "DISK_CRIT_PENALTY": "40",
    "DISK_WARN_PENALTY": "10",
    "MEMORY_PRESSURE_PENALTY": "20",
    "BLOCKING_PENALTY": "10",
    "HEAVY_QUERY_PENALTY": "8",
    "INDEX_FRAGMENTATION_PENALTY": "10",
    "PRIVILEGED_ACCOUNT_PENALTY": "10",
    "FAILED_LOGIN_PENALTY": "15",
    "FAILED_JOB_PENALTY": "15",
    "AUTO_GROWTH_PENALTY": "10",
    "LOG_SPACE_PENALTY": "30",
    "RAM_SAMPLE_COUNT": "5",
    "RAM_SAMPLE_INTERVAL_SEC": "0.08",
}

ENGINE_ALIASES = {
    "mssql": "mssql",
    "sqlserver": "mssql",
    "sql_server": "mssql",
    "postgres": "postgresql",
    "postgresql": "postgresql",
    "pgsql": "postgresql",
}

DB_TARGET_PROFILE_KEYS = (
    "ACTIVE_DB_ENGINE",
    "MSSQL_DB_SERVER",
    "MSSQL_DB_PORT",
    "MSSQL_DB_NAME",
    "MSSQL_DB_USER",
    "MSSQL_DB_PASSWORD",
    "MSSQL_DB_DRIVER",
    "POSTGRES_DB_SERVER",
    "POSTGRES_DB_PORT",
    "POSTGRES_DB_NAME",
    "POSTGRES_DB_USER",
    "POSTGRES_DB_PASSWORD",
    "POSTGRES_DOCKER_CONTAINER",
    "PG_DUMP_BIN",
)


def normalize_db_engine(value: str | None, default: str = "mssql") -> str:
    key = str(value or "").strip().lower()
    return ENGINE_ALIASES.get(key, default)


def _strip_env_value(raw: str) -> str:
    text = str(raw or "").strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {'"', "'"}:
        return text[1:-1]
    return text


def load_legacy_engine_profiles_from_env() -> dict[str, dict[str, str]]:
    """Parse legacy repeated DB_* blocks from .env and map them to profile keys.

    This keeps backward compatibility for users who store both MSSQL and PostgreSQL
    blocks in the same .env without explicit MSSQL_DB_* / POSTGRES_DB_* keys.
    """

    profiles: dict[str, dict[str, str]] = {
        "mssql": {},
        "postgresql": {},
    }

    if not os.path.exists(ENV_PATH):
        return profiles

    active_engine: str | None = None
    try:
        with open(ENV_PATH, "r", encoding="utf-8", errors="ignore") as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue

                key, value = line.split("=", 1)
                key = key.strip()
                value = _strip_env_value(value)

                if key == "DB_ENGINE":
                    engine_candidate = normalize_db_engine(value, default="")
                    active_engine = engine_candidate if engine_candidate in profiles else None
                    continue

                if active_engine == "mssql":
                    if key == "DB_SERVER":
                        profiles["mssql"]["MSSQL_DB_SERVER"] = value
                    elif key == "DB_PORT":
                        profiles["mssql"]["MSSQL_DB_PORT"] = value
                    elif key == "DB_NAME":
                        profiles["mssql"]["MSSQL_DB_NAME"] = value
                    elif key == "DB_USER":
                        profiles["mssql"]["MSSQL_DB_USER"] = value
                    elif key == "DB_PASSWORD":
                        profiles["mssql"]["MSSQL_DB_PASSWORD"] = value
                    elif key == "DB_DRIVER":
                        profiles["mssql"]["MSSQL_DB_DRIVER"] = value

                if active_engine == "postgresql":
                    if key == "DB_SERVER":
                        profiles["postgresql"]["POSTGRES_DB_SERVER"] = value
                    elif key == "DB_PORT":
                        profiles["postgresql"]["POSTGRES_DB_PORT"] = value
                    elif key == "DB_NAME":
                        profiles["postgresql"]["POSTGRES_DB_NAME"] = value
                    elif key == "DB_USER":
                        profiles["postgresql"]["POSTGRES_DB_USER"] = value
                    elif key == "DB_PASSWORD":
                        profiles["postgresql"]["POSTGRES_DB_PASSWORD"] = value
                    elif key == "POSTGRES_DOCKER_CONTAINER":
                        profiles["postgresql"]["POSTGRES_DOCKER_CONTAINER"] = value
                    elif key in {"PG_DUMP_BIN", "PG_DUMP"}:
                        profiles["postgresql"]["PG_DUMP_BIN"] = value
    except Exception:
        return profiles

    return profiles


def _read_profile_value(
    cfg: dict[str, str],
    profile_key: str,
    fallback_key: str,
    fallback_engine: str,
    current_engine: str,
    default: str = "",
) -> str:
    profile_value = cfg.get(profile_key)
    if profile_value is not None and str(profile_value).strip() != "":
        return str(profile_value)

    if current_engine == fallback_engine:
        base_value = cfg.get(fallback_key)
        if base_value is not None:
            return str(base_value)

    return default


def load_db_target_settings(cfg: dict[str, str] | None = None) -> dict[str, str]:
    active_cfg = cfg or dotenv_values(ENV_PATH)
    current_engine = normalize_db_engine(active_cfg.get("DB_ENGINE"), default="mssql")
    active_engine = normalize_db_engine(active_cfg.get("ACTIVE_DB_ENGINE") or current_engine, default=current_engine)

    settings = {
        "ACTIVE_DB_ENGINE": active_engine,
        "MSSQL_DB_SERVER": _read_profile_value(active_cfg, "MSSQL_DB_SERVER", "DB_SERVER", "mssql", current_engine),
        "MSSQL_DB_PORT": _read_profile_value(active_cfg, "MSSQL_DB_PORT", "DB_PORT", "mssql", current_engine),
        "MSSQL_DB_NAME": _read_profile_value(active_cfg, "MSSQL_DB_NAME", "DB_NAME", "mssql", current_engine, "master"),
        "MSSQL_DB_USER": _read_profile_value(active_cfg, "MSSQL_DB_USER", "DB_USER", "mssql", current_engine),
        "MSSQL_DB_PASSWORD": _read_profile_value(active_cfg, "MSSQL_DB_PASSWORD", "DB_PASSWORD", "mssql", current_engine),
        "MSSQL_DB_DRIVER": _read_profile_value(
            active_cfg,
            "MSSQL_DB_DRIVER",
            "DB_DRIVER",
            "mssql",
            current_engine,
            "ODBC Driver 18 for SQL Server",
        ),
        "POSTGRES_DB_SERVER": _read_profile_value(active_cfg, "POSTGRES_DB_SERVER", "DB_SERVER", "postgresql", current_engine),
        "POSTGRES_DB_PORT": _read_profile_value(active_cfg, "POSTGRES_DB_PORT", "DB_PORT", "postgresql", current_engine, "5432"),
        "POSTGRES_DB_NAME": _read_profile_value(active_cfg, "POSTGRES_DB_NAME", "DB_NAME", "postgresql", current_engine, "postgres"),
        "POSTGRES_DB_USER": _read_profile_value(active_cfg, "POSTGRES_DB_USER", "DB_USER", "postgresql", current_engine),
        "POSTGRES_DB_PASSWORD": _read_profile_value(active_cfg, "POSTGRES_DB_PASSWORD", "DB_PASSWORD", "postgresql", current_engine),
        "POSTGRES_DOCKER_CONTAINER": str(active_cfg.get("POSTGRES_DOCKER_CONTAINER") or ""),
        "PG_DUMP_BIN": str(active_cfg.get("PG_DUMP_BIN") or active_cfg.get("PG_DUMP") or ""),
    }

    legacy_profiles = load_legacy_engine_profiles_from_env()
    for profile_key, profile_value in legacy_profiles.get("mssql", {}).items():
        if not str(settings.get(profile_key) or "").strip() and str(profile_value or "").strip():
            settings[profile_key] = str(profile_value)

    for profile_key, profile_value in legacy_profiles.get("postgresql", {}).items():
        if not str(settings.get(profile_key) or "").strip() and str(profile_value or "").strip():
            settings[profile_key] = str(profile_value)

    return settings


def build_active_db_target_updates(target: dict[str, str]) -> dict[str, str]:
    active_engine = target["ACTIVE_DB_ENGINE"]

    if active_engine == "mssql":
        required = {
            "MSSQL_DB_SERVER": target["MSSQL_DB_SERVER"],
            "MSSQL_DB_NAME": target["MSSQL_DB_NAME"],
            "MSSQL_DB_USER": target["MSSQL_DB_USER"],
            "MSSQL_DB_PASSWORD": target["MSSQL_DB_PASSWORD"],
        }
        missing = [k for k, v in required.items() if not str(v or "").strip()]
        if missing:
            raise ValueError("MSSQL profili eksik alanlar: " + ", ".join(missing))

        updates = {
            "ACTIVE_DB_ENGINE": "mssql",
            "DB_ENGINE": "mssql",
            "DB_SERVER": str(target["MSSQL_DB_SERVER"]),
            "DB_PORT": str(target["MSSQL_DB_PORT"]),
            "DB_NAME": str(target["MSSQL_DB_NAME"]),
            "DB_USER": str(target["MSSQL_DB_USER"]),
            "DB_PASSWORD": str(target["MSSQL_DB_PASSWORD"]),
            "DB_DRIVER": str(target["MSSQL_DB_DRIVER"] or "ODBC Driver 18 for SQL Server"),
        }
    else:
        required = {
            "POSTGRES_DB_SERVER": target["POSTGRES_DB_SERVER"],
            "POSTGRES_DB_NAME": target["POSTGRES_DB_NAME"],
            "POSTGRES_DB_USER": target["POSTGRES_DB_USER"],
            "POSTGRES_DB_PASSWORD": target["POSTGRES_DB_PASSWORD"],
        }
        missing = [k for k, v in required.items() if not str(v or "").strip()]
        if missing:
            raise ValueError("PostgreSQL profili eksik alanlar: " + ", ".join(missing))

        updates = {
            "ACTIVE_DB_ENGINE": "postgresql",
            "DB_ENGINE": "postgresql",
            "DB_SERVER": str(target["POSTGRES_DB_SERVER"]),
            "DB_PORT": str(target["POSTGRES_DB_PORT"] or "5432"),
            "DB_NAME": str(target["POSTGRES_DB_NAME"]),
            "DB_USER": str(target["POSTGRES_DB_USER"]),
            "DB_PASSWORD": str(target["POSTGRES_DB_PASSWORD"]),
            "POSTGRES_DOCKER_CONTAINER": str(target["POSTGRES_DOCKER_CONTAINER"]),
        }
        if str(target["PG_DUMP_BIN"]).strip():
            updates["PG_DUMP_BIN"] = str(target["PG_DUMP_BIN"]).strip()

    return updates


def apply_active_db_target() -> dict[str, str]:
    cfg = dotenv_values(ENV_PATH)
    target = load_db_target_settings(cfg)
    updates = build_active_db_target_updates(target)

    for key in DB_TARGET_PROFILE_KEYS:
        if key in target:
            persist_setting(key, str(target[key]))

    for key, value in updates.items():
        persist_setting(key, str(value))

    return updates

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
    data = {
        "TELEGRAM_THRESHOLD": telegram_threshold,
        "DISK_WARN_PCT": cfg.get("DISK_WARN_PCT", DEFAULT_SETTINGS["DISK_WARN_PCT"]),
        "DISK_CRIT_PCT": cfg.get("DISK_CRIT_PCT", DEFAULT_SETTINGS["DISK_CRIT_PCT"]),
        "LOG_USED_CRIT_PCT": cfg.get("LOG_USED_CRIT_PCT", DEFAULT_SETTINGS["LOG_USED_CRIT_PCT"]),
        "INDEX_FRAGMENTATION_PCT": cfg.get("INDEX_FRAGMENTATION_PCT", DEFAULT_SETTINGS["INDEX_FRAGMENTATION_PCT"]),
        "FAILED_LOGIN_ALERT": cfg.get("FAILED_LOGIN_ALERT", DEFAULT_SETTINGS["FAILED_LOGIN_ALERT"]),
        "FAILED_LOGIN_WINDOW_HOURS": cfg.get("FAILED_LOGIN_WINDOW_HOURS", DEFAULT_SETTINGS["FAILED_LOGIN_WINDOW_HOURS"]),
        "BACKUP_MAX_AGE_HOURS": cfg.get("BACKUP_MAX_AGE_HOURS", DEFAULT_SETTINGS["BACKUP_MAX_AGE_HOURS"]),
        "BACKUP_CHECK_REQUIRED": cfg.get("BACKUP_CHECK_REQUIRED", DEFAULT_SETTINGS["BACKUP_CHECK_REQUIRED"]),
        "BACKUP_UNSUPPORTED_PENALTY": cfg.get("BACKUP_UNSUPPORTED_PENALTY", DEFAULT_SETTINGS["BACKUP_UNSUPPORTED_PENALTY"]),
        "POSTGRES_BACKUP_MODE": cfg.get("POSTGRES_BACKUP_MODE", DEFAULT_SETTINGS["POSTGRES_BACKUP_MODE"]),
        "SYSADMIN_MAX_COUNT": cfg.get("SYSADMIN_MAX_COUNT", DEFAULT_SETTINGS["SYSADMIN_MAX_COUNT"]),
        "LONG_QUERY_SEC": cfg.get("LONG_QUERY_SEC", DEFAULT_SETTINGS["LONG_QUERY_SEC"]),
        "LARGE_QUERY_LOGICAL_READS": cfg.get("LARGE_QUERY_LOGICAL_READS", DEFAULT_SETTINGS["LARGE_QUERY_LOGICAL_READS"]),
        "QUERY_ANALYSIS_TOP_N": cfg.get("QUERY_ANALYSIS_TOP_N", DEFAULT_SETTINGS["QUERY_ANALYSIS_TOP_N"]),
        "QUERY_MIN_CALLS": cfg.get("QUERY_MIN_CALLS", DEFAULT_SETTINGS["QUERY_MIN_CALLS"]),
        "QUERY_AVG_SEC": cfg.get("QUERY_AVG_SEC", DEFAULT_SETTINGS["QUERY_AVG_SEC"]),
        "QUERY_TOTAL_SEC": cfg.get("QUERY_TOTAL_SEC", DEFAULT_SETTINGS["QUERY_TOTAL_SEC"]),
        "POSTGRES_JOB_LOOKBACK_HOURS": cfg.get("POSTGRES_JOB_LOOKBACK_HOURS", DEFAULT_SETTINGS["POSTGRES_JOB_LOOKBACK_HOURS"]),
        "POSTGRES_JOB_CONNECT_TIMEOUT_SEC": cfg.get("POSTGRES_JOB_CONNECT_TIMEOUT_SEC", DEFAULT_SETTINGS["POSTGRES_JOB_CONNECT_TIMEOUT_SEC"]),
        "POSTGRES_JOB_DB": cfg.get("POSTGRES_JOB_DB", DEFAULT_SETTINGS["POSTGRES_JOB_DB"]),
        "AUTOGROWTH_MIN_FREE_PCT": cfg.get("AUTOGROWTH_MIN_FREE_PCT", DEFAULT_SETTINGS["AUTOGROWTH_MIN_FREE_PCT"]),
        "SQL_AGENT_PENALTY": cfg.get("SQL_AGENT_PENALTY", DEFAULT_SETTINGS["SQL_AGENT_PENALTY"]),
        "OFFLINE_DB_PENALTY": cfg.get("OFFLINE_DB_PENALTY", DEFAULT_SETTINGS["OFFLINE_DB_PENALTY"]),
        "MISSING_BACKUP_PENALTY": cfg.get("MISSING_BACKUP_PENALTY", DEFAULT_SETTINGS["MISSING_BACKUP_PENALTY"]),
        "DISK_CRIT_PENALTY": cfg.get("DISK_CRIT_PENALTY", DEFAULT_SETTINGS["DISK_CRIT_PENALTY"]),
        "DISK_WARN_PENALTY": cfg.get("DISK_WARN_PENALTY", DEFAULT_SETTINGS["DISK_WARN_PENALTY"]),
        "MEMORY_PRESSURE_PENALTY": cfg.get("MEMORY_PRESSURE_PENALTY", DEFAULT_SETTINGS["MEMORY_PRESSURE_PENALTY"]),
        "BLOCKING_PENALTY": cfg.get("BLOCKING_PENALTY", DEFAULT_SETTINGS["BLOCKING_PENALTY"]),
        "HEAVY_QUERY_PENALTY": cfg.get("HEAVY_QUERY_PENALTY", DEFAULT_SETTINGS["HEAVY_QUERY_PENALTY"]),
        "INDEX_FRAGMENTATION_PENALTY": cfg.get("INDEX_FRAGMENTATION_PENALTY", DEFAULT_SETTINGS["INDEX_FRAGMENTATION_PENALTY"]),
        "PRIVILEGED_ACCOUNT_PENALTY": cfg.get("PRIVILEGED_ACCOUNT_PENALTY", DEFAULT_SETTINGS["PRIVILEGED_ACCOUNT_PENALTY"]),
        "FAILED_LOGIN_PENALTY": cfg.get("FAILED_LOGIN_PENALTY", DEFAULT_SETTINGS["FAILED_LOGIN_PENALTY"]),
        "FAILED_JOB_PENALTY": cfg.get("FAILED_JOB_PENALTY", DEFAULT_SETTINGS["FAILED_JOB_PENALTY"]),
        "AUTO_GROWTH_PENALTY": cfg.get("AUTO_GROWTH_PENALTY", DEFAULT_SETTINGS["AUTO_GROWTH_PENALTY"]),
        "LOG_SPACE_PENALTY": cfg.get("LOG_SPACE_PENALTY", DEFAULT_SETTINGS["LOG_SPACE_PENALTY"]),
        "RAM_SAMPLE_COUNT": cfg.get("RAM_SAMPLE_COUNT", DEFAULT_SETTINGS["RAM_SAMPLE_COUNT"]),
        "RAM_SAMPLE_INTERVAL_SEC": cfg.get("RAM_SAMPLE_INTERVAL_SEC", DEFAULT_SETTINGS["RAM_SAMPLE_INTERVAL_SEC"]),
    }
    data.update(load_db_target_settings(cfg))
    return data


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


def get_live_database_names() -> list[str]:
    adapter = get_db_adapter()
    conn = adapter.connect()
    try:
        cursor = conn.cursor()

        if isinstance(adapter, MSSQLAdapter):
            # Include ALL states (ONLINE, OFFLINE, RESTORING, …) so that offline
            # databases are never dropped from MonitoringConfig when settings are saved.
            cursor.execute("SELECT name FROM sys.databases ORDER BY name")
        elif isinstance(adapter, PostgresAdapter):
            # Include offline databases (datallowconn=false / datconnlimit=0) so they
            # remain in MonitoringConfig and are not silently filtered during health checks.
            cursor.execute(
                """
                SELECT datname
                FROM pg_database
                WHERE NOT datistemplate
                ORDER BY datname
                """
            )
        else:
            raise ValueError(f"Desteklenmeyen adapter: {type(adapter).__name__}")

        return [str(row[0]) for row in cursor.fetchall() if row and row[0] is not None]
    finally:
        try:
            conn.close()
        except Exception:
            pass


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
        "INDEX_FRAGMENTATION_PCT": float,
        "FAILED_LOGIN_ALERT": int,
        "FAILED_LOGIN_WINDOW_HOURS": int,
        "BACKUP_MAX_AGE_HOURS": int,
        "BACKUP_CHECK_REQUIRED": int,
        "BACKUP_UNSUPPORTED_PENALTY": int,
        "POSTGRES_BACKUP_MODE": str,
        "SYSADMIN_MAX_COUNT": int,
        "LONG_QUERY_SEC": float,
        "LARGE_QUERY_LOGICAL_READS": int,
        "QUERY_ANALYSIS_TOP_N": int,
        "QUERY_MIN_CALLS": int,
        "QUERY_AVG_SEC": float,
        "QUERY_TOTAL_SEC": float,
        "POSTGRES_JOB_LOOKBACK_HOURS": int,
        "POSTGRES_JOB_CONNECT_TIMEOUT_SEC": int,
        "POSTGRES_JOB_DB": str,
        "AUTOGROWTH_MIN_FREE_PCT": float,
        "SQL_AGENT_PENALTY": int,
        "OFFLINE_DB_PENALTY": int,
        "MISSING_BACKUP_PENALTY": int,
        "DISK_CRIT_PENALTY": int,
        "DISK_WARN_PENALTY": int,
        "MEMORY_PRESSURE_PENALTY": int,
        "BLOCKING_PENALTY": int,
        "HEAVY_QUERY_PENALTY": int,
        "INDEX_FRAGMENTATION_PENALTY": int,
        "PRIVILEGED_ACCOUNT_PENALTY": int,
        "FAILED_LOGIN_PENALTY": int,
        "FAILED_JOB_PENALTY": int,
        "AUTO_GROWTH_PENALTY": int,
        "LOG_SPACE_PENALTY": int,
        "RAM_SAMPLE_COUNT": int,
        "RAM_SAMPLE_INTERVAL_SEC": float,
        "ACTIVE_DB_ENGINE": str,
        "MSSQL_DB_SERVER": str,
        "MSSQL_DB_PORT": str,
        "MSSQL_DB_NAME": str,
        "MSSQL_DB_USER": str,
        "MSSQL_DB_PASSWORD": str,
        "MSSQL_DB_DRIVER": str,
        "POSTGRES_DB_SERVER": str,
        "POSTGRES_DB_PORT": str,
        "POSTGRES_DB_NAME": str,
        "POSTGRES_DB_USER": str,
        "POSTGRES_DB_PASSWORD": str,
        "POSTGRES_DOCKER_CONTAINER": str,
        "PG_DUMP_BIN": str,
        "PG_DUMP": str,
    }

    alias_map = {
        "TELEGRAM_ALERT_THRESHOLD": "TELEGRAM_THRESHOLD",
        "PG_DUMP": "PG_DUMP_BIN",
    }

    db_target_keys = set(DB_TARGET_PROFILE_KEYS)

    if request.method == "GET":
        if request.args.get("defaults"):
            return jsonify(DEFAULT_SETTINGS)
        return jsonify(load_settings())

    data = request.get_json(silent=True) or {}
    updated = {}
    pending_db_target: dict[str, str] = {}
    for raw_key, raw_val in data.items():
        key = alias_map.get(raw_key, raw_key)
        if key not in allowed_keys:
            continue
        caster = allowed_keys[key]
        try:
            val = caster(raw_val)
        except Exception:
            return jsonify({"error": f"{key} geçersiz değer"}), 400

        val_str = str(val)
        if key in db_target_keys:
            pending_db_target[key] = val_str
            continue

        persist_setting(key, val_str)
        if key == "TELEGRAM_THRESHOLD":
            persist_setting("TELEGRAM_ALERT_THRESHOLD", val_str)
        updated[key] = val_str

    if pending_db_target:
        try:
            preview_cfg = dotenv_values(ENV_PATH)
            preview_cfg.update(pending_db_target)

            target_preview = load_db_target_settings(preview_cfg)
            active_updates = build_active_db_target_updates(target_preview)
        except ValueError as e:
            return jsonify({"error": str(e)}), 400

        # Persist profile keys explicitly to keep two-way switching stable.
        for key in DB_TARGET_PROFILE_KEYS:
            if key not in target_preview:
                continue
            persist_setting(key, str(target_preview[key]))
            updated[key] = str(target_preview[key])

        for key, value in active_updates.items():
            persist_setting(key, str(value))
            updated[key] = str(value)

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

        placeholders = ", ".join(["?"] * len(all_dbs))
        cursor.execute(
            f"""
            UPDATE MonitoringConfig
            SET is_monitored = 0,
                updated_at = datetime('now')
            WHERE db_name NOT IN ({placeholders})
            """,
            tuple(all_dbs),
        )

        conn.commit()
        conn.close()
        return jsonify({"status": "ok"})

    cursor.execute("SELECT db_name, is_monitored FROM MonitoringConfig")
    rows = cursor.fetchall()
    monitored_map = {row["db_name"]: bool(row["is_monitored"]) for row in rows}

    warning = None
    database_names = []
    system_db_names: set[str] = set()
    try:
        adapter = get_db_adapter()
        system_db_names = {str(name).lower() for name in adapter.get_system_databases()}
        database_names = get_live_database_names()
    except Exception as e:
        warning = f"Canli veritabani listesi alinamadi: {e}"
        database_names = sorted(monitored_map.keys())

    payload = []
    for db_name in database_names:
        payload.append({
            "name": db_name,
            "is_monitored": monitored_map.get(db_name, True),
            "is_system": str(db_name).lower() in system_db_names,
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
