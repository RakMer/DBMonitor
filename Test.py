import sqlite3
import os
import requests
import app
import re
import html
import hashlib
from datetime import datetime
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from db_adapters import get_db_runtime


# .env dosyasından bağlantı bilgilerini yükle
load_dotenv()

# DB_ENGINE degerine gore adapter secilir (varsayilan: mssql).
# Bu sayede baglanti katmani, izleme mantigindan ayristirilmis olur.
try:
    db_adapter, health_strategy = get_db_runtime()
except Exception as e:
    raise ValueError(f"❌ Veritabani adapteri baslatilamadi: {e}") from e

# Geriye donuk uyumluluk icin mevcut degisken adlarini koruyoruz.
DB_ENGINE = health_strategy.engine_name
server = db_adapter.server
database = db_adapter.database
username = db_adapter.username
password = db_adapter.password
conn_str = db_adapter.get_connection_string()

TELEGRAM_THRESHOLD     = int(os.getenv("TELEGRAM_THRESHOLD") or os.getenv("TELEGRAM_ALERT_THRESHOLD") or 70)
DISK_WARN_PCT          = float(os.getenv('DISK_WARN_PCT', 80))
DISK_CRIT_PCT          = float(os.getenv('DISK_CRIT_PCT', 90))
LOG_USED_CRIT_PCT      = float(os.getenv('LOG_USED_CRIT_PCT', 70))
INDEX_FRAGMENTATION_PCT = float(os.getenv('INDEX_FRAGMENTATION_PCT', 30))
INDEX_FRAGMENTATION_MIN_PAGES = int(os.getenv('INDEX_FRAGMENTATION_MIN_PAGES', 1000))
FAILED_LOGIN_ALERT     = int(os.getenv('FAILED_LOGIN_ALERT', 10))
FAILED_LOGIN_WINDOW_HOURS = int(os.getenv('FAILED_LOGIN_WINDOW_HOURS', 24))
BACKUP_MAX_AGE_HOURS   = int(os.getenv('BACKUP_MAX_AGE_HOURS', 24))
SYSADMIN_MAX_COUNT     = int(os.getenv('SYSADMIN_MAX_COUNT', 2))
LONG_QUERY_SEC         = float(os.getenv('LONG_QUERY_SEC', 30))
LARGE_QUERY_LOGICAL_READS = int(os.getenv('LARGE_QUERY_LOGICAL_READS', 1000000))
QUERY_ANALYSIS_TOP_N   = int(os.getenv('QUERY_ANALYSIS_TOP_N', 5))
QUERY_MIN_CALLS        = int(os.getenv('QUERY_MIN_CALLS', 2))
QUERY_AVG_SEC          = float(os.getenv('QUERY_AVG_SEC', LONG_QUERY_SEC))
QUERY_TOTAL_SEC        = float(os.getenv('QUERY_TOTAL_SEC', max(LONG_QUERY_SEC * QUERY_MIN_CALLS, LONG_QUERY_SEC)))
SYSTEM_DATABASES       = {name.lower() for name in db_adapter.get_system_databases()}

QUERY_NOISE_PATTERNS = (
    "dbmonitor_stress_table",
    "from pg_stat_statements",
    "from pg_stat_activity",
    "from pg_stat_database",
    "from pg_settings",
    "from pg_database",
    "from pg_ls_waldir",
    "from cron.job_run_details",
    "from pgagent.pga_joblog",
    "from sys.dm_exec_query_stats",
    "from sys.dm_exec_sql_text",
    "from sys.dm_exec_requests",
    "from sys.dm_db_index_physical_stats",
    "from sys.master_files",
    "from sys.databases",
    "dbcc sqlperf",
    "waitfor delay",
    "pg_sleep(",
)


def parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {'1', 'true', 'yes', 'on'}


CHECK_SYSTEM_DB_BACKUP = parse_bool_env('CHECK_SYSTEM_DB_BACKUP', True)
CHECK_SYSTEM_DB_AUTOGROWTH = parse_bool_env('CHECK_SYSTEM_DB_AUTOGROWTH', True)
CHECK_SYSTEM_DB_INDEX = parse_bool_env('CHECK_SYSTEM_DB_INDEX', False)


def sanitize_sql_text(text: str | None, max_len: int = 140) -> str:
    if not text:
        return ""
    normalized = " ".join(text.split())
    return normalized[:max_len] + ("..." if len(normalized) > max_len else "")


def is_monitor_or_stress_query(sql_text: str | None) -> bool:
    normalized = " ".join(str(sql_text or "").lower().split())
    if not normalized:
        return False
    return any(pattern in normalized for pattern in QUERY_NOISE_PATTERNS)


def get_query_identity(query_row: dict[str, object]) -> str:
    query_id = str(query_row.get("query_id") or "").strip()
    if query_id:
        return query_id

    sql_text = str(query_row.get("query_text") or "")
    normalized = " ".join(sql_text.lower().split())
    if not normalized:
        return "unknown"
    return "fp:" + hashlib.sha1(normalized.encode("utf-8", errors="ignore")).hexdigest()[:12]


def truncate_label(value: str, max_len: int = 42) -> str:
    if len(value) <= max_len:
        return value
    return value[: max_len - 3] + "..."


def build_telegram_penalty_lines(penalties: list[str]) -> str:
    if not penalties:
        return "• Belirtilen ceza yok."

    long_query_items = []
    auto_growth_items = []
    index_fragment_items = []
    other_items = []

    for p in penalties:
        if "Uzun/Büyük Sorgu:" in p:
            long_query_items.append(p)
        elif "Auto Growth:" in p:
            auto_growth_items.append(p)
        elif "is heavily fragmented" in p and "Index [" in p:
            index_fragment_items.append(p)
        else:
            other_items.append(p)

    lines = [f"• {html.escape(item)}" for item in other_items]

    if long_query_items:
        lines.append(f"• [-8] Uzun/Büyük Sorgu: {len(long_query_items)} adet")
        for item in long_query_items[:2]:
            db_match = re.search(r"DB=([^,]+)", item)
            max_match = re.search(r"Max=([0-9.]+)s", item)
            reads_match = re.search(r"AvgReads=([0-9]+)", item)
            sql_match = re.search(r"SQL='(.+)'$", item)

            db_name = html.escape(db_match.group(1) if db_match else "unknown")
            max_sec = max_match.group(1) if max_match else "?"
            avg_reads = reads_match.group(1) if reads_match else "?"
            sql_short_raw = sanitize_sql_text(sql_match.group(1), 70) if sql_match else "SQL bilgisi yok"
            sql_short = html.escape(sql_short_raw)
            lines.append(f"  - DB={db_name} | Max={max_sec}s | AvgReads={avg_reads} | SQL='{sql_short}'")

        if len(long_query_items) > 2:
            lines.append(f"  - +{len(long_query_items) - 2} adet daha")

    if auto_growth_items:
        lines.append(f"• [-10] Auto Growth: {len(auto_growth_items)} dosya")
        for item in auto_growth_items[:2]:
            ag_match = re.search(r"Auto Growth: (.+?) veritabanının '(.+?)' dosyası", item)
            if ag_match:
                db_name = html.escape(ag_match.group(1))
                file_name = html.escape(ag_match.group(2))
                lines.append(f"  - {db_name}.{file_name}")
            else:
                lines.append(f"  - {html.escape(sanitize_sql_text(item, 90))}")

        if len(auto_growth_items) > 2:
            lines.append(f"  - +{len(auto_growth_items) - 2} dosya daha")

    if index_fragment_items:
        lines.append(f"• [-10] Index Fragmentation: {len(index_fragment_items)} adet")
        for item in index_fragment_items[:2]:
            match = re.search(r"Index \[(.+?)\] on table \[(.+?)\] is heavily fragmented \(([0-9.]+)%\)", item)
            if match:
                index_name = html.escape(truncate_label(match.group(1), 38))
                table_name = html.escape(truncate_label(match.group(2), 42))
                frag_pct = match.group(3)
                lines.append(f"  - {table_name} | {index_name} | %{frag_pct}")
            else:
                lines.append(f"  - {html.escape(sanitize_sql_text(item, 90))}")

        if len(index_fragment_items) > 2:
            lines.append(f"  - +{len(index_fragment_items) - 2} adet daha")

    max_lines = 14
    if len(lines) > max_lines:
        hidden = len(lines) - max_lines
        lines = lines[:max_lines]
        lines.append(f"• ... ve {hidden} satır daha")

    return "\n".join(lines)

# --- TELEGRAM BİLDİRİM FONKSİYONU ---
def send_telegram_alert(score, penalties):
    token    = os.getenv('TELEGRAM_TOKEN')
    chat_ids = os.getenv('TELEGRAM_CHAT_IDS', '')
    if not token or not chat_ids:
        print("⚠️  Telegram bilgileri .env dosyasında eksik, bildirim atlandı.")
        return

    recipients = [cid.strip() for cid in chat_ids.split(',') if cid.strip()]

    penalty_lines = build_telegram_penalty_lines(penalties)

    if score >= 80:
        status_emoji = "✅"
        status_text  = "Sağlıklı"
    elif score >= 50:
        status_emoji = "⚠️"
        status_text  = "Uyarı"
    else:
        status_emoji = "🚨"
        status_text  = "KRİTİK"

    message = (
        f"{status_emoji} <b>DB Monitor Alarmı</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🖥️  <b>Sunucu:</b> {html.escape(server or 'unknown')}\n"
        f"📊 <b>Sağlık Skoru:</b> <b>{score}/100</b> — {status_text}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Aktif Alarmlar:</b>\n{penalty_lines}"
    )
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for chat_id in recipients:
        try:
            resp = requests.post(url, json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"}, timeout=10)
            if resp.ok:
                print(f"📨 Telegram bildirimi gönderildi! → chat_id: {chat_id} (Skor: {score})")
            else:
                print(f"⚠️  Telegram gönderimi başarısız (chat_id: {chat_id}): {resp.text}")
        except Exception as e:
            print(f"⚠️  Telegram hatası (chat_id: {chat_id}): {e}")

# --- SQLITE VERİTABANI KURULUMU ---
def init_sqlite_db():
    conn = sqlite3.connect('dbmonitor.sqlite3')
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS HealthHistory (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            check_date TEXT,
            score INTEGER
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS PenaltyLog (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            history_id INTEGER,
            penalty_desc TEXT,
            FOREIGN KEY(history_id) REFERENCES HealthHistory(id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS MonitoringConfig (
            db_name TEXT PRIMARY KEY,
            is_monitored INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT
        )
    ''')

    cursor.execute('''
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
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS DatabaseResourceSnapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_time TEXT NOT NULL,
            db_name TEXT NOT NULL,
            read_bytes_total INTEGER,
            write_bytes_total INTEGER,
            io_stall_ms_total INTEGER,
            UNIQUE(snapshot_time, db_name)
        )
    ''')

    cursor.execute('''
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
    ''')

    cursor.execute('''
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
    ''')
    
    conn.commit()
    conn.close()


def load_monitored_databases():
    conn = sqlite3.connect('dbmonitor.sqlite3')
    cursor = conn.cursor()
    cursor.execute("SELECT db_name, is_monitored FROM MonitoringConfig")
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        return None

    return {str(name).lower() for name, is_monitored in rows if int(is_monitored) == 1}


def is_database_monitored(db_name, monitored_db_set):
    if monitored_db_set is None:
        return True
    if not db_name:
        return False
    return str(db_name).lower() in monitored_db_set


def classify_wait_type(wait_type: str | None) -> str:
    if not wait_type:
        return "Other"
    wt = wait_type.upper()
    if wt.startswith("LCK_"):
        return "Lock"
    if wt.startswith("PAGEIOLATCH") or wt.startswith("IO_") or wt in {"WRITELOG", "ASYNC_IO_COMPLETION"}:
        return "Disk I/O"
    if wt.startswith("NETWORK") or wt in {"ASYNC_NETWORK_IO", "TRACEWRITE"}:
        return "Network"
    if wt.startswith("CX") or wt.startswith("SOS_SCHEDULER") or wt.startswith("THREADPOOL"):
        return "CPU/Scheduler"
    if wt.startswith("RESOURCE_SEMAPHORE") or wt.startswith("MEMORY_"):
        return "Memory"
    return "Other"


def collect_wait_metrics(cursor):
    snapshot_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ignore_waits = {
        'BROKER_EVENTHANDLER', 'BROKER_RECEIVE_WAITFOR', 'BROKER_TASK_STOP',
        'BROKER_TO_FLUSH', 'BROKER_TRANSMITTER', 'CHECKPOINT_QUEUE',
        'CHKPT', 'CLR_AUTO_EVENT', 'CLR_MANUAL_EVENT', 'CLR_SEMAPHORE',
        'DBMIRROR_DBM_EVENT', 'DBMIRROR_EVENTS_QUEUE', 'DBMIRROR_WORKER_QUEUE',
        'DBMIRRORING_CMD', 'DIRTY_PAGE_POLL', 'DISPATCHER_QUEUE_SEMAPHORE',
        'EXECSYNC', 'FSAGENT', 'FT_IFTS_SCHEDULER_IDLE_WAIT', 'FT_IFTSHC_MUTEX',
        'HADR_CLUSAPI_CALL', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
        'HADR_LOGCAPTURE_WAIT', 'HADR_NOTIFICATION_DEQUEUE', 'HADR_TIMER_TASK',
        'HADR_WORK_QUEUE', 'KSOURCE_WAKEUP', 'LAZYWRITER_SLEEP',
        'LOGMGR_QUEUE', 'MEMORY_ALLOCATION_EXT', 'ONDEMAND_TASK_QUEUE',
        'PARALLEL_REDO_DRAIN_WORKER', 'PARALLEL_REDO_LOG_CACHE',
        'PARALLEL_REDO_TRAN_LIST', 'PARALLEL_REDO_WORKER_SYNC',
        'PARALLEL_REDO_WORKER_WAIT_WORK', 'PREEMPTIVE_OS_FLUSHFILEBUFFERS',
        'PREEMPTIVE_XE_GETTARGETSTATE', 'PWAIT_ALL_COMPONENTS_INITIALIZED',
        'PWAIT_DIRECTLOGCONSUMER_GETNEXT', 'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
        'QDS_ASYNC_QUEUE', 'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
        'QDS_SHUTDOWN_QUEUE', 'REDO_THREAD_PENDING_WORK', 'REQUEST_FOR_DEADLOCK_SEARCH',
        'RESOURCE_QUEUE', 'SERVER_IDLE_CHECK', 'SLEEP_BPOOL_FLUSH', 'SLEEP_DBSTARTUP',
        'SLEEP_DCOMSTARTUP', 'SLEEP_MASTERDBREADY', 'SLEEP_MASTERMDREADY',
        'SLEEP_MASTERUPGRADED', 'SLEEP_MSDBSTARTUP', 'SLEEP_SYSTEMTASK',
        'SLEEP_TASK', 'SLEEP_TEMPDBSTARTUP', 'SNI_HTTP_ACCEPT', 'SP_SERVER_DIAGNOSTICS_SLEEP',
        'SQLTRACE_BUFFER_FLUSH', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 'SQLTRACE_WAIT_ENTRIES',
        'WAIT_FOR_RESULTS', 'WAITFOR', 'WAITFOR_TASKSHUTDOWN', 'WAIT_XTP_RECOVERY',
        'WAIT_XTP_HOST_WAIT', 'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', 'WAIT_XTP_CKPT_CLOSE',
        'XE_DISPATCHER_JOIN', 'XE_DISPATCHER_WAIT', 'XE_TIMER_EVENT'
    }

    cumulative_waits = []
    try:
        cursor.execute("""
            SELECT wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms
            FROM sys.dm_os_wait_stats
            WHERE wait_type NOT IN ({})
        """.format(
            ",".join(["?"] * len(ignore_waits))
        ), tuple(sorted(ignore_waits)))

        for wt, task_cnt, wait_ms, signal_ms in cursor.fetchall():
            cumulative_waits.append(
                {
                    "wait_type": wt,
                    "waiting_tasks_count_total": int(task_cnt or 0),
                    "wait_time_ms_total": int(wait_ms or 0),
                    "signal_wait_ms_total": int(signal_ms or 0),
                    "category": classify_wait_type(wt),
                }
            )
    except Exception:
        cumulative_waits = []

    active_waits = []
    try:
        cursor.execute("""
            SELECT
                r.session_id,
                DB_NAME(r.database_id) AS db_name,
                r.wait_type,
                r.wait_time,
                r.blocking_session_id
            FROM sys.dm_exec_requests r
            WHERE r.session_id <> @@SPID
              AND r.wait_type IS NOT NULL
              AND r.wait_time > 0
        """)
        for sid, db_name, wait_type, wait_ms, blocking_sid in cursor.fetchall():
            active_waits.append(
                {
                    "session_id": int(sid or 0),
                    "db_name": db_name or "unknown",
                    "wait_type": wait_type or "UNKNOWN",
                    "wait_time_ms": int(wait_ms or 0),
                    "blocking_session_id": int(blocking_sid or 0),
                    "category": classify_wait_type(wait_type),
                }
            )
    except Exception:
        active_waits = []

    return {
        "snapshot_time": snapshot_time,
        "cumulative_waits": cumulative_waits,
        "active_waits": active_waits,
    }


def save_wait_metrics(wait_snapshot):
    conn = sqlite3.connect('dbmonitor.sqlite3')
    cursor = conn.cursor()

    for row in wait_snapshot.get("cumulative_waits", []):
        cursor.execute(
            """
            INSERT INTO WaitSnapshots (
                snapshot_time, wait_type, wait_time_ms_total, signal_wait_ms_total,
                waiting_tasks_count_total, category
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_time, wait_type) DO UPDATE SET
                wait_time_ms_total = excluded.wait_time_ms_total,
                signal_wait_ms_total = excluded.signal_wait_ms_total,
                waiting_tasks_count_total = excluded.waiting_tasks_count_total,
                category = excluded.category
            """,
            (
                wait_snapshot["snapshot_time"],
                row["wait_type"],
                row["wait_time_ms_total"],
                row["signal_wait_ms_total"],
                row["waiting_tasks_count_total"],
                row["category"],
            ),
        )

    cursor.execute("DELETE FROM ActiveWaitSnapshots WHERE snapshot_time = ?", (wait_snapshot["snapshot_time"],))
    for row in wait_snapshot.get("active_waits", []):
        cursor.execute(
            """
            INSERT INTO ActiveWaitSnapshots (
                snapshot_time, session_id, db_name, wait_type, wait_time_ms, blocking_session_id, category
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                wait_snapshot["snapshot_time"],
                row["session_id"],
                row["db_name"],
                row["wait_type"],
                row["wait_time_ms"],
                row["blocking_session_id"],
                row["category"],
            ),
        )

    conn.commit()
    conn.close()


def collect_resource_metrics(cursor, monitored_databases):
    snapshot_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cpu_pct = 0.0
    try:
        cursor.execute("""
            SELECT TOP 1 CONVERT(XML, record) AS rec
            FROM sys.dm_os_ring_buffers
            WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR'
              AND record LIKE '%<SystemHealth>%'
            ORDER BY [timestamp] DESC
        """)
        row = cursor.fetchone()
        if row and row[0]:
            root = ET.fromstring(str(row[0]))
            sql_cpu = root.find('.//SystemHealth/ProcessUtilization')
            idle_cpu = root.find('.//SystemHealth/SystemIdle')
            if sql_cpu is not None and idle_cpu is not None:
                cpu_pct = float(sql_cpu.text or 0)
            elif idle_cpu is not None:
                cpu_pct = max(0.0, 100.0 - float(idle_cpu.text or 0))
    except Exception:
        cpu_pct = 0.0

    ram_used_pct = 0.0
    sql_mem_used_mb = 0.0
    try:
        cursor.execute("SELECT total_physical_memory_kb, available_physical_memory_kb FROM sys.dm_os_sys_memory")
        mem_row = cursor.fetchone()
        if mem_row and mem_row[0]:
            total_kb = float(mem_row[0] or 0)
            avail_kb = float(mem_row[1] or 0)
            if total_kb > 0:
                ram_used_pct = ((total_kb - avail_kb) / total_kb) * 100.0
    except Exception:
        ram_used_pct = 0.0

    try:
        cursor.execute("SELECT physical_memory_in_use_kb FROM sys.dm_os_process_memory")
        proc_mem = cursor.fetchone()
        sql_mem_used_mb = float(proc_mem[0] or 0) / 1024.0 if proc_mem else 0.0
    except Exception:
        sql_mem_used_mb = 0.0

    disk_read_total = 0
    disk_write_total = 0
    try:
        cursor.execute("SELECT SUM(num_of_bytes_read), SUM(num_of_bytes_written) FROM sys.dm_io_virtual_file_stats(NULL, NULL)")
        io_row = cursor.fetchone()
        disk_read_total = int(io_row[0] or 0) if io_row else 0
        disk_write_total = int(io_row[1] or 0) if io_row else 0
    except Exception:
        disk_read_total = 0
        disk_write_total = 0

    net_sent_per_sec = 0.0
    net_recv_per_sec = 0.0
    try:
        cursor.execute("""
            SELECT counter_name, cntr_value
            FROM sys.dm_os_performance_counters
            WHERE counter_name IN ('Bytes Sent to Transport/sec', 'Bytes Received from Transport/sec')
        """)
        for c_name, c_value in cursor.fetchall():
            val = float(c_value or 0)
            if c_name == 'Bytes Sent to Transport/sec':
                net_sent_per_sec += val
            elif c_name == 'Bytes Received from Transport/sec':
                net_recv_per_sec += val
    except Exception:
        net_sent_per_sec = 0.0
        net_recv_per_sec = 0.0

    db_snapshots = []
    try:
        cursor.execute("""
            SELECT
                DB_NAME(vfs.database_id) AS db_name,
                SUM(vfs.num_of_bytes_read) AS read_bytes_total,
                SUM(vfs.num_of_bytes_written) AS write_bytes_total,
                SUM(vfs.io_stall_read_ms + vfs.io_stall_write_ms) AS io_stall_ms_total
            FROM sys.dm_io_virtual_file_stats(NULL, NULL) vfs
            GROUP BY vfs.database_id
        """)
        for db_name, read_total, write_total, stall_total in cursor.fetchall():
            if not is_database_monitored(db_name, monitored_databases):
                continue
            db_snapshots.append(
                {
                    "db_name": db_name,
                    "read_bytes_total": int(read_total or 0),
                    "write_bytes_total": int(write_total or 0),
                    "io_stall_ms_total": int(stall_total or 0),
                }
            )
    except Exception:
        db_snapshots = []

    return {
        "snapshot_time": snapshot_time,
        "cpu_pct": round(cpu_pct, 2),
        "ram_used_pct": round(ram_used_pct, 2),
        "sql_mem_used_mb": round(sql_mem_used_mb, 2),
        "disk_read_bytes_total": disk_read_total,
        "disk_write_bytes_total": disk_write_total,
        "net_sent_bytes_per_sec": round(net_sent_per_sec, 2),
        "net_recv_bytes_per_sec": round(net_recv_per_sec, 2),
        "db_snapshots": db_snapshots,
    }


def check_index_fragmentation(cursor, monitored_databases, strategy):
    penalties = []

    try:
        frag_rows = strategy.get_index_fragmentation(
            cursor,
            INDEX_FRAGMENTATION_MIN_PAGES,
            INDEX_FRAGMENTATION_PCT,
        )
    except Exception as e:
        print(f"⚠️ INDEX FRAGMENTATION: Fragmentation verisi alinamadi: {e}")
        return penalties

    for row in frag_rows:
        db_name = str(row.get("db_name") or "")
        if not db_name:
            continue

        if not is_database_monitored(db_name, monitored_databases):
            continue

        if (not CHECK_SYSTEM_DB_INDEX) and db_name.lower() in SYSTEM_DATABASES:
            continue

        try:
            table_name = str(row.get("table_name") or "unknown_table")
            index_name = str(row.get("index_name") or "unknown_index")
            frag_value = float(row.get("fragmentation_pct") or 0)
            penalties.append(
                {
                    "score": -10,
                    "desc": f"🚨 Index [{index_name}] on table [{db_name}.{table_name}] is heavily fragmented ({frag_value:.1f}%).",
                }
            )
        except Exception:
            continue

    return penalties


def save_resource_metrics(snapshot):
    conn = sqlite3.connect('dbmonitor.sqlite3')
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO ResourceSnapshots (
            snapshot_time, cpu_pct, ram_used_pct, sql_mem_used_mb,
            disk_read_bytes_total, disk_write_bytes_total,
            net_sent_bytes_per_sec, net_recv_bytes_per_sec
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot["snapshot_time"],
            snapshot["cpu_pct"],
            snapshot["ram_used_pct"],
            snapshot["sql_mem_used_mb"],
            snapshot["disk_read_bytes_total"],
            snapshot["disk_write_bytes_total"],
            snapshot["net_sent_bytes_per_sec"],
            snapshot["net_recv_bytes_per_sec"],
        ),
    )

    for db_row in snapshot.get("db_snapshots", []):
        cursor.execute(
            """
            INSERT INTO DatabaseResourceSnapshots (
                snapshot_time, db_name, read_bytes_total, write_bytes_total, io_stall_ms_total
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(snapshot_time, db_name) DO UPDATE SET
                read_bytes_total = excluded.read_bytes_total,
                write_bytes_total = excluded.write_bytes_total,
                io_stall_ms_total = excluded.io_stall_ms_total
            """,
            (
                snapshot["snapshot_time"],
                db_row["db_name"],
                db_row["read_bytes_total"],
                db_row["write_bytes_total"],
                db_row["io_stall_ms_total"],
            ),
        )

    conn.commit()
    conn.close()

# --- VERİLERİ SQLITE'A KAYDETME FONKSİYONU ---
def save_to_sqlite(score, penalties):
    conn = sqlite3.connect('dbmonitor.sqlite3')
    cursor = conn.cursor()
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    cursor.execute("INSERT INTO HealthHistory (check_date, score) VALUES (?, ?)", (now, score))
    history_id = cursor.lastrowid
    
    for penalty in penalties:
        cursor.execute("INSERT INTO PenaltyLog (history_id, penalty_desc) VALUES (?, ?)", (history_id, penalty))
        
    conn.commit()
    conn.close()
    print(f"\n💾 Geçmiş Kaydedildi! SQLite -> Tarih: {now} | Skor: {score}")

# --- ANA KONTROL FONKSİYONU ---
def run_health_check_with_score():
    health_score = 100
    penalties = []
    
    init_sqlite_db()
    monitored_databases = load_monitored_databases()

    if monitored_databases is None:
        print("ℹ️ DB filtreleme: Kayitli secim yok, tum veritabanlari izlenecek.")
    elif not monitored_databases:
        print("⚠️ DB filtreleme: Hic veritabani secilmemis, per-db kontroller ceza uretmeyecek.")
    else:
        print(f"ℹ️ DB filtreleme: {len(monitored_databases)} veritabani secili.")
    
    try:
        try:
            conn = db_adapter.connect()
        except Exception as conn_err:
            raise ConnectionError(f"{DB_ENGINE} baglantisi kurulamadi: {conn_err}") from conn_err

        conn.autocommit = True 
        cursor = conn.cursor()
        print("🔍 Sistem Analizi Başlıyor...\n" + "="*50)

        resource_snapshot = collect_resource_metrics(cursor, monitored_databases)
        save_resource_metrics(resource_snapshot)
        wait_snapshot = collect_wait_metrics(cursor)
        save_wait_metrics(wait_snapshot)
        print(
            f"ℹ️ RESOURCE SNAPSHOT: CPU %{resource_snapshot['cpu_pct']:.1f} | "
            f"RAM %{resource_snapshot['ram_used_pct']:.1f} | "
            f"SQL Mem {resource_snapshot['sql_mem_used_mb']:.0f} MB"
        )
        print(
            f"ℹ️ WAIT SNAPSHOT: {len(wait_snapshot['cumulative_waits'])} wait tipi, "
            f"{len(wait_snapshot['active_waits'])} aktif bekleme"
        )
        
        # 1. SQL Agent Durumu (engine strategy)
        try:
            agent_status = health_strategy.get_agent_status(cursor)
        except Exception as e:
            agent_status = None
            print(f"⚠️ AGENT: Durum sorgulanamadi: {e}")

        if agent_status is None:
            print("ℹ️ AGENT: Bu veritabani motorunda servis/agent kontrolu uygulanmiyor.")
        elif str(agent_status).lower() != "running":
            health_score -= 30
            penalties.append("[-30] SQL Agent Çalışmıyor!")
            print(f"🔴 SQL Agent Durumu: {agent_status}")
        else:
            print(f"🟢 SQL Agent Durumu: {agent_status}")

        # 2. Veritabanı Durumları
        try:
            offline_dbs = health_strategy.get_offline_databases(cursor)
        except Exception as e:
            offline_dbs = []
            print(f"⚠️ OFFLINE DB: Durumlar alinamadi: {e}")

        # Offline DB durumu altyapi riski oldugu icin MonitoringConfig filtresinden bagimsiz izlenir.
        filtered_offline_dbs = [db for db in offline_dbs if is_database_monitored(db[0], monitored_databases)]
        effective_offline_dbs = offline_dbs

        if not effective_offline_dbs:
            print("🟢 Tüm Veritabanları ONLINE durumda.")
        else:
            for db in effective_offline_dbs:
                health_score -= 20
                penalties.append(f"[-20] {db[0]} veritabanı {db[1]} durumunda!")
                print(f"🔴 Sorunlu Veritabanı: {db[0]} ({db[1]})")

            ignored_by_monitoring = max(0, len(effective_offline_dbs) - len(filtered_offline_dbs))
            if monitored_databases is not None and ignored_by_monitoring > 0:
                print(
                    f"ℹ️ OFFLINE DB: {ignored_by_monitoring} DB MonitoringConfig disinda olsa da offline oldugu icin skora dahil edildi."
                )

        # 3. Yedekleme Kontrolü
        backup_excluded_dbs = {'tempdb'}
        if not CHECK_SYSTEM_DB_BACKUP:
            backup_excluded_dbs.update(SYSTEM_DATABASES)

        try:
            missing_backups = health_strategy.get_missing_backups(
                cursor,
                backup_excluded_dbs,
                BACKUP_MAX_AGE_HOURS,
            )
        except Exception as e:
            missing_backups = None
            print(f"⚠️ BACKUP: Yedek bilgisi alinamadi: {e}")

        if missing_backups is None:
            print("ℹ️ BACKUP: Bu motor icin standart yedek kontrolu uygulanmadi.")
        else:
            filtered_missing_backups = [db for db in missing_backups if is_database_monitored(db, monitored_databases)]

            if not filtered_missing_backups:
                print("🟢 Tüm veritabanlarının güncel yedeği var.")
            else:
                health_score -= 50
                penalties.append(f"[-50] Son {BACKUP_MAX_AGE_HOURS} saatte yedeği alınmayan veritabanları var!")
                print(f"🔴 Yedeği Olmayan DB Sayısı: {len(filtered_missing_backups)}")
                print(f"ℹ️ BACKUP: Yedeksiz DB'ler: {', '.join(sorted(filtered_missing_backups))}")

        # 4. Disk Doluluk Oranı
        try:
            disks = health_strategy.get_disk_usage(cursor)
        except Exception as e:
            disks = None
            print(f"⚠️ DISK: Disk metrikleri alinamadi: {e}")

        if disks is None:
            print("ℹ️ DISK: Bu motor icin disk doluluk sorgusu atlandi.")
        else:
            for disk in disks:
                drive_letter = str(disk.get("drive") or "UnknownMount")
                free_pct = disk.get("free_pct")
                if free_pct is None:
                    print(f"ℹ️ Disk metrik atlandı: {drive_letter} için free space bilgisi yok.")
                    continue

                used_pct = 100 - float(free_pct)

                if used_pct >= DISK_CRIT_PCT:
                    health_score -= 40
                    penalties.append(f"[-40] {drive_letter} diski kritik seviyede dolu! (%{used_pct:.2f})")
                    print(f"🔴 DİSK KRİTİK: {drive_letter} %{used_pct:.2f} Dolu!")
                elif used_pct >= DISK_WARN_PCT:
                    health_score -= 10
                    penalties.append(f"[-10] {drive_letter} diski dolmaya yaklaşıyor! (%{used_pct:.2f})")
                else:
                    print(f"🟢 Disk Durumu OK: {drive_letter} %{used_pct:.2f} Dolu")

        # 5. Memory Pressure Kontrolü
        try:
            memory_low = health_strategy.get_memory_pressure(cursor)
        except Exception as e:
            memory_low = None
            print(f"⚠️ MEMORY: Bellek baskisi sorgulanamadi: {e}")

        if memory_low is None:
            print("ℹ️ MEMORY: Bu motor icin bellek baskisi kontrolu uygulanmadi.")
        elif memory_low:
            health_score -= 20
            penalties.append("[-20] Sunucuda RAM darboğazı (Memory Pressure) var!")
            print("🔴 MEMORY: SQL Server bellek sıkıntısı çekiyor!")
        else:
            print("🟢 MEMORY: RAM durumu stabil.")

        # 6. Blocking Sorgular
        try:
            blocks = health_strategy.get_active_blocks(cursor)
        except Exception as e:
            blocks = []
            print(f"⚠️ BLOCKING: Kilit analizi alinamadi: {e}")

        if not blocks:
            print("🟢 BLOCKING: Sistemde birbirini kilitleyen sorgu yok.")
        else:
            print(f"🔴 BLOCKING: {len(blocks)} adet kilitlenen işlem var!")
            for block in blocks:
                session_id = int(block.get("session_id") or 0)
                blocking_session_id = int(block.get("blocking_session_id") or 0)
                wait_seconds = float(block.get("wait_seconds") or 0)
                health_score -= 10
                penalties.append(
                    f"[-10] Session {session_id}, Session {blocking_session_id} tarafından {wait_seconds:.1f} saniyedir bloklanıyor!"
                )

        # 6.1 Uzun Süren ve Büyük Sorgular (Query Stats)
        top_n = max(1, min(QUERY_ANALYSIS_TOP_N, 20))
        try:
            heavy_queries = health_strategy.get_heavy_queries(cursor, top_n)

            matched_queries = []
            filtered_noise_count = 0
            for q in heavy_queries:
                q_max_sec = float(q.get("max_elapsed_sec") or 0)
                q_avg_sec = float(q.get("avg_elapsed_sec") or 0)
                q_total_sec = float(q.get("total_elapsed_sec") or 0)
                q_avg_reads = float(q.get("avg_logical_reads") or 0)
                q_exec_count = int(q.get("execution_count") or 0)
                q_db_name = str(q.get("db_name") or "unknown")
                q_text = str(q.get("query_text") or "")

                if is_monitor_or_stress_query(q_text):
                    filtered_noise_count += 1
                    continue

                if not is_database_monitored(q_db_name, monitored_databases):
                    continue

                if q_avg_sec <= 0 and q_total_sec > 0 and q_exec_count > 0:
                    q_avg_sec = q_total_sec / q_exec_count

                has_enough_calls = q_exec_count >= QUERY_MIN_CALLS
                is_slow_query = has_enough_calls and q_avg_sec >= QUERY_AVG_SEC and q_total_sec >= QUERY_TOTAL_SEC
                is_large_query = has_enough_calls and q_avg_reads >= LARGE_QUERY_LOGICAL_READS

                if is_slow_query or is_large_query:
                    q["avg_elapsed_sec"] = q_avg_sec
                    q["total_elapsed_sec"] = q_total_sec
                    matched_queries.append(q)

            if filtered_noise_count > 0:
                print(f"ℹ️ QUERY STATS: {filtered_noise_count} adet izleme/stress kaynakli sorgu filtrelendi.")

            if matched_queries:
                print(f"🔴 QUERY STATS: {len(matched_queries)} adet uzun/büyük sorgu tespit edildi.")
                for q in matched_queries:
                    q_db_name = str(q.get("db_name") or "unknown")
                    q_identity = get_query_identity(q)
                    q_max_sec = float(q.get("max_elapsed_sec") or 0)
                    q_avg_sec = float(q.get("avg_elapsed_sec") or 0)
                    q_total_sec = float(q.get("total_elapsed_sec") or 0)
                    q_avg_reads = int(float(q.get("avg_logical_reads") or 0))
                    q_exec_count = int(q.get("execution_count") or 0)
                    q_snippet = sanitize_sql_text(str(q.get("query_text") or ""))

                    health_score -= 8
                    penalties.append(
                        f"[-8] Uzun/Büyük Sorgu: DB={q_db_name}, QID={q_identity}, Max={q_max_sec:.1f}s, AvgSec={q_avg_sec:.1f}s, TotalSec={q_total_sec:.1f}s, AvgReads={q_avg_reads}, Exec={q_exec_count}, SQL='{q_snippet}'"
                    )
            else:
                print("🟢 QUERY STATS: Uzun süre çalışan veya büyük sorgu bulunamadı.")
        except Exception as e:
            print(f"⚠️ QUERY STATS: Sorgu analizi atlandı (yetki/erişim sorunu olabilir): {e}")

        # 6.2 Index Fragmentation Kontrolu
        index_penalties = check_index_fragmentation(cursor, monitored_databases, health_strategy)
        if not index_penalties:
            print("🟢 INDEX FRAGMENTATION: Kritik seviyede parçalanmış index bulunamadı.")
        else:
            print(f"🔴 INDEX FRAGMENTATION: {len(index_penalties)} adet yüksek parçalanmış index tespit edildi.")
            for idx_penalty in index_penalties:
                score_delta = int(idx_penalty.get("score", -10))
                health_score += score_delta
                penalties.append(f"[{score_delta}] {idx_penalty.get('desc', 'Index fragmentation sorunu')}")

        # 7. Güvenlik ve Denetim Kontrolü
        try:
            privileged_accounts = health_strategy.get_privileged_accounts(cursor)
        except Exception as e:
            privileged_accounts = None
            print(f"⚠️ SECURITY: Yetkili hesap listesi alinamadi: {e}")

        if privileged_accounts is None:
            print("ℹ️ SECURITY: Yetkili hesap sayisi kontrolu bu motor icin uygulanmadi.")
        elif len(privileged_accounts) > SYSADMIN_MAX_COUNT:
            health_score -= 10
            penalties.append(f"[-10] Güvenlik Riski: Çok fazla yetkili kullanıcı var! ({len(privileged_accounts)} ekstra hesap)")
            print(f"🔴 SECURITY: Çok fazla yetkili hesap! ({len(privileged_accounts)} ekstra hesap)")
        else:
            print("🟢 SECURITY: Yetkili hesap sayısı normal.")

        try:
            failed_login_count = health_strategy.get_failed_login_count(cursor, FAILED_LOGIN_WINDOW_HOURS)
        except Exception as e:
            failed_login_count = None
            print(f"⚠️ SECURITY: Basarisiz giris sayisi alinamadi: {e}")

        if failed_login_count is None:
            print("ℹ️ SECURITY: Basarisiz giris kontrolu bu motor icin uygulanmadi.")
        elif failed_login_count > FAILED_LOGIN_ALERT:
            health_score -= 15
            penalties.append(f"[-15] Güvenlik İhlali: Son {FAILED_LOGIN_WINDOW_HOURS} saatte {failed_login_count} adet başarısız giriş (Login Failed) tespit edildi!")
            print(f"🔴 SECURITY: Brute-force/Login tehlikesi! ({failed_login_count} deneme)")
        elif failed_login_count > 0:
            print(f"🟡 SECURITY: Son {FAILED_LOGIN_WINDOW_HOURS} saatte {failed_login_count} adet hatalı giriş yapılmış.")
        else:
            print("🟢 SECURITY: Şüpheli giriş denemesi yok.")

        # 8. Job Kontrolü
        try:
            failed_jobs = health_strategy.get_failed_jobs(cursor)
        except Exception as e:
            failed_jobs = None
            print(f"⚠️ JOBS: Job bilgisi alinamadi: {e}")

        if failed_jobs is None:
            print("ℹ️ JOBS: Bu motor icin job izleme uygulanmadi.")
        elif not failed_jobs:
            print("🟢 JOBS: Son 24 saatte hata veren görev (Job) yok.")
        else:
            print(f"🔴 JOBS: {len(failed_jobs)} adet görev hata verdi!")
            for job_name in failed_jobs:
                health_score -= 15
                penalties.append(f"[-15] Job Hatası: '{job_name}' isimli görev başarısız oldu!")

        # 9. AUTO GROWTH (OTOMATİK BÜYÜME) KONTROLÜ
        try:
            growth_files = health_strategy.get_auto_growth_files(cursor)
        except Exception as e:
            growth_files = None
            print(f"⚠️ AUTO GROWTH: Buyume ayarlari alinamadi: {e}")

        if growth_files is None:
            print("ℹ️ AUTO GROWTH: Bu motor icin auto-growth kontrolu uygulanmadi.")
        else:
            bad_growth_count = 0
            skipped_system_growth = 0
            for f in growth_files:
                db_name = f.get("db_name")
                file_name = f.get("file_name")
                issue_desc = str(f.get("issue_desc") or "").strip()
                is_pct = int(f.get("is_percent_growth") or 0)
                growth_pages = int(f.get("growth_pages") or 0)

                is_cluster_level_issue = bool(issue_desc) and not db_name

                if not is_cluster_level_issue and not is_database_monitored(db_name, monitored_databases):
                    continue

                if (not CHECK_SYSTEM_DB_AUTOGROWTH) and db_name and str(db_name).lower() in SYSTEM_DATABASES:
                    skipped_system_growth += 1
                    continue

                if issue_desc:
                    target_name = str(file_name or db_name or "unknown")
                    health_score -= 10
                    penalties.append(f"[-10] Auto Growth: {target_name} icin {issue_desc}.")
                    bad_growth_count += 1
                    continue

                if is_pct == 1 and growth_pages > 0:
                    health_score -= 10
                    penalties.append(f"[-10] Auto Growth: {db_name} veritabanının '{file_name}' dosyası YÜZDELİK (%) büyümeye ayarlı!")
                    bad_growth_count += 1
                elif is_pct == 0 and growth_pages <= 128 and growth_pages > 0:
                    health_score -= 10
                    penalties.append(f"[-10] Auto Growth: {db_name} veritabanının '{file_name}' dosyası çok düşük (1 MB altı) büyümeye ayarlı!")
                    bad_growth_count += 1

            if bad_growth_count > 0:
                print(f"🔴 AUTO GROWTH: {bad_growth_count} adet dosyada yanlış büyüme ayarı var (Performans Riski)!")
            else:
                print("🟢 AUTO GROWTH: Veritabanı büyüme ayarları stabil.")

            if skipped_system_growth > 0:
                print(f"ℹ️ AUTO GROWTH: {skipped_system_growth} sistem DB dosyası (CHECK_SYSTEM_DB_AUTOGROWTH=0) ceza hesaplamasından hariç tutuldu.")

        # 10. LOG FILE RISK CHECK
        try:
            log_spaces = health_strategy.get_log_space_usage(cursor)
        except Exception as e:
            log_spaces = None
            print(f"⚠️ LOG SPACE: Log kullanimi alinamadi: {e}")

        if log_spaces is None:
            print("ℹ️ LOG SPACE: Bu motor icin log doluluk kontrolu uygulanmadi.")
        else:
            bad_log_count = 0
            checked_log_db_count = 0
            last_used_pct = None

            for log in log_spaces:
                db_name = log.get("db_name")
                used_pct_raw = log.get("used_pct")

                if not is_database_monitored(db_name, monitored_databases):
                    continue

                if used_pct_raw is None:
                    continue

                used_pct = float(used_pct_raw)
                checked_log_db_count += 1
                last_used_pct = used_pct

                if used_pct >= LOG_USED_CRIT_PCT:
                    health_score -= 30
                    penalties.append(f"[-30] Log Dosyası Riski: {db_name} veritabanının işlem günlüğü (Log) %{used_pct:.2f} dolu!")
                    print(f"🔴 LOG KRİTİK: {db_name} Log Dosyası %{used_pct:.2f} dolu!")
                    bad_log_count += 1

            if not log_spaces:
                print("🟢 LOG SPACE: Log doluluk sorgusu boş döndü (kontrol edilecek veri yok).")
            elif checked_log_db_count == 0:
                print("ℹ️ LOG SPACE: İzleme kapsamındaki veritabanları için log doluluk verisi bulunamadı.")
            elif bad_log_count == 0 and last_used_pct is not None:
                print(f"🟢 LOG SPACE: Tüm veritabanlarının log doluluk oranları güvenli seviyede. Son okunan doluluk: %{last_used_pct:.0f}")

        print("=" * 50)
        print(f"🏆 GÜNCEL SUNUCU SAĞLIK SKORU: {health_score} / 100")
        
        save_to_sqlite(health_score, penalties)

        if health_score >= 70:
            # Sağlık düzeldi, flag ve zamanları sıfırla
            app.current_message_statu = 0
            app.alert_sent_time = None
            app.alert_resend_after = None

        if health_score < TELEGRAM_THRESHOLD:
            import random
            from datetime import timedelta
            now_dt = datetime.now()

            # İlk mesaj veya bekleme süresi dolmuşsa gönder
            should_send = (
                app.current_message_statu == 0
                or (app.alert_resend_after is not None and now_dt >= app.alert_resend_after)
            )

            if should_send:
                send_telegram_alert(health_score, penalties)
                app.current_message_statu = 1
                app.alert_sent_time = now_dt
                # Bir sonraki gönderiim için 1-3 saat arası rastgele bir süre belirle
                wait_seconds = random.randint(1 * 3600, 3 * 3600)
                app.alert_resend_after = now_dt + timedelta(seconds=wait_seconds)
                print(f"🔔 Bir sonraki alarm gönderiimi için bekleme süresi: {wait_seconds // 3600:.1f} saat ({app.alert_resend_after.strftime('%H:%M:%S')})")
                
        conn.close()
        
    except Exception as e:
        print(f"❌ Hata Oluştu: {e}")

if __name__ == "__main__":
    run_health_check_with_score()