"""Engine-specific health check query strategies for DBMonitor.

This module keeps SQL text and database-engine specifics out of Test.py.
The monitoring loop remains responsible for score calculations and penalties.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
import os
import re
import shutil
import subprocess
import time

try:
    from psycopg2 import sql as pg_sql
except Exception:
    pg_sql = None


POSTGRES_BACKUP_FILE_EXTENSIONS = (
    ".backup",
    ".dump",
    ".sql",
    ".bak",
    ".tar",
    ".gz",
    ".tgz",
    ".xz",
    ".zst",
)

POSTGRES_BACKUP_CONTENT_SCAN_BYTES = 262144

PG_SIZE_UNIT_TO_BYTES = {
    "": 1,
    "b": 1,
    "bytes": 1,
    "kb": 1024,
    "mb": 1024 ** 2,
    "gb": 1024 ** 3,
    "tb": 1024 ** 4,
    "8kb": 8192,
}

PG_AUTH_FAILURE_PATTERNS = (
    "password authentication failed",
    "authentication failed",
    "no pg_hba.conf entry",
)


def _parse_optional_bool_env(*names: str) -> bool | None:
    """Parse optional boolean env values.

    Returns:
    - True/False when a supported value exists.
    - None when variable is missing or invalid.
    """
    true_values = {"1", "true", "yes", "on"}
    false_values = {"0", "false", "no", "off"}

    for name in names:
        raw = os.getenv(name)
        if raw is None:
            continue

        normalized = str(raw).strip().lower()
        if normalized in true_values:
            return True
        if normalized in false_values:
            return False

    return None


def _get_postgres_docker_container() -> str:
    """Resolve Docker container name based on explicit docker/local mode.

    POSTGRES_DOCKER / POSTGRES_USE_DOCKER:
    - 1/true/on  => Docker mode enabled
    - 0/false/off => Local mode forced
    - missing      => backward-compatible auto mode (container var decides)
    """
    container = (os.getenv("POSTGRES_DOCKER_CONTAINER") or "").strip()
    docker_mode = _parse_optional_bool_env("POSTGRES_DOCKER", "POSTGRES_USE_DOCKER")

    if docker_mode is None:
        return container
    if docker_mode is False:
        return ""
    return container


def _normalize_backup_token(value: str) -> str:
    return "".join(ch for ch in str(value).lower() if ch.isalnum())


def _backup_content_mentions_database(content: str, db_name: str) -> bool:
    db_escaped = re.escape(str(db_name).strip())
    patterns = (
        rf"create\s+database\s+(?:if\s+not\s+exists\s+)?\"?{db_escaped}\"?",
        rf"\\connect\s+\"?{db_escaped}\"?",
        rf"--\s*database\s*:\s*\"?{db_escaped}\"?",
        rf"--\s*name\s*:\s*\"?{db_escaped}\"?\s*;\s*type\s*:\s*database",
        rf"alter\s+database\s+\"?{db_escaped}\"?",
    )
    return any(re.search(pattern, content, flags=re.IGNORECASE) for pattern in patterns)


def _parse_pg_size_to_bytes(setting_value: str | int | float | None, unit: str | None) -> int | None:
    if setting_value is None:
        return None

    try:
        numeric = float(setting_value)
    except (TypeError, ValueError):
        return None

    unit_key = (unit or "").strip().lower()
    multiplier = PG_SIZE_UNIT_TO_BYTES.get(unit_key)
    if multiplier is None:
        return None

    size_bytes = int(numeric * multiplier)
    return size_bytes if size_bytes > 0 else None


def _is_safe_sql_identifier(name: str | None) -> bool:
    if not name:
        return False
    return re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", str(name)) is not None


def _resolve_postgres_log_dir(data_directory: str | None, log_directory: str | None) -> str | None:
    data_dir = (data_directory or "").strip()
    log_dir = (log_directory or "").strip()
    if not data_dir or not log_dir:
        return None

    if os.path.isabs(log_dir):
        return log_dir
    return os.path.normpath(os.path.join(data_dir, log_dir))


def _count_auth_failures_from_logs(log_dir: str, window_hours: int) -> int | None:
    if not log_dir or not os.path.isdir(log_dir):
        return None

    cutoff_epoch = time.time() - (max(1, int(window_hours)) * 3600)
    candidates: list[tuple[str, float]] = []

    try:
        with os.scandir(log_dir) as entries:
            for entry in entries:
                if not entry.is_file():
                    continue
                try:
                    mtime = float(entry.stat().st_mtime)
                except OSError:
                    continue
                if mtime < cutoff_epoch:
                    continue
                candidates.append((entry.path, mtime))
    except OSError:
        return None

    if not candidates:
        return 0

    candidates.sort(key=lambda item: item[1], reverse=True)
    count = 0

    for file_path, _mtime in candidates[:30]:
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as log_file:
                for line in log_file:
                    lower_line = line.lower()
                    if any(pattern in lower_line for pattern in PG_AUTH_FAILURE_PATTERNS):
                        count += 1
        except OSError:
            continue

    return count


def _count_auth_failures_from_docker_logs(container_name: str, window_hours: int) -> int | None:
    if not container_name:
        return None

    since_value = f"{max(1, int(window_hours))}h"
    try:
        result = subprocess.run(
            ["docker", "logs", "--since", since_value, container_name],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=15,
            check=False,
        )
    except Exception:
        return None

    if result.returncode != 0:
        return None

    output = (result.stdout or "") + "\n" + (result.stderr or "")
    if not output.strip():
        return 0

    count = 0
    for line in output.splitlines():
        lower_line = line.lower()
        if any(pattern in lower_line for pattern in PG_AUTH_FAILURE_PATTERNS):
            count += 1
    return count


def _get_free_pct_from_docker_path(container_name: str, path: str) -> float | None:
    if not container_name or not path:
        return None

    try:
        result = subprocess.run(
            ["docker", "exec", container_name, "df", "-P", path],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=10,
            check=False,
        )
    except Exception:
        return None

    if result.returncode != 0:
        return None

    lines = [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]
    if len(lines) < 2:
        return None

    data_row = re.split(r"\s+", lines[-1])
    if len(data_row) < 6:
        return None

    try:
        total_kb = float(data_row[1])
        avail_kb = float(data_row[3])
    except (TypeError, ValueError):
        return None

    if total_kb <= 0:
        return None

    return (avail_kb / total_kb) * 100.0


def _get_postgres_storage_paths(cursor) -> list[str]:
    """Return unique PostgreSQL storage roots from data_directory and tablespaces."""
    candidate_paths: list[str] = []

    try:
        cursor.execute("SELECT setting FROM pg_settings WHERE name = 'data_directory'")
        row = cursor.fetchone()
        if row and row[0]:
            candidate_paths.append(str(row[0]))
    except Exception:
        pass

    try:
        cursor.execute("SELECT pg_tablespace_location(oid) FROM pg_tablespace")
        for row in cursor.fetchall():
            location = str(row[0] or "").strip()
            if location:
                candidate_paths.append(location)
    except Exception:
        pass

    unique_paths: list[str] = []
    seen: set[str] = set()
    for path in candidate_paths:
        normalized = os.path.normpath(path)
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_paths.append(normalized)
    return unique_paths


def _collect_recent_backup_files(backup_dir: str, max_age_hours: int) -> list[tuple[str, str]]:
    """Collect recent candidate backup files from BACKUP_DIR."""
    if not backup_dir or not os.path.isdir(backup_dir):
        return []

    cutoff_epoch = time.time() - (max(1, int(max_age_hours)) * 3600)
    recent_files: list[tuple[str, str]] = []

    try:
        with os.scandir(backup_dir) as entries:
            for entry in entries:
                if not entry.is_file():
                    continue

                name_lower = entry.name.lower()
                _base, extension = os.path.splitext(name_lower)
                if extension and extension not in POSTGRES_BACKUP_FILE_EXTENSIONS:
                    continue

                try:
                    if entry.stat().st_mtime < cutoff_epoch:
                        continue
                except OSError:
                    continue

                recent_files.append((name_lower, entry.path))
    except OSError:
        return []

    return recent_files


def _is_probable_cluster_backup_file(file_name_lower: str, content: str) -> bool:
    token = _normalize_backup_token(file_name_lower)
    filename_markers = (
        "pgdumpall",
        "alldatabases",
        "all_dbs",
        "clusterbackup",
        "clusterdump",
    )
    if any(marker in token for marker in filename_markers):
        return True

    content_lower = str(content or "").lower()
    content_markers = (
        "database cluster dump",
        "pg_dumpall",
        "-- globals",
    )
    return any(marker in content_lower for marker in content_markers)


def _match_recent_backup_files_to_databases(
    db_names: list[str],
    recent_files: list[tuple[str, str]],
) -> tuple[set[str], bool]:
    """Match DB names against recent backup files by filename and file head content."""
    if not db_names or not recent_files:
        return set(), False

    normalized_file_names = [_normalize_backup_token(name) for name, _path in recent_files]
    matched: set[str] = set()
    cluster_backup_detected = False

    for db_name in db_names:
        db_token = _normalize_backup_token(db_name)
        if not db_token:
            continue
        if any(db_token in file_token for file_token in normalized_file_names):
            matched.add(db_name)

    unmatched = [db_name for db_name in db_names if db_name not in matched]
    for name_lower, file_path in recent_files:
        try:
            with open(file_path, "rb") as backup_file:
                content = backup_file.read(POSTGRES_BACKUP_CONTENT_SCAN_BYTES).decode("utf-8", errors="ignore")
        except OSError:
            continue

        if _is_probable_cluster_backup_file(name_lower, content):
            cluster_backup_detected = True

        if not unmatched:
            continue

        for db_name in unmatched:
            if db_name in matched:
                continue
            if _backup_content_mentions_database(content, db_name):
                matched.add(db_name)

    if cluster_backup_detected:
        matched.update(db_names)

    return matched, cluster_backup_detected


def _get_postgres_archive_backup_signal(cursor, max_age_hours: int) -> tuple[bool | None, str]:
    """Return archive signal status as (has_recent_signal|None, reason)."""
    try:
        cursor.execute(
            """
            SELECT
                LOWER(COALESCE(current_setting('archive_mode'), 'off')) AS archive_mode,
                (
                    (last_archived_time IS NOT NULL
                     AND last_archived_time >= clock_timestamp() - (%s * INTERVAL '1 hour'))
                    OR
                    (
                        last_archived_time IS NULL
                        AND COALESCE(archived_count, 0) = 0
                        AND stats_reset IS NOT NULL
                        AND stats_reset >= clock_timestamp() - (%s * INTERVAL '1 hour')
                    )
                ) AS has_recent_archive_signal
            FROM pg_stat_archiver
            """,
            (max_age_hours, max_age_hours),
        )
    except Exception:
        return None, "pg_stat_archiver_unavailable"

    row = cursor.fetchone()
    if not row:
        return None, "pg_stat_archiver_no_rows"

    archive_mode = str(row[0] or "off").strip().lower()
    if archive_mode not in {"on", "always"}:
        return None, f"archive_mode_{archive_mode}"

    has_recent_archive_signal = bool(row[1])
    return has_recent_archive_signal, "archive_signal_ok" if has_recent_archive_signal else "archive_signal_stale"


def _probe_postgres_db_connectivity(db_names: list[str]) -> list[tuple[str, str]]:
    """Return (db_name, state_desc) for DBs that cannot be reached via direct connect."""
    if not db_names:
        return []

    db_server = (os.getenv("DB_SERVER") or "").strip()
    db_user = (os.getenv("DB_USER") or "").strip()
    db_password = os.getenv("DB_PASSWORD") or ""
    db_port = (os.getenv("DB_PORT") or "5432").strip()

    if not db_server or not db_user:
        return []

    probe_timeout = max(1, int(os.getenv("POSTGRES_OFFLINE_PROBE_TIMEOUT_SEC", "3")))

    try:
        import psycopg2
    except Exception:
        return []

    unreachable: list[tuple[str, str]] = []
    for db_name in db_names:
        try:
            conn = psycopg2.connect(
                host=db_server,
                port=db_port,
                dbname=db_name,
                user=db_user,
                password=db_password,
                connect_timeout=probe_timeout,
            )
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
            finally:
                conn.close()
        except Exception as exc:
            err = str(exc).strip().splitlines()[0] if str(exc).strip() else "connection failed"
            err_lower = err.lower()

            if "is not currently accepting connections" in err_lower:
                state_desc = "OFFLINE"
            elif "permission denied for database" in err_lower:
                state_desc = "CONNECT PRIVILEGE DENIED"
            else:
                short_err = err[:120]
                state_desc = f"UNREACHABLE: {short_err}"

            unreachable.append((str(db_name), state_desc))

    return unreachable


def _connect_postgres_env_db(database_name: str, timeout_sec: int = 5):
    """Open a PostgreSQL connection using DB_* environment variables."""
    db_server = (os.getenv("DB_SERVER") or "").strip()
    db_user = (os.getenv("DB_USER") or "").strip()
    db_password = os.getenv("DB_PASSWORD") or ""
    db_port = (os.getenv("DB_PORT") or "5432").strip()

    if not db_server or not db_user or not database_name:
        return None

    try:
        import psycopg2
    except Exception:
        return None

    try:
        return psycopg2.connect(
            host=db_server,
            port=db_port,
            dbname=database_name,
            user=db_user,
            password=db_password,
            connect_timeout=max(1, int(timeout_sec)),
        )
    except Exception:
        return None


def _detect_postgres_schedulers(job_cursor) -> list[str]:
    """Detect installed PostgreSQL scheduler extensions/tables in current DB."""
    schedulers: list[str] = []

    # pg_cron detection
    try:
        job_cursor.execute(
            """
            SELECT n.nspname
            FROM pg_extension e
            JOIN pg_namespace n ON n.oid = e.extnamespace
            WHERE e.extname = 'pg_cron'
            """
        )
        row = job_cursor.fetchone()
    except Exception:
        row = None

    if row and row[0]:
        raw_schema = str(row[0])
        candidate_schemas: list[str] = []
        for candidate in ("cron", raw_schema):
            if candidate and candidate not in candidate_schemas:
                candidate_schemas.append(candidate)

        for candidate in candidate_schemas:
            if not _is_safe_sql_identifier(candidate):
                continue
            try:
                job_cursor.execute(
                    "SELECT to_regclass(%s), to_regclass(%s)",
                    (f"{candidate}.job_run_details", f"{candidate}.job"),
                )
                reg_rows = job_cursor.fetchone()
            except Exception:
                continue

            has_run_details = bool(reg_rows and reg_rows[0])
            has_job_table = bool(reg_rows and reg_rows[1])
            if has_run_details and has_job_table:
                schedulers.append("pg_cron")
                break

    # pgAgent detection
    try:
        job_cursor.execute("SELECT to_regclass('pgagent.pga_joblog'), to_regclass('pgagent.pga_job')")
        reg_rows = job_cursor.fetchone()
        has_pgagent = bool(reg_rows and reg_rows[0]) and bool(reg_rows and reg_rows[1])
        if has_pgagent:
            schedulers.append("pgAgent")
    except Exception:
        pass

    return schedulers


class HealthCheckStrategy(ABC):
    """Abstract query strategy used by the monitoring loop."""

    @property
    @abstractmethod
    def engine_name(self) -> str:
        """Return canonical engine name (mssql or postgres)."""

    @abstractmethod
    def get_agent_status(self, cursor) -> str | None:
        """Return agent/service status string, or None when not applicable."""

    @abstractmethod
    def get_offline_databases(self, cursor) -> list[tuple[str, str]]:
        """Return list of (db_name, state_desc) that are not online."""

    @abstractmethod
    def get_missing_backups(
        self,
        cursor,
        excluded_databases: set[str],
        max_age_hours: int,
    ) -> list[str] | None:
        """Return DB names without fresh backups, or None if check is unsupported."""

    def get_backup_check_info(self) -> dict[str, object] | None:
        """Return latest backup-check metadata collected by get_missing_backups."""
        return None

    @abstractmethod
    def get_disk_usage(self, cursor) -> list[dict[str, object]] | None:
        """Return disk rows as [{'drive': str, 'free_pct': float|None}], or None."""

    @abstractmethod
    def get_memory_pressure(self, cursor) -> bool | None:
        """Return True/False for memory pressure, or None if unsupported."""

    @abstractmethod
    def get_active_blocks(self, cursor) -> list[dict[str, object]]:
        """Return active blocking rows.

        Expected keys per row:
        - session_id: int
        - blocking_session_id: int
        - wait_seconds: float
        """

    @abstractmethod
    def get_heavy_queries(self, cursor, top_n: int) -> list[dict[str, object]]:
        """Return query rows for long/large query analysis.

        Expected keys per row:
        - db_name: str
        - query_id: str
        - max_elapsed_sec: float
        - avg_elapsed_sec: float
        - total_elapsed_sec: float
        - avg_logical_reads: float
        - execution_count: int
        - query_text: str
        """

    @abstractmethod
    def get_index_fragmentation(
        self,
        cursor,
        min_pages: int,
        threshold_pct: float,
    ) -> list[dict[str, object]]:
        """Return fragmentation/bloat rows.

        Expected keys per row:
        - db_name: str
        - table_name: str
        - index_name: str
        - fragmentation_pct: float
        - page_count: int
        """

    @abstractmethod
    def get_privileged_accounts(self, cursor) -> list[str] | None:
        """Return privileged account names, or None if unsupported."""

    @abstractmethod
    def get_failed_login_count(self, cursor, window_hours: int) -> int | None:
        """Return failed login count in time window, or None if unsupported."""

    @abstractmethod
    def get_failed_jobs(self, cursor) -> list[str] | None:
        """Return failed job names, or None if unsupported."""

    def get_job_scheduler_info(self, cursor) -> dict[str, object] | None:
        """Return scheduler presence metadata for observability.

        Optional keys:
        - found: bool
        - schedulers: list[str]
        - database: str
        """
        return None

    @abstractmethod
    def get_auto_growth_files(self, cursor) -> list[dict[str, object]] | None:
        """Return auto-growth rows, or None if unsupported."""

    @abstractmethod
    def get_log_space_usage(self, cursor) -> list[dict[str, object]] | None:
        """Return log usage rows as [{'db_name': str, 'used_pct': float}], or None."""

    def get_connection_utilization(self, cursor) -> dict[str, object] | None:
        """Return connection utilization metrics, or None if unsupported.

        Expected keys:
        - max_connections: int
        - effective_max_connections: int
        - active_connections: int
        - utilization_pct: float
        """
        return None

    def get_replication_status(self, cursor) -> list[dict[str, object]] | None:
        """Return replication health rows, or None if unsupported.

        Expected keys per row:
        - target: str
        - state: str
        - lag_seconds: float
        - role: str
        """
        return None

    def get_long_transactions(self, cursor, min_age_seconds: int) -> list[dict[str, object]] | None:
        """Return long-running transaction rows, or None if unsupported.

        Expected keys per row:
        - pid: int
        - db_name: str
        - state: str
        - age_seconds: float
        - query_text: str
        """
        return None


class MSSQLHealthStrategy(HealthCheckStrategy):
    """MSSQL implementation that keeps existing T-SQL checks intact."""

    @property
    def engine_name(self) -> str:
        return "mssql"

    def get_agent_status(self, cursor) -> str | None:
        cursor.execute("SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE 'SQL Server Agent%'")
        row = cursor.fetchone()
        return str(row[0]) if row and row[0] is not None else None

    def get_offline_databases(self, cursor) -> list[tuple[str, str]]:
        cursor.execute("SELECT name, state_desc FROM sys.databases WHERE state_desc != 'ONLINE'")
        return [(str(name), str(state)) for name, state in cursor.fetchall()]

    def get_missing_backups(
        self,
        cursor,
        excluded_databases: set[str],
        max_age_hours: int,
    ) -> list[str] | None:
        safe_window_hours = -max(1, int(max_age_hours))
        backup_query = """
        SELECT d.name
        FROM sys.databases d
        LEFT JOIN msdb.dbo.backupset b
            ON d.name = b.database_name
           AND b.type IN ('D', 'I')
           AND b.backup_finish_date >= DATEADD(HOUR, ?, GETDATE())
        WHERE b.backup_finish_date IS NULL
        """
        cursor.execute(backup_query, (safe_window_hours,))

        excluded = {str(name).strip().lower() for name in excluded_databases}
        missing = []
        for row in cursor.fetchall():
            db_name = str(row[0] or "").strip()
            if not db_name:
                continue
            if db_name.lower() in excluded:
                continue
            missing.append(db_name)
        return missing

    def get_disk_usage(self, cursor) -> list[dict[str, object]] | None:
        cursor.execute(
            """
            SELECT DISTINCT
                vs.volume_mount_point AS Drive,
                CAST(vs.available_bytes AS FLOAT) / CAST(vs.total_bytes AS FLOAT) * 100 AS FreeSpacePct
            FROM sys.master_files AS f
            CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS vs
            """
        )
        rows = []
        for drive, free_pct in cursor.fetchall():
            rows.append({"drive": drive or "UnknownMount", "free_pct": free_pct})
        return rows

    def get_memory_pressure(self, cursor) -> bool | None:
        cursor.execute("SELECT process_physical_memory_low FROM sys.dm_os_process_memory")
        row = cursor.fetchone()
        return bool(int(row[0])) if row and row[0] is not None else None

    def get_active_blocks(self, cursor) -> list[dict[str, object]]:
        cursor.execute(
            """
            SELECT session_id, blocking_session_id, wait_time/1000.0 AS wait_seconds
            FROM sys.dm_exec_requests
            WHERE blocking_session_id <> 0
            """
        )
        rows = []
        for session_id, blocking_session_id, wait_seconds in cursor.fetchall():
            rows.append(
                {
                    "session_id": int(session_id or 0),
                    "blocking_session_id": int(blocking_session_id or 0),
                    "wait_seconds": float(wait_seconds or 0),
                }
            )
        return rows

    def get_heavy_queries(self, cursor, top_n: int) -> list[dict[str, object]]:
        safe_top_n = max(1, int(top_n))
        query_stats_sql = """
        WITH ranked_queries AS (
            SELECT
                COALESCE(DB_NAME(st.dbid), DB_NAME(pa.plan_dbid), 'unknown') AS db_name,
                (CAST(qs.max_elapsed_time AS FLOAT) / 1000000.0) AS max_elapsed_sec,
                (CAST(qs.total_elapsed_time AS FLOAT) / 1000000.0) AS total_elapsed_sec,
                (CAST(qs.total_elapsed_time AS FLOAT) / 1000000.0) / NULLIF(qs.execution_count, 0) AS avg_elapsed_sec,
                (CAST(qs.total_logical_reads AS FLOAT) / NULLIF(qs.execution_count, 0)) AS avg_logical_reads,
                qs.execution_count,
                sys.fn_varbintohexstr(qs.query_hash) AS query_id,
                qs.last_execution_time,
                SUBSTRING(
                    st.text,
                    (qs.statement_start_offset / 2) + 1,
                    (
                        (
                            CASE qs.statement_end_offset
                                WHEN -1 THEN DATALENGTH(st.text)
                                ELSE qs.statement_end_offset
                            END - qs.statement_start_offset
                        ) / 2
                    ) + 1
                ) AS query_text,
                ROW_NUMBER() OVER (ORDER BY qs.max_elapsed_time DESC) AS rn
            FROM sys.dm_exec_query_stats qs
            CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
            OUTER APPLY (
                SELECT TOP (1) TRY_CONVERT(INT, pa.value) AS plan_dbid
                FROM sys.dm_exec_plan_attributes(qs.plan_handle) pa
                WHERE pa.attribute = 'dbid'
            ) pa
            WHERE qs.execution_count > 0
        )
        SELECT
            db_name,
            max_elapsed_sec,
            total_elapsed_sec,
            avg_elapsed_sec,
            avg_logical_reads,
            execution_count,
            query_id,
            last_execution_time,
            query_text
        FROM ranked_queries
        WHERE rn <= ?
        ORDER BY rn
        """

        cursor.execute(query_stats_sql, (safe_top_n,))
        rows = []
        for db_name, max_sec, total_sec, avg_sec, avg_reads, execution_count, query_id, _last_exec, query_text in cursor.fetchall():
            rows.append(
                {
                    "db_name": db_name or "unknown",
                    "query_id": str(query_id or ""),
                    "max_elapsed_sec": float(max_sec or 0),
                    "avg_elapsed_sec": float(avg_sec or 0),
                    "total_elapsed_sec": float(total_sec or 0),
                    "avg_logical_reads": float(avg_reads or 0),
                    "execution_count": int(execution_count or 0),
                    "query_text": query_text or "",
                }
            )
        return rows

    def get_index_fragmentation(
        self,
        cursor,
        min_pages: int,
        threshold_pct: float,
    ) -> list[dict[str, object]]:
        cursor.execute(
            """
            SELECT
                DB_NAME(ips.database_id) AS db_name,
                OBJECT_NAME(ips.object_id, ips.database_id) AS table_name,
                ips.index_id,
                MAX(ips.avg_fragmentation_in_percent) AS fragmentation_pct,
                MAX(ips.page_count) AS page_count
            FROM sys.dm_db_index_physical_stats(NULL, NULL, NULL, NULL, 'SAMPLED') ips
            WHERE ips.database_id IS NOT NULL
              AND ips.index_id > 0
            GROUP BY ips.database_id, ips.object_id, ips.index_id
            HAVING MAX(ips.avg_fragmentation_in_percent) > ?
               AND MAX(ips.page_count) > ?
            ORDER BY MAX(ips.avg_fragmentation_in_percent) DESC
            """,
            (threshold_pct, min_pages),
        )

        rows_out: list[dict[str, object]] = []
        for db_name_value, table_name, index_id, frag_pct, page_count in cursor.fetchall():
            db_name = str(db_name_value or "").strip()
            table = str(table_name or "").strip()
            if not db_name or not table:
                continue

            rows_out.append(
                {
                    "db_name": db_name,
                    "table_name": table,
                    "index_name": f"index_id_{int(index_id or 0)}",
                    "fragmentation_pct": float(frag_pct or 0),
                    "page_count": int(page_count or 0),
                }
            )

        return rows_out

    def get_privileged_accounts(self, cursor) -> list[str] | None:
        cursor.execute(
            """
            SELECT name
            FROM sys.server_principals
            WHERE IS_SRVROLEMEMBER('sysadmin', name) = 1
              AND name NOT LIKE 'NT SERVICE\\%'
              AND name NOT LIKE 'NT AUTHORITY\\%'
              AND name != 'sa'
            """
        )
        return [str(row[0]) for row in cursor.fetchall()]

    def get_failed_login_count(self, cursor, window_hours: int) -> int | None:
        safe_window_hours = max(1, int(window_hours))
        failed_login_query = """
        SET NOCOUNT ON;
        DECLARE @WindowHours INT = ?;
        DECLARE @ErrorLog TABLE (LogDate DATETIME, ProcessInfo NVARCHAR(100), Text NVARCHAR(MAX));
        INSERT INTO @ErrorLog EXEC sys.xp_readerrorlog 0, 1, N'Login failed';
        SELECT COUNT(*) FROM @ErrorLog WHERE LogDate >= DATEADD(HOUR, -@WindowHours, GETDATE());
        """
        cursor.execute(failed_login_query, (safe_window_hours,))
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    def get_failed_jobs(self, cursor) -> list[str] | None:
        cursor.execute(
            """
            SELECT j.name
            FROM msdb.dbo.sysjobs j
            JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
            WHERE h.run_status = 0
              AND h.run_date >= CONVERT(VARCHAR(8), GETDATE()-1, 112)
            """
        )
        return [str(row[0]) for row in cursor.fetchall()]

    def get_job_scheduler_info(self, cursor) -> dict[str, object] | None:
        return {"found": True, "schedulers": ["SQL Agent"], "database": "msdb"}

    def get_auto_growth_files(self, cursor) -> list[dict[str, object]] | None:
        cursor.execute(
            """
            SELECT DB_NAME(database_id) AS DBName, name AS FileName, is_percent_growth, growth
            FROM sys.master_files
            WHERE state = 0
            """
        )

        rows = []
        for db_name, file_name, is_percent_growth, growth in cursor.fetchall():
            rows.append(
                {
                    "db_name": db_name,
                    "file_name": file_name,
                    "is_percent_growth": int(is_percent_growth or 0),
                    "growth_pages": int(growth or 0),
                }
            )
        return rows

    def get_log_space_usage(self, cursor) -> list[dict[str, object]] | None:
        cursor.execute("DBCC SQLPERF(LOGSPACE);")
        rows = []
        for log in cursor.fetchall():
            # Row layout: Database Name, Log Size (MB), Log Space Used (%), Status
            db_name = log[0] if len(log) > 0 else None
            used_pct = log[2] if len(log) > 2 else None
            rows.append({"db_name": db_name, "used_pct": float(used_pct or 0) if used_pct is not None else None})
        return rows


class PostgresHealthStrategy(HealthCheckStrategy):
    """PostgreSQL implementation using pg_catalog and statistics views.

    Notes:
    - Disk usage is collected from PostgreSQL data/tablespace mount points via OS stats.
    - Log space is approximated from WAL directory size against max_wal_size.
    - Memory pressure is approximated by low shared buffer cache hit ratio.
    - Some MSSQL-specific checks (SQL Agent, auto-growth, SQL Server backup catalog)
      do not have direct PostgreSQL equivalents and return None.
    - Index fragmentation is approximated via dead tuple ratio (table bloat proxy).
    """

    def __init__(self) -> None:
        self._last_backup_check_info: dict[str, object] | None = None

    @property
    def engine_name(self) -> str:
        return "postgres"

    def get_agent_status(self, cursor) -> str | None:
        return None

    def get_connection_utilization(self, cursor) -> dict[str, object] | None:
        cursor.execute(
            """
            SELECT
                current_setting('max_connections')::INT AS max_connections,
                COALESCE(current_setting('superuser_reserved_connections', true), '3')::INT AS reserved_connections,
                COUNT(*) FILTER (WHERE backend_type = 'client backend')::INT AS active_client_connections
            FROM pg_stat_activity
            """
        )
        row = cursor.fetchone()
        if not row:
            return None

        max_connections = int(row[0] or 0)
        reserved_connections = max(0, int(row[1] or 0))
        active_connections = int(row[2] or 0)

        effective_max = max(1, max_connections - reserved_connections)
        utilization_pct = (float(active_connections) / float(effective_max)) * 100.0

        return {
            "max_connections": max_connections,
            "reserved_connections": reserved_connections,
            "effective_max_connections": effective_max,
            "active_connections": active_connections,
            "utilization_pct": utilization_pct,
        }

    def get_replication_status(self, cursor) -> list[dict[str, object]] | None:
        try:
            cursor.execute("SELECT pg_is_in_recovery()")
            row = cursor.fetchone()
            is_standby = bool(row and row[0])
        except Exception:
            return None

        rows_out: list[dict[str, object]] = []

        if is_standby:
            try:
                cursor.execute(
                    """
                    SELECT
                        EXTRACT(EPOCH FROM (clock_timestamp() - COALESCE(pg_last_xact_replay_timestamp(), clock_timestamp())))
                    """
                )
                lag_row = cursor.fetchone()
                lag_seconds = float(lag_row[0] or 0) if lag_row else 0.0
            except Exception:
                lag_seconds = 0.0

            rows_out.append(
                {
                    "target": "standby_local",
                    "state": "recovery",
                    "lag_seconds": max(0.0, lag_seconds),
                    "role": "standby",
                }
            )
            return rows_out

        try:
            cursor.execute(
                """
                SELECT
                    COALESCE(application_name, 'unknown') AS target,
                    COALESCE(state, 'unknown') AS state,
                    EXTRACT(
                        EPOCH FROM GREATEST(
                            COALESCE(write_lag, INTERVAL '0 second'),
                            COALESCE(flush_lag, INTERVAL '0 second'),
                            COALESCE(replay_lag, INTERVAL '0 second')
                        )
                    ) AS lag_seconds
                FROM pg_stat_replication
                """
            )
        except Exception:
            return None

        for target, state, lag_seconds in cursor.fetchall():
            rows_out.append(
                {
                    "target": str(target or "unknown"),
                    "state": str(state or "unknown"),
                    "lag_seconds": float(lag_seconds or 0),
                    "role": "primary",
                }
            )

        return rows_out

    def get_long_transactions(self, cursor, min_age_seconds: int) -> list[dict[str, object]] | None:
        safe_age = max(1, int(min_age_seconds))

        cursor.execute(
            """
            SELECT
                pid,
                COALESCE(datname, current_database()) AS db_name,
                COALESCE(state, 'unknown') AS state,
                EXTRACT(EPOCH FROM (clock_timestamp() - xact_start)) AS age_seconds,
                COALESCE(query, '') AS query_text
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
              AND xact_start IS NOT NULL
              AND (clock_timestamp() - xact_start) >= (%s * INTERVAL '1 second')
            ORDER BY age_seconds DESC
            LIMIT 25
            """,
            (safe_age,),
        )

        rows_out = []
        for pid, db_name, state, age_seconds, query_text in cursor.fetchall():
            rows_out.append(
                {
                    "pid": int(pid or 0),
                    "db_name": str(db_name or "unknown"),
                    "state": str(state or "unknown"),
                    "age_seconds": float(age_seconds or 0),
                    "query_text": str(query_text or ""),
                }
            )

        return rows_out

    def get_offline_databases(self, cursor) -> list[tuple[str, str]]:
        cursor.execute(
            """
            SELECT
                   datname,
                   datallowconn,
                   datconnlimit,
                   has_database_privilege(current_user, datname, 'CONNECT') AS can_connect,
                   CASE
                       WHEN NOT datallowconn THEN 'OFFLINE'
                       WHEN datconnlimit = 0 THEN 'CONNECTION LIMIT 0'
                       WHEN NOT has_database_privilege(current_user, datname, 'CONNECT') THEN 'CONNECT PRIVILEGE DENIED'
                   END AS state_desc
            FROM pg_database
            WHERE (
                    NOT datallowconn
                    OR datconnlimit = 0
                    OR NOT has_database_privilege(current_user, datname, 'CONNECT')
                  )
              AND NOT datistemplate
            """
        )

        rows_out: list[tuple[str, str]] = []
        db_names_for_probe: list[str] = []

        for datname, datallowconn, datconnlimit, can_connect, state_desc in cursor.fetchall():
            db_name = str(datname)
            state = str(state_desc or "UNKNOWN")

            # If connection is disabled and user also lacks connect privilege,
            # keep the strongest infrastructure-level reason first.
            if not bool(datallowconn):
                state = "OFFLINE"
            elif int(datconnlimit or 0) == 0:
                state = "CONNECTION LIMIT 0"
            elif not bool(can_connect):
                state = "CONNECT PRIVILEGE DENIED"
            else:
                db_names_for_probe.append(db_name)
                continue

            rows_out.append((db_name, state))

        # Fallback: probe direct connect per DB to catch runtime-inaccessible states.
        # This is useful when catalog flags are not sufficient in managed/container setups.
        probed_unreachable = _probe_postgres_db_connectivity(db_names_for_probe)
        if probed_unreachable:
            existing = {name.lower() for name, _state in rows_out}
            for name, state in probed_unreachable:
                if name.lower() not in existing:
                    rows_out.append((name, state))

        return rows_out

    def get_missing_backups(
        self,
        cursor,
        excluded_databases: set[str],
        max_age_hours: int,
    ) -> list[str] | None:
        self._last_backup_check_info = None

        cursor.execute(
            """
            SELECT datname
            FROM pg_database
            WHERE datallowconn = TRUE
              AND NOT datistemplate
            ORDER BY datname
            """
        )
        db_names = [str(row[0]) for row in cursor.fetchall()]

        excluded_lower = {str(name).lower() for name in excluded_databases}
        candidate_dbs = [name for name in db_names if name.lower() not in excluded_lower]

        backup_mode = (os.getenv("POSTGRES_BACKUP_MODE") or "auto").strip().lower()
        if backup_mode not in {"auto", "file", "archive", "file_or_archive"}:
            backup_mode = "auto"

        backup_dir = os.getenv("BACKUP_DIR", "").strip()
        backup_dir_configured = bool(backup_dir)

        info: dict[str, object] = {
            "engine": "postgres",
            "mode": backup_mode,
            "source": "none",
            "candidate_count": len(candidate_dbs),
            "candidate_databases": list(candidate_dbs),
            "backup_dir_configured": backup_dir_configured,
            "backup_dir": backup_dir,
            "status": "ok",
            "reason": "",
        }

        if not candidate_dbs:
            info.update({"source": "none", "missing_count": 0, "matched_count": 0})
            self._last_backup_check_info = info
            return []

        def finalize(result: list[str] | None, **extra: object) -> list[str] | None:
            info.update(extra)
            if result is None:
                info["status"] = "unsupported"
                info["missing_count"] = None
            else:
                info["status"] = "ok"
                info["missing_count"] = len(result)
                info["matched_count"] = len(candidate_dbs) - len(result)
            self._last_backup_check_info = info
            return result

        # FILE MODE (explicit or auto when BACKUP_DIR is configured)
        file_mode_selected = (
            backup_mode == "file"
            or (backup_mode in {"auto", "file_or_archive"} and backup_dir_configured)
        )

        if file_mode_selected:
            if not backup_dir_configured:
                return finalize(None, source="files", reason="backup_dir_not_configured")
            if not os.path.isdir(backup_dir):
                return finalize(None, source="files", reason="backup_dir_not_found")

            recent_files = _collect_recent_backup_files(backup_dir, max_age_hours)
            matched_dbs, cluster_file_detected = _match_recent_backup_files_to_databases(candidate_dbs, recent_files)
            missing = [name for name in candidate_dbs if name not in matched_dbs]

            return finalize(
                missing,
                source="files",
                reason="files_evaluated",
                recent_backup_file_count=len(recent_files),
                cluster_file_detected=cluster_file_detected,
            )

        # ARCHIVE MODE (explicit, or auto when no BACKUP_DIR is configured)
        archive_mode_selected = backup_mode in {"archive", "auto", "file_or_archive"}
        if archive_mode_selected:
            has_recent_archive_signal, archive_reason = _get_postgres_archive_backup_signal(cursor, max_age_hours)
            if has_recent_archive_signal is None:
                return finalize(None, source="archive", reason=archive_reason)
            if has_recent_archive_signal:
                return finalize([], source="archive", reason=archive_reason)
            return finalize(list(candidate_dbs), source="archive", reason=archive_reason)

        return finalize(None, source="none", reason="invalid_backup_mode")

    def get_backup_check_info(self) -> dict[str, object] | None:
        if self._last_backup_check_info is None:
            return None
        return dict(self._last_backup_check_info)

    def get_disk_usage(self, cursor) -> list[dict[str, object]] | None:
        candidate_paths = _get_postgres_storage_paths(cursor)

        if not candidate_paths:
            return None

        rows: list[dict[str, object]] = []
        seen_devices: set[int] = set()

        for path in candidate_paths:
            try:
                stat_info = os.stat(path)
                if stat_info.st_dev in seen_devices:
                    continue
                seen_devices.add(stat_info.st_dev)

                usage = shutil.disk_usage(path)
                if usage.total <= 0:
                    continue

                free_pct = (float(usage.free) / float(usage.total)) * 100.0
                rows.append({"drive": path, "free_pct": free_pct})
            except OSError:
                continue

        if rows:
            return rows

        docker_container = _get_postgres_docker_container()
        if docker_container:
            docker_rows: list[dict[str, object]] = []
            for path in candidate_paths:
                free_pct = _get_free_pct_from_docker_path(docker_container, path)
                if free_pct is None:
                    continue
                docker_rows.append({"drive": f"{docker_container}:{path}", "free_pct": free_pct})

            if docker_rows:
                return docker_rows

        # DB may run on a different host/container so local filesystem lookup can fail.
        # Return a soft row so monitor output indicates limited visibility instead of unsupported.
        return [{"drive": candidate_paths[0], "free_pct": None}]

    def get_memory_pressure(self, cursor) -> bool | None:
        cursor.execute(
            """
            SELECT
                COALESCE(SUM(blks_hit), 0) AS blks_hit,
                COALESCE(SUM(blks_read), 0) AS blks_read
            FROM pg_stat_database
            WHERE datname NOT IN ('template0', 'template1')
            """
        )
        row = cursor.fetchone()
        if not row:
            return None

        blks_hit = float(row[0] or 0)
        blks_read = float(row[1] or 0)
        total_blocks = blks_hit + blks_read

        # Very small sample sizes are noisy; avoid false alarms.
        if total_blocks < 10000:
            return False

        cache_hit_ratio = blks_hit / total_blocks
        return cache_hit_ratio < 0.90

    def get_active_blocks(self, cursor) -> list[dict[str, object]]:
        cursor.execute(
            """
            SELECT
                a.pid AS session_id,
                b.blocking_pid AS blocking_session_id,
                EXTRACT(EPOCH FROM (clock_timestamp() - COALESCE(a.query_start, a.xact_start, a.backend_start))) AS wait_seconds
            FROM pg_stat_activity a
            JOIN LATERAL unnest(pg_blocking_pids(a.pid)) AS b(blocking_pid) ON TRUE
            WHERE a.pid <> pg_backend_pid()
            """
        )

        rows = []
        for session_id, blocking_session_id, wait_seconds in cursor.fetchall():
            rows.append(
                {
                    "session_id": int(session_id or 0),
                    "blocking_session_id": int(blocking_session_id or 0),
                    "wait_seconds": float(wait_seconds or 0),
                }
            )
        return rows

    def get_heavy_queries(self, cursor, top_n: int) -> list[dict[str, object]]:
        # Preferred source: pg_stat_statements (historical, aggregated stats).
        try:
            cursor.execute(
                """
                SELECT
                    COALESCE(d.datname, current_database()) AS db_name,
                    COALESCE(s.max_exec_time, 0) / 1000.0 AS max_elapsed_sec,
                    COALESCE(s.mean_exec_time, 0) / 1000.0 AS avg_elapsed_sec,
                    COALESCE(s.total_exec_time, 0) / 1000.0 AS total_elapsed_sec,
                    CASE
                        WHEN s.calls > 0 THEN (COALESCE(s.shared_blks_hit, 0) + COALESCE(s.shared_blks_read, 0))::FLOAT / s.calls
                        ELSE 0
                    END AS avg_logical_reads,
                    COALESCE(s.calls, 0) AS execution_count,
                    COALESCE(s.queryid::text, '') AS query_id,
                    s.query AS query_text
                FROM pg_stat_statements s
                LEFT JOIN pg_database d ON d.oid = s.dbid
                ORDER BY s.max_exec_time DESC
                LIMIT %s
                """,
                (top_n,),
            )

            rows = []
            for db_name, max_elapsed_sec, avg_elapsed_sec, total_elapsed_sec, avg_logical_reads, execution_count, query_id, query_text in cursor.fetchall():
                rows.append(
                    {
                        "db_name": db_name or "unknown",
                        "query_id": str(query_id or ""),
                        "max_elapsed_sec": float(max_elapsed_sec or 0),
                        "avg_elapsed_sec": float(avg_elapsed_sec or 0),
                        "total_elapsed_sec": float(total_elapsed_sec or 0),
                        "avg_logical_reads": float(avg_logical_reads or 0),
                        "execution_count": int(execution_count or 0),
                        "query_text": query_text or "",
                    }
                )
            return rows
        except Exception:
            # Fallback: currently running queries from pg_stat_activity.
            cursor.execute(
                """
                SELECT
                    current_database() AS db_name,
                    EXTRACT(EPOCH FROM (clock_timestamp() - COALESCE(query_start, xact_start, backend_start))) AS max_elapsed_sec,
                    EXTRACT(EPOCH FROM (clock_timestamp() - COALESCE(query_start, xact_start, backend_start))) AS avg_elapsed_sec,
                    EXTRACT(EPOCH FROM (clock_timestamp() - COALESCE(query_start, xact_start, backend_start))) AS total_elapsed_sec,
                    0::FLOAT AS avg_logical_reads,
                    1 AS execution_count,
                                        SUBSTRING(md5(COALESCE(query, '')), 1, 16) AS query_id,
                    query AS query_text
                FROM pg_stat_activity
                WHERE state <> 'idle'
                  AND pid <> pg_backend_pid()
                ORDER BY max_elapsed_sec DESC
                LIMIT %s
                """,
                (top_n,),
            )

            rows = []
            for db_name, max_elapsed_sec, avg_elapsed_sec, total_elapsed_sec, avg_logical_reads, execution_count, query_id, query_text in cursor.fetchall():
                rows.append(
                    {
                        "db_name": db_name or "unknown",
                        "query_id": str(query_id or ""),
                        "max_elapsed_sec": float(max_elapsed_sec or 0),
                        "avg_elapsed_sec": float(avg_elapsed_sec or 0),
                        "total_elapsed_sec": float(total_elapsed_sec or 0),
                        "avg_logical_reads": float(avg_logical_reads or 0),
                        "execution_count": int(execution_count or 0),
                        "query_text": query_text or "",
                    }
                )
            return rows

    def get_index_fragmentation(
        self,
        cursor,
        min_pages: int,
        threshold_pct: float,
    ) -> list[dict[str, object]]:
        # PostgreSQL does not expose SQL Server-style index fragmentation percentage.
        # We use dead tuple ratio from pg_stat_user_tables as a practical bloat proxy.
        cursor.execute(
            """
            SELECT
                current_database() AS db_name,
                (schemaname || '.' || relname) AS table_name,
                'table_bloat_proxy' AS index_name,
                CASE
                    WHEN (n_live_tup + n_dead_tup) = 0 THEN 0
                    ELSE (n_dead_tup::FLOAT * 100.0) / (n_live_tup + n_dead_tup)
                END AS fragmentation_pct,
                GREATEST(1, pg_relation_size(relid) / 8192) AS page_count
            FROM pg_stat_user_tables
            WHERE (n_live_tup + n_dead_tup) > 0
              AND (
                    CASE
                        WHEN (n_live_tup + n_dead_tup) = 0 THEN 0
                        ELSE (n_dead_tup::FLOAT * 100.0) / (n_live_tup + n_dead_tup)
                    END
                  ) > %s
              AND GREATEST(1, pg_relation_size(relid) / 8192) > %s
            ORDER BY fragmentation_pct DESC
            """,
            (threshold_pct, min_pages),
        )

        rows = []
        for db_name, table_name, index_name, frag_pct, page_count in cursor.fetchall():
            rows.append(
                {
                    "db_name": str(db_name or ""),
                    "table_name": str(table_name or ""),
                    "index_name": str(index_name or ""),
                    "fragmentation_pct": float(frag_pct or 0),
                    "page_count": int(page_count or 0),
                }
            )
        return rows

    def get_privileged_accounts(self, cursor) -> list[str] | None:
        cursor.execute(
            """
            SELECT rolname
            FROM pg_roles
            WHERE rolsuper = TRUE
              AND rolname <> 'postgres'
            """
        )
        return [str(row[0]) for row in cursor.fetchall()]

    def get_failed_login_count(self, cursor, window_hours: int) -> int | None:
        try:
            cursor.execute(
                """
                SELECT
                    current_setting('data_directory') AS data_directory,
                    current_setting('log_directory', true) AS log_directory
                """
            )
            row = cursor.fetchone()
        except Exception:
            return None

        if not row:
            return None

        data_directory = str(row[0] or "")
        log_directory = str(row[1] or "")
        resolved_log_dir = _resolve_postgres_log_dir(data_directory, log_directory)
        if resolved_log_dir:
            file_based_count = _count_auth_failures_from_logs(resolved_log_dir, window_hours)
            if file_based_count is not None:
                return file_based_count

        docker_container = _get_postgres_docker_container()
        return _count_auth_failures_from_docker_logs(docker_container, window_hours)

    def get_failed_jobs(self, cursor) -> list[str] | None:
        lookback_hours = max(1, int(os.getenv("POSTGRES_JOB_LOOKBACK_HOURS", "24")))

        def _collect_failed_jobs_with_cursor(job_cursor) -> tuple[list[str] | None, bool]:
            failed_jobs: list[str] = []
            scheduler_found = False
            query_failed = False

            # pg_cron (per-database extension)
            schedulers = _detect_postgres_schedulers(job_cursor)
            has_pg_cron = "pg_cron" in schedulers
            if has_pg_cron:
                scheduler_found = True
                # Re-fetch extension schema used by pg_cron for query construction.
                try:
                    job_cursor.execute(
                        """
                        SELECT n.nspname
                        FROM pg_extension e
                        JOIN pg_namespace n ON n.oid = e.extnamespace
                        WHERE e.extname = 'pg_cron'
                        """
                    )
                    row = job_cursor.fetchone()
                except Exception:
                    row = None

                raw_schema = str(row[0] or "cron")
                candidate_schemas = []
                for candidate in ("cron", raw_schema):
                    if candidate and candidate not in candidate_schemas:
                        candidate_schemas.append(candidate)

                schema_name = None
                for candidate in candidate_schemas:
                    if not _is_safe_sql_identifier(candidate):
                        continue
                    try:
                        job_cursor.execute(
                            "SELECT to_regclass(%s), to_regclass(%s)",
                            (f"{candidate}.job_run_details", f"{candidate}.job"),
                        )
                        reg_rows = job_cursor.fetchone()
                    except Exception:
                        continue

                    has_run_details = bool(reg_rows and reg_rows[0])
                    has_job_table = bool(reg_rows and reg_rows[1])
                    if has_run_details and has_job_table:
                        schema_name = candidate
                        break

                if schema_name:
                    if pg_sql is None:
                        query_failed = True
                        rows = []
                    else:
                        query = pg_sql.SQL(
                            """
                            SELECT
                                d.jobid,
                                COALESCE(d.command, j.command, '') AS command_text,
                                COALESCE(d.status, 'unknown') AS run_status
                            FROM {}.job_run_details d
                            LEFT JOIN {}.job j ON j.jobid = d.jobid
                            WHERE COALESCE(d.end_time, d.start_time) >= (clock_timestamp() - (%s * INTERVAL '1 hour'))
                              AND LOWER(COALESCE(d.status, '')) NOT IN ('succeeded', 'success')
                            ORDER BY COALESCE(d.end_time, d.start_time) DESC
                            LIMIT 100
                            """
                        ).format(
                            pg_sql.Identifier(schema_name),
                            pg_sql.Identifier(schema_name),
                        )

                        try:
                            job_cursor.execute(query, (lookback_hours,))
                            rows = job_cursor.fetchall()
                        except Exception:
                            query_failed = True
                            rows = []

                    for jobid, command_text, run_status in rows:
                        status_text = str(run_status or "unknown")
                        command_short = str(command_text or "").strip().replace("\n", " ")
                        if len(command_short) > 80:
                            command_short = command_short[:77] + "..."
                        failed_jobs.append(f"pg_cron job #{jobid} status={status_text} command='{command_short}'")
                else:
                    query_failed = True

            # pgAgent fallback (if installed)
            has_pgagent = "pgAgent" in schedulers

            if has_pgagent:
                scheduler_found = True
                try:
                    job_cursor.execute(
                        """
                        SELECT
                            j.jobname,
                            l.jlgstatus,
                            l.jlgstart
                        FROM pgagent.pga_joblog l
                        JOIN pgagent.pga_job j ON j.jobid = l.jlgjobid
                        WHERE l.jlgstart >= (clock_timestamp() - (%s * INTERVAL '1 hour'))
                          AND COALESCE(l.jlgstatus, '') NOT IN ('s', 'S')
                        ORDER BY l.jlgstart DESC
                        LIMIT 100
                        """,
                        (lookback_hours,),
                    )
                    pgagent_rows = job_cursor.fetchall()
                except Exception:
                    query_failed = True
                    pgagent_rows = []

                for jobname, status, started_at in pgagent_rows:
                    failed_jobs.append(f"pgAgent job '{jobname}' status={status} start={started_at}")

            if not scheduler_found:
                return [], False
            if query_failed and not failed_jobs:
                return None, True
            return failed_jobs, True

        local_jobs, local_has_scheduler = _collect_failed_jobs_with_cursor(cursor)
        if local_has_scheduler and local_jobs is not None:
            return local_jobs

        scheduler_query_failed = bool(local_has_scheduler and local_jobs is None)

        current_db = ""
        try:
            cursor.execute("SELECT current_database()")
            row = cursor.fetchone()
            current_db = str(row[0] or "") if row else ""
        except Exception:
            current_db = ""

        candidate_job_dbs: list[str] = []
        env_job_db = (os.getenv("POSTGRES_JOB_DB") or "").strip()
        for db_name in (env_job_db, "postgres"):
            if not db_name:
                continue
            if db_name == current_db:
                continue
            if db_name in candidate_job_dbs:
                continue
            candidate_job_dbs.append(db_name)

        connect_timeout = max(2, int(os.getenv("POSTGRES_JOB_CONNECT_TIMEOUT_SEC", "5")))
        for db_name in candidate_job_dbs:
            job_conn = _connect_postgres_env_db(db_name, timeout_sec=connect_timeout)
            if not job_conn:
                continue

            try:
                job_cursor = job_conn.cursor()
                remote_jobs, remote_has_scheduler = _collect_failed_jobs_with_cursor(job_cursor)
            finally:
                try:
                    job_conn.close()
                except Exception:
                    pass

            if remote_has_scheduler:
                if remote_jobs is None:
                    scheduler_query_failed = True
                    continue
                return remote_jobs

        if scheduler_query_failed:
            return None

        # No scheduler extensions detected on reachable DBs.
        return []

    def get_job_scheduler_info(self, cursor) -> dict[str, object] | None:
        current_db = ""
        try:
            cursor.execute("SELECT current_database()")
            row = cursor.fetchone()
            current_db = str(row[0] or "") if row else ""
        except Exception:
            current_db = ""

        local_schedulers = _detect_postgres_schedulers(cursor)
        if local_schedulers:
            return {
                "found": True,
                "schedulers": local_schedulers,
                "database": current_db,
            }

        candidate_job_dbs: list[str] = []
        env_job_db = (os.getenv("POSTGRES_JOB_DB") or "").strip()
        for db_name in (env_job_db, "postgres"):
            if not db_name:
                continue
            if db_name == current_db:
                continue
            if db_name in candidate_job_dbs:
                continue
            candidate_job_dbs.append(db_name)

        connect_timeout = max(2, int(os.getenv("POSTGRES_JOB_CONNECT_TIMEOUT_SEC", "5")))
        for db_name in candidate_job_dbs:
            job_conn = _connect_postgres_env_db(db_name, timeout_sec=connect_timeout)
            if not job_conn:
                continue

            try:
                job_cursor = job_conn.cursor()
                schedulers = _detect_postgres_schedulers(job_cursor)
            finally:
                try:
                    job_conn.close()
                except Exception:
                    pass

            if schedulers:
                return {
                    "found": True,
                    "schedulers": schedulers,
                    "database": db_name,
                }

        return {
            "found": False,
            "schedulers": [],
            "database": current_db,
        }

    def get_auto_growth_files(self, cursor) -> list[dict[str, object]] | None:
        candidate_paths = _get_postgres_storage_paths(cursor)
        if not candidate_paths:
            return None

        min_free_pct = float(os.getenv("AUTOGROWTH_MIN_FREE_PCT", "15"))
        rows: list[dict[str, object]] = []
        seen_devices: set[int] = set()

        for path in candidate_paths:
            try:
                stat_info = os.stat(path)
            except OSError:
                continue

            if stat_info.st_dev in seen_devices:
                continue
            seen_devices.add(stat_info.st_dev)

            try:
                usage = shutil.disk_usage(path)
            except OSError:
                continue

            if usage.total <= 0:
                continue

            free_pct = (float(usage.free) / float(usage.total)) * 100.0
            if free_pct < min_free_pct:
                rows.append(
                    {
                        "db_name": None,
                        "file_name": path,
                        "issue_desc": f"cluster depolama buyume alani dusuk (bos alan=%{free_pct:.2f}, esik=%{min_free_pct:.1f})",
                    }
                )

        if rows:
            return rows

        docker_container = _get_postgres_docker_container()
        if docker_container:
            for path in candidate_paths:
                free_pct = _get_free_pct_from_docker_path(docker_container, path)
                if free_pct is None:
                    continue
                if free_pct < min_free_pct:
                    rows.append(
                        {
                            "db_name": None,
                            "file_name": f"{docker_container}:{path}",
                            "issue_desc": f"container depolama buyume alani dusuk (bos alan=%{free_pct:.2f}, esik=%{min_free_pct:.1f})",
                        }
                    )

        return rows

    def get_log_space_usage(self, cursor) -> list[dict[str, object]] | None:
        try:
            cursor.execute("SELECT setting, unit FROM pg_settings WHERE name = 'max_wal_size'")
            row = cursor.fetchone()
            if not row:
                return None
            max_wal_bytes = _parse_pg_size_to_bytes(row[0], row[1])
            if max_wal_bytes is None:
                return None
        except Exception:
            return None

        wal_bytes: int | None = None
        try:
            cursor.execute("SELECT COALESCE(SUM(size), 0)::BIGINT FROM pg_ls_waldir()")
            wal_row = cursor.fetchone()
            if wal_row and wal_row[0] is not None:
                wal_bytes = int(wal_row[0])
        except Exception:
            wal_bytes = None

        if wal_bytes is None:
            return None

        used_pct = (float(wal_bytes) / float(max_wal_bytes)) * 100.0
        return [{"db_name": "__cluster__", "scope": "cluster", "used_pct": used_pct}]
