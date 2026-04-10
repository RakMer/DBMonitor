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


def _normalize_backup_token(value: str) -> str:
    return "".join(ch for ch in str(value).lower() if ch.isalnum())


def _backup_content_mentions_database(content: str, db_name: str) -> bool:
    db_escaped = re.escape(str(db_name).strip())
    patterns = (
        rf"create\s+database\s+(?:if\s+not\s+exists\s+)?\"?{db_escaped}\"?",
        rf"\\connect\s+\"?{db_escaped}\"?",
        rf"--\s*database\s*:\s*\"?{db_escaped}\"?",
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


def _quote_pg_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


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


def _find_recent_backed_up_databases(
    db_names: list[str],
    backup_dir: str,
    max_age_hours: int,
) -> set[str]:
    if not db_names or not backup_dir or not os.path.isdir(backup_dir):
        return set()

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
        return set()

    if not recent_files:
        return set()

    normalized_file_names = [_normalize_backup_token(name) for name, _path in recent_files]
    matched: set[str] = set()

    for db_name in db_names:
        db_token = _normalize_backup_token(db_name)
        if not db_token:
            continue
        if any(db_token in file_token for file_token in normalized_file_names):
            matched.add(db_name)

    unmatched = [db_name for db_name in db_names if db_name not in matched]
    if not unmatched:
        return matched

    for _name_lower, file_path in recent_files:
        try:
            with open(file_path, "rb") as backup_file:
                content = backup_file.read(POSTGRES_BACKUP_CONTENT_SCAN_BYTES).decode("utf-8", errors="ignore")
        except OSError:
            continue

        for db_name in unmatched:
            if db_name in matched:
                continue
            if _backup_content_mentions_database(content, db_name):
                matched.add(db_name)

    return matched


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
        - max_elapsed_sec: float
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

    @abstractmethod
    def get_auto_growth_files(self, cursor) -> list[dict[str, object]] | None:
        """Return auto-growth rows, or None if unsupported."""

    @abstractmethod
    def get_log_space_usage(self, cursor) -> list[dict[str, object]] | None:
        """Return log usage rows as [{'db_name': str, 'used_pct': float}], or None."""


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
        excluded_db_sql = ", ".join(f"'{db}'" for db in sorted(excluded_databases)) or "''"
        backup_query = f"""
        SELECT d.name
        FROM sys.databases d
        LEFT JOIN msdb.dbo.backupset b
            ON d.name = b.database_name
           AND b.type IN ('D', 'I')
           AND b.backup_finish_date >= DATEADD(HOUR, -{max_age_hours}, GETDATE())
        WHERE d.name NOT IN ({excluded_db_sql}) AND b.backup_finish_date IS NULL
        """
        cursor.execute(backup_query)
        return [str(row[0]) for row in cursor.fetchall()]

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
        query_stats_sql = f"""
        SELECT TOP ({top_n})
            COALESCE(DB_NAME(st.dbid), DB_NAME(pa.plan_dbid), 'unknown') AS db_name,
            (CAST(qs.max_elapsed_time AS FLOAT) / 1000000.0) AS max_elapsed_sec,
            (CAST(qs.total_logical_reads AS FLOAT) / NULLIF(qs.execution_count, 0)) AS avg_logical_reads,
            qs.execution_count,
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
            ) AS query_text
        FROM sys.dm_exec_query_stats qs
        CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
        OUTER APPLY (
            SELECT TOP (1) TRY_CONVERT(INT, pa.value) AS plan_dbid
            FROM sys.dm_exec_plan_attributes(qs.plan_handle) pa
            WHERE pa.attribute = 'dbid'
        ) pa
        WHERE qs.execution_count > 0
        ORDER BY qs.max_elapsed_time DESC
        """

        cursor.execute(query_stats_sql)
        rows = []
        for db_name, max_sec, avg_reads, execution_count, _last_exec, query_text in cursor.fetchall():
            rows.append(
                {
                    "db_name": db_name or "unknown",
                    "max_elapsed_sec": float(max_sec or 0),
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
        rows_out: list[dict[str, object]] = []

        cursor.execute("SELECT name FROM sys.databases WHERE state_desc = 'ONLINE'")
        db_rows = cursor.fetchall()

        for row in db_rows:
            db_name = str(row[0] or "")
            if not db_name:
                continue

            safe_db_name = db_name.replace("]", "]]")
            frag_query = f"""
            SELECT
                N'{safe_db_name}' AS db_name,
                obj.name AS table_name,
                idx.name AS index_name,
                ips.avg_fragmentation_in_percent,
                ips.page_count
            FROM sys.dm_db_index_physical_stats(DB_ID(N'{safe_db_name}'), NULL, NULL, NULL, 'SAMPLED') ips
            INNER JOIN [{safe_db_name}].sys.indexes idx
                ON ips.object_id = idx.object_id
               AND ips.index_id = idx.index_id
            INNER JOIN [{safe_db_name}].sys.objects obj
                ON ips.object_id = obj.object_id
            WHERE obj.type = 'U'
              AND obj.is_ms_shipped = 0
              AND idx.index_id > 0
              AND ips.avg_fragmentation_in_percent > ?
              AND ips.page_count > ?
            ORDER BY ips.avg_fragmentation_in_percent DESC
            """

            try:
                cursor.execute(frag_query, (threshold_pct, min_pages))
            except Exception:
                # A single DB can fail due to permission/state issues; continue scanning others.
                continue

            for db_name_value, table_name, index_name, frag_pct, page_count in cursor.fetchall():
                rows_out.append(
                    {
                        "db_name": str(db_name_value or ""),
                        "table_name": str(table_name or ""),
                        "index_name": str(index_name or ""),
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
        failed_login_query = f"""
        SET NOCOUNT ON;
        DECLARE @ErrorLog TABLE (LogDate DATETIME, ProcessInfo NVARCHAR(100), Text NVARCHAR(MAX));
        INSERT INTO @ErrorLog EXEC sys.xp_readerrorlog 0, 1, N'Login failed';
        SELECT COUNT(*) FROM @ErrorLog WHERE LogDate >= DATEADD(HOUR, -{window_hours}, GETDATE());
        """
        cursor.execute(failed_login_query)
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

    @property
    def engine_name(self) -> str:
        return "postgres"

    def get_agent_status(self, cursor) -> str | None:
        return None

    def get_offline_databases(self, cursor) -> list[tuple[str, str]]:
        cursor.execute(
            """
            SELECT datname,
                   CASE WHEN datallowconn THEN 'ONLINE' ELSE 'OFFLINE' END AS state_desc
            FROM pg_database
            WHERE NOT datallowconn
              AND NOT datistemplate
            """
        )
        return [(str(name), str(state)) for name, state in cursor.fetchall()]

    def get_missing_backups(
        self,
        cursor,
        excluded_databases: set[str],
        max_age_hours: int,
    ) -> list[str] | None:
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
        if not candidate_dbs:
            return []

        backup_dir = os.getenv("BACKUP_DIR", "").strip()
        backed_up_from_files = _find_recent_backed_up_databases(candidate_dbs, backup_dir, max_age_hours)
        if backed_up_from_files:
            return [name for name in candidate_dbs if name not in backed_up_from_files]

        # PostgreSQL has no built-in per-database backup catalog like MSDB.
        # We use WAL archive recency as a practical cluster-level backup signal.
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
            return None

        row = cursor.fetchone()
        if not row:
            return None

        archive_mode = str(row[0] or "off")
        has_recent_archive_signal = bool(row[1])

        if archive_mode not in {"on", "always"}:
            # Without archive mode or file evidence, a reliable freshness check is unavailable.
            return None

        if has_recent_archive_signal:
            return []

        return candidate_dbs

    def get_disk_usage(self, cursor) -> list[dict[str, object]] | None:
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
                    CASE
                        WHEN s.calls > 0 THEN (COALESCE(s.shared_blks_hit, 0) + COALESCE(s.shared_blks_read, 0))::FLOAT / s.calls
                        ELSE 0
                    END AS avg_logical_reads,
                    COALESCE(s.calls, 0) AS execution_count,
                    s.query AS query_text
                FROM pg_stat_statements s
                LEFT JOIN pg_database d ON d.oid = s.dbid
                ORDER BY s.max_exec_time DESC
                LIMIT %s
                """,
                (top_n,),
            )

            rows = []
            for db_name, max_elapsed_sec, avg_logical_reads, execution_count, query_text in cursor.fetchall():
                rows.append(
                    {
                        "db_name": db_name or "unknown",
                        "max_elapsed_sec": float(max_elapsed_sec or 0),
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
                    0::FLOAT AS avg_logical_reads,
                    1 AS execution_count,
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
            for db_name, max_elapsed_sec, avg_logical_reads, execution_count, query_text in cursor.fetchall():
                rows.append(
                    {
                        "db_name": db_name or "unknown",
                        "max_elapsed_sec": float(max_elapsed_sec or 0),
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

        docker_container = (os.getenv("POSTGRES_DOCKER_CONTAINER") or "").strip()
        return _count_auth_failures_from_docker_logs(docker_container, window_hours)

    def get_failed_jobs(self, cursor) -> list[str] | None:
        try:
            cursor.execute(
                """
                SELECT n.nspname
                FROM pg_extension e
                JOIN pg_namespace n ON n.oid = e.extnamespace
                WHERE e.extname = 'pg_cron'
                """
            )
            row = cursor.fetchone()
        except Exception:
            return None

        if not row or not row[0]:
            # pg_cron kurulu degilse job kontrolu bu motor icin desteklenmez.
            return None

        schema_name = str(row[0])
        if not _is_safe_sql_identifier(schema_name):
            return None

        schema_quoted = _quote_pg_identifier(schema_name)
        query = f"""
        SELECT
            d.jobid,
            COALESCE(d.command, j.command, '') AS command_text,
            COALESCE(d.status, 'unknown') AS run_status
        FROM {schema_quoted}.job_run_details d
        LEFT JOIN {schema_quoted}.job j ON j.jobid = d.jobid
        WHERE COALESCE(d.end_time, d.start_time) >= (clock_timestamp() - INTERVAL '24 hours')
          AND LOWER(COALESCE(d.status, '')) NOT IN ('succeeded', 'success')
        ORDER BY COALESCE(d.end_time, d.start_time) DESC
        LIMIT 100
        """

        try:
            cursor.execute(query)
            rows = cursor.fetchall()
        except Exception:
            rows = []

        failed_jobs: list[str] = []
        for jobid, command_text, run_status in rows:
            status_text = str(run_status or "unknown")
            command_short = str(command_text or "").strip().replace("\n", " ")
            if len(command_short) > 80:
                command_short = command_short[:77] + "..."
            failed_jobs.append(f"pg_cron job #{jobid} status={status_text} command='{command_short}'")

        if failed_jobs:
            return failed_jobs

        # pgAgent fallback (if installed).
        try:
            cursor.execute("SELECT to_regclass('pgagent.pga_joblog')")
            pgagent_joblog = cursor.fetchone()
            cursor.execute("SELECT to_regclass('pgagent.pga_job')")
            pgagent_job = cursor.fetchone()
        except Exception:
            return None

        has_pgagent = bool(pgagent_joblog and pgagent_joblog[0]) and bool(pgagent_job and pgagent_job[0])
        if not has_pgagent:
            return None

        try:
            cursor.execute(
                """
                SELECT
                    j.jobname,
                    l.jlgstatus,
                    l.jlgstart
                FROM pgagent.pga_joblog l
                JOIN pgagent.pga_job j ON j.jobid = l.jlgjobid
                WHERE l.jlgstart >= (clock_timestamp() - INTERVAL '24 hours')
                  AND COALESCE(l.jlgstatus, '') NOT IN ('s', 'S')
                ORDER BY l.jlgstart DESC
                LIMIT 100
                """
            )
            pgagent_rows = cursor.fetchall()
        except Exception:
            return None

        if not pgagent_rows:
            return []

        for jobname, status, started_at in pgagent_rows:
            failed_jobs.append(f"pgAgent job '{jobname}' status={status} start={started_at}")

        return failed_jobs

    def get_auto_growth_files(self, cursor) -> list[dict[str, object]] | None:
        return None

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

        try:
            cursor.execute("SELECT current_database()")
            db_row = cursor.fetchone()
            db_name = str(db_row[0]) if db_row and db_row[0] else "postgres"
        except Exception:
            db_name = "postgres"

        used_pct = (float(wal_bytes) / float(max_wal_bytes)) * 100.0
        return [{"db_name": db_name, "used_pct": used_pct}]
