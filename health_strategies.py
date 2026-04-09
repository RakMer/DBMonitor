"""Engine-specific health check query strategies for DBMonitor.

This module keeps SQL text and database-engine specifics out of Test.py.
The monitoring loop remains responsible for score calculations and penalties.
"""

from __future__ import annotations

from abc import ABC, abstractmethod


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
    - Some MSSQL-specific checks (SQL Agent, auto-growth, SQL Server backup catalog,
      DBCC log space) do not have direct PostgreSQL equivalents and return None.
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
            """
        )
        return [(str(name), str(state)) for name, state in cursor.fetchall()]

    def get_missing_backups(
        self,
        cursor,
        excluded_databases: set[str],
        max_age_hours: int,
    ) -> list[str] | None:
        return None

    def get_disk_usage(self, cursor) -> list[dict[str, object]] | None:
        return None

    def get_memory_pressure(self, cursor) -> bool | None:
        return None

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
        return None

    def get_failed_jobs(self, cursor) -> list[str] | None:
        return None

    def get_auto_growth_files(self, cursor) -> list[dict[str, object]] | None:
        return None

    def get_log_space_usage(self, cursor) -> list[dict[str, object]] | None:
        return None
