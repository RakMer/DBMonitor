"""Database adapter layer for DBMonitor.

This module provides a small Strategy/Adapter abstraction so DBMonitor can switch
between database engines (MSSQL, PostgreSQL) using only environment variables.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
import importlib
import os
from typing import TYPE_CHECKING
from dotenv import load_dotenv

if TYPE_CHECKING:
    from health_strategies import HealthCheckStrategy


def _looks_like_port_only(value: str | None) -> bool:
    if value is None:
        return False
    trimmed = value.strip()
    return bool(trimmed) and trimmed.isdigit() and 1 <= len(trimmed) <= 5


class DatabaseAdapter(ABC):
    """Abstract adapter contract for database engines.

    Every concrete adapter must implement:
    - `connect`: create and return a live DB connection
    - `get_system_databases`: return engine-specific system DB names
    - `get_connection_string`: build engine-specific connection payload
    """

    def __init__(
        self,
        *,
        server: str,
        database: str,
        username: str,
        password: str,
        port: str | None = None,
        connect_timeout: int = 10,
    ) -> None:
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.connect_timeout = connect_timeout

    @abstractmethod
    def connect(self):
        """Create and return a live database connection."""

    @abstractmethod
    def get_system_databases(self) -> set[str]:
        """Return system/internal database names for the engine."""

    @abstractmethod
    def get_connection_string(self) -> str:
        """Return connection string/DSN for the underlying driver."""


class MSSQLAdapter(DatabaseAdapter):
    """MSSQL adapter implementation using pyodbc."""

    def __init__(
        self,
        *,
        server: str,
        database: str,
        username: str,
        password: str,
        driver: str = "ODBC Driver 18 for SQL Server",
        port: str | None = None,
        connect_timeout: int = 10,
    ) -> None:
        super().__init__(
            server=server,
            database=database,
            username=username,
            password=password,
            port=port,
            connect_timeout=connect_timeout,
        )
        self.driver = driver

    def _build_server_target(self) -> str:
        # SQL Server accepts SERVER as "host" or "host,port".
        if self.port and "," not in self.server:
            return f"{self.server},{self.port}"
        return self.server

    def get_connection_string(self) -> str:
        server_target = self._build_server_target()
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={server_target};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout={self.connect_timeout};"
        )

    def get_system_databases(self) -> set[str]:
        return {"master", "model", "msdb", "tempdb"}

    def connect(self):
        # Import lazily so PostgreSQL-only deployments do not require pyodbc.
        try:
            import pyodbc
        except Exception as exc:
            raise RuntimeError(
                "MSSQL driver dependency missing. Install 'pyodbc' to use DB_ENGINE=mssql."
            ) from exc

        try:
            return pyodbc.connect(self.get_connection_string(), timeout=self.connect_timeout)
        except Exception as exc:
            raise ConnectionError(f"MSSQL connection failed: {exc}") from exc


class PostgresAdapter(DatabaseAdapter):
    """PostgreSQL adapter implementation using psycopg2-binary."""

    def get_connection_string(self) -> str:
        # psycopg2 accepts DSN key/value format.
        pg_port = self.port or "5432"
        return (
            f"host={self.server} "
            f"port={pg_port} "
            f"dbname={self.database} "
            f"user={self.username} "
            f"password={self.password} "
            f"connect_timeout={self.connect_timeout}"
        )

    def get_system_databases(self) -> set[str]:
        # template0/template1 are PostgreSQL internal template databases.
        return {"postgres", "template0", "template1"}

    def connect(self):
        # Import lazily so MSSQL-only deployments do not require psycopg2.
        try:
            psycopg2 = importlib.import_module("psycopg2")
        except Exception as exc:
            raise RuntimeError(
                "PostgreSQL driver dependency missing. Install 'psycopg2-binary' to use DB_ENGINE=postgres."
            ) from exc

        try:
            return psycopg2.connect(self.get_connection_string())
        except Exception as exc:
            raise ConnectionError(f"PostgreSQL connection failed: {exc}") from exc


def get_db_adapter() -> DatabaseAdapter:
    """Factory method that returns the proper adapter from DB_ENGINE.

    Supported values:
    - mssql / sqlserver / sql_server
    - postgres / postgresql / pgsql

    Defaults to MSSQL for backward compatibility.
    """

    # Safe to call multiple times; keeps module independent from caller behavior.
    load_dotenv()

    engine = (os.getenv("DB_ENGINE") or "mssql").strip().lower()

    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    port = os.getenv("DB_PORT")

    if not all([server, database, username, password]):
        raise ValueError(
            "Missing DB connection vars in .env. Required: DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD"
        )

    if engine in {"mssql", "sqlserver", "sql_server"}:
        driver = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
        return MSSQLAdapter(
            server=server,
            database=database,
            username=username,
            password=password,
            driver=driver,
            port=port,
        )

    if engine in {"postgres", "postgresql", "pgsql"}:
        if _looks_like_port_only(server):
            raise ValueError(
                "Invalid PostgreSQL config: DB_SERVER looks like a port value. "
                "Set DB_SERVER to a hostname/IP (e.g. 127.0.0.1) and DB_PORT to 5432."
            )
        return PostgresAdapter(
            server=server,
            database=database,
            username=username,
            password=password,
            port=port or "5432",
        )

    raise ValueError(
        f"Unsupported DB_ENGINE='{engine}'. Use one of: mssql, postgres."
    )


def get_health_strategy(adapter: DatabaseAdapter | None = None) -> "HealthCheckStrategy":
    """Factory method that returns the proper health strategy.

    Accepts an optional adapter to avoid double-reading environment variables.
    """

    active_adapter = adapter or get_db_adapter()

    # Lazy import avoids dependency cycles and keeps adapters independent.
    from health_strategies import MSSQLHealthStrategy, PostgresHealthStrategy

    if isinstance(active_adapter, MSSQLAdapter):
        return MSSQLHealthStrategy()

    if isinstance(active_adapter, PostgresAdapter):
        return PostgresHealthStrategy()

    raise ValueError(f"Unsupported adapter type: {type(active_adapter).__name__}")


def get_db_runtime() -> tuple[DatabaseAdapter, "HealthCheckStrategy"]:
    """Return both adapter and health strategy for the active DB_ENGINE."""

    adapter = get_db_adapter()
    strategy = get_health_strategy(adapter)
    return adapter, strategy
