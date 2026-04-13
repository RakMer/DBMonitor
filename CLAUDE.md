# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DBMonitor is a database health monitoring tool for MSSQL Server and PostgreSQL. It calculates a 100-point health score from 13 metrics, stores results in SQLite, visualizes them via a Flask dashboard, and sends Telegram alerts/commands.

## Commands

**Setup:**
```bash
python3 -m venv DBvenv
source DBvenv/bin/activate  # Windows: DBvenv\Scripts\activate
pip install flask pyodbc python-dotenv requests pyTelegramBotAPI psycopg2-binary
```

**Run:**
```bash
python app.py               # Flask dashboard at http://127.0.0.1:5050
python Test.py              # Run a health check (also scheduled via cron)
python telegram_listener.py # Start the Telegram bot (long-running)
python stress_test.py       # Load test (test environments only)
```

**Cron (production):**
```
*/5 * * * * /path/to/DBvenv/bin/python /path/to/DBMonitor/Test.py
```

There is no test suite. Manual verification is done by running `Test.py` and checking the dashboard.

## Architecture

Four processes share one SQLite database (`dbmonitor.sqlite3`):

```
Test.py  ──(subprocess)──  app.py (Flask + Dashboard)
   │                            │
   │    db_adapters.py           │  reads SQLite history
   │    health_strategies.py     │
   │                            │
   └──→ SQLite ←────────────────┘
          ↑
   telegram_listener.py
   (also spawns Test.py via /check)
```

**`db_adapters.py`** — Connection and engine abstraction. `get_db_adapter()` returns `MSSQLAdapter` or `PostgresAdapter` based on `DB_ENGINE`. `get_db_runtime()` returns `(adapter, strategy)` together.

**`health_strategies.py`** — Engine-specific SQL queries. `MSSQLHealthStrategy` uses DMVs; `PostgresHealthStrategy` uses `pg_stat_*` tables. Both implement the same interface consumed by `Test.py`.

**`Test.py`** — Monitoring engine. Calls the strategy layer for each of the 13 checks, computes penalties, writes `HealthHistory` + `PenaltyLog` to SQLite, and sends Telegram alerts when score < `TELEGRAM_THRESHOLD`.

**`app.py`** — Flask server. Reads SQLite for the dashboard. Triggers `Test.py` as a subprocess via `POST /api/run-check`; the caller polls `GET /api/run-check-status`. Implements optional Basic Auth via `DASHBOARD_USER`/`DASHBOARD_PASS`.

**`telegram_listener.py`** — Long-polling Telegram bot. Authenticates by `TELEGRAM_CHAT_IDS` whitelist. Commands: `/listdb`, `/statusdb`, `/stopdb`, `/startdb`, `/restartdb`, `/takebackup [db] [full|diff]`, `/check`, `/help`.

## Health Score

Starts at 100; penalties are subtracted:

| Check | Penalty |
|---|---|
| SQL Agent not running (MSSQL only) | −30 |
| Each offline database | −20 |
| No recent backup (> `BACKUP_MAX_AGE_HOURS`) | −50 total |
| Disk ≥ `DISK_CRIT_PCT` % | −40; ≥ `DISK_WARN_PCT` % → −10 |
| Memory pressure detected | −20 |
| Each blocking session | −10 |
| Each long/large query | −8 |
| Each fragmented index | −10 |
| Sysadmin accounts over limit | −10 |
| Failed login threshold exceeded | −15 |
| Each failed agent/cron job | −15 |
| Each file with incorrect auto-growth (MSSQL) | −10 |
| Each DB with log/WAL ≥ `LOG_USED_CRIT_PCT` % full | −30 |

Score 80–100 = green, 50–79 = yellow, 0–49 = red.

## Code Conventions

- **No ORM** — use raw SQL via `pyodbc` (MSSQL) or `psycopg2` (PostgreSQL) and `sqlite3`.
- **Functional style** — small helper functions; no class hierarchies beyond the existing adapter/strategy pattern.
- **Preserve existing API field names and JSON schema** — the dashboard JavaScript depends on exact field names from Flask endpoints.
- The async run-check flow (`POST /api/run-check` → poll `GET /api/run-check-status`) must not be replaced with a synchronous blocking call.

## Hard Constraints

1. **Do not change SQLite table or column names** without a proper migration — the schema is shared by all processes and has no migration framework.
2. **Do not weaken system DB protection** in `telegram_listener.py` — `master`, `tempdb`, `model`, `msdb` must never be startable/stoppable via bot commands.
3. **Do not bypass the Telegram whitelist** (`TELEGRAM_CHAT_IDS`) — it is the sole authentication mechanism.
4. **Preserve the adapter/strategy contract** — `db_adapters.py` and `health_strategies.py` are the abstraction boundary that lets the rest of the code stay engine-agnostic. New engine-specific logic belongs in the strategy class, not in `Test.py` or `app.py`.
5. **ODBC driver name** in `.env` (`DB_DRIVER`) must exactly match what is installed on the host (`IM002` errors come from a mismatch).

## Required `.env` Variables

```
DB_ENGINE=mssql              # or: postgresql
DB_SERVER=
DB_NAME=
DB_USER=
DB_PASSWORD=
DB_DRIVER=ODBC Driver 18 for SQL Server   # MSSQL only
DB_PORT=                     # PostgreSQL only
TELEGRAM_TOKEN=
TELEGRAM_CHAT_IDS=           # comma-separated chat IDs
```

All threshold variables (`DISK_WARN_PCT`, `DISK_CRIT_PCT`, `TELEGRAM_THRESHOLD`, etc.) are optional and have defaults set in the code. Feature flags `CHECK_SYSTEM_DB_BACKUP`, `CHECK_SYSTEM_DB_AUTOGROWTH`, `CHECK_SYSTEM_DB_INDEX` control whether system databases are included in those specific
