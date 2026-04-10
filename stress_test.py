"""
stress_test.py — DB Monitor Kaos Maymunu (Chaos Monkey)
DİKKAT: Bu betik sadece test ortamlarında çalıştırılmalıdır!

Desteklenen motorlar:
- MSSQL
- PostgreSQL

Motor seçimi .env içindeki DB_ENGINE değeri ile yapılır.
"""

import importlib
import threading
import time
import os
from dotenv import load_dotenv
from db_adapters import MSSQLAdapter, PostgresAdapter, get_db_adapter

# .env dosyasından bilgileri al
load_dotenv()

DB_ADAPTER = get_db_adapter()
IS_MSSQL = isinstance(DB_ADAPTER, MSSQLAdapter)
IS_POSTGRES = isinstance(DB_ADAPTER, PostgresAdapter)

if not (IS_MSSQL or IS_POSTGRES):
    raise ValueError(f"Desteklenmeyen adapter: {type(DB_ADAPTER).__name__}")

ENGINE_NAME = "mssql" if IS_MSSQL else "postgresql"
DEFAULT_TABLE = "ObezTablo" if IS_MSSQL else "dbmonitor_stress_table"
DEFAULT_SCHEMA = "dbo" if IS_MSSQL else "public"

STRESS_DATABASE = (os.getenv("STRESS_DATABASE") or DB_ADAPTER.database or "").strip()
STRESS_TABLE_RAW = (os.getenv("STRESS_TABLE") or DEFAULT_TABLE).strip()

HACKER_INTERVAL_SEC = float(os.getenv("STRESS_HACKER_INTERVAL_SEC", "1.0"))
HEAVY_QUERY_INTERVAL_SEC = float(os.getenv("STRESS_HEAVY_QUERY_INTERVAL_SEC", "2.0"))
LOG_SPAM_INTERVAL_SEC = float(os.getenv("STRESS_LOG_INTERVAL_SEC", "0.05"))
BLOCK_KEEPALIVE_SEC = float(os.getenv("STRESS_BLOCK_KEEPALIVE_SEC", "10.0"))


def _is_safe_identifier(value: str) -> bool:
    if not value or not value.strip():
        return False
    return all(ch.isalnum() or ch in {"_", " "} for ch in value)


def _parse_table_name(raw_name: str, default_schema: str) -> tuple[str, str]:
    parts = [part.strip() for part in raw_name.split(".") if part.strip()]
    if len(parts) == 1:
        schema_name = default_schema
        table_name = parts[0]
    elif len(parts) == 2:
        schema_name, table_name = parts
    else:
        raise ValueError(
            "STRESS_TABLE degeri gecersiz. Ornek format: tablo_adi veya schema.tablo_adi"
        )

    if not _is_safe_identifier(schema_name) or not _is_safe_identifier(table_name):
        raise ValueError("STRESS_TABLE icinde yalnizca harf/rakam/altcizgi/bosluk kullanin.")

    return schema_name, table_name


def _quote_identifier(name: str) -> str:
    if IS_MSSQL:
        return f"[{name.replace(']', ']]')}]"
    return f'"{name.replace('"', '""')}"'


STRESS_SCHEMA, STRESS_TABLE = _parse_table_name(STRESS_TABLE_RAW, DEFAULT_SCHEMA)
TABLE_REF = f"{_quote_identifier(STRESS_SCHEMA)}.{_quote_identifier(STRESS_TABLE)}"


def _load_driver_module():
    module_name = "pyodbc" if IS_MSSQL else "psycopg2"
    try:
        return importlib.import_module(module_name)
    except Exception as exc:
        raise RuntimeError(
            f"{ENGINE_NAME} stress testi icin '{module_name}' modulu bulunamadi."
        ) from exc


DB_DRIVER_MODULE = _load_driver_module()


def _sleep(seconds: float) -> None:
    time.sleep(max(0.0, seconds))


def _build_mssql_server_target() -> str:
    assert isinstance(DB_ADAPTER, MSSQLAdapter)
    server_target = DB_ADAPTER.server
    if DB_ADAPTER.port and "," not in server_target:
        return f"{server_target},{DB_ADAPTER.port}"
    return server_target


def _connect_valid(autocommit: bool = True):
    if IS_MSSQL:
        assert isinstance(DB_ADAPTER, MSSQLAdapter)
        conn_str = (
            f"DRIVER={{{DB_ADAPTER.driver}}};"
            f"SERVER={_build_mssql_server_target()};"
            f"DATABASE={STRESS_DATABASE};"
            f"UID={DB_ADAPTER.username};"
            f"PWD={DB_ADAPTER.password};"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=10;"
        )
        conn = DB_DRIVER_MODULE.connect(conn_str, timeout=10)
        conn.autocommit = autocommit
        return conn

    assert isinstance(DB_ADAPTER, PostgresAdapter)
    conn = DB_DRIVER_MODULE.connect(
        host=DB_ADAPTER.server,
        port=int(DB_ADAPTER.port or "5432"),
        dbname=STRESS_DATABASE,
        user=DB_ADAPTER.username,
        password=DB_ADAPTER.password,
        connect_timeout=10,
    )
    conn.autocommit = autocommit
    return conn


def _attempt_invalid_login():
    if IS_MSSQL:
        assert isinstance(DB_ADAPTER, MSSQLAdapter)
        invalid_conn_str = (
            f"DRIVER={{{DB_ADAPTER.driver}}};"
            f"SERVER={_build_mssql_server_target()};"
            f"DATABASE={STRESS_DATABASE};"
            f"UID=korsan_kullanici;"
            f"PWD=yanlis_sifre_123;"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout=1;"
        )
        DB_DRIVER_MODULE.connect(invalid_conn_str, timeout=1)
        return

    assert isinstance(DB_ADAPTER, PostgresAdapter)
    DB_DRIVER_MODULE.connect(
        host=DB_ADAPTER.server,
        port=int(DB_ADAPTER.port or "5432"),
        dbname=STRESS_DATABASE,
        user="korsan_kullanici",
        password="yanlis_sifre_123",
        connect_timeout=1,
    )


def _ensure_stress_table():
    conn = _connect_valid(autocommit=True)
    cursor = conn.cursor()

    if IS_MSSQL:
        object_name = f"{STRESS_SCHEMA}.{STRESS_TABLE}".replace("'", "''")
        cursor.execute(
            f"""
            IF OBJECT_ID(N'{object_name}', N'U') IS NULL
            BEGIN
                CREATE TABLE {TABLE_REF} (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    payload NVARCHAR(4000) NULL,
                    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
                );
            END;

            IF NOT EXISTS (SELECT 1 FROM {TABLE_REF})
            BEGIN
                INSERT INTO {TABLE_REF} (payload) VALUES (REPLICATE(N'X', 2000));
            END;
            """
        )
    else:
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TABLE_REF} (
                id BIGSERIAL PRIMARY KEY,
                payload TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cursor.execute(
            f"""
            INSERT INTO {TABLE_REF} (payload)
            SELECT repeat('x', 2000)
            WHERE NOT EXISTS (SELECT 1 FROM {TABLE_REF});
            """
        )

    conn.close()

def hacker_simulasyonu():
    """Sürekli hatalı şifreyle girmeye çalışarak Brute-Force alarmını tetikler."""
    print("🏴‍☠️ [Hacker] Başarısız giriş denemeleri başlatılıyor...")
    while True:
        try:
            _attempt_invalid_login()
        except Exception:
            pass
        _sleep(HACKER_INTERVAL_SEC)

def memory_cpu_canavari():
    """Obez tabloyu rastgele sıralayarak RAM ve CPU'yu felç eder."""
    print("🔥 [CPU/RAM Canavarı] Ağır ve optimize edilmemiş sorgular gönderiliyor...")
    try:
        conn = _connect_valid(autocommit=True)
        cursor = conn.cursor()
        while True:
            if IS_MSSQL:
                cursor.execute(f"SELECT TOP (50000) * FROM {TABLE_REF} ORDER BY NEWID()")
            else:
                cursor.execute(f"SELECT * FROM {TABLE_REF} ORDER BY random() LIMIT 50000")
            cursor.fetchall()
            _sleep(HEAVY_QUERY_INTERVAL_SEC)
    except Exception as e:
        print(f"🔥 CPU Hata: {e}")

def blocking_yaratici():
    """Bir satırı kilitler ve bırakmaz, diğer işlem onu beklerken Blocking alarmı öter."""
    print("🔒 [Blocking] Kilitlenen işlemler yaratılıyor...")
    try:
        # 1. İşlem: Satır kilidini alır ve transaction açık kalarak kilidi tutar.
        conn1 = _connect_valid(autocommit=False)
        cursor1 = conn1.cursor()
        if IS_MSSQL:
            cursor1.execute(f"SELECT TOP (1) * FROM {TABLE_REF} WITH (UPDLOCK, HOLDLOCK)")
            blocking_sql = f"SELECT TOP (1) * FROM {TABLE_REF} WITH (UPDLOCK, ROWLOCK)"
            blocking_params = None
        else:
            cursor1.execute(f"SELECT ctid FROM {TABLE_REF} LIMIT 1 FOR UPDATE")
            row = cursor1.fetchone()
            if not row:
                print("🔒 Blocking Hata: Kilitlenecek satır bulunamadı.")
                conn1.close()
                return
            blocking_sql = f"SELECT * FROM {TABLE_REF} WHERE ctid = %s FOR UPDATE"
            blocking_params = (row[0],)
        
        # 2. İşlem: Aynı satırı kilitlemeye çalışır ve beklemeye düşer.
        def blocked_query():
            try:
                conn2 = _connect_valid(autocommit=False)
                cursor2 = conn2.cursor()
                if blocking_params is None:
                    cursor2.execute(blocking_sql)
                else:
                    cursor2.execute(blocking_sql, blocking_params)
            except Exception:
                pass
        
        t = threading.Thread(target=blocked_query)
        t.daemon = True
        t.start()
        
        while True:
            _sleep(BLOCK_KEEPALIVE_SEC)
    except Exception as e:
        print(f"🔒 Blocking Hata: {e}")

def log_sisirici():
    """Çok hızlı DML işlemleriyle İşlem Günlüğü (Log Space) diskini doldurur."""
    print("📈 [Log Şişirici] Saniyede onlarca INSERT/DELETE ile Log dosyası dolduruluyor...")
    try:
        conn = _connect_valid(autocommit=True)
        cursor = conn.cursor()
        while True:
            if IS_MSSQL:
                cursor.execute(f"INSERT INTO {TABLE_REF} (payload) VALUES (REPLICATE(N'Y', 2000));")
                cursor.execute(f"DELETE TOP (1) FROM {TABLE_REF};")
            else:
                cursor.execute(f"INSERT INTO {TABLE_REF} (payload) VALUES (repeat('y', 2000));")
                cursor.execute(
                    f"DELETE FROM {TABLE_REF} WHERE ctid IN (SELECT ctid FROM {TABLE_REF} LIMIT 1);"
                )
            _sleep(LOG_SPAM_INTERVAL_SEC)
    except Exception as e:
        print(f"📈 Log Hata: {e}")

if __name__ == "__main__":
    if not STRESS_DATABASE:
        raise ValueError("STRESS_DATABASE (veya DB_NAME) bos olamaz.")

    _ensure_stress_table()

    print("="*60)
    print("🐒 DB MONITOR - KAOS MAYMUNU (STRESS TEST) BAŞLATILDI!")
    print(f"🔧 Motor: {ENGINE_NAME}")
    print(f"🗂️  Hedef Veritabani: {STRESS_DATABASE}")
    print(f"📌 Hedef Tablo: {TABLE_REF}")
    print("="*60)
    print("Sistemi durdurmak için CTRL+C yapabilirsiniz.\n")
    
    # Tüm sabotaj senaryolarını eşzamanlı iş parçacıkları (thread) olarak başlat
    threads = [
        threading.Thread(target=hacker_simulasyonu),
        threading.Thread(target=memory_cpu_canavari),
        threading.Thread(target=blocking_yaratici),
        threading.Thread(target=log_sisirici)
    ]
    
    for thread in threads:
        thread.daemon = True # Ana program kapatılınca arka plan işlemleri de ölsün
        thread.start()
        
    try:
        # Ana thread'i hayatta tut
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Kaos Maymunu durduruldu. Sistem normale dönüyor...")