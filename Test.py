import pyodbc
import sqlite3
import os
import requests
import app
import re
from datetime import datetime
from dotenv import load_dotenv


# .env dosyasından bağlantı bilgilerini yükle
load_dotenv()

# MSSQL Bağlantı Bilgileri
server   = os.getenv('DB_SERVER')
database = os.getenv('DB_NAME')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
driver   = os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server')



if not all([server, database, username, password]):
    raise ValueError("❌ .env dosyasında eksik bağlantı bilgisi var! DB_SERVER, DB_NAME, DB_USER, DB_PASSWORD kontrol edin.")

conn_str = f'DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;'

TELEGRAM_THRESHOLD     = int(os.getenv("TELEGRAM_THRESHOLD") or os.getenv("TELEGRAM_ALERT_THRESHOLD") or 70)
DISK_WARN_PCT          = float(os.getenv('DISK_WARN_PCT', 80))
DISK_CRIT_PCT          = float(os.getenv('DISK_CRIT_PCT', 90))
LOG_USED_CRIT_PCT      = float(os.getenv('LOG_USED_CRIT_PCT', 70))
FAILED_LOGIN_ALERT     = int(os.getenv('FAILED_LOGIN_ALERT', 10))
FAILED_LOGIN_WINDOW_HOURS = int(os.getenv('FAILED_LOGIN_WINDOW_HOURS', 24))
BACKUP_MAX_AGE_HOURS   = int(os.getenv('BACKUP_MAX_AGE_HOURS', 24))
SYSADMIN_MAX_COUNT     = int(os.getenv('SYSADMIN_MAX_COUNT', 2))
LONG_QUERY_SEC         = float(os.getenv('LONG_QUERY_SEC', 30))
LARGE_QUERY_LOGICAL_READS = int(os.getenv('LARGE_QUERY_LOGICAL_READS', 1000000))
QUERY_ANALYSIS_TOP_N   = int(os.getenv('QUERY_ANALYSIS_TOP_N', 5))
SYSTEM_DATABASES       = {'master', 'model', 'msdb', 'tempdb'}


def parse_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {'1', 'true', 'yes', 'on'}


CHECK_SYSTEM_DB_BACKUP = parse_bool_env('CHECK_SYSTEM_DB_BACKUP', True)
CHECK_SYSTEM_DB_AUTOGROWTH = parse_bool_env('CHECK_SYSTEM_DB_AUTOGROWTH', False)


def sanitize_sql_text(text: str | None, max_len: int = 140) -> str:
    if not text:
        return ""
    normalized = " ".join(text.split())
    return normalized[:max_len] + ("..." if len(normalized) > max_len else "")


def build_telegram_penalty_lines(penalties: list[str]) -> str:
    if not penalties:
        return "• Belirtilen ceza yok."

    long_query_items = []
    auto_growth_items = []
    other_items = []

    for p in penalties:
        if "Uzun/Büyük Sorgu:" in p:
            long_query_items.append(p)
        elif "Auto Growth:" in p:
            auto_growth_items.append(p)
        else:
            other_items.append(p)

    lines = [f"• {item}" for item in other_items]

    if long_query_items:
        lines.append(f"• [-8] Uzun/Büyük Sorgu: {len(long_query_items)} adet")
        for item in long_query_items[:2]:
            db_match = re.search(r"DB=([^,]+)", item)
            max_match = re.search(r"Max=([0-9.]+)s", item)
            reads_match = re.search(r"AvgReads=([0-9]+)", item)
            sql_match = re.search(r"SQL='(.+)'$", item)

            db_name = db_match.group(1) if db_match else "unknown"
            max_sec = max_match.group(1) if max_match else "?"
            avg_reads = reads_match.group(1) if reads_match else "?"
            sql_short = sanitize_sql_text(sql_match.group(1), 70) if sql_match else "SQL bilgisi yok"
            lines.append(f"  - DB={db_name} | Max={max_sec}s | AvgReads={avg_reads} | SQL='{sql_short}'")

        if len(long_query_items) > 2:
            lines.append(f"  - +{len(long_query_items) - 2} adet daha")

    if auto_growth_items:
        lines.append(f"• [-10] Auto Growth: {len(auto_growth_items)} dosya")
        for item in auto_growth_items[:2]:
            ag_match = re.search(r"Auto Growth: (.+?) veritabanının '(.+?)' dosyası", item)
            if ag_match:
                db_name = ag_match.group(1)
                file_name = ag_match.group(2)
                lines.append(f"  - {db_name}.{file_name}")
            else:
                lines.append(f"  - {sanitize_sql_text(item, 90)}")

        if len(auto_growth_items) > 2:
            lines.append(f"  - +{len(auto_growth_items) - 2} dosya daha")

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
        f"🖥️  <b>Sunucu:</b> {server}\n"
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
        conn = pyodbc.connect(conn_str)
        conn.autocommit = True 
        cursor = conn.cursor()
        print("🔍 Sistem Analizi Başlıyor...\n" + "="*50)
        
        # 1. SQL Agent Durumu
        cursor.execute("SELECT status_desc FROM sys.dm_server_services WHERE servicename LIKE 'SQL Server Agent%'")
        agent_status = cursor.fetchone()[0]
        if agent_status != "Running":
            health_score -= 30
            penalties.append("[-30] SQL Agent Çalışmıyor!")
            print(f"🔴 SQL Agent Durumu: {agent_status}")
        else:
            print(f"🟢 SQL Agent Durumu: {agent_status}")
        
        # 2. Veritabanı Durumları
        cursor.execute("SELECT name, state_desc FROM sys.databases WHERE state_desc != 'ONLINE'")
        offline_dbs = cursor.fetchall()
        filtered_offline_dbs = [db for db in offline_dbs if is_database_monitored(db[0], monitored_databases)]

        if not filtered_offline_dbs:
            print("🟢 Tüm Veritabanları ONLINE durumda.")
        else:
            for db in filtered_offline_dbs:
                health_score -= 20
                penalties.append(f"[-20] {db[0]} veritabanı {db[1]} durumunda!")
                print(f"🔴 Sorunlu Veritabanı: {db[0]} ({db[1]})")

        # 3. Yedekleme Kontrolü
        backup_excluded_dbs = {'tempdb'}
        if not CHECK_SYSTEM_DB_BACKUP:
            backup_excluded_dbs.update(SYSTEM_DATABASES)
        excluded_db_sql = ", ".join(f"'{db}'" for db in sorted(backup_excluded_dbs))

        backup_query = f"""
        SELECT d.name 
        FROM sys.databases d
        LEFT JOIN msdb.dbo.backupset b 
            ON d.name = b.database_name 
           AND b.type = 'D' 
           AND b.backup_finish_date >= DATEADD(HOUR, -{BACKUP_MAX_AGE_HOURS}, GETDATE())
        WHERE d.name NOT IN ({excluded_db_sql}) AND b.backup_finish_date IS NULL
        """
        cursor.execute(backup_query)
        missing_backups = cursor.fetchall()
        filtered_missing_backups = [db for db in missing_backups if is_database_monitored(db[0], monitored_databases)]
        
        if not filtered_missing_backups:
            print("🟢 Tüm veritabanlarının güncel yedeği var.")
        else:
            health_score -= 50
            penalties.append("[-50] Son 24 saatte yedeği alınmayan veritabanları var!")
            print(f"🔴 Yedeği Olmayan DB Sayısı: {len(filtered_missing_backups)}")

        # 4. Disk Doluluk Oranı
        disk_query = """
        SELECT DISTINCT
            vs.volume_mount_point AS Drive,
            CAST(vs.available_bytes AS FLOAT) / CAST(vs.total_bytes AS FLOAT) * 100 AS FreeSpacePct
        FROM sys.master_files AS f
        CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS vs
        """
        cursor.execute(disk_query)
        disks = cursor.fetchall()
        
        for disk in disks:
            drive_letter = disk[0]
            free_pct = disk[1]
            used_pct = 100 - free_pct
            
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
        cursor.execute("SELECT process_physical_memory_low FROM sys.dm_os_process_memory")
        memory_low = cursor.fetchone()[0]
        
        if memory_low == 1:
            health_score -= 20
            penalties.append("[-20] Sunucuda RAM darboğazı (Memory Pressure) var!")
            print("🔴 MEMORY: SQL Server bellek sıkıntısı çekiyor!")
        else:
            print("🟢 MEMORY: RAM durumu stabil.")

        # 6. Blocking Sorgular
        blocking_query = "SELECT session_id, blocking_session_id, wait_time/1000 AS WaitSeconds FROM sys.dm_exec_requests WHERE blocking_session_id <> 0"
        cursor.execute(blocking_query)
        blocks = cursor.fetchall()
        
        if not blocks:
            print("🟢 BLOCKING: Sistemde birbirini kilitleyen sorgu yok.")
        else:
            print(f"🔴 BLOCKING: {len(blocks)} adet kilitlenen işlem var!")
            for block in blocks:
                health_score -= 10
                penalties.append(f"[-10] Session {block[0]}, Session {block[1]} tarafından {block[2]} saniyedir bloklanıyor!")

        # 6.1 Uzun Süren ve Büyük Sorgular (Query Stats)
        top_n = max(1, min(QUERY_ANALYSIS_TOP_N, 20))
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
        try:
            cursor.execute(query_stats_sql)
            heavy_queries = cursor.fetchall()

            matched_queries = []
            for q in heavy_queries:
                q_max_sec = float(q[1] or 0)
                q_avg_reads = float(q[2] or 0)
                q_db_name = q[0] or "unknown"

                if not is_database_monitored(q_db_name, monitored_databases):
                    continue

                if q_max_sec >= LONG_QUERY_SEC or q_avg_reads >= LARGE_QUERY_LOGICAL_READS:
                    matched_queries.append(q)

            if matched_queries:
                print(f"🔴 QUERY STATS: {len(matched_queries)} adet uzun/büyük sorgu tespit edildi.")
                for q in matched_queries:
                    q_db_name = q[0] or "unknown"
                    q_max_sec = float(q[1] or 0)
                    q_avg_reads = int(float(q[2] or 0))
                    q_exec_count = int(q[3] or 0)
                    q_snippet = sanitize_sql_text(q[5])

                    health_score -= 8
                    penalties.append(
                        f"[-8] Uzun/Büyük Sorgu: DB={q_db_name}, Max={q_max_sec:.1f}s, AvgReads={q_avg_reads}, Exec={q_exec_count}, SQL='{q_snippet}'"
                    )
            else:
                print("🟢 QUERY STATS: Uzun süre çalışan veya büyük sorgu bulunamadı.")
        except pyodbc.Error as e:
            print(f"⚠️ QUERY STATS: Sorgu analizi atlandı (yetki/erişim sorunu olabilir): {e}")

        # 7. Güvenlik ve Denetim Kontrolü
        sysadmin_query = """
        SELECT name 
        FROM sys.server_principals 
        WHERE IS_SRVROLEMEMBER('sysadmin', name) = 1 
        AND name NOT LIKE 'NT SERVICE\\%' 
        AND name NOT LIKE 'NT AUTHORITY\\%' 
        AND name != 'sa'
        """
        cursor.execute(sysadmin_query)
        sysadmins = cursor.fetchall()
        
        if len(sysadmins) > SYSADMIN_MAX_COUNT:
            health_score -= 10
            penalties.append(f"[-10] Güvenlik Riski: Çok fazla 'sysadmin' yetkili kullanıcı var! ({len(sysadmins)} ekstra hesap)")
            print(f"🔴 SECURITY: Çok fazla sysadmin yetkisi! ({len(sysadmins)} ekstra hesap)")
        else:
            print("🟢 SECURITY: Sysadmin hesap sayısı normal.")

        failed_login_query = f"""
        SET NOCOUNT ON;
        DECLARE @ErrorLog TABLE (LogDate DATETIME, ProcessInfo NVARCHAR(100), Text NVARCHAR(MAX));
        INSERT INTO @ErrorLog EXEC sys.xp_readerrorlog 0, 1, N'Login failed';
        SELECT COUNT(*) FROM @ErrorLog WHERE LogDate >= DATEADD(HOUR, -{FAILED_LOGIN_WINDOW_HOURS}, GETDATE());
        """
        cursor.execute(failed_login_query)
        failed_login_count = cursor.fetchone()[0]

        if failed_login_count > FAILED_LOGIN_ALERT:
            health_score -= 15
            penalties.append(f"[-15] Güvenlik İhlali: Son {FAILED_LOGIN_WINDOW_HOURS} saatte {failed_login_count} adet başarısız giriş (Login Failed) tespit edildi!")
            print(f"🔴 SECURITY: Brute-force/Login tehlikesi! ({failed_login_count} deneme)")
        elif failed_login_count > 0:
            print(f"🟡 SECURITY: Son {FAILED_LOGIN_WINDOW_HOURS} saatte {failed_login_count} adet hatalı giriş yapılmış.")
        else:
            print("🟢 SECURITY: Şüpheli giriş denemesi yok.")

        # 8. SQL Agent Jobs Kontrolü
        job_query = """
        SELECT j.name, h.run_date, h.run_time, h.message
        FROM msdb.dbo.sysjobs j
        JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
        WHERE h.run_status = 0
        AND h.run_date >= CONVERT(VARCHAR(8), GETDATE()-1, 112)
        """
        cursor.execute(job_query)
        failed_jobs = cursor.fetchall()
        
        if not failed_jobs:
            print("🟢 JOBS: Son 24 saatte hata veren görev (Job) yok.")
        else:
            print(f"🔴 JOBS: {len(failed_jobs)} adet görev hata verdi!")
            for job in failed_jobs:
                health_score -= 15
                penalties.append(f"[-15] Job Hatası: '{job[0]}' isimli görev başarısız oldu!")
                
        # 9. AUTO GROWTH (OTOMATİK BÜYÜME) KONTROLÜ
        auto_growth_query = """
        SELECT DB_NAME(database_id) AS DBName, name AS FileName, is_percent_growth, growth
        FROM sys.master_files
        WHERE state = 0
        """
        cursor.execute(auto_growth_query)
        growth_files = cursor.fetchall()
        
        bad_growth_count = 0
        skipped_system_growth = 0
        for f in growth_files:
            db_name = f[0]
            file_name = f[1]
            is_pct = f[2]
            growth_pages = f[3]

            if not is_database_monitored(db_name, monitored_databases):
                continue

            if (not CHECK_SYSTEM_DB_AUTOGROWTH) and db_name and db_name.lower() in SYSTEM_DATABASES:
                skipped_system_growth += 1
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

        # 10. LOG FILE RISK CHECK (YENİ EKLENDİ)
        cursor.execute("DBCC SQLPERF(LOGSPACE);")
        log_spaces = cursor.fetchall()
        
        bad_log_count = 0
        last_used_pct = None
        for log in log_spaces:
            db_name = log[0]
            used_pct = float(log[2])  # Log Space Used (%) kolonu

            if not is_database_monitored(db_name, monitored_databases):
                continue

            last_used_pct = used_pct
            
            if used_pct >= LOG_USED_CRIT_PCT:
                health_score -= 30
                penalties.append(f"[-30] Log Dosyası Riski: {db_name} veritabanının işlem günlüğü (Log) %{used_pct:.2f} dolu!")
                print(f"🔴 LOG KRİTİK: {db_name} Log Dosyası %{used_pct:.2f} dolu!")
                bad_log_count += 1

        if not log_spaces:
            print("🟢 LOG SPACE: Log doluluk sorgusu boş döndü (kontrol edilecek veri yok).")
        elif bad_log_count == 0:
            print(f"🟢 LOG SPACE: Tüm veritabanlarının log doluluk oranları güvenli seviyede. Son okunan doluluk: %{int(last_used_pct)}")

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