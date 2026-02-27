import pyodbc
import sqlite3
from datetime import datetime

# MSSQL Bağlantı Bilgileri
server = '10.20.2.23' 
database = 'master' 
username = 'sa' 
password = 'Baran1q2w3e!' 

conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;'

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
        if not offline_dbs:
            print("🟢 Tüm Veritabanları ONLINE durumda.")
        else:
            for db in offline_dbs:
                health_score -= 20
                penalties.append(f"[-20] {db[0]} veritabanı {db[1]} durumunda!")
                print(f"🔴 Sorunlu Veritabanı: {db[0]} ({db[1]})")

        # 3. Yedekleme Kontrolü
        backup_query = """
        SELECT d.name 
        FROM sys.databases d
        LEFT JOIN msdb.dbo.backupset b ON d.name = b.database_name AND b.type = 'D' AND b.backup_finish_date >= GETDATE() - 1
        WHERE d.name NOT IN ('tempdb') AND b.backup_finish_date IS NULL
        """
        cursor.execute(backup_query)
        missing_backups = cursor.fetchall()
        
        if not missing_backups:
            print("🟢 Tüm veritabanlarının güncel yedeği var.")
        else:
            health_score -= 50
            penalties.append("[-50] Son 24 saatte yedeği alınmayan veritabanları var!")
            print(f"🔴 Yedeği Olmayan DB Sayısı: {len(missing_backups)}")

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
            
            if used_pct >= 90:
                health_score -= 40
                penalties.append(f"[-40] {drive_letter} diski kritik seviyede dolu! (%{used_pct:.2f})")
                print(f"🔴 DİSK KRİTİK: {drive_letter} %{used_pct:.2f} Dolu!")
            elif used_pct >= 80:
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

        # 7. Güvenlik ve Denetim Kontrolü
        sysadmin_query = """
        SELECT name 
        FROM sys.server_principals 
        WHERE IS_SRVROLEMEMBER('sysadmin', name) = 1 
        AND name NOT LIKE 'NT SERVICE\%' 
        AND name NOT LIKE 'NT AUTHORITY\%' 
        AND name != 'sa'
        """
        cursor.execute(sysadmin_query)
        sysadmins = cursor.fetchall()
        
        if len(sysadmins) > 2:
            health_score -= 10
            penalties.append(f"[-10] Güvenlik Riski: Çok fazla 'sysadmin' yetkili kullanıcı var! ({len(sysadmins)} ekstra hesap)")
            print(f"🔴 SECURITY: Çok fazla sysadmin yetkisi! ({len(sysadmins)} ekstra hesap)")
        else:
            print("🟢 SECURITY: Sysadmin hesap sayısı normal.")

        failed_login_query = """
        SET NOCOUNT ON;
        DECLARE @ErrorLog TABLE (LogDate DATETIME, ProcessInfo NVARCHAR(100), Text NVARCHAR(MAX));
        INSERT INTO @ErrorLog EXEC sys.xp_readerrorlog 0, 1, N'Login failed';
        SELECT COUNT(*) FROM @ErrorLog WHERE LogDate >= GETDATE() - 1;
        """
        cursor.execute(failed_login_query)
        failed_login_count = cursor.fetchone()[0]

        if failed_login_count > 10:
            health_score -= 15
            penalties.append(f"[-15] Güvenlik İhlali: Son 24 saatte {failed_login_count} adet başarısız giriş (Login Failed) tespit edildi!")
            print(f"🔴 SECURITY: Brute-force/Login tehlikesi! ({failed_login_count} deneme)")
        elif failed_login_count > 0:
            print(f"🟡 SECURITY: Son 24 saatte {failed_login_count} adet hatalı giriş yapılmış.")
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
        for f in growth_files:
            db_name = f[0]
            file_name = f[1]
            is_pct = f[2]
            growth_pages = f[3]
            
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

        # 10. LOG FILE RISK CHECK (YENİ EKLENDİ)
        cursor.execute("DBCC SQLPERF(LOGSPACE);")
        log_spaces = cursor.fetchall()
        
        bad_log_count = 0
        for log in log_spaces:
            db_name = log[0]
            used_pct = float(log[2]) # Log Space Used (%) kolonu
            
            if used_pct >= 90:
                health_score -= 30
                penalties.append(f"[-30] Log Dosyası Riski: {db_name} veritabanının işlem günlüğü (Log) %{used_pct:.2f} dolu!")
                print(f"🔴 LOG KRİTİK: {db_name} Log Dosyası %{used_pct:.2f} dolu!")
                bad_log_count += 1
                
        if bad_log_count == 0:
            print(f"🟢 LOG SPACE: Tüm veritabanlarının log doluluk oranları güvenli seviyede. Doluluk oranı: {int(used_pct)}")

        print("=" * 50)
        print(f"🏆 GÜNCEL SUNUCU SAĞLIK SKORU: {health_score} / 100")
        
        save_to_sqlite(health_score, penalties)
                
        conn.close()
        
    except Exception as e:
        print(f"❌ Hata Oluştu: {e}")

if __name__ == "__main__":
    run_health_check_with_score()