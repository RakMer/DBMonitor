"""
stress_test.py — DB Monitor Kaos Maymunu (Chaos Monkey)
DİKKAT: Bu betik sadece test ortamlarında çalıştırılmalıdır!
"""

import pyodbc
import threading
import time
import os
from dotenv import load_dotenv

# .env dosyasından bilgileri al
load_dotenv()

DB_SERVER   = os.getenv("DB_SERVER")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DRIVER   = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")

# Geçerli ve bilerek geçersiz yapılmış bağlantı stringleri
VALID_CONN_STR = f"DRIVER={{{DB_DRIVER}}};SERVER={DB_SERVER};DATABASE=DBMonitor_TestDB;UID={DB_USER};PWD={DB_PASSWORD};TrustServerCertificate=yes;"
INVALID_CONN_STR = f"DRIVER={{{DB_DRIVER}}};SERVER={DB_SERVER};DATABASE=master;UID=korsan_kullanici;PWD=yanlis_sifre_123;TrustServerCertificate=yes;"

def hacker_simulasyonu():
    """Sürekli hatalı şifreyle girmeye çalışarak Brute-Force alarmını tetikler."""
    print("🏴‍☠️ [Hacker] Başarısız giriş denemeleri başlatılıyor...")
    while True:
        try:
            # Zaman aşımını çok kısa tutuyoruz ki hemen hata versin ve loga düşsün
            pyodbc.connect(INVALID_CONN_STR, timeout=1)
        except pyodbc.Error:
            pass
        time.sleep(1) # Saniyede 1 şüpheli giriş denemesi

def memory_cpu_canavari():
    """Obez tabloyu rastgele sıralayarak RAM ve CPU'yu felç eder."""
    print("🔥 [CPU/RAM Canavarı] Ağır ve optimize edilmemiş sorgular gönderiliyor...")
    try:
        conn = pyodbc.connect(VALID_CONN_STR)
        cursor = conn.cursor()
        while True:
            # NEWID() komutu tüm tabloyu RAM'e alıp rastgele sıralamaya zorlar (Ağır bir işlemdir)
            cursor.execute("SELECT TOP 50000 * FROM ObezTablo ORDER BY NEWID()")
            cursor.fetchall()
            time.sleep(2)
    except Exception as e:
        print(f"🔥 CPU Hata: {e}")

def blocking_yaratici():
    """Bir satırı kilitler ve bırakmaz, diğer işlem onu beklerken Blocking alarmı öter."""
    print("🔒 [Blocking] Kilitlenen işlemler yaratılıyor...")
    try:
        # 1. İşlem: Kaydı günceller ama işlemi (Transaction) kapatmaz (Kilit koyar)
        conn1 = pyodbc.connect(VALID_CONN_STR, autocommit=False)
        cursor1 = conn1.cursor()
        cursor1.execute("BEGIN TRAN; UPDATE TOP (1) ObezTablo SET Tarih = GETDATE();")
        
        # 2. İşlem: Aynı kaydı okumaya çalışır ve kilitlenip süresiz beklemeye geçer
        def blocked_query():
            try:
                conn2 = pyodbc.connect(VALID_CONN_STR)
                conn2.cursor().execute("SELECT * FROM ObezTablo")
            except:
                pass
        
        t = threading.Thread(target=blocked_query)
        t.daemon = True
        t.start()
        
        while True:
            time.sleep(10) # 1. işlem kilidi sonsuza kadar tutmaya devam eder
    except Exception as e:
        print(f"🔒 Blocking Hata: {e}")

def log_sisirici():
    """Çok hızlı DML işlemleriyle İşlem Günlüğü (Log Space) diskini doldurur."""
    print("📈 [Log Şişirici] Saniyede onlarca INSERT/DELETE ile Log dosyası dolduruluyor...")
    try:
        conn = pyodbc.connect(VALID_CONN_STR, autocommit=True)
        cursor = conn.cursor()
        while True:
            cursor.execute("INSERT INTO ObezTablo DEFAULT VALUES;")
            cursor.execute("DELETE TOP (1) FROM ObezTablo;")
            time.sleep(0.05) # Saniyede 20 işlem
    except Exception as e:
        print(f"📈 Log Hata: {e}")

if __name__ == "__main__":
    print("="*60)
    print("🐒 DB MONITOR - KAOS MAYMUNU (STRESS TEST) BAŞLATILDI!")
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