# DBMonitor AI Handoff (Current State)

Bu dokuman, projeyi devralacak baska bir yapay zekanin kod tabanini hizlica ve dogru sekilde anlamasi icin hazirlanmistir.

## 1. Projenin Amaci
DBMonitor, MSSQL saglik izleme ve operasyon araci olarak calisir.
- MSSQL metriklerini toplar.
- 100 uzerinden saglik skoru hesaplar.
- Sonuclari SQLite'a yazar.
- Flask dashboard uzerinden gorsellestirir.
- Telegram uzerinden alarm ve uzaktan DB operasyonu saglar.

Ana dosyalar:
- app.py
- Test.py
- telegram_listener.py
- templates/index.html
- dbmonitor.sqlite3

## 2. Cekirdek Mimari
Sistem 3 ana surec ve 1 paylasilan depodan olusur:

1) Monitor motoru (Test.py)
- MSSQL'e pyodbc ile baglanir.
- Kontrolleri calistirir.
- Ceza listesi ve skor uretir.
- SQLite'a yazar.
- Skor esiginin altina dusunce Telegram bildirimi gonderir.

2) Web/API katmani (app.py)
- Dashboard HTML render eder.
- Ayar endpointlerini, run-check endpointlerini, kaynak ve wait analizi endpointlerini saglar.
- Test.py dosyasini arka thread + subprocess ile tetikler.

3) Telegram dinleyici (telegram_listener.py)
- Whitelist tabanli komut kabul eder.
- DB status/list/start/stop/restart/backup/check komutlarini calistirir.

4) Veri deposu (dbmonitor.sqlite3)
- HealthHistory
- PenaltyLog
- MonitoringConfig
- ResourceSnapshots
- DatabaseResourceSnapshots
- WaitSnapshots
- ActiveWaitSnapshots

## 3. Veri Akisi
1. Test.py kontrolu calisir.
2. Skor ve cezalar SQLite tablolarina kaydolur.
3. app.py bu kayitlari okuyup dashboard ve API yaniti uretir.
4. Telegram komutu /check cagrildiginda Test.py tekrar calistirilabilir.
5. Alarm modunda Test.py Telegram API'ye dogrudan mesaj gonderir.

## 4. Health Score Mantigi (Mevcut)
Baslangic skor: 100

Kontroller (sira ve temel ceza mantigi):
1. SQL Agent durumu: calismiyorsa -30
2. Offline veritabanlari: her biri -20
3. Yedekleme tazeligi: uygun yedek yoksa toplu -50
4. Disk doluluk: kritikte -40, uyarida -10
5. Memory pressure: varsa -20
6. Blocking: her blok kaydi -10
7. Query stats (uzun/buyuk sorgular): her eslesen kayit -8
8. Index fragmentation: her eslesen index -10
9. Sysadmin sayisi esik ustu: -10
10. Failed login sayisi esik ustu: -15
11. Basarisiz SQL Agent jobs: her kayit -15
12. Auto growth hatali ayar: her dosya -10
13. Log dosyasi doluluk kritige ulasirsa: DB basina -30

Notlar:
- Backup kontrolu D ve I tiplerini kabul eder.
- DB filtreleme mekanizmasi MonitoringConfig ile calisir.
- Sistem DB dahil etme davranisi bazi kontrollerde env flag ile yonetilir.

## 5. API Endpointleri (app.py)
- GET /
  Dashboard sayfasi.

- GET/POST /api/settings
  Esik ve ayarlari okur/gunceller.

- GET/POST /api/monitoring-databases
  Izlenecek DB secimlerini yonetir.

- POST /api/run-check
  Test.py kontrolunu arka planda tetikler.

- GET /api/run-check-status
  Arka plan kontrol durumunu dondurur.

- GET /api/resource-metrics
  Kaynak trendleri, DB dagilimi ve spike ozetini dondurur.

- GET /api/wait-analysis
  Wait tipi dagilimlari, trend ve blocking ozetini dondurur.

## 6. Telegram Komutlari (telegram_listener.py)
Temel komutlar:
- /help
- /listdb
- /statusdb [db]
- /stopdb [db]
- /startdb [db]
- /restartdb [db]
- /takebackup [db] [full|diff]
- /check

Guvenlik:
- TELEGRAM_CHAT_IDS whitelist zorunlu.
- master, model, msdb, tempdb korumasi var.
- DB adi icin basit injection filtresi var.

## 7. .env Degiskenleri (Onemli Olanlar)
Baglanti:
- DB_SERVER
- DB_NAME
- DB_USER
- DB_PASSWORD
- DB_DRIVER

Alarm ve esikler:
- TELEGRAM_THRESHOLD (alias: TELEGRAM_ALERT_THRESHOLD)
- DISK_WARN_PCT
- DISK_CRIT_PCT
- LOG_USED_CRIT_PCT
- FAILED_LOGIN_ALERT
- FAILED_LOGIN_WINDOW_HOURS
- BACKUP_MAX_AGE_HOURS
- SYSADMIN_MAX_COUNT
- LONG_QUERY_SEC
- LARGE_QUERY_LOGICAL_READS
- INDEX_FRAGMENTATION_PCT
- INDEX_FRAGMENTATION_MIN_PAGES

Davranis flagleri:
- CHECK_SYSTEM_DB_BACKUP
- CHECK_SYSTEM_DB_AUTOGROWTH
- CHECK_SYSTEM_DB_INDEX

Telegram:
- TELEGRAM_TOKEN
- TELEGRAM_CHAT_IDS
- BACKUP_DIR

Flask:
- FLASK_DEBUG
- FLASK_HOST
- FLASK_PORT
- FLASK_COOKIE_SECURE

## 8. UI/Frontend Durumu
- Tek sayfa dashboard templates/index.html dosyasinda.
- Chart.js ile trend grafikler var.
- Tema secimi, ayar paneli, tablo filtreleme, csv export var.
- Oto yenileme ve manuel run-check tetikleme var.

## 9. Bilinen Riskler / Dikkat Noktalari
1. Mimari su an MSSQL'e sikica bagli.
2. Telegram mesajlari parse_mode=HTML kullaniyor; dinamik metinler escape edilmezse parse hatasi riski olur.
3. Backup path davranisi ortama gore degisebilir (SQL Server servis hesabi izinleri kritik).
4. app.py ve Test.py arasinda moduller arasi paylasilan durum kullaniliyor.

## 10. Son Donem Davranis Degisiklikleri
- Index fragmentation kontrolu eklendi.
- Log-space ve disk metriklerinde None durumlari icin korumalar eklendi.
- Telegram penalty satirlarinda okunabilirlik ve guvenli format iyilestirmeleri yapildi.
- /takebackup komutu full/diff secenekli hale getirildi.

## 11. Calistirma Komutlari
- python app.py
- python Test.py
- python telegram_listener.py
- python stress_test.py (yalniz test ortami)

## 12. Kisa Devralma Plani (Baska AI icin)
1. Once app.py endpoint sozlesmesini incele, bozma.
2. Sonra Test.py skor sirasini koruyarak degisiklik yap.
3. Telegram tarafinda whitelist + sistem DB korumasini asla gevsetme.
4. SQLite tablo/kolon isimlerini migration olmadan degistirme.
5. Yeni ozellik eklerken once API/DB uyumunu, sonra UI entegrasyonunu yap.

## 13. Gelecek Icin Dogal Adimlar
- Motor bagimsiz adapter mimarisi (MSSQL -> PostgreSQL/MySQL)
- Profile-based coklu sunucu izleme
- Role-based access control
- Alarm runbook/ack mekanizmasi
- Test coverage arttirma

Bu dosya, repo anlik durumunu teknik devralma odakli ozetler.
