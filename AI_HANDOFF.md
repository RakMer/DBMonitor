# DBMonitor AI Handoff (Current State)

Bu dokuman, projeyi devralacak baska bir yapay zekanin kod tabanini hizlica ve dogru sekilde anlamasi icin hazirlanmistir.

## 1. Projenin Amaci
DBMonitor, veritabani saglik izleme ve operasyon araci olarak calisir.
- MSSQL ve PostgreSQL metriklerini toplar.
- 100 uzerinden saglik skoru hesaplar.
- Sonuclari SQLite'a yazar.
- Flask dashboard uzerinden gorsellestirir.
- Telegram uzerinden alarm ve uzaktan DB operasyonu saglar.

Ana dosyalar:
- app.py
- Test.py
- health_strategies.py
- db_adapters.py
- telegram_listener.py
- templates/index.html
- dbmonitor.sqlite3

## 2. Cekirdek Mimari
Sistem 4 ana surec ve 1 paylasilan depodan olusur:

1) Monitor motoru (Test.py)
- `get_db_runtime()` ile aktif motoru (mssql/postgres) alir.
- Kontrolleri sirali calistirir.
- Ceza listesi ve skor uretir.
- SQLite'a yazar.
- Skor esiginin altina dusunce Telegram bildirimi gonderir.

2) Strategy/Adapter katmani (health_strategies.py + db_adapters.py)
- `db_adapters.py`: baglanti/driver ve motor secimi.
- `health_strategies.py`: motora ozel sorgular (MSSQLHealthStrategy, PostgresHealthStrategy).

3) Web/API katmani (app.py)
- Dashboard HTML render eder.
- Ayar endpointlerini, run-check endpointlerini, kaynak ve wait analizi endpointlerini saglar.
- Coklu baglanti hedeflerini SQLite'ta saklar ve aktif profile uygular.
- Engine/profil gecisini `ACTIVE_DB_ENGINE`, `MSSQL_DB_*`, `POSTGRES_DB_*` anahtarlariyla yonetir.
- Test.py dosyasini arka thread + subprocess ile tetikler.
- `/api/monitoring-databases` canli DB listesini aktif motordan alir.

4) Telegram dinleyici (telegram_listener.py)
- Whitelist tabanli komut kabul eder.
- DB status/list/start/stop/restart/backup/check komutlarini calistirir.

5) Veri deposu (dbmonitor.sqlite3)
- HealthHistory
- PenaltyLog
- MonitoringConfig
- ConnectionTargets
- ResourceSnapshots
- DatabaseResourceSnapshots
- WaitSnapshots
- ActiveWaitSnapshots

## 3. Veri Akisi
1. Test.py kontrolu calisir.
2. Skor ve cezalar SQLite tablolarina kaydolur.
3. app.py bu kayitlari okuyup dashboard ve API yaniti uretir.
4. Telegram komutu `/check` cagrildiginda Test.py tekrar calistirilabilir.
5. Alarm modunda Test.py Telegram API'ye dogrudan mesaj gonderir.

## 4. Health Score Mantigi (Mevcut)
Baslangic skor: 100

Kontroller (sira ve temel ceza mantigi):
1. SQL Agent durumu (MSSQL): calismiyorsa -30
2. Offline veritabanlari: her biri -20
3. Yedekleme tazeligi: uygun yedek yoksa toplu -50
4. Disk doluluk: kritikte -40, uyarida -10
5. Memory pressure: varsa -20
6. Blocking: her blok kaydi -10
7. Query stats (uzun/buyuk sorgular): her eslesen kayit -8
8. Index fragmentation/bloat: her eslesen kayit -10
9. Yetkili hesap sayisi esik ustu: -10
10. Failed login sayisi esik ustu: -15
11. Basarisiz job kayitlari: her kayit -15
12. Auto growth hatali ayar (MSSQL): her dosya -10
13. Log/WAL doluluk kritige ulasirsa: DB basina -30

Notlar:
- DB filtreleme mekanizmasi MonitoringConfig ile calisir.
- Sistem DB dahil etme davranisi bazi kontrollerde env flag ile yonetilir.
- PostgreSQL backup kontrolu once `BACKUP_DIR` dosya/icerik eslesmesi, sonra `pg_stat_archiver` sinyalini kullanir.

## 5. PostgreSQL'e Ozel Uygulamalar
- Backup kontrolu: `BACKUP_DIR` dosya adi + SQL dump icerigi + WAL archive sinyali.
- Disk metrik fallback: host yolu erisilemezse Docker icinde `df -P` ile olcum (`POSTGRES_DOCKER_CONTAINER`).
- Memory pressure: cache hit ratio heuristigi.
- Log space: `pg_ls_waldir()` / `max_wal_size`.
- Failed login: log dosyasi, olmazsa Docker logs fallback.
- Job izleme: pg_cron (primary), pgAgent (fallback).

## 6. Query Analizi Son Durum
Son iyilestirmeler:
1. Izleme/stress kaynakli sorgular filtreleniyor.
2. Alarm kosulu tek metrikten birlesik esige cevildi.
3. Her query sonucuna kimlik eklendi (`QID`).

Detaylar:
- Gurultu filtreleri: `QUERY_NOISE_PATTERNS`.
- Birlesik esik: `QUERY_MIN_CALLS`, `QUERY_AVG_SEC`, `QUERY_TOTAL_SEC`, `LARGE_QUERY_LOGICAL_READS`.
- Kimlik alani:
  - MSSQL: `query_hash` (hex)
  - PostgreSQL: `queryid`
  - Fallback: SQL metninden fingerprint

## 7. API Endpointleri (app.py)
- GET /
  Dashboard sayfasi.

- GET/POST /api/settings
  Esik ve ayarlari okur/gunceller.
  DB hedefi guncellemelerinde once staged validation yapar, sonra profile + runtime anahtarlari yazar.

- GET/POST /api/monitoring-databases
  Izlenecek DB secimlerini yonetir (aktif motora gore canli liste).

- POST /api/run-check
  Test.py kontrolunu arka planda tetikler.

- GET /api/run-check-status
  Arka plan kontrol durumunu dondurur.

- GET /api/resource-metrics
  Kaynak trendleri, DB dagilimi ve spike ozetini dondurur.

- GET /api/wait-analysis
  Wait tipi dagilimlari, trend ve blocking ozetini dondurur.

- GET /api/connection-targets
  Kayitli baglanti hedeflerini listeler.

- POST /api/connection-targets/activate
  Secilen hedefi aktif yapar ve .env profile/runtime alanlarini uygular.

- GET /api/connection-targets/<target_id>
  Tek hedefin detayini (duzenleme alanlari dahil) dondurur.

- POST /api/connection-targets/update
  Hedef bilgilerini gunceller; istenirse ayni anda aktif hale getirir.

- POST /api/connection-targets/delete
  Kayitli hedefi siler.

## 8. Telegram Komutlari (telegram_listener.py)
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
- `TELEGRAM_CHAT_IDS` whitelist zorunlu.
- Sistem DB korumasi var (ozellikle MSSQL sistem DB'leri).
- DB adi icin basit injection filtresi var.

## 9. .env Degiskenleri (Onemli Olanlar)
Aktif runtime baglantisi:
- DB_ENGINE (mssql|postgresql)
- DB_SERVER
- DB_PORT
- DB_NAME
- DB_USER
- DB_PASSWORD
- DB_DRIVER

Profil bazli baglanti anahtarlari:
- ACTIVE_DB_ENGINE
- MSSQL_DB_SERVER
- MSSQL_DB_PORT
- MSSQL_DB_NAME
- MSSQL_DB_USER
- MSSQL_DB_PASSWORD
- MSSQL_DB_DRIVER
- POSTGRES_DB_SERVER
- POSTGRES_DB_PORT
- POSTGRES_DB_NAME
- POSTGRES_DB_USER
- POSTGRES_DB_PASSWORD
- POSTGRES_DOCKER_CONTAINER
- PG_DUMP_BIN (alias: PG_DUMP)

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
- QUERY_ANALYSIS_TOP_N
- QUERY_MIN_CALLS
- QUERY_AVG_SEC
- QUERY_TOTAL_SEC
- INDEX_FRAGMENTATION_PCT
- INDEX_FRAGMENTATION_MIN_PAGES

Davranis flagleri:
- CHECK_SYSTEM_DB_BACKUP
- CHECK_SYSTEM_DB_AUTOGROWTH
- CHECK_SYSTEM_DB_INDEX

PostgreSQL container fallback:
- POSTGRES_DOCKER_CONTAINER
- PG_DUMP_BIN (alias: PG_DUMP)

Telegram:
- TELEGRAM_TOKEN
- TELEGRAM_CHAT_IDS
- BACKUP_DIR

Flask:
- FLASK_DEBUG
- FLASK_HOST
- FLASK_PORT
- FLASK_COOKIE_SECURE

## 10. Bilinen Riskler / Dikkat Noktalari
1. `.env` icinde tekrar eden `DB_*` bloklari profile/runtime secimini karistirabilir; profile anahtarlarina agirlik verilmeli.
2. PostgreSQL uzak baglantida en sik kirilma noktasi `pg_hba.conf` ADDRESS uyusmazligidir (istemci IP dogru olmali).
3. PostgreSQL hata metni UTF-8 disi locale ile gelirse istemci tarafinda `utf-8 codec` hatasi gorulebilir; once auth/pg_hba kok nedeni kontrol edilmeli.
4. Docker log tabanli failed-login sayisi stress test sonrasi sisik kalabilir.
5. Disk metrikleri host-container topolojisine bagimli; fallback olmasa `None` gelebilir.
6. Telegram mesajlari `parse_mode=HTML` kullaniyor; dinamik metinler escape edilmezse parse hatasi riski olur.
7. SQLite tablo/kolon adlari migration olmadan degistirilmemeli.

## 11. Son Donem Davranis Degisiklikleri
- Adapter/strategy yapisi ile Postgres destegi genisletildi.
- app.py canli DB listesini motora gore cekiyor ve stale MonitoringConfig kayitlarini pasifliyor.
- Postgres backup kontrolu file+content+archive sinyali ile iyilestirildi.
- Postgres disk metriğine Docker fallback eklendi.
- Failed login icin Docker logs fallback eklendi.
- pg_cron/pgAgent job kontrolleri eklendi.
- Query analizinde noise filtre + birlesik esik + QID kimligi eklendi.
- `.env` icin profile tabanli engine gecisi eklendi (`ACTIVE_DB_ENGINE`, `MSSQL_DB_*`, `POSTGRES_DB_*`).
- `/api/settings` tarafinda DB hedef guncellemeleri staged validate + atomik persist akisina alindi.
- `ConnectionTargets` tablosu ve hedef listeleme/aktif etme/duzenleme/silme endpointleri eklendi.
- Dashboard'da kayitli hedef secimi + duzenle/sil paneli eklendi; otomatik yenileme 60 sn -> 300 sn yapildi.
- Telegram PostgreSQL yedek akisi iyilestirildi: `NoneType.close` hatasi giderildi, Docker `pg_dump` ve `PG_DUMP` alias destegi eklendi.
- README'ye PostgreSQL uzak baglanti kurulumu ve sik hata cozumleri eklendi.

## 12. Calistirma Komutlari
- python app.py
- python Test.py
- python telegram_listener.py
- python stress_test.py (yalniz test ortami)

## 13. Kisa Devralma Plani (Baska AI icin)
1. Once `db_adapters.py` + `health_strategies.py` sozlesmesini incele, motor davranisini bozma.
2. Test.py skor sirasini koruyarak degisiklik yap.
3. app.py endpoint sozlesmesini bozma (ozellikle `/api/monitoring-databases` akisi).
4. Telegram tarafinda whitelist + sistem DB korumasini asla gevsetme.
5. SQLite tablo/kolon isimlerini migration olmadan degistirme.

## 14. Gelecek Icin Dogal Adimlar
- Query QID bilgisini dashboard/API'ye acik alan olarak tasimak.
- Blocking analizine wait event/type ve blocker query ozeti eklemek.
- Alarm ack/runbook mekanizmasi eklemek.
- Lokal v1 stabilizasyonu icin preflight checks + smoke test akisi olusturmak.
- SQLite WAL/busy-timeout iyilestirmeleri ile eszamanli yazma dayanikliligini arttirmak.
- Test coverage arttirma (en az API smoke + DB adapter baglanti testleri).

## 15. Guncel Durum (2026-04-15)
- Coklu sunucu/profil bazli gecis altyapisi backend ve frontend tarafinda aktif durumda.
- PostgreSQL uzak baglanti sorunlarinda dogru `pg_hba.conf` istemci IP eslesmesi kritik bulgu olarak dogrulandi.
- Lokal ortamda birincil hedef: v1 stabilizasyon (konfig hijyeni, preflight, smoke test, operasyonel runbook).
- Sonraki faz hedefi: uzak sunucuda ayni stabilite kriterleriyle v1 roll-out.

Bu dosya, repo anlik durumunu teknik devralma odakli ozetler.
