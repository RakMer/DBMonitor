# DB Monitor Teknik Dokuman

## 1) Amac
DB Monitor, MSSQL sunuculari icin saglik, performans ve operasyonel yonetim odakli bir izleme aracidir.

Temel hedefler:
- Periyodik saglik kontrolu yapmak
- Skor uretmek ve cezalari detayli kaydetmek
- Dashboard uzerinden analiz ve ayar yonetimi saglamak
- Telegram uzerinden bildirim ve uzaktan DB komutlari vermek
- Kaynak tuketimi ve wait-time analizi sunmak

## 2) Teknoloji Stack
- Backend: Flask (`app.py`)
- Monitor motoru: Python + pyodbc (`Test.py`)
- UI: Jinja2 + Bootstrap + Chart.js (`templates/index.html`)
- Lokal veri deposu: SQLite (`dbmonitor.sqlite3`)
- Bildirim/uzaktan komut: pyTelegramBotAPI (`telegram_listener.py`)

## 3) Mimari Akis
1. `Test.py` MSSQL'e baglanir, kontrolleri ve metrikleri toplar.
2. Sonuclari SQLite'a yazar (`HealthHistory`, `PenaltyLog`, kaynak ve wait tablolari).
3. `app.py` dashboard ve API endpointleri ile bu veriyi sunar.
4. `telegram_listener.py` komutlari dinler ve belirli operasyonlari calistirir.
5. UI tarafi endpointlerden veri cekip grafik ve tablolari olusturur.

## 4) Baslatma
### 4.1 Dashboard
```bash
python app.py
```
Dashboard: `http://127.0.0.1:5050`

### 4.2 Saglik Kontrolu (tek calistirma)
```bash
python Test.py
```

### 4.3 Telegram Listener
```bash
python telegram_listener.py
```

## 5) Konfigurasyon (.env)
Asgari gerekli:
```env
DB_SERVER=...
DB_NAME=master
DB_USER=...
DB_PASSWORD=...
DB_DRIVER=SQL Server
```

Notlar:
- Driver adi, sistemde kurulu ODBC driver adi ile birebir eslesmelidir.
- IM002 hatasi genelde yanlis/kurulu olmayan driver isminden kaynaklanir.
- Esik ayarlari (`TELEGRAM_THRESHOLD`, `DISK_WARN_PCT`, vb.) runtime'da API ile guncellenebilir.

## 6) Dashboard API'leri
### 6.1 Ayarlar
- `GET /api/settings`
- `GET /api/settings?defaults=1`
- `POST /api/settings`

### 6.2 DB Izleme Secimi
- `GET /api/monitoring-databases`
- `POST /api/monitoring-databases`

### 6.3 Manuel Kontrol Calistirma
- `POST /api/run-check`
- `GET /api/run-check-status`

Bu akis asenkrondur: kontrol arka thread'de calisir, UI status endpointini poll eder.

### 6.4 Kaynak Tuketimi
- `GET /api/resource-metrics`

Donen ana alanlar:
- `trend`: CPU, RAM, Disk I/O, Ag trendleri
- `distribution`: DB bazli I/O dagilimi
- `spikes`: onceki doneme gore ani artis analizi

### 6.5 Wait-Time Analysis
- `GET /api/wait-analysis`

Donen ana alanlar:
- `top_waits`: en yuksek wait delta tipleri
- `category_breakdown`: Lock, Disk I/O, Network vb. kategori kirilimi
- `wait_trend`: snapshotlar arasi toplam wait delta trendi
- `blocking_summary`: blocked/blocking session ozeti

## 7) Veri Modeli (SQLite)
### 7.1 Skor ve Cezalar
- `HealthHistory`
- `PenaltyLog`

### 7.2 DB Secim Konfigurasyonu
- `MonitoringConfig`

### 7.3 Kaynak Metrikleri
- `ResourceSnapshots`
- `DatabaseResourceSnapshots`

### 7.4 Wait Analizi
- `WaitSnapshots`
- `ActiveWaitSnapshots`

## 8) Saglik Skoru Mantigi (Ozet)
Skor 100'den baslar ve tespit edilen risklere gore puan duser.

Ornek kontrol gruplari:
- SQL Agent
- Offline DB
- Backup tazeligi
- Disk doluluk
- Memory pressure
- Blocking
- Uzun/agir sorgular
- Guvenlik ve basarisiz login
- Job hatalari
- Auto growth ve log doluluk

## 9) UI Bolumleri
- Saglik skoru ve durum kartlari
- Aktif alarm listesi
- Skor trend grafikleri
- Kaynak tuketimi trend + DB dagilimi + spike listesi
- Wait breakdown + top wait + wait trend + blocking ozeti
- Gecmis tablo (arama, filtre, pagination, CSV export)
- Ayarlar paneli (esik degerleri + izlenecek DB secimi)

## 10) Telegram Ozellikleri
Komutlar:
- `/help`, `/listdb`, `/statusdb`
- `/stopdb`, `/startdb`, `/restartdb`
- `/takebackup`, `/check`

Guvenlik:
- Chat ID whitelist zorunlu
- Sistem DB'lere mudahale engeli (`master`, `tempdb`, `model`, `msdb`)

## 11) Performans ve Isletim Notlari
- Kontrol calistirma asenkron oldugu icin Flask worker bloklanmaz.
- Kaynak ve wait trendlerinde anlamli gorunum icin en az 2-3 snapshot gerekir.
- Dashboard auto refresh davranisi UI ve meta refresh ile desteklenir.

## 12) Sorun Giderme
### 12.1 IM002 ODBC Hatasi
Belirti:
`[IM002] Data source name not found and no default driver specified`

Kontrol:
- `DB_DRIVER` degeri sistemdeki driver ismi ile ayni mi?
- Process yeniden baslatildi mi?

### 12.2 UnicodeEncodeError (Windows cp1252)
`run-check` alt process icin UTF-8 zorlandigi halde devam ederse:
- Python ve terminal encoding kontrol edilmeli
- Scriptlerde asiri/bozuk unicode karakter var mi bakilmali

### 12.3 Wait/Kaynak Grafikleri Bos
- En az bir kez `Test.py` calismis olmali
- Trend ve delta icin en az iki snapshot gereklidir

## 13) Gelistirme Yol Haritasi (Oneri)
- Wait type aciklama sozlugu (insan okunur aciklamalar)
- P95/P99 query latency trendleri
- Alarm runbook linkleri
- Multi-instance / environment destegi
- Role-based access control

---
Bu dokuman, DB Monitor'un mevcut kod tabanina gore hazirlanmistir ve yeni ozellik eklendikce guncellenmelidir.
