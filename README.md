# 🗄️ DB Monitor

MSSQL sunucusunun sağlık durumunu otomatik olarak izleyen, skorlayan, modern bir web dashboard'unda gösteren ve Telegram üzerinden çift yönlü yönetim imkânı sunan kurumsal bir veritabanı izleme aracı.

---

## 📋 İçindekiler

- [Genel Bakış](#genel-bakış)
- [Özellikler](#özellikler)
- [Proje Yapısı](#proje-yapısı)
- [Gereksinimler](#gereksinimler)
- [Kurulum](#kurulum)
- [Yapılandırma (.env)](#yapılandırma-env)
- [Kullanım](#kullanım)
- [Telegram Bot Komutları](#telegram-bot-komutları)
- [Sağlık Skoru Hesaplama](#sağlık-skoru-hesaplama)
- [Veritabanı Şeması](#veritabanı-şeması)
- [Dashboard Ekranları](#dashboard-ekranları)

---

## 🎯 Genel Bakış

DB Monitor, hedef MSSQL sunucusuna bağlanarak 10 farklı kritik metriği analiz eder, 100 üzerinden bir **Sağlık Skoru** hesaplar ve tüm verileri yerel bir SQLite veritabanına kaydeder. Flask tabanlı web arayüzü bu verileri canlı olarak görselleştirir. Skor belirli bir eşiğin altına düştüğünde Telegram üzerinden bildirim gönderir; aynı zamanda Telegram botu aracılığıyla veritabanlarını uzaktan yönetmeye olanak tanır.

---

## ✨ Özellikler

### 🔍 İzleme Motoru (`Test.py`)
| # | Kontrol | Ceza |
|---|---------|------|
| 1 | SQL Server Agent durumu | -30 puan |
| 2 | Çevrimdışı veritabanları | -20 puan / DB |
| 3 | 24 saatlik yedekleme kontrolü | -50 puan |
| 4 | Disk doluluk oranı (≥%90 kritik, ≥%80 uyarı) | -40 / -10 puan |
| 5 | Memory Pressure (RAM darboğazı) | -20 puan |
| 6 | Blocking sorgular | -10 puan / blok |
| 7 | Sysadmin hesap sayısı ve brute-force tespiti | -10 / -15 puan |
| 8 | Başarısız SQL Agent Job'ları | -15 puan / job |
| 9 | Yanlış Auto Growth ayarları | -10 puan / dosya |
| 10 | Log dosyası doluluk oranı (≥%90) | -30 puan |

### 📊 Web Dashboard (`app.py` + `templates/index.html`)
- **Dinamik Skor Halkası** — Skora göre Yeşil / Sarı / Kırmızı tema
- **Aktif Alarmlar** — Anlık ceza logları listesi
- **Trend Grafiği** — Chart.js ile son 20 kontrolün zaman serisi
- **Geçmiş Tablosu** — Tüm kontrol kayıtları ve ceza detayları
- **Otomatik Yenileme** — 60 saniyede bir sayfa güncellenir

### 🤖 Telegram Entegrasyonu (`telegram_listener.py`)
- **Tek Yönlü Bildirim** — Skor eşik değerinin altına düşünce otomatik alarm mesajı
- **Çift Yönlü Yönetim** — Telegram komutlarıyla veritabanlarını uzaktan yönet
- **Whitelist Güvenliği** — Sadece yetkili Chat ID'ler komut çalıştırabilir
- **Sistem DB Koruması** — `master`, `tempdb`, `model`, `msdb`'ye müdahale engellenir
- **Otomatik Yeniden Bağlanma** — Bağlantı koparsa 10 saniye sonra otomatik başlar

---

## 📁 Proje Yapısı

```
DBMonitor/
├── Test.py                  # İzleme motoru (MSSQL → SQLite + Telegram bildirimi)
├── app.py                   # Flask web sunucusu
├── telegram_listener.py     # Telegram çift yönlü bot dinleyicisi
├── dbmonitor.sqlite3        # Yerel veri deposu (otomatik oluşur)
├── templates/
│   └── index.html           # Dashboard arayüzü
├── .env                     # Bağlantı bilgileri ve Telegram ayarları (git'e eklenmez)
├── .gitignore               # Hassas dosyaların repo dışı tutulması
├── DBvenv/                  # Python sanal ortamı
└── README.md
```

---

## ⚙️ Gereksinimler

- Python 3.10+
- MSSQL Server (ODBC Driver 18 for SQL Server)
- macOS / Linux / Windows

### Python Paketleri
```
flask
pyodbc
python-dotenv
requests
pyTelegramBotAPI
```

### Sistem Gereksinimleri
- **macOS/Linux:** `unixODBC` + `ODBC Driver 18 for SQL Server`
- **Windows:** Microsoft ODBC Driver 18 for SQL Server

---

## 🚀 Kurulum

### 1. Repoyu klonla
```bash
git clone https://github.com/RakMer/DBMonitor.git
cd DBMonitor
```

### 2. Sanal ortam oluştur ve aktif et
```bash
python3 -m venv DBvenv
source DBvenv/bin/activate      # macOS / Linux
# DBvenv\Scripts\activate       # Windows
```

### 3. Bağımlılıkları yükle
```bash
pip install flask pyodbc python-dotenv requests pyTelegramBotAPI
```

### 4. macOS için ODBC sürücüsünü kur (gerekiyorsa)
```bash
brew install unixodbc
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew install msodbcsql18
```

### 5. `.env` dosyasını oluştur
Aşağıdaki şablonu kullanarak `.env` dosyasını doldurun (bkz. [Yapılandırma](#yapılandırma-env)).

---

## 🔧 Yapılandırma (.env)

Proje kök dizininde `.env` dosyası oluşturun:

```env
# MSSQL Bağlantı Bilgileri
DB_SERVER=10.20.2.23
DB_NAME=master
DB_USER=sa
DB_PASSWORD=*****
DB_DRIVER=ODBC Driver 18 for SQL Server

# Telegram Bildirim
TELEGRAM_TOKEN=<BotFather'dan alınan token>
TELEGRAM_CHAT_IDS=111111111,222222222   # Virgülle ayrılmış yetkili Chat ID'ler
TELEGRAM_ALERT_THRESHOLD=70             # Bu skorun altına düşünce bildirim gönderilir
BACKUP_DIR=C:\\Backups                 # /takebackup için yedeklerin yazılacağı klasör (opsiyonel, yoksa C:\Backups kullanılır)
BACKUP_MAX_AGE_HOURS=24                 # Yedek kontrolü için maksimum yaş (saat)
DISK_WARN_PCT=80                        # Disk doluluk uyarı eşiği (%)
DISK_CRIT_PCT=90                        # Disk doluluk kritik eşiği (%)
LOG_USED_CRIT_PCT=80                    # Log dosyası kritik doluluk eşiği (%)
FAILED_LOGIN_ALERT=10                   # Bu sayının üstünde başarısız giriş varsa alarm yaz
FAILED_LOGIN_WINDOW_HOURS=24            # Başarısız giriş sorgu penceresi (saat)
SYSADMIN_MAX_COUNT=2                    # Ek sysadmin sayısı bu değeri aşarsa alarm yaz
```

> ⚠️ `.env` dosyası `.gitignore` tarafından repo dışında tutulmaktadır. Asla commit etmeyin.

---

## 📖 Kullanım

### İzleme motorunu çalıştır
```bash
python Test.py
```
Her çalıştırmada MSSQL sunucusu analiz edilir, sonuçlar `dbmonitor.sqlite3`'e kaydedilir ve skor eşik değerinin altındaysa Telegram bildirimi gönderilir.

### Dashboard'u başlat
```bash
python app.py
```
Tarayıcıda **http://127.0.0.1:5050** adresini aç.

### Telegram bot dinleyicisini başlat
```bash
python telegram_listener.py
```
Bot arka planda çalışır ve Telegram'dan gelen komutları dinler.

### Otomatik zamanlama (Windows — Görev Zamanlayıcı)
`Test.py`'yi her 5 dakikada bir çalıştırmak için Windows Görev Zamanlayıcı'ya ekleyin.

### Otomatik zamanlama (macOS/Linux — cron)
```bash
crontab -e
# Aşağıdaki satırı ekle:
*/5 * * * * /path/to/DBvenv/bin/python /path/to/DBMonitor/Test.py
```

---

## 🤖 Telegram Bot Komutları

Bot yalnızca `.env` dosyasındaki `TELEGRAM_CHAT_IDS` listesindeki kullanıcılardan komut kabul eder.

| Komut | Açıklama |
|-------|----------|
| `/help` | Komut listesini gösterir |
| `/listdb` | Tüm veritabanlarını ve durumlarını listeler |
| `/statusdb [db_adı]` | Belirtilen veritabanının detaylı durumunu gösterir |
| `/stopdb [db_adı]` | Veritabanını OFFLINE yapar |
| `/startdb [db_adı]` | Veritabanını ONLINE yapar |
| `/restartdb [db_adı]` | OFFLINE → 3s bekleme → ONLINE (yeniden başlatır) |
| `/takebackup [db_adı]` | Veritabanının yedeğini alır (varsayılan klasör: C:\Backups) |
| `/check` | Anlık sağlık kontrolünü tetikler ve skoru gönderir |

> 🛡️ `master`, `tempdb`, `model`, `msdb` sistem veritabanlarına tüm komutlar engellenir.

---

## 🏆 Sağlık Skoru Hesaplama

Skor **100** puandan başlar ve tespit edilen her sorun için belirlenen miktarda düşer. Minimum değer **0**'dır.

| Skor Aralığı | Durum | Renk |
|-------------|-------|------|
| 80 – 100 | ✅ Sağlıklı | 🟢 Yeşil |
| 50 – 79 | ⚠️ Uyarı | 🟡 Sarı |
| 0 – 49 | 🚨 Kritik | 🔴 Kırmızı |

---

## 🗃️ Veritabanı Şeması

```sql
-- Her kontrolün ana skor kaydı
CREATE TABLE HealthHistory (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    check_date TEXT,     -- "YYYY-MM-DD HH:MM:SS"
    score      INTEGER   -- 0 ile 100 arası
);

-- O anki skoru düşüren alarmlar
CREATE TABLE PenaltyLog (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    history_id   INTEGER,  -- HealthHistory.id referansı
    penalty_desc TEXT,     -- Ceza açıklaması (örn. "[-30] SQL Agent Çalışmıyor!")
    FOREIGN KEY(history_id) REFERENCES HealthHistory(id)
);
```

---

## 🖥️ Dashboard Ekranları

- **Skor Kartı** — Mevcut skor, durum etiketi ve son kontrol zamanı
- **Durum Özeti** — Kontrol ID, aktif alarm sayısı ve geçmiş kayıt sayısı
- **Aktif Alarmlar** — Ceza yoksa yeşil başarı mesajı, varsa kırmızı liste
- **Trend Grafiği** — Zaman ekseninde skor değişimi; arka planda skor bölgesi renklendirmesi
- **Geçmiş Tablosu** — Tüm kontroller, skorlar, durum etiketleri ve ilgili cezalar

---

## 🔒 Güvenlik

- Tüm hassas bilgiler (şifre, token, IP) `.env` dosyasında tutulur ve **repo'ya eklenmez**
- Telegram botu yalnızca `TELEGRAM_CHAT_IDS`'deki yetkili kullanıcılardan komut kabul eder
- Yetkisiz erişim denemeleri loglanır
- SQL Injection koruması: komut argümanlarında tehlikeli karakterler filtrelenir
- Sistem veritabanlarına (`master`, `tempdb`, `model`, `msdb`) uzaktan müdahale engellenir

---

## 📄 Lisans

MIT License — Detaylar için `LICENSE` dosyasına bakın.