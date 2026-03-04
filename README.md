# 🗄️ DB Monitor

MSSQL sunucusunun sağlık durumunu otomatik olarak izleyen, skorlayan ve modern bir web dashboard'unda gösteren kurumsal bir veritabanı izleme aracı.

---

## 📋 İçindekiler

- [Genel Bakış](#genel-bakış)
- [Özellikler](#özellikler)
- [Proje Yapısı](#proje-yapısı)
- [Gereksinimler](#gereksinimler)
- [Kurulum](#kurulum)
- [Kullanım](#kullanım)
- [Sağlık Skoru Hesaplama](#sağlık-skoru-hesaplama)
- [Veritabanı Şeması](#veritabanı-şeması)
- [Dashboard Ekranları](#dashboard-ekranları)

---

## 🎯 Genel Bakış

DB Monitor, hedef MSSQL sunucusuna bağlanarak 10 farklı kritik metriği analiz eder, 100 üzerinden bir **Sağlık Skoru** hesaplar ve tüm verileri yerel bir SQLite veritabanına kaydeder. Flask tabanlı web arayüzü bu verileri canlı olarak görselleştirir.

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

---

## 📁 Proje Yapısı

```
DBMonitor/
├── Test.py               # İzleme motoru (MSSQL → SQLite)
├── app.py                # Flask web sunucusu
├── dbmonitor.sqlite3     # Yerel veri deposu (otomatik oluşur)
├── templates/
│   └── index.html        # Dashboard arayüzü
├── DBvenv/               # Python sanal ortamı
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
pip install flask pyodbc
```

### 4. macOS için ODBC sürücüsünü kur (gerekiyorsa)
```bash
brew install unixodbc
brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew install msodbcsql18
```

### 5. Bağlantı bilgilerini güncelle
`Test.py` dosyasında aşağıdaki satırları kendi sunucu bilgilerinizle değiştirin:
```python
server   = '10.20.2.23'
database = 'master'
username = 'sa'
password = '********'
```

---

## 📖 Kullanım

### İzleme motorunu çalıştır
```bash
python Test.py
```
Her çalıştırmada MSSQL sunucusu analiz edilir ve sonuçlar `dbmonitor.sqlite3` veritabanına kaydedilir.

### Dashboard'u başlat
```bash
python app.py
```
Tarayıcıda **http://127.0.0.1:5050** adresini aç.

### Otomatik zamanlama (isteğe bağlı)
Motoru her 5 dakikada bir otomatik çalıştırmak için `cron` kullanabilirsiniz:
```bash
crontab -e
# Aşağıdaki satırı ekle:
*/5 * * * * /path/to/DBvenv/bin/python /path/to/DBMonitor/Test.py
```

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

## 🔒 Güvenlik Notu

`Test.py` içindeki MSSQL şifresini doğrudan koda yazmak yerine bir `.env` dosyası veya ortam değişkeni kullanmanız önerilir. `.env` dosyasını `.gitignore`'a eklemeyi unutmayın.

---

## 📄 Lisans

MIT License — Detaylar için `LICENSE` dosyasına bakın.