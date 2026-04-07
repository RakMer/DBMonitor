# DBMonitor Project Guidelines

## Code Style
- Python kodu mevcut dosyalardaki sade, fonksiyon odakli stili izler: [app.py](app.py), [Test.py](Test.py), [telegram_listener.py](telegram_listener.py).
- ORM kullanma; proje pyodbc + sqlite3 ile ham SQL kullanir.
- Kucuk yardimci fonksiyonlar tercih et (ornegin bool/env parse veya SQL metni kisaltma).
- Yeni kodda mevcut API alan adlarini ve JSON semasini koru.

## Architecture
- Izleme motoru: [Test.py](Test.py)
: MSSQL metriklerini toplar, skor hesaplar, SQLite'a yazar, esik altinda Telegram bildirimi gonderir.
- Web/API katmani: [app.py](app.py)
: Flask endpointleri ve dashboard verisi; agir kontrol calistirma islemleri arka thread ile yapilir.
- Telegram komut katmani: [telegram_listener.py](telegram_listener.py)
: whitelist tabanli uzaktan DB operasyonlari.
- UI: [templates/index.html](templates/index.html)
: dashboard, grafikler ve istemci tarafi polling.

## Build and Test
- Ortam kurulumu:
  - `python3 -m venv DBvenv`
  - `source DBvenv/bin/activate`
  - `pip install flask pyodbc python-dotenv requests pyTelegramBotAPI`
- Uygulamayi calistirma:
  - `python app.py`
  - `python Test.py`
  - `python telegram_listener.py`
- Yuk/stres testi (yalniz test ortami): `python stress_test.py`

## Conventions
- `.env` zorunludur; `DB_SERVER`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` eksikse calisma hata verir.
- ODBC driver adi sistemde kurulu adla birebir eslesmelidir (`IM002` icin ilk kontrol noktasi).
- `master`, `tempdb`, `model`, `msdb` gibi sistem DB'lerine operasyon acarken koruma mantigini bozma.
- Dashboard tarafinda asenkron run-check akisina uy: tetikleme + durum polling modeli.
- Varsayilan veri deposu `dbmonitor.sqlite3`; tablo ve kolon adlarini migration olmadan degistirme.

## Safety and Secrets
- `.env`, tokenlar, sifreler veya chat id'leri loglama/commit etme.
- `.gitignore` kapsamindaki hassas dosyalari repo disinda tut.

## Docs (Link, Don't Embed)
- Kurulum, kullanim, ozellikler: [README.md](README.md)
- Mimari, endpointler, sorun giderme: [TOOL_DOKUMAN.md](TOOL_DOKUMAN.md)
- Yol haritasi: [TooDo.md](TooDo.md)
