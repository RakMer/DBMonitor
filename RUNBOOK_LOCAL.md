# DBMonitor Local Operasyonel Runbook (Docker Dahil)

Bu kilavuz sadece local calisma ortami icindir.
Kapsam: baglanti kopmasi, Telegram alarm sorunu, Docker tabanli PostgreSQL kontrolleri.

## 1) Hizli Nabiz Kontrolu (2 Dakika)

1. Sanal ortami ac:

```bash
cd /Users/mert/Documents/GitHub/DBMonitor
source DBvenv/bin/activate
```

2. Son loglari kontrol et:

```bash
tail -n 80 logs/app.log
tail -n 80 logs/test.log
tail -n 80 logs/telegram.log
```

3. Tek sefer saglik kontrolu calistir:

```bash
python Test.py
```

4. Dashboard ayakta mi bak:

```bash
python app.py
# Ayrica: http://127.0.0.1:5050
```

## 2) Veritabani Baglantisi Koptugunda Ne Yapmali?

Belirti:
- `python app.py` veya `python Test.py` preflight/connection hatasi verir.
- Dashboard bos gelir veya run-check `failed` doner.

Adimlar:

1. `.env` dogrula:

```bash
grep -E '^(DB_ENGINE|DB_SERVER|DB_PORT|DB_NAME|DB_USER|DB_PASSWORD|DB_DRIVER|POSTGRES_DOCKER|POSTGRES_USE_DOCKER|POSTGRES_DOCKER_CONTAINER|PG_DUMP_BIN)=' .env
```

2. MSSQL ise ODBC driver var mi bak:

```bash
python - <<'PY'
import pyodbc
print(pyodbc.drivers())
PY
```

3. Test scripti ile hatayi yeniden uret:

```bash
python Test.py
```

4. Loglardan kok nedeni cek:

```bash
rg -n "PREFLIGHT|baglan|connect|IM002|timeout|error" logs/*.log
```

5. SQLite dosyasini hangi process kullaniyor bak (kilitlenme suphelerinde):

```bash
# SQLite dosyasini hangi process kullaniyor? (ozellikle Linux)
fuser dbmonitor.sqlite3

# fuser yoksa (macOS/Linux alternatifi)
lsof dbmonitor.sqlite3

# Kilit/WAL dosyalarinin varligini kontrol et
ls -l dbmonitor.sqlite3-*
```

6. Port cakismasi var mi bak (zombi servis):

```bash
# Postgres icin (istenen hizli komut)
lsof -i :5432

# MSSQL icin (istenen hizli komut)
lsof -i :1433

# Daha nokta atisi (sadece LISTEN) filtreli gorunum
# Postgres portunu hangi process dinliyor?
lsof -nP -iTCP:5432 -sTCP:LISTEN

# MSSQL portunu hangi process dinliyor?
lsof -nP -iTCP:1433 -sTCP:LISTEN
```

7. Checklist sorusu: Internet/Wi-Fi degisti mi? (ozellikle PostgreSQL pg_hba.conf icin)

```bash
# macOS: aktif Wi-Fi IP'sini kontrol et
ipconfig getifaddr en0

# Alternatif: tum yerel IP bilgilerini gor
ifconfig | rg "inet "
```

Not:
- Ev/ofis/staj agi degisiminde istemci IP degisir.
- PostgreSQL tarafinda `pg_hba.conf` sadece eski IP'ye izin veriyorsa baglanti kopar.

8. Duzeltme sonrasi sira:

```bash
python Test.py
python app.py
```

Sik lokal nedenler:
- Yanlis `DB_DRIVER` (ozellikle IM002)
- Yanlis host/port
- Yanlis sifre
- Docker container kapali (PostgreSQL)
- SQLite dosyasi baska bir process tarafindan tutuluyor
- 5432/1433 portunu zombi process dinliyor
- Wi-Fi/ag degisimi nedeniyle istemci IP'si degisti (pg_hba.conf uyumsuz)

## 3) Telegram Alarmi Gelmezse Nereye Bakmali?

Belirti:
- Skor dusse bile Telegram mesaji yok.
- `/check` veya `/help` komutuna donus yok.

Adimlar:

1. Telegram ayarlari var mi:

```bash
grep -E '^(TELEGRAM_TOKEN|TELEGRAM_CHAT_IDS|TELEGRAM_THRESHOLD)=' .env
```

2. Listener calisiyor mu:

```bash
python telegram_listener.py
```

3. Whitelist testi:
- Yetkili chat ID'den `/help` gonder.
- Donus yoksa once `logs/telegram.log` bak.

4. Telegram API erisimi testi:

```bash
python - <<'PY'
import os
import requests
token = os.getenv("TELEGRAM_TOKEN", "")
url = f"https://api.telegram.org/bot{token}/getMe"
r = requests.get(url, timeout=10)
print("status:", r.status_code)
print("ok:", r.json().get("ok"))
PY
```

5. Alarm tetikleme kontrolu:
- `TELEGRAM_THRESHOLD` degerini gecici olarak yuksek tut (ornek `100`).
- `python Test.py` calistir.
- `logs/test.log` ve `logs/telegram.log` icinde `TELEGRAM`, `ALERT`, `SEND` kelimelerini ara.

```bash
rg -n "TELEGRAM|ALERT|SEND|chat|unauthorized" logs/test.log logs/telegram.log
```

## 4) Docker Dahil Lokal Kontrol Listesi (PostgreSQL)

1. Container ayakta mi:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

2. Container logu:

```bash
docker logs --since 15m <container_name> | tail -n 120
```

3. Ic baglanti testi:

```bash
docker exec <container_name> pg_isready -U <db_user> -d <db_name>
```

4. `pg_dump` var mi (backup sorunu icin):

```bash
docker exec <container_name> pg_dump --version
```

Not:
- `.env` icinde `POSTGRES_DOCKER=1` ise `POSTGRES_DOCKER_CONTAINER` degeri zorunludur.
- Local mod icin `POSTGRES_DOCKER=0` kullanin.
- Host binary kullaniliyorsa `PG_DUMP_BIN` yolunu net ver.

## 5) Kisa Karar Agaci

1. Checklist: Internet/Wi-Fi degisti mi? (istemci IP degisti ise pg_hba.conf kontrol et).
2. Uygulama acilmiyor: once `python app.py` preflight mesaji + `logs/app.log`.
3. Run-check fail: `python Test.py` + `logs/test.log`.
4. Telegram yok: `python telegram_listener.py` + `logs/telegram.log` + whitelist.
5. PostgreSQL Docker: `docker ps`, `docker logs`, `pg_isready`.

## 6) Lokal Acil Komut Seti

```bash
cd /Users/mert/Documents/GitHub/DBMonitor
source DBvenv/bin/activate

python Test.py
python app.py
python telegram_listener.py

rg -n "ERROR|FAILED|PREFLIGHT|TELEGRAM|IM002|timeout" logs/*.log
```
# Belirli bir analiz döngüsünün tüm hikayesini çek
grep "CID-XXXXXX" logs/*.log