"""
telegram_listener.py — DB Monitor Çift Yönlü Telegram Bot Dinleyicisi

Bu betik 7/24 arka planda çalışır ve Telegram üzerinden gelen komutlarla
MSSQL veritabanlarını yönetir (Online/Offline/Restart/Status).

Komutlar:
    /stopdb      [db_adı]  → Veritabanını OFFLINE yapar
    /startdb     [db_adı]  → Veritabanını ONLINE yapar
    /restartdb   [db_adı]  → OFFLINE → bekleme → ONLINE (Restart)
    /statusdb    [db_adı]  → Veritabanının mevcut durumunu gösterir
    /listdb                → Tüm veritabanlarını ve durumlarını listeler
    /takebackup  [db_adı]  → Veritabanının yedeğini alır
    /check                 → Anlık sağlık kontrolü tetikler ve skoru gönderir
    /help                  → Kullanılabilir komutları gösterir

Güvenlik:
    Sadece .env dosyasındaki TELEGRAM_CHAT_IDS listesindeki kullanıcılar
    komut çalıştırabilir. Yetkisiz erişim loglanır ve reddedilir.

Gereksinimler:
    pip install pyTelegramBotAPI pyodbc python-dotenv
"""

import os
import time
import logging
import pyodbc
import telebot
import Test
from datetime import datetime
from dotenv import load_dotenv

# ============================================================
# YAPILANDIRMA
# ============================================================

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_IDS_RAW   = os.getenv("TELEGRAM_CHAT_IDS", "")
ALLOWED_CHAT_IDS = {int(cid.strip()) for cid in CHAT_IDS_RAW.split(",") if cid.strip()}

DB_SERVER   = os.getenv("DB_SERVER")
DB_NAME     = os.getenv("DB_NAME", "master")
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DRIVER   = os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")

CONN_STR = (
    f"DRIVER={{{DB_DRIVER}}};"
    f"SERVER={DB_SERVER};"
    f"DATABASE=master;"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
    f"TrustServerCertificate=yes;"
)

# Dokunulması yasak sistem veritabanları
PROTECTED_DBS = {"master", "tempdb", "model", "msdb"}

# ============================================================
# LOGLAMA AYARLARI
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DBMonitorBot")

# ============================================================
# DOĞRULAMA
# ============================================================

if not TELEGRAM_TOKEN:
    raise ValueError("❌ TELEGRAM_TOKEN .env dosyasında tanımlı değil!")
if not ALLOWED_CHAT_IDS:
    raise ValueError("❌ TELEGRAM_CHAT_IDS .env dosyasında tanımlı değil!")
if not all([DB_SERVER, DB_USER, DB_PASSWORD]):
    raise ValueError("❌ Veritabanı bağlantı bilgileri .env dosyasında eksik!")

# ============================================================
# BOT OLUŞTUR
# ============================================================

bot = telebot.TeleBot(TELEGRAM_TOKEN)

# ============================================================
# YARDIMCI FONKSİYONLAR
# ============================================================


def is_authorized(message) -> bool:
    """Mesajı gönderenin yetkili olup olmadığını kontrol eder."""
    chat_id = message.chat.id
    if chat_id in ALLOWED_CHAT_IDS:
        return True

    user = message.from_user
    user_info = f"{user.first_name} {user.last_name or ''} (@{user.username or 'N/A'}, id:{chat_id})"
    logger.warning(f"⛔ YETKİSİZ ERİŞİM DENEMES İ: {user_info} → '{message.text}'")
    bot.reply_to(
        message,
        "⛔ <b>Yetkisiz Erişim</b>\n\n"
        "Bu komutu kullanma yetkiniz bulunmuyor.\n"
        "Bu girişim loglanmıştır.",
        parse_mode="HTML",
    )
    return False


def get_db_connection():
    """MSSQL bağlantısı oluşturur. Hata durumunda None döner."""
    try:
        conn = pyodbc.connect(CONN_STR, timeout=10)
        conn.autocommit = True
        return conn
    except pyodbc.Error as e:
        logger.error(f"❌ MSSQL bağlantı hatası: {e}")
        return None


def validate_db_name(db_name: str) -> str | None:
    """
    Veritabanı adını doğrular.
    - Boş mu?
    - Sistem DB mi?
    - SQL Injection riski var mı?
    Geçerliyse temizlenmiş adı döner, değilse None.
    """
    if not db_name or not db_name.strip():
        return None

    clean = db_name.strip().strip("[]")

    # Basit SQL injection koruması
    dangerous_chars = [";", "--", "'", '"', "/*", "*/", "xp_", "exec", "drop", "delete"]
    for ch in dangerous_chars:
        if ch.lower() in clean.lower():
            return None

    return clean


def is_protected(db_name: str) -> bool:
    """Sistem veritabanlarına müdahaleyi engeller."""
    return db_name.lower() in PROTECTED_DBS


def send_typing(chat_id):
    """Yazıyor... göstergesi gönderir."""
    try:
        bot.send_chat_action(chat_id, "typing")
    except Exception:
        pass


# ============================================================
# BOT KOMUTLARI
# ============================================================

@bot.message_handler(commands=["deneme"])
def deneme(message):
    conn = get_db_connection()
    cursor = conn.cursor()
    bot.reply_to(message,"Deneme", parse_mode="HTML")

@bot.message_handler(commands=["help", "start"])
def cmd_help(message):
    """Kullanılabilir komutları listeler."""
    if not is_authorized(message):
        return

    help_text = (
        "🤖 <b>DB Monitor Bot — Komut Listesi</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>🗄️ Veritabanı Yönetimi</b>\n"
        "🔴 <code>/stopdb [db_adı]</code>\n"
        "    → Veritabanını OFFLINE yapar\n\n"
        "🟢 <code>/startdb [db_adı]</code>\n"
        "    → Veritabanını ONLINE yapar\n\n"
        "🔄 <code>/restartdb [db_adı]</code>\n"
        "    → Veritabanını yeniden başlatır\n\n"
        "<b>📊 İzleme &amp; Sorgulama</b>\n"
        "📊 <code>/statusdb [db_adı]</code>\n"
        "    → Veritabanının detaylı durumunu gösterir\n\n"
        "📋 <code>/listdb</code>\n"
        "    → Tüm veritabanlarını ve durumlarını listeler\n\n"
        "🏥 <code>/check</code>\n"
        "    → Anlık sağlık kontrolü çalıştırır ve skoru gönderir\n\n"
        "<b>💾 Yedekleme</b>\n"
        "💾 <code>/takebackup [db_adı]</code>\n"
        "    → Veritabanının yedeğini alır (C:\\Backups\\)\n\n"
        "<b>ℹ️ Genel</b>\n"
        "❓ <code>/help</code>\n"
        "    → Bu yardım mesajını gösterir\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🖥️ Bağlı Sunucu: <code>{DB_SERVER}</code>\n"
        f"👤 Yetkili Kullanıcı Sayısı: {len(ALLOWED_CHAT_IDS)}"
    )
    bot.reply_to(message, help_text, parse_mode="HTML")
    logger.info(f"ℹ️  /help komutu kullanıldı → chat_id: {message.chat.id}")


@bot.message_handler(commands=["listdb"])
def cmd_listdb(message):
    """Sunucudaki tüm veritabanlarını ve durumlarını listeler."""
    if not is_authorized(message):
        return

    send_typing(message.chat.id)
    conn = get_db_connection()
    if not conn:
        bot.reply_to(message, "❌ Veritabanı sunucusuna bağlanılamadı!", parse_mode="HTML")
        return

    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name, state_desc, CAST(DATABASEPROPERTYEX(name, 'Recovery') AS NVARCHAR(50)) AS RecoveryModel "
            "FROM sys.databases ORDER BY name"
        )
        rows = cursor.fetchall()
        conn.close()

        status_icons = {
            "ONLINE": "🟢",
            "OFFLINE": "🔴",
            "RESTORING": "🟡",
            "RECOVERING": "🟡",
            "SUSPECT": "🔴",
            "EMERGENCY": "🔴",
        }

        lines = []
        for row in rows:
            icon = status_icons.get(row[1], "⚪")
            lines.append(f"  {icon} <code>{row[0]}</code> — {row[1]} ({row[2]})")

        text = (
            f"📋 <b>Veritabanı Listesi</b> — <code>{DB_SERVER}</code>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            + "\n".join(lines)
            + f"\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Toplam: <b>{len(rows)}</b> veritabanı"
        )
        bot.reply_to(message, text, parse_mode="HTML")
        logger.info(f"📋 /listdb → {len(rows)} veritabanı listelendi")

    except pyodbc.Error as e:
        bot.reply_to(message, f"❌ SQL Hatası:\n<code>{e}</code>", parse_mode="HTML")
        logger.error(f"❌ /listdb SQL hatası: {e}")


@bot.message_handler(commands=["statusdb"])
def cmd_statusdb(message):
    """Belirtilen veritabanının durumunu sorgular."""
    if not is_authorized(message):
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        bot.reply_to(
            message,
            "⚠️ <b>Kullanım:</b> <code>/statusdb [veritabani_adi]</code>",
            parse_mode="HTML",
        )
        return

    db_name = validate_db_name(args[1])
    if not db_name:
        bot.reply_to(message, "⚠️ Geçersiz veritabanı adı!", parse_mode="HTML")
        return

    send_typing(message.chat.id)
    conn = get_db_connection()
    if not conn:
        bot.reply_to(message, "❌ Veritabanı sunucusuna bağlanılamadı!", parse_mode="HTML")
        return

    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name, state_desc, create_date, "
            "CAST(DATABASEPROPERTYEX(name, 'Recovery') AS NVARCHAR(50)), "
            "CAST(DATABASEPROPERTYEX(name, 'Collation') AS NVARCHAR(100)) "
            "FROM sys.databases WHERE name = ?",
            (db_name,),
        )
        row = cursor.fetchone()
        conn.close()

        if not row:
            bot.reply_to(
                message,
                f"⚠️ <code>{db_name}</code> adında bir veritabanı bulunamadı!",
                parse_mode="HTML",
            )
            return

        state = row[1]
        icon = {"ONLINE": "🟢", "OFFLINE": "🔴"}.get(state, "🟡")

        text = (
            f"{icon} <b>Veritabanı Durumu</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📛 <b>Ad:</b> <code>{row[0]}</code>\n"
            f"📊 <b>Durum:</b> {state}\n"
            f"📅 <b>Oluşturulma:</b> {row[2]}\n"
            f"♻️ <b>Recovery:</b> {row[3]}\n"
            f"🔤 <b>Collation:</b> {row[4]}"
        )
        bot.reply_to(message, text, parse_mode="HTML")
        logger.info(f"📊 /statusdb {db_name} → {state}")

    except pyodbc.Error as e:
        bot.reply_to(message, f"❌ SQL Hatası:\n<code>{e}</code>", parse_mode="HTML")
        logger.error(f"❌ /statusdb {db_name} SQL hatası: {e}")


@bot.message_handler(commands=["stopdb"])
def cmd_stopdb(message):
    """Belirtilen veritabanını OFFLINE yapar."""
    if not is_authorized(message):
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        bot.reply_to(
            message,
            "⚠️ <b>Kullanım:</b> <code>/stopdb [veritabani_adi]</code>",
            parse_mode="HTML",
        )
        return

    db_name = validate_db_name(args[1])
    if not db_name:
        bot.reply_to(message, "⚠️ Geçersiz veritabanı adı!", parse_mode="HTML")
        return

    if is_protected(db_name):
        bot.reply_to(
            message,
            f"🛡️ <b>Reddedildi!</b>\n<code>{db_name}</code> bir sistem veritabanıdır ve kapatılamaz.",
            parse_mode="HTML",
        )
        logger.warning(f"🛡️ Sistem DB'ye müdahale engellendi: /stopdb {db_name} → chat_id: {message.chat.id}")
        return

    send_typing(message.chat.id)
    user = message.from_user
    user_info = f"{user.first_name} {user.last_name or ''}"

    conn = get_db_connection()
    if not conn:
        bot.reply_to(message, "❌ Veritabanı sunucusuna bağlanılamadı!", parse_mode="HTML")
        return

    try:
        cursor = conn.cursor()

        # Önce mevcut durumu kontrol et
        cursor.execute("SELECT state_desc FROM sys.databases WHERE name = ?", (db_name,))
        row = cursor.fetchone()
        if not row:
            bot.reply_to(message, f"⚠️ <code>{db_name}</code> bulunamadı!", parse_mode="HTML")
            conn.close()
            return

        if row[0] == "OFFLINE":
            bot.reply_to(message, f"ℹ️ <code>{db_name}</code> zaten OFFLINE durumda.", parse_mode="HTML")
            conn.close()
            return

        bot.reply_to(
            message,
            f"⏳ <code>{db_name}</code> kapatılıyor...\nAktif bağlantılar düşürülecek.",
            parse_mode="HTML",
        )

        cursor.execute(f"ALTER DATABASE [{db_name}] SET OFFLINE WITH ROLLBACK IMMEDIATE")
        conn.close()

        bot.send_message(
            message.chat.id,
            f"🔴 <b>Veritabanı Kapatıldı</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📛 <b>DB:</b> <code>{db_name}</code>\n"
            f"📊 <b>Durum:</b> OFFLINE\n"
            f"👤 <b>İşlemi Yapan:</b> {user_info}\n"
            f"🕐 <b>Zaman:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            parse_mode="HTML",
        )
        logger.info(f"🔴 /stopdb {db_name} → OFFLINE (by {user_info})")

    except pyodbc.Error as e:
        bot.reply_to(message, f"❌ <b>SQL Hatası:</b>\n<code>{e}</code>", parse_mode="HTML")
        logger.error(f"❌ /stopdb {db_name} SQL hatası: {e}")


@bot.message_handler(commands=["startdb"])
def cmd_startdb(message):
    """Belirtilen veritabanını ONLINE yapar."""
    if not is_authorized(message):
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        bot.reply_to(
            message,
            "⚠️ <b>Kullanım:</b> <code>/startdb [veritabani_adi]</code>",
            parse_mode="HTML"
        )
        return

    db_name = validate_db_name(args[1])
    if not db_name:
        bot.reply_to(message, "⚠️ Geçersiz veritabanı adı!", parse_mode="HTML")
        return

    if is_protected(db_name):
        bot.reply_to(
            message,
            f"🛡️ <b>Reddedildi!</b>\n<code>{db_name}</code> bir sistem veritabanıdır.",
            parse_mode="HTML",
        )
        return

    send_typing(message.chat.id)
    user = message.from_user
    user_info = f"{user.first_name} {user.last_name or ''}"

    conn = get_db_connection()
    if not conn:
        bot.reply_to(message, "❌ Veritabanı sunucusuna bağlanılamadı!", parse_mode="HTML")
        return

    try:
        cursor = conn.cursor()

        cursor.execute("SELECT state_desc FROM sys.databases WHERE name = ?", (db_name,))
        row = cursor.fetchone()
        if not row:
            bot.reply_to(message, f"⚠️ <code>{db_name}</code> bulunamadı!", parse_mode="HTML")
            conn.close()
            return

        if row[0] == "ONLINE":
            bot.reply_to(message, f"ℹ️ <code>{db_name}</code> zaten ONLINE durumda.", parse_mode="HTML")
            conn.close()
            return

        bot.reply_to(message, f"⏳ <code>{db_name}</code> başlatılıyor...", parse_mode="HTML")

        cursor.execute(f"ALTER DATABASE [{db_name}] SET ONLINE")
        conn.close()

        bot.send_message(
            message.chat.id,
            f"🟢 <b>Veritabanı Başlatıldı</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📛 <b>DB:</b> <code>{db_name}</code>\n"
            f"📊 <b>Durum:</b> ONLINE\n"
            f"👤 <b>İşlemi Yapan:</b> {user_info}\n"
            f"🕐 <b>Zaman:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            parse_mode="HTML",
        )
        logger.info(f"🟢 /startdb {db_name} → ONLINE (by {user_info})")

    except pyodbc.Error as e:
        bot.reply_to(message, f"❌ <b>SQL Hatası:</b>\n<code>{e}</code>", parse_mode="HTML")
        logger.error(f"❌ /startdb {db_name} SQL hatası: {e}")

@bot.message_handler(commands=["takebackup"])
def take_backup(message):


    zaman = datetime.now().strftime("%Y%m%d_%H%M%S")

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        bot.reply_to(
            message,
            "⚠️ <b>Kullanım:</b> <code>/startdb [veritabani_adi]</code>",
            parse_mode="HTML",   
        )
        return

    db_name = validate_db_name(args[1])
    if not db_name:
        bot.reply_to(message,"⚠️ Geçersiz Veritabanı adı!",parse_mode="HTML")
        return
    if is_protected(db_name):
        bot.reply_to(message,"🛡️ <b>Reddedildi!</b>\n<code>{db_name}</code> bir sistem veritabanıdır.",
                     parse_mode="HTML")
        return
    conn = get_db_connection()
    if not conn:
        bot.reply(message,"❌ Veritabanı sunucusuna bağlanılamadı!", parse_mode="HTML")
        return
    try:
        cursor = conn.cursor()
        conn.autocommit = True
        zaman = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_yolu = f"C:\\Backups\\{db_name}_{zaman}.bak"
        bekleme_mesaji = bot.reply_to(message, f"⏳ <b>{db_name}</b> yedekleniyor, lütfen bekleyin...", parse_mode="HTML")

        # 3. SQL yedekleme komutunu çalıştır
        cursor.execute(f"BACKUP DATABASE [{db_name}] TO DISK = '{backup_yolu}' WITH COMPRESSION, INIT")

        # PYODBC HAYAT KURTARAN DOKUNUŞ: 
        # SQL'in ürettiği "Yüzde 10 tamamlandı" gibi mesajları tüketiyoruz ki dosya diske tam yazılabilsin.
        while cursor.nextset():
            pass

        # 4. İşlem bittikten sonra diske gidip dosyanın gerçekten oluştuğunu DOĞRULAYALIM
        if os.path.exists(backup_yolu):
            dosya_boyutu_mb = os.path.getsize(backup_yolu) / (1024 * 1024)
            bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=bekleme_mesaji.message_id,
                text=f"✅ <b>{db_name}</b> başarıyla yedeklendi ve doğrulandı!\n📂 <b>Yol:</b> <code>{backup_yolu}</code>\n📦 <b>Boyut:</b> {dosya_boyutu_mb:.2f} MB",
                parse_mode="HTML"
            )
        else:
            bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=bekleme_mesaji.message_id,
                text=f"❌ SQL hata vermedi ancak yedek dosyası (<code>{backup_yolu}</code>) diskte bulunamadı! (Bot ve SQL aynı sunucuda mı?)",
                parse_mode="HTML"
            )

    except pyodbc.Error as e:
        bot.reply_to(message, f"❌ <b>SQL Hatası:</b>\n<code>{e}</code>", parse_mode="HTML")
        # logger.error(f"❌ /takebackup {db_name} SQL hatası: {e}")

@bot.message_handler(commands=["restartdb"])
def cmd_restartdb(message):
    """Veritabanını yeniden başlatır (OFFLINE → bekleme → ONLINE)."""
    if not is_authorized(message):
        return

    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        bot.reply_to(
            message,
            "⚠️ <b>Kullanım:</b> <code>/restartdb [veritabani_adi]</code>",
            parse_mode="HTML",
        )
        return

    db_name = validate_db_name(args[1])
    if not db_name:
        bot.reply_to(message, "⚠️ Geçersiz veritabanı adı!", parse_mode="HTML")
        return

    if is_protected(db_name):
        bot.reply_to(
            message,
            f"🛡️ <b>Reddedildi!</b>\n<code>{db_name}</code> bir sistem veritabanıdır ve yeniden başlatılamaz.",
            parse_mode="HTML",
        )
        return

    send_typing(message.chat.id)
    user = message.from_user
    user_info = f"{user.first_name} {user.last_name or ''}"

    conn = get_db_connection()
    if not conn:
        bot.reply_to(message, "❌ Veritabanı sunucusuna bağlanılamadı!", parse_mode="HTML")
        return

    try:
        cursor = conn.cursor()

        cursor.execute("SELECT state_desc FROM sys.databases WHERE name = ?", (db_name,))
        row = cursor.fetchone()
        if not row:
            bot.reply_to(message, f"⚠️ <code>{db_name}</code> bulunamadı!", parse_mode="HTML")
            conn.close()
            return

        # Adım 1: OFFLINE
        bot.reply_to(
            message,
            f"🔄 <b>Restart başlatıldı:</b> <code>{db_name}</code>\n\n"
            f"🔴 <b>Adım 1/3:</b> Kapatılıyor...",
            parse_mode="HTML",
        )
        cursor.execute(f"ALTER DATABASE [{db_name}] SET OFFLINE WITH ROLLBACK IMMEDIATE")
        logger.info(f"🔄 /restartdb {db_name} → Adım 1: OFFLINE")

        # Adım 2: Bekleme
        bot.send_message(
            message.chat.id,
            f"⏳ <b>Adım 2/3:</b> 3 saniye bekleniyor...",
            parse_mode="HTML",
        )
        time.sleep(3)

        # Adım 3: ONLINE
        bot.send_message(
            message.chat.id,
            f"🟢 <b>Adım 3/3:</b> Başlatılıyor...",
            parse_mode="HTML",
        )
        cursor.execute(f"ALTER DATABASE [{db_name}] SET ONLINE")
        conn.close()

        bot.send_message(
            message.chat.id,
            f"✅ <b>Veritabanı Yeniden Başlatıldı</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📛 <b>DB:</b> <code>{db_name}</code>\n"
            f"📊 <b>Durum:</b> ONLINE ✅\n"
            f"👤 <b>İşlemi Yapan:</b> {user_info}\n"
            f"🕐 <b>Zaman:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            parse_mode="HTML",
        )
        logger.info(f"✅ /restartdb {db_name} → ONLINE (restart tamamlandı, by {user_info})")

    except pyodbc.Error as e:
        bot.reply_to(message, f"❌ <b>Restart sırasında SQL hatası:</b>\n<code>{e}</code>", parse_mode="HTML")
        logger.error(f"❌ /restartdb {db_name} SQL hatası: {e}")

        # Güvenlik: Hata olursa DB'yi ONLINE yapmaya çalış
        try:
            conn2 = get_db_connection()
            if conn2:
                conn2.cursor().execute(f"ALTER DATABASE [{db_name}] SET ONLINE")
                conn2.close()
                bot.send_message(
                    message.chat.id,
                    f"⚠️ Hata sonrası <code>{db_name}</code> ONLINE duruma geri alındı.",
                    parse_mode="HTML",
                )
                logger.info(f"⚠️ Hata sonrası {db_name} ONLINE'a geri alındı")
        except Exception:
            logger.error(f"❌ {db_name} ONLINE'a geri alınamadı!")

@bot.message_handler(commands=["check"])
def check(message):
    Test.run_health_check_with_score()
    


# ============================================================
# TANINMAYAN KOMUT
# ============================================================


@bot.message_handler(func=lambda msg: True)
def cmd_unknown(message):
    """Tanınmayan mesajlar için yardım yönlendirmesi."""
    if not is_authorized(message):
        return

    bot.reply_to(
        message,
        "❓ Tanınmayan komut.\n\nKullanılabilir komutlar için /help yazın.",
        parse_mode="HTML",
    )


# ============================================================
# ANA BAŞLATMA
# ============================================================

if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("🤖 DB Monitor Telegram Bot başlatılıyor...")
    logger.info(f"🖥️  Sunucu: {DB_SERVER}")
    logger.info(f"👥 Yetkili kullanıcı sayısı: {len(ALLOWED_CHAT_IDS)}")
    logger.info(f"📡 Dinleme başlıyor (polling)...")
    logger.info("=" * 50)

    # Sonsuz döngüde dinleme — bağlantı koparsa otomatik yeniden bağlan
    while True:
        try:
            bot.polling(none_stop=True, interval=1, timeout=30)
        except Exception as e:
            logger.error(f"❌ Bot polling hatası: {e}")
            logger.info("🔄 10 saniye sonra yeniden bağlanılıyor...")
            time.sleep(10)
