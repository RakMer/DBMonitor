"""
telegram_listener.py — DB Monitor Çift Yönlü Telegram Bot Dinleyicisi

Bu betik 7/24 arka planda çalışır ve Telegram üzerinden gelen komutlarla
MSSQL veya PostgreSQL veritabanlarını yönetir (Online/Offline/Restart/Status).

Komutlar:
    /stopdb      [db_adı]  → Veritabanını OFFLINE yapar
    /startdb     [db_adı]  → Veritabanını ONLINE yapar
    /restartdb   [db_adı]  → OFFLINE → bekleme → ONLINE (Restart)
    /statusdb    [db_adı]  → Veritabanının mevcut durumunu gösterir
    /listdb                → Tüm veritabanlarını ve durumlarını listeler
    /takebackup  [db_adı] [full|diff] → Veritabanının yedeğini alır (Varsayılan: full)
    /check                 → Anlık sağlık kontrolü tetikler ve skoru gönderir
    /help                  → Kullanılabilir komutları gösterir

Güvenlik:
    Sadece .env dosyasındaki TELEGRAM_CHAT_IDS listesindeki kullanıcılar
    komut çalıştırabilir. Yetkisiz erişim loglanır ve reddedilir.

Gereksinimler:
    pip install pyTelegramBotAPI pyodbc psycopg2-binary python-dotenv
"""

import os
import time
import subprocess
import telebot
import Test
from datetime import datetime
from dotenv import load_dotenv
from db_adapters import MSSQLAdapter, PostgresAdapter, get_db_adapter
from log_utils import emit_log, setup_process_logger

# ============================================================
# YAPILANDIRMA
# ============================================================

load_dotenv()
logger = setup_process_logger("telegram")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_IDS_RAW   = os.getenv("TELEGRAM_CHAT_IDS", "")
ALLOWED_CHAT_IDS = {int(cid.strip()) for cid in CHAT_IDS_RAW.split(",") if cid.strip()}

try:
    DB_ADAPTER = get_db_adapter()
except Exception as e:
    raise ValueError(f"❌ Veritabanı adapteri başlatılamadı: {e}") from e

IS_MSSQL = isinstance(DB_ADAPTER, MSSQLAdapter)
IS_POSTGRES = isinstance(DB_ADAPTER, PostgresAdapter)
DB_ENGINE = "mssql" if IS_MSSQL else "postgresql"

DB_SERVER = DB_ADAPTER.server
DB_NAME = DB_ADAPTER.database
DB_USER = DB_ADAPTER.username
DB_PASSWORD = DB_ADAPTER.password
DB_PORT = getattr(DB_ADAPTER, "port", None)

# Dokunulması yasak sistem veritabanları
if IS_MSSQL:
    PROTECTED_DBS = {"master", "tempdb", "model", "msdb"}
else:
    PROTECTED_DBS = {"postgres", "template0", "template1"}

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


def get_db_connection(database_override: str | None = None):
    """Aktif motora gore veritabani baglantisi olusturur. Hata durumunda None doner."""
    try:
        if IS_POSTGRES and database_override:
            adapter = PostgresAdapter(
                server=DB_ADAPTER.server,
                database=database_override,
                username=DB_ADAPTER.username,
                password=DB_ADAPTER.password,
                port=DB_ADAPTER.port,
                connect_timeout=getattr(DB_ADAPTER, "connect_timeout", 10),
            )
            conn = adapter.connect()
        else:
            conn = DB_ADAPTER.connect()

        try:
            conn.autocommit = True
        except Exception:
            pass
        return conn
    except Exception as e:
        logger.error(f"❌ {DB_ENGINE.upper()} bağlantı hatası: {e}")
        return None


def quote_pg_identifier(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def get_postgres_admin_database(target_db: str) -> str:
    preferred = (os.getenv("POSTGRES_MAINTENANCE_DB") or "postgres").strip() or "postgres"
    candidates = [preferred, "postgres", "template1"]
    target_lower = str(target_db or "").lower()
    for candidate in candidates:
        if candidate.lower() != target_lower:
            return candidate
    return "postgres"


def parse_optional_bool_env(*names: str) -> bool | None:
    """Parse optional boolean env values.

    Returns True/False for supported values, None for missing/invalid values.
    """
    true_values = {"1", "true", "yes", "on"}
    false_values = {"0", "false", "no", "off"}

    for name in names:
        raw = os.getenv(name)
        if raw is None:
            continue

        normalized = str(raw).strip().lower()
        if normalized in true_values:
            return True
        if normalized in false_values:
            return False

    return None


def get_postgres_docker_container() -> str:
    """Resolve docker container according to explicit docker/local mode.

    POSTGRES_DOCKER / POSTGRES_USE_DOCKER:
    - 1/true/on  => docker mode enabled
    - 0/false/off => local mode forced
    - missing      => auto mode (container value decides)
    """
    container = (os.getenv("POSTGRES_DOCKER_CONTAINER") or "").strip()
    docker_mode = parse_optional_bool_env("POSTGRES_DOCKER", "POSTGRES_USE_DOCKER")

    if docker_mode is None:
        return container
    if docker_mode is False:
        return ""
    return container


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


def register_bot_commands():
    """Telegram istemcilerinde slash komut önerileri için komut listesini yayınlar."""
    commands = [
        telebot.types.BotCommand("help", "Komut listesini gösterir"),
        telebot.types.BotCommand("listdb", "Tum veritabanlarini listeler"),
        telebot.types.BotCommand("statusdb", "Veritabani durumunu gosterir"),
        telebot.types.BotCommand("stopdb", "Veritabanini OFFLINE yapar"),
        telebot.types.BotCommand("startdb", "Veritabanini ONLINE yapar"),
        telebot.types.BotCommand("restartdb", "Veritabanini yeniden baslatir"),
        telebot.types.BotCommand("takebackup", "Veritabani yedegi alir (full/diff)"),
        telebot.types.BotCommand("check", "Anlik saglik kontrolu yapar"),
    ]
    try:
        # Farkli sohbet tiplerinde (ozel/grup) komut onerilerinin gorunmesi icin tum scope'lara yaz.
        bot.set_my_commands(commands)
        bot.set_my_commands(commands, scope=telebot.types.BotCommandScopeAllPrivateChats())
        bot.set_my_commands(commands, scope=telebot.types.BotCommandScopeAllGroupChats())
        bot.set_my_commands(commands, scope=telebot.types.BotCommandScopeAllChatAdministrators())

        # Mobil istemcilerde komut tusunu zorla gorunur hale getir.
        bot.set_chat_menu_button(menu_button=telebot.types.MenuButtonCommands())
        logger.info("✅ Telegram komut onerileri guncellendi")
    except Exception as e:
        logger.warning(f"⚠️ Telegram komut onerileri guncellenemedi: {e}")


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
        "💾 <code>/takebackup [db_adı] [full|diff]</code>\n"
        "    → Veritabanının yedeğini alır (Tam veya Diferansiyel)\n\n"
        "<b>ℹ️ Genel</b>\n"
        "❓ <code>/help</code>\n"
        "    → Bu yardım mesajını gösterir\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🖥️ Bağlı Sunucu: <code>{DB_SERVER}</code>\n"
        f"🧠 Motor: <code>{DB_ENGINE}</code>\n"
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
    if IS_MSSQL:
        conn = get_db_connection()
    else:
        conn = get_db_connection(get_postgres_admin_database(""))
    if not conn:
        bot.reply_to(message, "❌ Veritabanı yönetim bağlantısı kurulamadı!", parse_mode="HTML")
        return

    try:
        cursor = conn.cursor()
        if IS_MSSQL:
            cursor.execute(
                "SELECT name, state_desc, CAST(DATABASEPROPERTYEX(name, 'Recovery') AS NVARCHAR(50)) AS RecoveryModel "
                "FROM sys.databases ORDER BY name"
            )
        else:
            cursor.execute(
                """
                SELECT
                    datname,
                    CASE
                        WHEN NOT datallowconn THEN 'OFFLINE'
                        WHEN datconnlimit = 0 THEN 'CONNECTION LIMIT 0'
                        ELSE 'ONLINE'
                    END AS state_desc,
                    pg_get_userbyid(datdba) AS owner_name
                FROM pg_database
                WHERE NOT datistemplate
                ORDER BY datname
                """
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

    except Exception as e:
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
    if IS_MSSQL:
        conn = get_db_connection()
    else:
        conn = get_db_connection(get_postgres_admin_database(db_name))
    if not conn:
        bot.reply_to(message, "❌ Veritabanı sunucusuna bağlanılamadı!", parse_mode="HTML")
        return

    try:
        cursor = conn.cursor()
        if IS_MSSQL:
            cursor.execute(
                "SELECT name, state_desc, create_date, "
                "CAST(DATABASEPROPERTYEX(name, 'Recovery') AS NVARCHAR(50)), "
                "CAST(DATABASEPROPERTYEX(name, 'Collation') AS NVARCHAR(100)) "
                "FROM sys.databases WHERE name = ?",
                (db_name,),
            )
        else:
            cursor.execute(
                """
                SELECT
                    datname,
                    CASE
                        WHEN NOT datallowconn THEN 'OFFLINE'
                        WHEN datconnlimit = 0 THEN 'CONNECTION LIMIT 0'
                        ELSE 'ONLINE'
                    END AS state_desc,
                    pg_get_userbyid(datdba) AS owner_name,
                    datconnlimit,
                    datcollate,
                    pg_encoding_to_char(encoding) AS encoding_name
                FROM pg_database
                WHERE datname = %s
                """,
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

        if IS_MSSQL:
            text = (
                f"{icon} <b>Veritabanı Durumu</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📛 <b>Ad:</b> <code>{row[0]}</code>\n"
                f"📊 <b>Durum:</b> {state}\n"
                f"📅 <b>Oluşturulma:</b> {row[2]}\n"
                f"♻️ <b>Recovery:</b> {row[3]}\n"
                f"🔤 <b>Collation:</b> {row[4]}"
            )
        else:
            text = (
                f"{icon} <b>Veritabanı Durumu</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📛 <b>Ad:</b> <code>{row[0]}</code>\n"
                f"📊 <b>Durum:</b> {state}\n"
                f"👤 <b>Sahibi:</b> {row[2]}\n"
                f"🔢 <b>Conn Limit:</b> {row[3]}\n"
                f"🔤 <b>Collation:</b> {row[4]}\n"
                f"🧩 <b>Encoding:</b> {row[5]}"
            )
        bot.reply_to(message, text, parse_mode="HTML")
        logger.info(f"📊 /statusdb {db_name} → {state}")

    except Exception as e:
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

    if IS_MSSQL:
        conn = get_db_connection()
    else:
        conn = get_db_connection(get_postgres_admin_database(db_name))
    if not conn:
        bot.reply_to(message, "❌ Veritabanı sunucusuna bağlanılamadı!", parse_mode="HTML")
        return

    try:
        if IS_MSSQL:
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
        else:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    datallowconn,
                    CASE
                        WHEN NOT datallowconn THEN 'OFFLINE'
                        WHEN datconnlimit = 0 THEN 'CONNECTION LIMIT 0'
                        ELSE 'ONLINE'
                    END AS state_desc
                FROM pg_database
                WHERE datname = %s
                """,
                (db_name,),
            )
            row = cursor.fetchone()
            if not row:
                bot.reply_to(message, f"⚠️ <code>{db_name}</code> bulunamadı!", parse_mode="HTML")
                conn.close()
                return

            if not bool(row[0]):
                bot.reply_to(message, f"ℹ️ <code>{db_name}</code> zaten OFFLINE durumda.", parse_mode="HTML")
                conn.close()
                return

            bot.reply_to(
                message,
                f"⏳ <code>{db_name}</code> kapatılıyor...\nAktif bağlantılar düşürülecek.",
                parse_mode="HTML",
            )

            cursor.execute(f"ALTER DATABASE {quote_pg_identifier(db_name)} WITH ALLOW_CONNECTIONS = false")
            cursor.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()",
                (db_name,),
            )
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

    except Exception as e:
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

    if IS_MSSQL:
        conn = get_db_connection()
    else:
        conn = get_db_connection(get_postgres_admin_database(db_name))
    if not conn:
        bot.reply_to(message, "❌ Veritabanı yönetim bağlantısı kurulamadı!", parse_mode="HTML")
        return

    try:
        if IS_MSSQL:
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
        else:
            cursor = conn.cursor()
            cursor.execute("SELECT datallowconn FROM pg_database WHERE datname = %s", (db_name,))
            row = cursor.fetchone()
            if not row:
                bot.reply_to(message, f"⚠️ <code>{db_name}</code> bulunamadı!", parse_mode="HTML")
                conn.close()
                return

            if bool(row[0]):
                bot.reply_to(message, f"ℹ️ <code>{db_name}</code> zaten ONLINE durumda.", parse_mode="HTML")
                conn.close()
                return

            bot.reply_to(message, f"⏳ <code>{db_name}</code> başlatılıyor...", parse_mode="HTML")
            cursor.execute(f"ALTER DATABASE {quote_pg_identifier(db_name)} WITH ALLOW_CONNECTIONS = true")
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

    except Exception as e:
        bot.reply_to(message, f"❌ <b>SQL Hatası:</b>\n<code>{e}</code>", parse_mode="HTML")
        logger.error(f"❌ /startdb {db_name} SQL hatası: {e}")

@bot.message_handler(commands=["takebackup"])
def take_backup(message):
    if not is_authorized(message):
        return

    args = message.text.split(maxsplit=2)
    if len(args) < 2:
        bot.reply_to(
            message,
            "⚠️ <b>Kullanım:</b> <code>/takebackup [veritabani_adi] [full|diff]</code>",
            parse_mode="HTML",
        )
        return

    db_name = validate_db_name(args[1])
    if not db_name:
        bot.reply_to(message, "⚠️ Geçersiz Veritabanı adı!", parse_mode="HTML")
        return
        
    backup_type = "full"
    if len(args) == 3:
        backup_type = args[2].lower()
        if backup_type not in ["full", "diff"]:
            bot.reply_to(message, "⚠️ Geçersiz yedekleme türü! Lütfen 'full' veya 'diff' yazın.")
            return

    if IS_POSTGRES and backup_type != "full":
        bot.reply_to(
            message,
            "⚠️ PostgreSQL için yalnızca full yedek desteklenir.\nKullanım: <code>/takebackup [db_adı] full</code>",
            parse_mode="HTML",
        )
        return

    if is_protected(db_name):
        bot.reply_to(
            message,
            f"🛡️ <b>Reddedildi!</b>\n<code>{db_name}</code> bir sistem veritabanıdır.",
            parse_mode="HTML",
        )
        return

    backup_dir = os.getenv("BACKUP_DIR", r"/Users/mert/Backups")
    os.makedirs(backup_dir, exist_ok=True)

    conn = None
    if IS_MSSQL:
        conn = get_db_connection()
        if not conn:
            bot.reply_to(message, "❌ Veritabanı sunucusuna bağlanılamadı!", parse_mode="HTML")
            return

    try:
        zaman = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_suffix = "full" if backup_type == "full" else "diff"
        postgres_docker_container = get_postgres_docker_container() if IS_POSTGRES else ""
        pg_dump_bin = (os.getenv("PG_DUMP_BIN") or os.getenv("PG_DUMP") or "pg_dump").strip() or "pg_dump"
        backup_ext = ".bak" if IS_MSSQL else (".sql" if postgres_docker_container else ".backup")
        backup_yolu = os.path.join(backup_dir, f"{db_name}_{backup_suffix}_{zaman}{backup_ext}")

        tur_etiketi = "Tam (Full)" if backup_type == "full" else "Diferansiyel (Diff)"

        bekleme_mesaji = bot.reply_to(
            message,
            f"⏳ <b>{db_name}</b> yedekleniyor ({tur_etiketi}), lütfen bekleyin...",
            parse_mode="HTML",
        )

        if IS_MSSQL:
            cursor = conn.cursor()
            conn.autocommit = True

            # 3. SQL yedekleme komutunu çalıştır
            if backup_type == "full":
                sql_query = f"BACKUP DATABASE [{db_name}] TO DISK = ? WITH COMPRESSION, INIT"
            else:
                sql_query = f"BACKUP DATABASE [{db_name}] TO DISK = ? WITH DIFFERENTIAL, COMPRESSION, INIT"

            cursor.execute(sql_query, (backup_yolu,))

            # SQL'in ürettiği ilerleme mesajlarını tüket
            while cursor.nextset():
                pass

        else:
            if conn:
                conn.close()
            timeout_sec = max(30, int(os.getenv("PG_BACKUP_TIMEOUT_SEC", "600")))
            docker_mode = parse_optional_bool_env("POSTGRES_DOCKER", "POSTGRES_USE_DOCKER")

            if docker_mode is True and not postgres_docker_container:
                raise RuntimeError(
                    "POSTGRES_DOCKER=1 ama POSTGRES_DOCKER_CONTAINER bos. "
                    "Docker modu icin container adini da tanimlayin."
                )

            if postgres_docker_container:
                cmd = ["docker", "exec", "-i"]
                if DB_PASSWORD:
                    cmd.extend(["-e", f"PGPASSWORD={DB_PASSWORD}"])
                cmd.extend([
                    postgres_docker_container,
                    pg_dump_bin,
                    "-U", str(DB_USER),
                    str(db_name),
                ])

                with open(backup_yolu, "wb") as dump_file:
                    result = subprocess.run(
                        cmd,
                        stdout=dump_file,
                        stderr=subprocess.PIPE,
                        timeout=timeout_sec,
                        check=False,
                    )

                if result.returncode != 0:
                    err_text = (result.stderr or b"").decode("utf-8", errors="ignore").strip() or "Bilinmeyen hata"
                    raise RuntimeError(err_text[:500])
            else:
                pg_port = str(DB_PORT or "5432")
                cmd = [
                    pg_dump_bin,
                    "-h", str(DB_SERVER),
                    "-p", pg_port,
                    "-U", str(DB_USER),
                    "-d", str(db_name),
                    "-F", "c",
                    "-f", backup_yolu,
                ]

                env = os.environ.copy()
                env["PGPASSWORD"] = str(DB_PASSWORD or "")
                result = subprocess.run(
                    cmd,
                    env=env,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="ignore",
                    timeout=timeout_sec,
                    check=False,
                )
                if result.returncode != 0:
                    err_text = (result.stderr or result.stdout or "Bilinmeyen hata").strip()
                    raise RuntimeError(err_text[:500])

        # BACKUP komutu hatasiz tamamlandiysa islemi basarili kabul et.
        # Bot ve SQL farkli sunucularda calisiyorsa dosya yolu bot tarafinda gorunmeyebilir.
        size_line = ""
        if os.path.exists(backup_yolu):
            dosya_boyutu_mb = os.path.getsize(backup_yolu) / (1024 * 1024)
            size_line = f"\n📦 <b>Boyut:</b> {dosya_boyutu_mb:.2f} MB"

        bot.edit_message_text(
            chat_id=message.chat.id,
            message_id=bekleme_mesaji.message_id,
            text=(
                f"✅ <b>{db_name}</b> başarıyla yedeklendi!\n"
                f"🛡️ <b>Tür:</b> {tur_etiketi}\n"
                f"📂 <b>Yol:</b> <code>{backup_yolu}</code>{size_line}"
            ),
            parse_mode="HTML",
        )

    except Exception as e:
        bot.reply_to(message, f"❌ <b>Yedekleme Hatası:</b>\n<code>{e}</code>", parse_mode="HTML")
    finally:
        try:
            conn.close()
        except Exception:
            pass

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

    if IS_MSSQL:
        conn = get_db_connection()
    else:
        conn = get_db_connection(get_postgres_admin_database(db_name))
    if not conn:
        bot.reply_to(message, "❌ Veritabanı yönetim bağlantısı kurulamadı!", parse_mode="HTML")
        return

    try:
        if IS_MSSQL:
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
        else:
            cursor = conn.cursor()
            cursor.execute("SELECT datname FROM pg_database WHERE datname = %s", (db_name,))
            row = cursor.fetchone()
            if not row:
                bot.reply_to(message, f"⚠️ <code>{db_name}</code> bulunamadı!", parse_mode="HTML")
                conn.close()
                return

            bot.reply_to(
                message,
                f"🔄 <b>Restart başlatıldı:</b> <code>{db_name}</code>\n\n"
                f"🔴 <b>Adım 1/3:</b> Bağlantılar kapatılıyor...",
                parse_mode="HTML",
            )
            cursor.execute(f"ALTER DATABASE {quote_pg_identifier(db_name)} WITH ALLOW_CONNECTIONS = false")
            cursor.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()",
                (db_name,),
            )
            logger.info(f"🔄 /restartdb {db_name} → Adım 1: ALLOW_CONNECTIONS=FALSE")

            bot.send_message(
                message.chat.id,
                f"⏳ <b>Adım 2/3:</b> 3 saniye bekleniyor...",
                parse_mode="HTML",
            )
            time.sleep(3)

            bot.send_message(
                message.chat.id,
                f"🟢 <b>Adım 3/3:</b> Bağlantılar açılıyor...",
                parse_mode="HTML",
            )
            cursor.execute(f"ALTER DATABASE {quote_pg_identifier(db_name)} WITH ALLOW_CONNECTIONS = true")
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

    except Exception as e:
        bot.reply_to(message, f"❌ <b>Restart sırasında SQL hatası:</b>\n<code>{e}</code>", parse_mode="HTML")
        logger.error(f"❌ /restartdb {db_name} SQL hatası: {e}")

        # Güvenlik: Hata olursa DB'yi ONLINE yapmaya çalış
        try:
            conn2 = get_db_connection(get_postgres_admin_database(db_name) if IS_POSTGRES else None)
            if conn2:
                if IS_MSSQL:
                    conn2.cursor().execute(f"ALTER DATABASE [{db_name}] SET ONLINE")
                else:
                    conn2.cursor().execute(f"ALTER DATABASE {quote_pg_identifier(db_name)} WITH ALLOW_CONNECTIONS = true")
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
    if not is_authorized(message):
        return
    send_typing(message.chat.id)
    bot.reply_to(
        message,
        "⏳ <b>Anlik saglik kontrolu baslatildi.</b>\nSonuc birazdan paylasilacak.",
        parse_mode="HTML",
    )

    try:
        Test.run_health_check_with_score()
        bot.send_message(
            message.chat.id,
            "✅ <b>Saglik kontrolu tamamlandi.</b>\nGuncel skor dashboard ve kayitlara yazildi.",
            parse_mode="HTML",
        )
        logger.info(f"🏥 /check tamamlandi -> chat_id: {message.chat.id}")
    except Exception as e:
        bot.send_message(
            message.chat.id,
            f"❌ <b>Saglik kontrolu calisirken hata olustu:</b>\n<code>{e}</code>",
            parse_mode="HTML",
        )
        logger.error(f"❌ /check hatasi -> chat_id: {message.chat.id}, hata: {e}")
    


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
    emit_log(
        logger,
        "INFO",
        "TELEGRAM_BOT_START",
        "Telegram bot sureci baslatiliyor",
        correlation_id="startup",
        context={"engine": DB_ENGINE, "server": DB_SERVER, "authorized_chat_count": len(ALLOWED_CHAT_IDS)},
    )

    logger.info("=" * 50)
    logger.info("🤖 DB Monitor Telegram Bot başlatılıyor...")
    logger.info(f"🧠 Motor: {DB_ENGINE}")
    logger.info(f"🖥️  Sunucu: {DB_SERVER}")
    logger.info(f"👥 Yetkili kullanıcı sayısı: {len(ALLOWED_CHAT_IDS)}")
    logger.info(f"📡 Dinleme başlıyor (polling)...")
    logger.info("=" * 50)

    register_bot_commands()

    # Sonsuz döngüde dinleme — bağlantı koparsa otomatik yeniden bağlan
    while True:
        try:
            bot.polling(none_stop=True, interval=1, timeout=30)
        except Exception as e:
            logger.error(f"❌ Bot polling hatası: {e}")
            logger.info("🔄 10 saniye sonra yeniden bağlanılıyor...")
            time.sleep(10)
