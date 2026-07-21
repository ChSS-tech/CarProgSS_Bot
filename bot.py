import logging
import re
import traceback
import asyncio
import os
import datetime as dt
from datetime import datetime, timedelta
from collections import defaultdict
import aiosqlite

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, BotCommand, WebAppInfo
from telegram.request import HTTPXRequest
from telegram.error import BadRequest, TimedOut, NetworkError
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ConversationHandler, ContextTypes


# --- НАСТРОЙКИ ---
import os

# ОТЛАДКА: показываем все переменные
print("=== ВСЕ ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ ===")
for key, value in os.environ.items():
    if "TOKEN" in key or "BOT" in key or "MANAGER" in key:
        print(f"{key} = {value[:10]}...")
print("=================================")

# ===== ТОКЕН И ID ПРЯМО В КОДЕ =====
TOKEN = "TELEGRAM_BOT_TOKEN"
MANAGER_CHAT_ID = MANAGER_CHAT_ID
DB_NAME = "appointments.db"

print(f"✅ Токен установлен: {TOKEN[:10]}... (длина: {len(TOKEN)})")
print(f"✅ ID менеджера: {MANAGER_CHAT_ID}")

MAX_REQUESTS_PER_HOUR = int(os.getenv("MAX_REQUESTS_PER_HOUR", "3"))
MAX_REQUESTS_PER_DAY = int(os.getenv("MAX_REQUESTS_PER_DAY", "5"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "10"))
MAX_ACTIVE_CONVERSATIONS = int(os.getenv("MAX_ACTIVE_CONVERSATIONS", "2"))
MAX_PHOTOS = int(os.getenv("MAX_PHOTOS", "5"))
MAX_PHOTO_SIZE = int(os.getenv("MAX_PHOTO_SIZE", str(10 * 1024 * 1024)))
ALLOWED_PHOTO_TYPES = os.getenv("ALLOWED_PHOTO_TYPES", "image/jpeg,image/png,image/webp").split(",")
WORKS_PER_PAGE = int(os.getenv("WORKS_PER_PAGE", "5"))
MAX_WORK_PHOTOS = int(os.getenv("MAX_WORK_PHOTOS", "10"))
CLEANUP_INTERVAL_MINUTES = int(os.getenv("CLEANUP_INTERVAL_MINUTES", "30"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=getattr(logging, LOG_LEVEL)
)
logger = logging.getLogger(__name__)

# --- СОСТОЯНИЯ ---
class States:
    NAME, PHONE, CAR_BRAND, CAR_MODEL, CAR_YEAR, SERVICE, PROBLEM, PHOTO = range(8)
    WORK_TITLE, WORK_DESCRIPTION, WORK_CAR_INFO, WORK_SERVICE_TYPE, WORK_RESULT, WORK_PHOTOS = range(7, 13)

# --- УСЛУГИ ---
SERVICES = {
    "consultation": {"name": "💻 Компьютерная диагностика", "price": "от 1 000 ₽", "desc": "Диагностика кодов ошибки а так же систем автомобиля"},
    "project": {"name": "📈 Чип тюнинг", "price": "от 15 000 ₽", "desc": "Отключение DPF, EGR, Stage1, Euro2"},
    "repair": {"name": "🔧 Ремонт ЭБУ и эл.систем авто", "price": "от 3 000 ₽", "desc": "Ремонт блоков управления и проводки"},
}

STATUSES = {
    'new': '🆕 Новая', 'in_progress': '🔄 В работе', 'confirmed': '✅ Подтверждена',
    'completed': '✔️ Выполнена', 'cancelled': '❌ Отменена'
}

ABOUT_TEXT = (
    "🏢 <b>CarProgSS</b>\n"
    "Профессиональный чип-тюнинг и автоэлектроника.️\n\n"
    "⚡️ Что мы делаем:\n"
    "• Чип-тюнинг (мощность + крутящий момент)\n"
    "• Профессиональная диагностика всех электронных систем\n"
    "• Ремонт ЭБУ и электронных блоков\n"
    "• Глубокая диагностика автоэлектроники (осциллограф, CAN-шина)\n\n"
    "🌿 Отключение экологических систем:\n"
    "• DPF (сажевый фильтр) — программно и физически\n"
    "• EGR (клапан рециркуляции)\n"
    "• AdBlue (система нейтрализации)\n\n"
    "Избавляем от ошибок, аварийного режима и дорогих ремонтов. Работаем чисто, без последствий для мотора.\n\n"
    "💻 Все марки авто. Оборудование премиум-класса. Гарантия.\n\n"
    "📞 Звоните: +7 (979) 064-89-69\n📧 Telegram: @CarProgSS\n\n🚗 Приезжайте — вернем машине динамику и надежность!"
)

# --- БАЗА ДАННЫХ ---
class Database:
    def __init__(self, db_name=None):
        self.db_name = db_name or DB_NAME
        self._lock = None

    async def _get_lock(self):
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def _get_conn(self):
        conn = await aiosqlite.connect(self.db_name)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def init_sync(self):
        import sqlite3
        conn = sqlite3.connect(self.db_name)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        
        # Проверяем существование колонки problem_description
        cursor = conn.execute("PRAGMA table_info(appointments)")
        columns = [row[1] for row in cursor.fetchall()]
        
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER, username TEXT,
                name TEXT NOT NULL, phone TEXT NOT NULL,
                car_brand TEXT, car_model TEXT, car_year TEXT,
                service TEXT, date_time TEXT, photos TEXT,
                problem_description TEXT DEFAULT '',
                status TEXT DEFAULT 'new',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS status_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                appointment_id INTEGER,
                old_status TEXT, new_status TEXT, changed_by INTEGER,
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (appointment_id) REFERENCES appointments (id)
            );
            CREATE TABLE IF NOT EXISTS works (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL, description TEXT,
                car_info TEXT, service_type TEXT, result TEXT, photos TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_published INTEGER DEFAULT 1
            );
        ''')
        
        # Добавляем колонку если её нет
        if 'problem_description' not in columns:
            conn.execute("ALTER TABLE appointments ADD COLUMN problem_description TEXT DEFAULT ''")
            conn.commit()
        
        conn.commit()
        conn.close()

    async def add_appointment(self, user_id: int, username: str, data: dict) -> int:
        lock = await self._get_lock()
        async with lock:
            try:
                conn = await self._get_conn()
                try:
                    cursor = await conn.execute('''
                        INSERT INTO appointments (user_id, username, name, phone, car_brand, car_model, car_year, service, date_time, photos, problem_description)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (user_id, username, data.get('name', ''), data.get('phone', ''),
                          data.get('car_brand', ''), data.get('car_model', ''), data.get('car_year', ''),
                          data.get('service', ''), data.get('date_time', ''),
                          ','.join(data.get('photos', [])),
                          data.get('problem_description', '')))
                    await conn.commit()
                    return cursor.lastrowid
                finally:
                    await conn.close()
            except Exception as e:
                logger.error(f"Ошибка при добавлении заявки: {e}")
                return None

    async def update_status(self, appointment_id: int, new_status: str, changed_by: int) -> bool:
        lock = await self._get_lock()
        async with lock:
            try:
                conn = await self._get_conn()
                try:
                    cursor = await conn.execute('SELECT status FROM appointments WHERE id = ?', (appointment_id,))
                    row = await cursor.fetchone()
                    if not row:
                        return False
                    old_status = row[0]
                    await conn.execute('UPDATE appointments SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?',
                                       (new_status, appointment_id))
                    await conn.execute('INSERT INTO status_history (appointment_id, old_status, new_status, changed_by) VALUES (?, ?, ?, ?)',
                                       (appointment_id, old_status, new_status, changed_by))
                    await conn.commit()
                    return True
                finally:
                    await conn.close()
            except Exception as e:
                logger.error(f"Ошибка при обновлении статуса: {e}")
                return False

    async def get_appointment(self, appointment_id: int) -> dict:
        try:
            conn = await self._get_conn()
            try:
                cursor = await conn.execute('SELECT * FROM appointments WHERE id = ?', (appointment_id,))
                row = await cursor.fetchone()
                if row:
                    result = dict(row)
                    result['photos'] = result['photos'].split(',') if result['photos'] else []
                    return result
                return None
            finally:
                await conn.close()
        except Exception as e:
            logger.error(f"Ошибка при получении заявки: {e}")
            return None

    async def get_appointments(self, status: str = None, limit: int = 10, offset: int = 0) -> list:
        try:
            conn = await self._get_conn()
            try:
                if status:
                    cursor = await conn.execute(
                        'SELECT * FROM appointments WHERE status = ? ORDER BY created_at DESC LIMIT ? OFFSET ?',
                        (status, limit, offset))
                else:
                    cursor = await conn.execute(
                        'SELECT * FROM appointments ORDER BY created_at DESC LIMIT ? OFFSET ?',
                        (limit, offset))
                rows = await cursor.fetchall()
                appointments = [dict(row) for row in rows]
                for app in appointments:
                    app['photos'] = app['photos'].split(',') if app['photos'] else []
                return appointments
            finally:
                await conn.close()
        except Exception as e:
            logger.error(f"Ошибка при получении списка заявок: {e}")
            return []

    async def get_user_appointments(self, user_id: int, limit: int = 5, offset: int = 0) -> list:
        try:
            conn = await self._get_conn()
            try:
                cursor = await conn.execute(
                    'SELECT * FROM appointments WHERE user_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?',
                    (user_id, limit, offset))
                rows = await cursor.fetchall()
                appointments = [dict(row) for row in rows]
                for app in appointments:
                    app['photos'] = app['photos'].split(',') if app['photos'] else []
                return appointments
            finally:
                await conn.close()
        except Exception as e:
            logger.error(f"Ошибка при получении заявок пользователя: {e}")
            return []

    async def get_user_appointments_count(self, user_id: int) -> int:
        try:
            conn = await self._get_conn()
            try:
                cursor = await conn.execute('SELECT COUNT(*) as count FROM appointments WHERE user_id = ?', (user_id,))
                row = await cursor.fetchone()
                return row['count'] if row else 0
            finally:
                await conn.close()
        except Exception as e:
            logger.error(f"Ошибка при подсчете заявок пользователя: {e}")
            return 0

    async def get_statistics(self) -> dict:
        try:
            conn = await self._get_conn()
            try:
                stats = {}
                cursor = await conn.execute('SELECT COUNT(*) as count FROM appointments')
                stats['total'] = (await cursor.fetchone())['count']
                cursor = await conn.execute('SELECT status, COUNT(*) as count FROM appointments GROUP BY status')
                stats['by_status'] = {row['status']: row['count'] for row in await cursor.fetchall()}
                cursor = await conn.execute("SELECT COUNT(*) as count FROM appointments WHERE date(created_at) = date('now')")
                stats['today'] = (await cursor.fetchone())['count']
                cursor = await conn.execute("SELECT COUNT(*) as count FROM appointments WHERE created_at >= datetime('now', '-7 days')")
                stats['week'] = (await cursor.fetchone())['count']
                return stats
            finally:
                await conn.close()
        except Exception as e:
            logger.error(f"Ошибка при получении статистики: {e}")
            return {'total': 0, 'by_status': {}, 'today': 0, 'week': 0}

    async def add_work(self, title: str, description: str, car_info: str = "",
                       service_type: str = "", result: str = "", photos: list = None) -> int:
        lock = await self._get_lock()
        async with lock:
            try:
                conn = await self._get_conn()
                try:
                    cursor = await conn.execute(
                        'INSERT INTO works (title, description, car_info, service_type, result, photos) VALUES (?, ?, ?, ?, ?, ?)',
                        (title, description, car_info, service_type, result,
                         ','.join(photos) if photos else ''))
                    await conn.commit()
                    return cursor.lastrowid
                finally:
                    await conn.close()
            except Exception as e:
                logger.error(f"Ошибка при добавлении работы: {e}")
                return None

    async def get_works(self, limit: int = 5, offset: int = 0, published_only: bool = True) -> list:
        try:
            conn = await self._get_conn()
            try:
                query = 'SELECT * FROM works'
                params = []
                if published_only:
                    query += ' WHERE is_published = 1'
                query += ' ORDER BY created_at DESC LIMIT ? OFFSET ?'
                params.extend([limit, offset])
                cursor = await conn.execute(query, params)
                rows = await cursor.fetchall()
                works = [dict(row) for row in rows]
                for work in works:
                    work['photos'] = work['photos'].split(',') if work['photos'] else []
                return works
            finally:
                await conn.close()
        except Exception as e:
            logger.error(f"Ошибка при получении работ: {e}")
            return []

    async def get_work(self, work_id: int) -> dict:
        try:
            conn = await self._get_conn()
            try:
                cursor = await conn.execute('SELECT * FROM works WHERE id = ?', (work_id,))
                row = await cursor.fetchone()
                if row:
                    work = dict(row)
                    work['photos'] = work['photos'].split(',') if work['photos'] else []
                    return work
                return None
            finally:
                await conn.close()
        except Exception as e:
            logger.error(f"Ошибка при получении работы: {e}")
            return None

    async def get_works_count(self, published_only: bool = True) -> int:
        try:
            conn = await self._get_conn()
            try:
                if published_only:
                    cursor = await conn.execute('SELECT COUNT(*) as count FROM works WHERE is_published = 1')
                else:
                    cursor = await conn.execute('SELECT COUNT(*) as count FROM works')
                return (await cursor.fetchone())['count']
            finally:
                await conn.close()
        except Exception as e:
            logger.error(f"Ошибка при подсчете работ: {e}")
            return 0

    async def update_work(self, work_id: int, **kwargs) -> bool:
        lock = await self._get_lock()
        async with lock:
            try:
                conn = await self._get_conn()
                try:
                    allowed_fields = ['title', 'description', 'car_info', 'service_type', 'result', 'photos', 'is_published']
                    updates = []
                    params = []
                    for key, value in kwargs.items():
                        if key in allowed_fields:
                            if key == 'photos' and isinstance(value, list):
                                value = ','.join(value)
                            updates.append(f"{key} = ?")
                            params.append(value)
                    if updates:
                        params.append(work_id)
                        await conn.execute(f'UPDATE works SET {", ".join(updates)} WHERE id = ?', params)
                        await conn.commit()
                        return True
                    return False
                finally:
                    await conn.close()
            except Exception as e:
                logger.error(f"Ошибка при обновлении работы: {e}")
                return False

    async def delete_work(self, work_id: int) -> bool:
        lock = await self._get_lock()
        async with lock:
            try:
                conn = await self._get_conn()
                try:
                    cursor = await conn.execute('DELETE FROM works WHERE id = ?', (work_id,))
                    await conn.commit()
                    return cursor.rowcount > 0
                finally:
                    await conn.close()
            except Exception as e:
                logger.error(f"Ошибка при удалении работы: {e}")
                return False

# --- АНТИСПАМ ---
class AntiSpam:
    def __init__(self):
        self.request_history = defaultdict(list)
        self.active_conversations = defaultdict(list)
        self.blocked_users = {}
        self._last_cleanup = datetime.now()
        self._cleanup_interval = timedelta(hours=1)

    def _cleanup_old_data(self):
        now = datetime.now()
        for user_id in list(self.request_history.keys()):
            self.request_history[user_id] = [t for t in self.request_history[user_id] if now - t < timedelta(hours=24)]
            if not self.request_history[user_id]:
                del self.request_history[user_id]
        
        for user_id in list(self.active_conversations.keys()):
            self.active_conversations[user_id] = [t for t in self.active_conversations[user_id] if now - t < timedelta(minutes=15)]
            if not self.active_conversations[user_id]:
                del self.active_conversations[user_id]
                
        for user_id in list(self.blocked_users.keys()):
            if now >= self.blocked_users[user_id]:
                del self.blocked_users[user_id]
        self._last_cleanup = now

    def is_blocked(self, user_id: int) -> tuple:
        if user_id in self.blocked_users:
            if datetime.now() < self.blocked_users[user_id]:
                remaining = self.blocked_users[user_id] - datetime.now()
                minutes = int(remaining.total_seconds() / 60)
                return True, f"⏰ Вы заблокированы. Попробуйте через {minutes} мин."
            else:
                del self.blocked_users[user_id]
        return False, ""

    def block_user(self, user_id: int, hours: int = 1):
        self.blocked_users[user_id] = datetime.now() + timedelta(hours=hours)

    def can_make_request(self, user_id: int) -> tuple:
        now = datetime.now()
        if now - self._last_cleanup > self._cleanup_interval:
            self._cleanup_old_data()
        is_blocked, msg = self.is_blocked(user_id)
        if is_blocked:
            return False, msg
            
        self.active_conversations[user_id] = [t for t in self.active_conversations[user_id] if now - t < timedelta(minutes=15)]
        
        self.request_history[user_id] = [t for t in self.request_history[user_id] if now - t < timedelta(hours=24)]
        recent = [t for t in self.request_history[user_id] if now - t < timedelta(hours=1)]
        if len(recent) >= MAX_REQUESTS_PER_HOUR:
            self.block_user(user_id, 1)
            return False, f"🛑 Лимит заявок в час ({MAX_REQUESTS_PER_HOUR}). Блокировка на 1 час."
        today = [t for t in self.request_history[user_id] if t.date() == now.date()]
        if len(today) >= MAX_REQUESTS_PER_DAY:
            self.block_user(user_id, 24)
            return False, f"🛑 Лимит заявок в день ({MAX_REQUESTS_PER_DAY}). Блокировка на 24 часа."
        if self.request_history[user_id]:
            last = max(self.request_history[user_id])
            if now - last < timedelta(minutes=COOLDOWN_MINUTES):
                remaining = timedelta(minutes=COOLDOWN_MINUTES) - (now - last)
                return False, f"⏰ Подождите {int(remaining.total_seconds()/60)+1} мин."
        if len(self.active_conversations[user_id]) >= MAX_ACTIVE_CONVERSATIONS:
            return False, f"🛑 У вас уже {MAX_ACTIVE_CONVERSATIONS} активных сессий записи. Пожалуйста, завершите их или подождите."
        return True, ""

    def add_request(self, user_id: int):
        self.request_history[user_id].append(datetime.now())

    def start_conversation(self, user_id: int):
        self.active_conversations[user_id].append(datetime.now())

    def end_conversation(self, user_id: int):
        if user_id in self.active_conversations and self.active_conversations[user_id]:
            self.active_conversations[user_id].pop(0)
        if user_id in self.active_conversations and not self.active_conversations[user_id]:
            del self.active_conversations[user_id]

    def get_user_stats(self, user_id: int) -> str:
        now = datetime.now()
        recent = len([t for t in self.request_history[user_id] if now - t < timedelta(hours=1)])
        today = len([t for t in self.request_history[user_id] if t.date() == now.date()])
        return f"Заявок за час: {recent}/{MAX_REQUESTS_PER_HOUR}, за день: {today}/{MAX_REQUESTS_PER_DAY}"

    def get_memory_stats(self) -> dict:
        return {
            'history_users': len(self.request_history),
            'history_entries': sum(len(v) for v in self.request_history.values()),
            'active_conversations': sum(len(v) for v in self.active_conversations.values()),
            'blocked_users': len(self.blocked_users)
        }

anti_spam = AntiSpam()
db = Database()

# --- ХЕЛПЕРЫ ---
def get_main_menu_keyboard(user_id: int) -> InlineKeyboardMarkup:
    keyboard = [
        [InlineKeyboardButton("📋 Услуги", callback_data="services")],
        [InlineKeyboardButton("📸 Наши работы", callback_data="works_page_0")],
        [InlineKeyboardButton("📝 Записаться", callback_data="appointment")],
        [InlineKeyboardButton("🗂 Мои заявки", callback_data="client_apps_0")],
        [InlineKeyboardButton("ℹ️ О нас", callback_data="about")],
    ]
    if user_id == MANAGER_CHAT_ID:
        keyboard.append([InlineKeyboardButton("⚙️ Админ-панель", callback_data="admin_panel")])
    return InlineKeyboardMarkup(keyboard)

def get_back_keyboard(callback: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data=callback)]])

def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

async def send_media_group_safe(bot, chat_id: int, photos: list, caption_prefix: str = "📸"):
    if not photos:
        return
    if len(photos) == 1:
        await bot.send_photo(chat_id=chat_id, photo=photos[0], caption=f"{caption_prefix}")
        return
    media_group = []
    for i, photo_id in enumerate(photos):
        media_group.append(InputMediaPhoto(media=photo_id, caption=f"{caption_prefix} {i+1}/{len(photos)}" if i == 0 else ""))
    for attempt in range(MAX_RETRIES):
        try:
            await bot.send_media_group(chat_id=chat_id, media=media_group)
            return
        except (TimedOut, NetworkError) as e:
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(2 ** attempt)
            else:
                raise
        except BadRequest:
            raise

# --- ВАЛИДАЦИЯ ---
def validate_name(name: str) -> tuple:
    name = ' '.join(name.split())
    if not name or len(name) < 2:
        return False, "❌ Имя должно быть не менее 2 символов"
    if len(name) > 100:
        return False, "❌ Имя слишком длинное"
    if not re.match(r'^[а-яА-ЯёЁa-zA-Z\s\-]+$', name):
        return False, "❌ Только буквы, пробелы и дефисы"
    return True, name

def validate_phone(phone: str) -> tuple:
    clean = re.sub(r'[\s\(\)\-]', '', phone)
    if re.match(r'^(\+7|8)?[0-9]{10}$', clean) or re.match(r'^\+[0-9]{7,15}$', clean):
        return True, phone
    return False, "❌ Неверный формат. Пример: +7(999)123-45-67"

def validate_car_info(info: str, field_name: str) -> tuple:
    info = ' '.join(info.split())
    if not info or len(info) > 50:
        return False, f"❌ {field_name} должно быть от 1 до 50 символов"
    if re.search(r'[<>&]', info):
        return False, f"❌ Недопустимые символы"
    return True, info

def validate_car_year(year: str) -> tuple:
    try:
        y = int(year.strip())
        if 1900 <= y <= datetime.now().year + 1:
            return True, str(y)
    except ValueError:
        pass
    return False, f"❌ Введите год числом (1900-{datetime.now().year + 1})"

def validate_photo(size: int, mime: str = None) -> tuple:
    if mime and mime not in ALLOWED_PHOTO_TYPES:
        return False, "❌ Разрешены только JPEG, PNG, WebP"
    if size > MAX_PHOTO_SIZE:
        return False, f"❌ Максимальный размер: {MAX_PHOTO_SIZE // (1024*1024)} МБ"
    return True, ""

# --- ДЕКОРАТОРЫ ---
def check_spam(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        can, msg = anti_spam.can_make_request(user_id)
        if not can:
            if update.callback_query:
                await update.callback_query.answer()
                await update.callback_query.edit_message_text(msg)
            else:
                await update.message.reply_text(msg)
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)
    return wrapper

def admin_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if update.effective_user.id != MANAGER_CHAT_ID:
            if update.callback_query:
                await update.callback_query.answer("❌ Доступ запрещен", show_alert=True)
            else:
                await update.message.reply_text("❌ Нет доступа.")
            return
        return await func(update, context, *args, **kwargs)
    return wrapper

# ========== ОБРАБОТЧИКИ НЕВЕРНОГО ФОРМАТА ==========
async def wrong_photo_format(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg_type = "текст"
    if update.message.sticker: msg_type = "стикер"
    elif update.message.document: msg_type = "документ"
    elif update.message.video: msg_type = "видео"
    elif update.message.voice: msg_type = "голосовое"
    elif update.message.audio: msg_type = "аудио"
    elif update.message.animation: msg_type = "GIF"
    keyboard = []
    if context.user_data.get("photos"):
        keyboard.append([InlineKeyboardButton("✅ Продолжить", callback_data="after_photos")])
    keyboard.append([InlineKeyboardButton("⏭ Пропустить", callback_data="skip_photos")])
    await update.message.reply_text(
        f"❌ Получен {msg_type} вместо фото.\n\nОтправьте <b>фото</b> или нажмите «Пропустить».",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML"
    )

async def wrong_work_photo_format(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg_type = "текст"
    if update.message.sticker: msg_type = "стикер"
    elif update.message.document: msg_type = "документ"
    elif update.message.video: msg_type = "видео"
    elif update.message.voice: msg_type = "голосовое"
    elif update.message.audio: msg_type = "аудио"
    elif update.message.animation: msg_type = "GIF"
    keyboard = [
        [InlineKeyboardButton("✅ Завершить", callback_data="wfin")],
        [InlineKeyboardButton("⏭ Пропустить", callback_data="wskip")],
    ]
    await update.message.reply_text(
        f"❌ Получен {msg_type} вместо фото.\n\nОтправьте <b>фото</b> или завершите добавление.",
        reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML"
    )

# ========== ГЛАВНОЕ МЕНЮ ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        await update.message.reply_text(
            "👋 Привет!\n\nЭто CarProgSS_Bot - твой помощник по ремонту автоэлектроники.\n\n"
            "✅ Чип-тюнинг\n✅ Ремонт ЭБУ и блоков\n✅ Программирование\n✅ Электрика любой сложности\n\n"
            "Выберите действие:",
            reply_markup=get_main_menu_keyboard(update.effective_user.id)
        )

# ========== ПРОСМОТР СВОИХ ЗАЯВОК (ДЛЯ КЛИЕНТА) ==========
async def client_show_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    
    page = int(query.data.replace("client_apps_", ""))
    appointments = await db.get_user_appointments(user_id, limit=5, offset=page * 5)
    total = await db.get_user_appointments_count(user_id)
    pages = max(1, (total + 4) // 5)
    
    if not appointments:
        await query.edit_message_text(
            "🗂 <b>Мои заявки</b>\n\nУ вас пока нет активных или прошлых заявок.\nВы можете создать новую, выбрав пункт «Записаться» в меню.",
            reply_markup=get_back_keyboard("back_main"),
            parse_mode="HTML"
        )
        return
        
    text = f"🗂 <b>Ваши заявки</b> (Страница {page + 1} из {pages})\n\n"
    keyboard = []
    
    for app in appointments:
        emoji = STATUSES.get(app['status'], app['status'])
        text += (
            f"<b>Заявка #{app['id']}</b> — {emoji}\n"
            f"🛠 Услуга: {escape_html(app['service'])}\n"
            f"📅 От: {app['created_at'][:16]}\n"
            f"━━━━━━━━━━━━\n"
        )
        keyboard.append([InlineKeyboardButton(f"🔎 Детали заявки #{app['id']}", callback_data=f"capp_view_{app['id']}_{page}")])
        
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️", callback_data=f"client_apps_{page-1}"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton("▶️", callback_data=f"client_apps_{page+1}"))
    if nav:
        keyboard.append(nav)
        
    keyboard.append([InlineKeyboardButton("🔙 В главное меню", callback_data="back_main")])
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def client_view_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    parts = query.data.split("_")
    aid = int(parts[2])
    return_page = int(parts[3])
    
    app = await db.get_appointment(aid)
    
    if not app or app['user_id'] != update.effective_user.id:
        await query.answer("❌ Заявка не найдена или доступ запрещен", show_alert=True)
        return
        
    emoji = STATUSES.get(app['status'], app['status'])
    problem = app.get('problem_description', '')
    
    text = (
        f"📝 <b>Детали вашей заявки #{app['id']}</b>\n\n"
        f"📊 Текущий статус: <b>{emoji}</b>\n"
        f"👤 Имя в заявке: {escape_html(app['name'])}\n"
        f"📞 Телефон: {escape_html(app['phone'])}\n"
        f"🚗 Автомобиль: {escape_html(app.get('car_brand','—'))} {escape_html(app.get('car_model','—'))} ({app.get('car_year','—')} г.)\n"
        f"🛠 Выбранная услуга: {escape_html(app.get('service','—'))}\n"
        f"📝 <b>Проблема:</b> {escape_html(problem) if problem else 'Не указана'}\n"
        f"📅 Создана: {app['created_at'][:16]}\n"
        f"📸 Прикреплено фото: {len(app.get('photos', []))} шт.\n"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 Назад к списку", callback_data=f"client_apps_{return_page}")]]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

# ========== АДМИН-ПАНЕЛЬ ==========
@admin_only
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    stats = await db.get_statistics()
    text = (
        "⚙️ <b>Админ-панель</b>\n\n"
        f"📊 Всего заявок: {stats['total']}\n📅 За сегодня: {stats['today']}\n📈 За неделю: {stats['week']}\n\n"
        "<b>По статусам:</b>\n"
    )
    for status, count in stats.get('by_status', {}).items():
        text += f"{STATUSES.get(status, status)}: {count}\n"
    keyboard = [
        [InlineKeyboardButton("📋 Все заявки", callback_data="admin_all")],
        [InlineKeyboardButton("📸 Управление работами", callback_data="admin_works_menu")],
        [InlineKeyboardButton("🆕 Новые", callback_data="admin_status_new")],
        [InlineKeyboardButton("🔄 В работе", callback_data="admin_status_in_progress")],
        [InlineKeyboardButton("✅ Подтвержденные", callback_data="admin_status_confirmed")],
        [InlineKeyboardButton("✔️ Выполненные", callback_data="admin_status_completed")],
        [InlineKeyboardButton("❌ Отмененные", callback_data="admin_status_cancelled")],
        [InlineKeyboardButton("🔙 В главное меню", callback_data="back_main")],
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

@admin_only
async def admin_show_appointments(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    status_filter = None
    title = "📋 Все заявки"
    if data.startswith("admin_status_"):
        status_filter = data.replace("admin_status_", "")
        title = f"Заявки: {STATUSES.get(status_filter, status_filter)}"
    elif data.startswith("admin_page_"):
        parts = data.split("_")
        page = int(parts[2])
        raw_status = "_".join(parts[3:])
        status_filter = raw_status if raw_status != "None" else context.user_data.get("admin_filter")
        title = context.user_data.get("admin_title", "Заявки")
        context.user_data["admin_page"] = page
    else:
        context.user_data["admin_page"] = 0
    context.user_data["admin_filter"] = status_filter
    context.user_data["admin_title"] = title
    page = context.user_data.get("admin_page", 0)
    appointments = await db.get_appointments(status=status_filter, limit=5, offset=page * 5)
    stats = await db.get_statistics()
    total = stats['by_status'].get(status_filter, 0) if status_filter else stats['total']
    if not appointments:
        await query.edit_message_text(f"{title}\n\nПока нет заявок.", reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("🔙 К админ-панели", callback_data="admin_panel")
        ]]))
        return
    text = f"{title}\n\nСтраница {page + 1} из {max(1, (total + 4) // 5)}\n\n"
    keyboard = []
    for app in appointments:
        emoji = STATUSES.get(app['status'], app['status'])
        text += f"<b>#{app['id']}</b> {emoji}\n👤 {escape_html(app['name'])}\n📞 {escape_html(app['phone'])}\n━━━━━━━━━━━━\n"
        keyboard.append([InlineKeyboardButton(f"📝 Заявка #{app['id']}", callback_data=f"av_{app['id']}")])
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️", callback_data=f"admin_page_{page-1}_{status_filter or 'None'}"))
    if len(appointments) == 5:
        nav.append(InlineKeyboardButton("▶️", callback_data=f"admin_page_{page+1}_{status_filter or 'None'}"))
    if nav:
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🔙 К админ-панели", callback_data="admin_panel")])
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def show_appointment_details(update: Update, context: ContextTypes.DEFAULT_TYPE, appointment: dict):
    query = update.callback_query
    emoji = STATUSES.get(appointment['status'], appointment['status'])
    problem = appointment.get('problem_description', '')
    
    text = (
        f"📝 <b>Заявка #{appointment['id']}</b>\nСтатус: {emoji}\n\n"
        f"👤 Имя: {escape_html(appointment['name'])}\n📞 Телефон: {escape_html(appointment['phone'])}\n"
        f"🚗 Авто: {escape_html(appointment.get('car_brand','—'))} {escape_html(appointment.get('car_model','—'))} ({appointment.get('car_year','—')} г.)\n"
        f"🛠 Услуга: {escape_html(appointment.get('service','—'))}\n"
        f"📝 <b>Проблема:</b> {escape_html(problem) if problem else 'Не указана'}\n"
        f"📅 Дата: {escape_html(appointment.get('date_time','Не указана'))}\n"
        f"📸 Фото: {len(appointment.get('photos',[]))} шт.\n"
        f"📅 Создана: {appointment['created_at'][:16] if appointment['created_at'] else '?'}\n"
    )
    if appointment.get('username'):
        text += f"👤 @{escape_html(appointment['username'])}\n"
    keyboard = []
    status_btns = []
    for status, e in STATUSES.items():
        if status != appointment['status']:
            status_btns.append(InlineKeyboardButton(e, callback_data=f"acs_{appointment['id']}_{status}"))
    for i in range(0, len(status_btns), 2):
        keyboard.append(status_btns[i:i+2])
    if appointment.get('photos'):
        keyboard.append([InlineKeyboardButton("📸 Показать фото", callback_data=f"aph_{appointment['id']}")])
    keyboard.append([
        InlineKeyboardButton("🔙 К списку", callback_data="abl"),
        InlineKeyboardButton("🏠 В админ-панель", callback_data="admin_panel")
    ])
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

@admin_only
async def admin_view_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    aid = int(query.data.replace("av_", ""))
    app = await db.get_appointment(aid)
    if app:
        await show_appointment_details(update, context, app)
    else:
        await query.answer("Заявка не найдена", show_alert=True)

@admin_only
async def admin_change_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    parts = query.data.split("_")
    aid = int(parts[1])
    new_status = "_".join(parts[2:])
    if await db.update_status(aid, new_status, update.effective_user.id):
        await query.answer(f"Статус изменен на {STATUSES.get(new_status, new_status)}", show_alert=True)
        app = await db.get_appointment(aid)
        if app:
            await show_appointment_details(update, context, app)
            
            client_id = app.get('user_id')
            if client_id:
                status_text = STATUSES.get(new_status, new_status)
                client_message = (
                    f"🔔 <b>Обновление по вашей заявке #{aid}!</b>\n\n"
                    f"🚗 Автомобиль: {escape_html(app.get('car_brand', ''))} {escape_html(app.get('car_model', ''))}\n"
                    f"📊 Текущий статус: <b>{status_text}</b>\n\n"
                    f"Спасибо, что выбрали CarProgSS! Если у вас возникли вопросы, мы на связи. 🙌"
                )
                try:
                    await context.bot.send_message(
                        chat_id=client_id,
                        text=client_message,
                        parse_mode="HTML"
                    )
                    logger.info(f"Уведомление о статусе {new_status} отправлено клиенту {client_id}")
                except Exception as e:
                    logger.error(f"Не удалось отправить уведомление клиенту {client_id}: {e}")
    else:
        await query.answer("Ошибка при изменении статуса", show_alert=True)

@admin_only
async def admin_show_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    aid = int(query.data.replace("aph_", ""))
    app = await db.get_appointment(aid)
    if not app or not app.get('photos'):
        await query.answer("Фото не найдены", show_alert=True)
        return
    try:
        await send_media_group_safe(context.bot, update.effective_chat.id, app['photos'], f"📸 Фото к заявке #{aid}")
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text="👆 Фото выше",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 Назад к заявке", callback_data=f"av_{aid}")
            ]])
        )
    except Exception as e:
        logger.error(f"Ошибка при отправке фото: {e}")
        await query.answer("❌ Ошибка при загрузке фото", show_alert=True)

@admin_only
async def admin_back_to_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data["admin_page"] = 0
    await admin_show_appointments(update, context)

@admin_only
async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = await db.get_statistics()
    text = (
        "📊 <b>Статистика</b>\n\n"
        f"📋 Всего заявок: {stats['total']}\n📅 За сегодня: {stats['today']}\n📈 За неделю: {stats['week']}\n\n"
        f"🚫 Заблокировано: {len(anti_spam.blocked_users)}\n"
        f"📝 Активных диалогов: {sum(len(v) for v in anti_spam.active_conversations.values())}"
    )
    await update.message.reply_text(text, parse_mode="HTML")

@admin_only
async def memory_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stats = anti_spam.get_memory_stats()
    text = (
        f"📊 <b>Память AntiSpam</b>\n\n"
        f"👥 История: {stats['history_users']} пользователей ({stats['history_entries']} записей)\n"
        f"🔄 Активных сессий: {stats['active_conversations']}\n🚫 Заблокировано: {stats['blocked_users']}"
    )
    await update.message.reply_text(text, parse_mode="HTML")

# ========== УСЛУГИ / О НАС ==========
async def show_services(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = [[InlineKeyboardButton(s["name"], callback_data=f"sv_{k}")] for k, s in SERVICES.items()]
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_main")])
    await query.edit_message_text("📋 <b>Наши услуги:</b>", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def show_service_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    key = query.data.replace("sv_", "")
    s = SERVICES.get(key)
    if s:
        text = f"<b>{s['name']}</b>\n💰 Цена: {s['price']}\n📝 {s['desc']}"
        keyboard = [
            [InlineKeyboardButton("📝 Записаться", callback_data=f"appoint_{key}")],
            [InlineKeyboardButton("🔙 К услугам", callback_data="services")],
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def show_about(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(ABOUT_TEXT, reply_markup=get_back_keyboard("back_main"), parse_mode="HTML")

# ========== НАШИ РАБОТЫ (КЛИЕНТ) ==========
async def show_works(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.replace("works_page_", "")) if query.data.startswith("works_page_") else 0
    works = await db.get_works(limit=WORKS_PER_PAGE, offset=page * WORKS_PER_PAGE)
    total = await db.get_works_count()
    pages = max(1, (total + WORKS_PER_PAGE - 1) // WORKS_PER_PAGE)
    if not works:
        await query.edit_message_text("📸 Пока нет работ.", reply_markup=get_back_keyboard("back_main"), parse_mode="HTML")
        return
    text = f"📸 <b>Наши работы</b> ({page + 1}/{pages})\n\n"
    keyboard = []
    for w in works:
        text += f"🔧 <b>{escape_html(w['title'])}</b>\n🚗 {escape_html(w.get('car_info',''))}\n📅 {w['created_at'][:10]}\n\n"
        keyboard.append([InlineKeyboardButton(f"📋 {w['title'][:30]}", callback_data=f"wd_{w['id']}")])
    nav = []
    if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"works_page_{page-1}"))
    if page < pages - 1: nav.append(InlineKeyboardButton("▶️", callback_data=f"works_page_{page+1}"))
    if nav: keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🔙 В главное меню", callback_data="back_main")])
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def show_work_detail(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    w = await db.get_work(int(query.data.replace("wd_", "")))
    if not w:
        await query.answer("Работа не найдена", show_alert=True)
        return
    text = (
        f"📸 <b>{escape_html(w['title'])}</b>\n\n"
        f"🚗 {escape_html(w.get('car_info','—'))}\n🛠 {escape_html(w.get('service_type','—'))}\n📅 {w['created_at'][:10]}\n\n"
        f"📝 {escape_html(w.get('description','—'))}\n\n✅ {escape_html(w.get('result','—'))}\n\n📸 Фото: {len(w['photos'])} шт."
    )
    keyboard = [[InlineKeyboardButton("🔙 К списку", callback_data="works_page_0")]]
    if w['photos']:
        keyboard.insert(0, [InlineKeyboardButton(f"📸 Фото ({len(w['photos'])})", callback_data=f"wp_{w['id']}")])
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def show_work_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    w = await db.get_work(int(query.data.replace("wp_", "")))
    if not w or not w['photos']:
        await query.answer("Фото не найдены", show_alert=True)
        return
    try:
        await send_media_group_safe(context.bot, update.effective_chat.id, w['photos'], f"📸 {w['title']}")
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text="👆 Фото работы выше",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🔙 К описанию", callback_data=f"wd_{w['id']}"),
                InlineKeyboardButton("📋 К списку", callback_data="works_page_0")
            ]])
        )
    except Exception as e:
        logger.error(f"Ошибка при отправке фото: {e}")
        await query.answer("❌ Ошибка при загрузке", show_alert=True)

# ========== АДМИН: РАБОТЫ ==========
@admin_only
async def admin_works_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    total = await db.get_works_count(False)
    keyboard = [
        [InlineKeyboardButton("📋 Все работы", callback_data="awlist_0")],
        [InlineKeyboardButton("➕ Добавить работу", callback_data="awadd")],
        [InlineKeyboardButton("🔙 К админ-панели", callback_data="admin_panel")]
    ]
    await query.edit_message_text(f"⚙️ <b>Управление работами</b>\n\n📸 Всего: {total}", reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

@admin_only
async def admin_works_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.replace("awlist_", ""))
    works = await db.get_works(limit=WORKS_PER_PAGE, offset=page * WORKS_PER_PAGE, published_only=False)
    total = await db.get_works_count(False)
    pages = max(1, (total + WORKS_PER_PAGE - 1) // WORKS_PER_PAGE)
    if not works:
        await query.edit_message_text("📸 Нет работ.", reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("➕ Добавить", callback_data="awadd"), InlineKeyboardButton("🔙 Назад", callback_data="admin_works_menu")
        ]]))
        return
    text = f"📸 <b>Все работы</b> ({page + 1}/{pages})\n\n"
    keyboard = []
    for w in works:
        s = "✅" if w['is_published'] else "👁"
        text += f"{s} {w['title'][:40]}\n"
        keyboard.append([InlineKeyboardButton(f"{s} {w['title'][:30]}", callback_data=f"awe_{w['id']}")])
    nav = []
    if page > 0: nav.append(InlineKeyboardButton("◀️", callback_data=f"awlist_{page-1}"))
    if page < pages - 1: nav.append(InlineKeyboardButton("▶️", callback_data=f"awlist_{page+1}"))
    if nav: keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("➕ Добавить", callback_data="awadd"), InlineKeyboardButton("🔙 Назад", callback_data="admin_works_menu")])
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

async def show_work_edit_details(update: Update, context: ContextTypes.DEFAULT_TYPE, work: dict):
    query = update.callback_query
    s = "✅ Опубликована" if work['is_published'] else "👁 Скрыта"
    text = (
        f"📸 <b>{work['title']}</b>\n\n🚗 {work.get('car_info','—')}\n🛠 {work.get('service_type','—')}\n"
        f"📸 Фото: {len(work.get('photos',[]))} шт.\n📊 {s}\n\n📝 {(work.get('description','—'))[:100]}..."
    )
    keyboard = [
        [InlineKeyboardButton("📸 Фото", callback_data=f"awps_{work['id']}")],
        [InlineKeyboardButton("👁 Скрыть" if work['is_published'] else "✅ Опубликовать", callback_data=f"awt_{work['id']}")],
        [InlineKeyboardButton("🗑 Удалить", callback_data=f"awd_{work['id']}")],
        [InlineKeyboardButton("🔙 К списку", callback_data="awlist_0")]
    ]
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")

@admin_only
async def admin_work_edit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    w = await db.get_work(int(query.data.replace("awe_", "")))
    if w: await show_work_edit_details(update, context, w)
    else: await query.answer("Работа не найдена", show_alert=True)

@admin_only
async def admin_work_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    wid = int(query.data.replace("awt_", ""))
    w = await db.get_work(wid)
    if w:
        await db.update_work(wid, is_published=not w['is_published'])
        await query.answer(f"Работа {'опубликована' if not w['is_published'] else 'скрыта'}!", show_alert=True)
        await show_work_edit_details(update, context, await db.get_work(wid))

@admin_only
async def admin_work_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if await db.delete_work(int(query.data.replace("awd_", ""))):
        await query.answer("Удалена!", show_alert=True)
        await admin_works_list(update, context)
    else:
        await query.answer("Ошибка при удалении", show_alert=True)

@admin_only
async def admin_work_photos_show(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    w = await db.get_work(int(query.data.replace("awps_", "")))
    if not w or not w['photos']:
        await query.answer("Фото не найдены", show_alert=True)
        return
    try:
        await send_media_group_safe(context.bot, update.effective_chat.id, w['photos'], f"📸 {w['title']}")
        await context.bot.send_message(
            chat_id=update.effective_chat.id, text="👆 Фото работы выше",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 К работе", callback_data=f"awe_{w['id']}")]])
        )
    except Exception as e:
        logger.error(f"Ошибка при отправке фото: {e}")
        await query.answer("❌ Ошибка при загрузке", show_alert=True)

# ========== ДОБАВЛЕНИЕ РАБОТЫ ==========
@admin_only
async def admin_work_add_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['new_work'] = {'photos': []}
    await query.edit_message_text("📸 <b>Новая работа</b>\n\nВведите название:", parse_mode="HTML")
    return States.WORK_TITLE

async def work_get_title(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t = update.message.text.strip()
    if len(t) < 3:
        await update.message.reply_text("❌ Минимум 3 символа:")
        return States.WORK_TITLE
    context.user_data['new_work']['title'] = t
    await update.message.reply_text("📝 Введите описание:")
    return States.WORK_DESCRIPTION

async def work_get_description(update: Update, context: ContextTypes.DEFAULT_TYPE):
    d = update.message.text.strip()
    if not d:
        await update.message.reply_text("❌ Не может быть пустым:")
        return States.WORK_DESCRIPTION
    context.user_data['new_work']['description'] = d
    await update.message.reply_text("🚗 Информация об авто (или '-'):")
    return States.WORK_CAR_INFO

async def work_get_car_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    v = update.message.text.strip()
    context.user_data['new_work']['car_info'] = '' if v == '-' else v
    await update.message.reply_text("🛠 Тип услуги (или '-'):")
    return States.WORK_SERVICE_TYPE

async def work_get_service_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    v = update.message.text.strip()
    context.user_data['new_work']['service_type'] = '' if v == '-' else v
    await update.message.reply_text("✅ Результат (или '-'):")
    return States.WORK_RESULT

async def work_get_result(update: Update, context: ContextTypes.DEFAULT_TYPE):
    v = update.message.text.strip()
    context.user_data['new_work']['result'] = '' if v == '-' else v
    keyboard = [
        [InlineKeyboardButton("⏭ Пропустить", callback_data="wskip")],
        [InlineKeyboardButton("✅ Завершить без фото", callback_data="wfin_nophoto")]
    ]
    await update.message.reply_text(f"📸 Отправьте фото (до {MAX_WORK_PHOTOS} шт.) или нажмите кнопку:", reply_markup=InlineKeyboardMarkup(keyboard))
    return States.WORK_PHOTOS

async def work_handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo:
        return await wrong_work_photo_format(update, context)
    nw = context.user_data.get('new_work', {})
    photos = nw.get('photos', [])
    if len(photos) >= MAX_WORK_PHOTOS:
        await update.message.reply_text(f"❌ Максимум {MAX_WORK_PHOTOS} фото.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ Завершить", callback_data="wfin")]]))
        return States.WORK_PHOTOS
    photos.append(update.message.photo[-1].file_id)
    nw['photos'] = photos
    context.user_data['new_work'] = nw
    await update.message.reply_text(
        f"📸 Фото {len(photos)}/{MAX_WORK_PHOTOS}. Отправьте ещё или завершите:",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Завершить", callback_data="wfin"),
            InlineKeyboardButton("⏭ Пропустить", callback_data="wskip")
        ]])
    )
    return States.WORK_PHOTOS

async def work_skip_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    await work_save_and_finish(update, context)

async def work_finish_no_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    context.user_data['new_work']['photos'] = []
    await work_save_and_finish(update, context)

async def work_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    await work_save_and_finish(update, context)

async def work_save_and_finish(update: Update, context: ContextTypes.DEFAULT_TYPE):
    nw = context.user_data.get('new_work', {})
    if not nw.get('title'):
        msg = update.callback_query.message if update.callback_query else update.message
        await msg.reply_text("❌ Нет названия")
        return ConversationHandler.END
    wid = await db.add_work(**{k: nw.get(k, '') for k in ['title', 'description', 'car_info', 'service_type', 'result']}, photos=nw.get('photos', []))
    if wid:
        text = f"✅ <b>Работа #{wid} добавлена!</b>\n📸 {escape_html(nw['title'])}\n🚗 {escape_html(nw.get('car_info','—'))}\n📸 Фото: {len(nw.get('photos',[]))} шт."
        keyboard = [[InlineKeyboardButton("📋 К списку", callback_data="awlist_0"), InlineKeyboardButton("➕ Ещё", callback_data="awadd")]]
    else:
        text = "❌ Ошибка при сохранении"
        keyboard = [[InlineKeyboardButton("🔙 К списку", callback_data="awlist_0")]]
    msg = update.callback_query.message if update.callback_query else update.message
    await msg.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="HTML")
    context.user_data.pop('new_work', None)
    return ConversationHandler.END

async def work_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop('new_work', None)
    await update.message.reply_text("❌ Добавление отменено.")
    return ConversationHandler.END

# ========== ЗАПИСЬ ==========
@check_spam
async def start_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.data.startswith("appoint_"):
        context.user_data["selected_service"] = query.data.replace("appoint_", "")
    else:
        context.user_data["selected_service"] = None
    context.user_data["photos"] = []
    context.user_data["problem_description"] = ""
    anti_spam.start_conversation(update.effective_user.id)
    await query.edit_message_text("📝 <b>Запись на услугу</b>\nВведите ваше имя:", parse_mode="HTML")
    return States.NAME

async def get_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ok, val = validate_name(update.message.text)
    if not ok:
        await update.message.reply_text(f"{val}\nВведите имя:")
        return States.NAME
    context.user_data["name"] = val
    await update.message.reply_text("📞 Введите номер телефона:")
    return States.PHONE

async def get_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ok, val = validate_phone(update.message.text)
    if not ok:
        await update.message.reply_text(f"{val}\nВведите номер:")
        return States.PHONE
    context.user_data["phone"] = val
    await update.message.reply_text("🚗 Введите марку авто:", reply_markup=get_back_keyboard("back_name"))
    return States.CAR_BRAND

async def back_to_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("📝 <b>Запись</b>\nВведите имя:", parse_mode="HTML")
    return States.NAME

async def get_car_brand(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ok, val = validate_car_info(update.message.text, "Марка")
    if not ok:
        await update.message.reply_text(f"{val}\nВведите марку:")
        return States.CAR_BRAND
    context.user_data["car_brand"] = val
    await update.message.reply_text("🚗 Введите модель:", reply_markup=get_back_keyboard("back_phone"))
    return States.CAR_MODEL

async def back_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("📞 Введите номер телефона:", reply_markup=get_back_keyboard("back_name"))
    return States.PHONE

async def get_car_model(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ok, val = validate_car_info(update.message.text, "Модель")
    if not ok:
        await update.message.reply_text(f"{val}\nВведите модель:")
        return States.CAR_MODEL
    context.user_data["car_model"] = val
    await update.message.reply_text("🚗 Введите год выпуска:", reply_markup=get_back_keyboard("back_car_brand"))
    return States.CAR_YEAR

async def back_car_brand(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("🚗 Введите марку:", reply_markup=get_back_keyboard("back_phone"))
    return States.CAR_BRAND

async def get_car_year(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ok, val = validate_car_year(update.message.text)
    if not ok:
        await update.message.reply_text(f"{val}\nВведите год:")
        return States.CAR_YEAR
    context.user_data["car_year"] = val
    if context.user_data.get("selected_service"):
        # Если услуга уже выбрана, сразу спрашиваем описание проблемы
        await update.message.reply_text(
            "📝 <b>Опишите проблему или желаемые услуги:</b>\n\n"
            "Например: «Не заводится, горит Check Engine, нужна диагностика»\n"
            "Или подробно опишите, что беспокоит в автомобиле.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⏭ Пропустить", callback_data="skip_problem")
            ]])
        )
        return States.PROBLEM
    keyboard = [[InlineKeyboardButton(s["name"], callback_data=f"choose_{k}")] for k, s in SERVICES.items()]
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_car_model")])
    await update.message.reply_text("🛠 Выберите услугу:", reply_markup=InlineKeyboardMarkup(keyboard))
    return States.SERVICE

async def back_car_model(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("🚗 Введите модель:", reply_markup=get_back_keyboard("back_car_brand"))
    return States.CAR_MODEL

async def choose_service(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    service_key = query.data.replace("choose_", "")
    
    if service_key not in SERVICES:
        await query.edit_message_text("❌ Услуга не найдена. Пожалуйста, начните сначала /start.")
        anti_spam.end_conversation(update.effective_user.id)
        return ConversationHandler.END
        
    context.user_data["selected_service"] = service_key
    await query.edit_message_text(f"✅ Выбрано: {SERVICES[service_key]['name']}")
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="📝 <b>Опишите проблему или желаемые услуги:</b>\n\n"
             "Например: «Не заводится, горит Check Engine, нужна диагностика»\n"
             "Или подробно опишите, что беспокоит в автомобиле.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("⏭ Пропустить", callback_data="skip_problem")
        ]])
    )
    return States.PROBLEM

async def get_problem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    problem = update.message.text.strip()
    if len(problem) > 1000:
        await update.message.reply_text("❌ Слишком длинное описание. Сократите до 1000 символов:")
        return States.PROBLEM
    context.user_data["problem_description"] = problem
    return await ask_for_photos(update, context)

async def skip_problem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    context.user_data["problem_description"] = ""
    await update.callback_query.edit_message_text("📝 Описание пропущено.")
    return await ask_for_photos(update, context)

async def ask_for_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text=f"📸 Прикрепите фото (до {MAX_PHOTOS} шт.) или нажмите «Пропустить»:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Назад", callback_data="back_car_model")],
            [InlineKeyboardButton("⏭ Пропустить", callback_data="skip_photos")]
        ])
    )
    return States.PHOTO

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo:
        return await wrong_photo_format(update, context)
    photos = context.user_data.get("photos", [])
    if len(photos) >= MAX_PHOTOS:
        await update.message.reply_text(
            f"❌ Максимум {MAX_PHOTOS} фото.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Продолжить", callback_data="after_photos")],
                [InlineKeyboardButton("🔙 Назад", callback_data="back_car_model")]
            ])
        )
        return States.PHOTO
    ok, msg = validate_photo(update.message.photo[-1].file_size)
    if not ok:
        await update.message.reply_text(f"{msg}\nОтправьте другое фото:")
        return States.PHOTO
    photos.append(update.message.photo[-1].file_id)
    context.user_data["photos"] = photos
    rem = MAX_PHOTOS - len(photos)
    keyboard = [[InlineKeyboardButton("✅ Продолжить", callback_data="after_photos")]] if photos else []
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_car_model")])
    await update.message.reply_text(
        f"📸 Фото {len(photos)}/{MAX_PHOTOS}. {'Ещё ' + str(rem) if rem > 0 else 'Нажмите «Продолжить»'}",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return States.PHOTO

async def skip_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    context.user_data["photos"] = []
    await update.callback_query.edit_message_text("📸 Фото не добавлены.")
    return await save_appointment(update, context)

async def after_photos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    await update.callback_query.edit_message_text(f"📸 Добавлено фото: {len(context.user_data.get('photos',[]))} шт.")
    return await save_appointment(update, context)

async def save_appointment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    uname = update.effective_user.username or ""
    skey = context.user_data.get("selected_service")
    sname = SERVICES[skey]["name"] if skey else "Не указана"
    problem = context.user_data.get("problem_description", "")
    anti_spam.add_request(uid)
    
    aid = await db.add_appointment(uid, uname, {
        'name': context.user_data['name'],
        'phone': context.user_data['phone'],
        'car_brand': context.user_data.get('car_brand', ''),
        'car_model': context.user_data.get('car_model', ''),
        'car_year': context.user_data.get('car_year', ''),
        'service': sname,
        'date_time': 'Не указана',
        'photos': context.user_data.get('photos', []),
        'problem_description': problem
    })
    
    msg = update.callback_query.message if update.callback_query else update.message
    
    if aid is None:
        await msg.reply_text("❌ Произошла непредвиденная ошибка при сохранении заявки. Пожалуйста, попробуйте позже.")
        anti_spam.end_conversation(uid)
        return ConversationHandler.END
        
    text = (
        f"✅ <b>Заявка #{aid} отправлена!</b>\n\n"
        f"👤 {escape_html(context.user_data['name'])}\n"
        f"📞 {escape_html(context.user_data['phone'])}\n"
        f"🚗 {escape_html(context.user_data.get('car_brand','—'))} {escape_html(context.user_data.get('car_model','—'))} ({context.user_data.get('car_year','—')} г.)\n"
        f"🛠 {escape_html(sname)}\n"
        f"📝 {escape_html(problem) if problem else 'Описание не указано'}\n"
        f"📸 Фото: {len(context.user_data.get('photos',[]))} шт.\n\n"
        f"Мы свяжемся с вами!"
    )
    await msg.reply_text(text, parse_mode="HTML")
    
    mtext = (
        f"🔔 <b>Новая запись #{aid}!</b>\n\n"
        f"👤 {escape_html(context.user_data['name'])}\n"
        f"📞 {escape_html(context.user_data['phone'])}\n"
        f"🚗 {escape_html(context.user_data.get('car_brand','—'))} {escape_html(context.user_data.get('car_model','—'))} ({context.user_data.get('car_year','—')} г.)\n"
        f"🛠 {escape_html(sname)}\n"
        f"📝 <b>Проблема:</b> {escape_html(problem) if problem else 'Не указана'}\n"
        f"📸 Фото: {len(context.user_data.get('photos',[]))} шт."
    )
    try:
        await context.bot.send_message(chat_id=MANAGER_CHAT_ID, text=mtext, parse_mode="HTML")
        photos = context.user_data.get("photos", [])
        if photos:
            await send_media_group_safe(context.bot, MANAGER_CHAT_ID, photos, f"📸 К заявке #{aid}")
    except Exception as e:
        logger.error(f"Ошибка уведомления менеджера: {e}")
    
    anti_spam.end_conversation(uid)
    await msg.reply_text("Что дальше?", reply_markup=InlineKeyboardMarkup([[
        InlineKeyboardButton("🏠 В главное меню", callback_data="back_main")
    ]]))
    return ConversationHandler.END

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    anti_spam.end_conversation(update.effective_user.id)
    await update.message.reply_text("❌ Запись отменена.")
    return ConversationHandler.END

async def back_to_main(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.callback_query.answer()
    anti_spam.end_conversation(update.effective_user.id)
    await update.callback_query.edit_message_text("👋 Выберите действие:", reply_markup=get_main_menu_keyboard(update.effective_user.id))
    return ConversationHandler.END

# ========== ПЕРИОДИЧЕСКАЯ ОЧИСТКА ==========
async def periodic_cleanup(context: ContextTypes.DEFAULT_TYPE):
    anti_spam._cleanup_old_data()
    s = anti_spam.get_memory_stats()
    logger.info(f"Cleanup: history={s['history_users']} users, active={s['active_conversations']}, blocked={s['blocked_users']}")

# ========== ЗАПУСК ==========
def main():
    logger.info("Инициализация бота...")
    db.init_sync()
    
    request_config = HTTPXRequest(connect_timeout=15.0, read_timeout=20.0)
    app = Application.builder().token(TOKEN).request(request_config).build()
    
    async def set_commands(application: Application):
        await application.bot.set_my_commands([
            BotCommand("start", "🏠 Главное меню")
        ])
    app.post_init = set_commands
    
    job_queue = app.job_queue
    job_queue.run_repeating(periodic_cleanup, interval=CLEANUP_INTERVAL_MINUTES * 60, first=10)
    job_queue.run_daily(periodic_cleanup, time=dt.time(hour=3, minute=0))
    
    conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_appointment, pattern="^appointment$|^appoint_")],
        states={
            States.NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_name)],
            States.PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_phone), CallbackQueryHandler(back_to_name, pattern="^back_name$")],
            States.CAR_BRAND: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_car_brand), CallbackQueryHandler(back_phone, pattern="^back_phone$")],
            States.CAR_MODEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_car_model), CallbackQueryHandler(back_car_brand, pattern="^back_car_brand$")],
            States.CAR_YEAR: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_car_year), CallbackQueryHandler(back_car_model, pattern="^back_car_model$")],
            States.SERVICE: [CallbackQueryHandler(choose_service, pattern="^choose_"), CallbackQueryHandler(back_car_model, pattern="^back_car_model$")],
            States.PROBLEM: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_problem),
                CallbackQueryHandler(skip_problem, pattern="^skip_problem$"),
            ],
            States.PHOTO: [
                MessageHandler(filters.PHOTO, handle_photo),
                MessageHandler(filters.ALL & ~filters.COMMAND, wrong_photo_format),
                CallbackQueryHandler(skip_photos, pattern="^skip_photos$"),
                CallbackQueryHandler(after_photos, pattern="^after_photos$"),
                CallbackQueryHandler(back_car_model, pattern="^back_car_model$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel), CallbackQueryHandler(back_to_main, pattern="^back_main$")],
    )
    
    work_conv_handler = ConversationHandler(
        entry_points=[CallbackQueryHandler(admin_work_add_start, pattern="^awadd$")],
        states={
            States.WORK_TITLE: [MessageHandler(filters.TEXT & ~filters.COMMAND, work_get_title)],
            States.WORK_DESCRIPTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, work_get_description)],
            States.WORK_CAR_INFO: [MessageHandler(filters.TEXT & ~filters.COMMAND, work_get_car_info)],
            States.WORK_SERVICE_TYPE: [MessageHandler(filters.TEXT & ~filters.COMMAND, work_get_service_type)],
            States.WORK_RESULT: [MessageHandler(filters.TEXT & ~filters.COMMAND, work_get_result)],
            States.WORK_PHOTOS: [
                MessageHandler(filters.PHOTO, work_handle_photo),
                MessageHandler(filters.ALL & ~filters.COMMAND, wrong_work_photo_format),
                CallbackQueryHandler(work_skip_photos, pattern="^wskip$"),
                CallbackQueryHandler(work_finish_no_photos, pattern="^wfin_nophoto$"),
                CallbackQueryHandler(work_finish, pattern="^wfin$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", work_cancel)],
    )
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("memory", memory_stats_command))
    app.add_handler(conv_handler)
    app.add_handler(work_conv_handler)
    app.add_handler(CallbackQueryHandler(show_services, pattern="^services$"))
    app.add_handler(CallbackQueryHandler(show_service_detail, pattern="^sv_"))
    app.add_handler(CallbackQueryHandler(show_about, pattern="^about$"))
    app.add_handler(CallbackQueryHandler(back_to_main, pattern="^back_main$"))
    app.add_handler(CallbackQueryHandler(show_works, pattern="^works_page_"))
    app.add_handler(CallbackQueryHandler(show_work_detail, pattern="^wd_"))
    app.add_handler(CallbackQueryHandler(show_work_photos, pattern="^wp_"))
    
    app.add_handler(CallbackQueryHandler(client_show_appointments, pattern="^client_apps_"))
    app.add_handler(CallbackQueryHandler(client_view_appointment, pattern="^capp_view_"))
    
    app.add_handler(CallbackQueryHandler(admin_panel, pattern="^admin_panel$"))
    app.add_handler(CallbackQueryHandler(admin_show_appointments, pattern="^admin_all$|^admin_status_|^admin_page_"))
    app.add_handler(CallbackQueryHandler(admin_view_appointment, pattern="^av_"))
    app.add_handler(CallbackQueryHandler(admin_change_status, pattern="^acs_"))
    app.add_handler(CallbackQueryHandler(admin_show_photos, pattern="^aph_"))
    app.add_handler(CallbackQueryHandler(admin_back_to_list, pattern="^abl$"))
    app.add_handler(CallbackQueryHandler(admin_works_menu, pattern="^admin_works_menu$"))
    app.add_handler(CallbackQueryHandler(admin_works_list, pattern="^awlist_"))
    app.add_handler(CallbackQueryHandler(admin_work_edit, pattern="^awe_"))
    app.add_handler(CallbackQueryHandler(admin_work_toggle, pattern="^awt_"))
    app.add_handler(CallbackQueryHandler(admin_work_delete, pattern="^awd_"))
    app.add_handler(CallbackQueryHandler(admin_work_photos_show, pattern="^awps_"))
    app.add_error_handler(error_handler)
    
    logger.info("🤖 Бот запущен...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Exception while handling an update:", exc_info=context.error)
    tb_list = traceback.format_exception(context.error)
    tb_string = ''.join(tb_list)
    logger.error(f"Traceback:\n{tb_string}")
    user_info = "Неизвестно"
    chat_id = None
    update_type = "Неизвестно"
    if update:
        if update.effective_user:
            user_info = f"@{update.effective_user.username}" if update.effective_user.username else f"ID:{update.effective_user.id}"
        if update.effective_chat:
            chat_id = update.effective_chat.id
        if update.message:
            update_type = f"Message: {update.message.text[:100] if update.message.text else 'No text'}"
        elif update.callback_query:
            update_type = f"CallbackQuery: {update.callback_query.data}"
    try:
        if chat_id:
            await context.bot.send_message(chat_id=chat_id, text="❌ Произошла ошибка. Попробуйте позже.")
    except Exception:
        pass
    try:
        error_text = (
            f"🚨 <b>Ошибка в боте!</b>\n\n"
            f"<b>Тип:</b> {escape_html(type(context.error).__name__)}\n"
            f"<b>Текст:</b> {escape_html(str(context.error)[:200])}\n"
            f"<b>Пользователь:</b> {escape_html(user_info)}\n"
            f"<b>Чат ID:</b> <code>{chat_id}</code>\n"
            f"<b>Update:</b> {escape_html(update_type)}\n\n"
            f"<b>Traceback:</b>\n<pre><code>{escape_html(tb_string[-1000:])}</code></pre>"
        )
        await context.bot.send_message(chat_id=MANAGER_CHAT_ID, text=error_text[:4000], parse_mode="HTML")
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление менеджеру: {e}")

if __name__ == "__main__":
    main()
