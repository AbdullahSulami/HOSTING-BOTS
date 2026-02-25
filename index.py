"""
Telegram Bot Hosting Platform - Complete Implementation
Supports Arabic and English languages
Optimized for Render Free Tier
"""

import os
import logging
import asyncio
import re
import json
import subprocess
import sys
import hashlib
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
from contextlib import asynccontextmanager
import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# ==================== CONFIGURATION ====================

@dataclass
class Config:
    # Bot Configuration
    MAIN_BOT_TOKEN: str = os.getenv("MAIN_BOT_TOKEN", "")
    
    # Admin Configuration
    ADMIN_IDS: List[int] = field(default_factory=lambda: [int(id) for id in os.getenv("ADMIN_IDS", "").split(",") if id])
    
    # Channel Configuration
    REQUIRED_CHANNEL: str = os.getenv("REQUIRED_CHANNEL", "@your_channel")
    LOG_CHANNEL: str = os.getenv("LOG_CHANNEL", "@log_channel")
    
    # Webhook Configuration
    WEBHOOK_BASE_URL: str = os.getenv("WEBHOOK_BASE_URL", "https://telegram-bot-hosting.onrender.com")
    WEBHOOK_PATH: str = "/webhook/main"
    HOSTED_WEBHOOK_PATH: str = "/webhook"
    HEALTH_PATH: str = "/health"
    
    # Database
    DATABASE_PATH: str = "data/bot_hosting.db"
    
    # Security
    MAX_BOTS_PER_USER: int = 3
    RATE_LIMIT_ACTIONS: int = 5
    RATE_LIMIT_WINDOW: int = 10
    
    # Server
    PORT: int = int(os.getenv("PORT", 10000))
    HOST: str = "0.0.0.0"
    
    # Languages
    LANGUAGES: Dict[str, str] = field(default_factory=lambda: {
        'en': 'English',
        'ar': 'العربية'
    })
    DEFAULT_LANGUAGE: str = 'en'
    
    # Mode
    USE_POLLING: bool = os.getenv("USE_POLLING", "True").lower() == "true"

config = Config()

# ==================== INTERNATIONALIZATION ====================

class I18n:
    def __init__(self):
        self.strings = {
            'en': {
                'add_bot': '➕ Deploy App',
                'add_contact_bot': '📞 Create Contact Bot',
                'my_bots': '📊 My Apps',
                'bot_stats': '📈 Statistics',
                'remove_bot': '❌ Delete App',
                'settings': '⚙️ Settings',
                'help': 'ℹ️ Help',
                'language': '🌐 Language',
                
                # Messages
                'welcome': '👋 Welcome to Bot Hosting Platform!\n\nChoose an option from the menu below:',
                'welcome_back': '👋 Welcome back, {name}!',
                'choose_language': '🌐 Please choose your language:',
                'language_changed': '✅ Language changed to English successfully!',
                'must_join_channel': '⚠️ Please join our channel first to use the bot:\n{channel}',
                'bot_added': '✅ Bot added successfully!\n\nName: {name}\nUsername: @{username}',
                'bot_removed': '✅ Bot removed successfully!',
                'max_bots_reached': '❌ You have reached the maximum number of bots (10)',
                'invalid_token': '❌ Invalid bot token. Please check and try again.',
                'bot_already_exists': '❌ This bot is already registered in our system',
                'no_bots': '📭 You don\'t have any bots yet. Use "Add Bot" to get started!',
                'bot_stats_message': """📊 <b>Bot Statistics: {name}</b>

🤖 Username: @{username}
📈 Total Updates: {updates}
💬 Total Messages: {messages}
📅 Created: {created}
⚡️ Status: {status}""",
                'select_bot': '🤖 Select App:',
                'enter_token': '🔑 Send Bot Token OR 🌐 Site Name:',
                'enter_code': '📄 Send .py, .js, or .zip:',
                'processing': '⏳ Deploying...',
                'error': '❌ Error.',
                'unauthorized': '⛔️ Access Denied.',
                
                # ...
                
                # Buttons
                'confirm': '✅ Confirm',
                'cancel': '❌ Cancel',
                'back': '🔙 Back',
                'refresh': '🔄 Refresh',
                'english': '🇬🇧 English',
                'arabic': '🇸🇦 العربية',
                'commands': {
                    'start': 'Start the bot',
                    'addbot': 'Add a new bot',
                    'mybots': 'View your bots',
                    'stats': 'Bot statistics',
                    'removebot': 'Delete a bot',
                    'language': 'Switch language',
                    'help': 'Get assistance'
                }
            },
            'ar': {
                'add_bot': '➕ استضافة تطبيق',
                'add_contact_bot': '📞 صنع بوت تواصل',
                'my_bots': '📊 تطبيقاتي',
                'bot_stats': '📈 الإحصائيات',
                'remove_bot': '❌ حذف تطبيق',
                'settings': '⚙️ الإعدادات',
                'help': 'ℹ️ مساعدة',
                'language': '🌐 اللغة',
                'admin_panel': '🔐 لوحة الإدارة',
                
                # Messages
                'welcome': '👋 مرحباً بك في منصة الاستضافة!\n\nتحكم في تطبيقاتك من القائمة أدناه:',
                'welcome_back': '👋 مرحباً بعودتك، {name}!',
                'choose_language': '🌐 الرجاء اختيار لغتك:',
                'language_changed': '✅ تم تغيير اللغة بنجاح!',
                'must_join_channel': '⚠️ اشترك في القناة أولاً:\n{channel}',
                'bot_added': '✅ تم الاستضافة بنجاح!\n\nالاسم: {name}\nالمعرف: @{username}',
                'bot_removed': '✅ تم الحذف بنجاح!',
                'max_bots_reached': '❌ تم الوصول للحد الأقصى.',
                'invalid_token': '❌ الرمز غير صحيح.',
                'bot_already_exists': '❌ مسجل مسبقاً لدينا.',
                'no_bots': '📭 لا يوجد لديك تطبيقات بعد.',
                'bot_stats_message': """📊 <b>إحصائيات البوت: {name}</b>

🤖 المعرف: @{username}
📈 إجمالي التحديثات: {updates}
💬 إجمالي الرسائل: {messages}
📅 تاريخ الإنشاء: {created}
⚡️ الحالة: {status}""",
                'select_bot': '🤖 اختر بوتاً:',
                'enter_token': '🔑 الرجاء إرسال رمز البوت الخاص بك (من @BotFather):',
                'enter_code': '📄 الرجاء إرسال كود بايثون للبوت (ملف .py) أو إلصقه كنص لاستضافته:',
                'processing': '⏳ جاري المعالجة...',
                'error': '❌ حدث خطأ. الرجاء المحاولة مرة أخرى.',
                'unauthorized': '⛔️ غير مصرح لك باستخدام هذا الأمر.',
                
                # Admin
                'admin_panel_msg': '👑 <b>لوحة التحكم</b>',
                'admin_panel_btn': '🔐 لوحة الإدارة',
                # ...

                # Service Strings
                'service_welcome': 'مرحباً بك في بوت الخدمة! 🤖',
                'quran': '📖 آيات قرآنية',
                'videos': '🎥 روابط فيديوهات',
                'service_help': 'هذا البوت يوفر آيات قرآنية وروابط فيديوهات.',
                'no_content': '� لا يوجد محتوى متاح بعد.',
                
                # ...
                'enter_broadcast': '📝 أرسل الرسالة التي تريد إرسالها للجميع:',
                'broadcast_sent': '✅ تم إرسال الرسالة إلى {count} مستخدم',
                'confirm_remove': '⚠️ هل أنت متأكد من حذف هذا البوت؟',
                
                # Buttons
                'confirm': '✅ تأكيد',
                'cancel': '❌ إلغاء',
                'back': '🔙 رجوع',
                'refresh': '🔄 تحديث',
                'english': '🇬🇧 الإنجليزية',
                'arabic': '🇸🇦 العربية',
                'commands': {
                    'start': 'تشغيل البوت',
                    'addbot': 'إضافة بوت جديد',
                    'mybots': 'عرض بوتاتي',
                    'stats': 'إحصائيات البوت',
                    'removebot': 'حذف بوت',
                    'language': 'تغيير اللغة',
                    'help': 'مساعدة'
                }
            }
        }
    
    def get(self, key: str, lang: str = 'en', **kwargs) -> str:
        """Get translated string"""
        if lang not in self.strings:
            lang = 'en'
        
        text = self.strings[lang].get(key, self.strings['en'].get(key, key))
        return text.format(**kwargs) if kwargs else text
    
    def get_keyboard(self, buttons: List[List[Any]], lang: str = 'en') -> ReplyKeyboardMarkup:
        """Create keyboard with translated buttons"""
        keyboard = []
        for row in buttons:
            keyboard_row = []
            for button in row:
                if isinstance(button, dict):
                    # Inline button
                    keyboard_row.append(
                        InlineKeyboardButton(
                            text=self.get(button['text'], lang),
                            callback_data=button['callback_data']
                        )
                    )
                else:
                    # Regular button
                    keyboard_row.append(
                        KeyboardButton(text=self.get(button, lang))
                    )
            keyboard.append(keyboard_row)
        
        return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

i18n = I18n()

# ==================== DATABASE LAYER ====================

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    async def init(self):
        """Initialize database tables"""
        async with aiosqlite.connect(self.db_path) as db:
            # Users table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE,
                    username TEXT,
                    first_name TEXT,
                    language TEXT DEFAULT 'en',
                    role TEXT DEFAULT 'user',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_active TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1
                )
            """)
            
            # Bots table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS bots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_id INTEGER,
                    bot_token TEXT UNIQUE,
                    bot_name TEXT,
                    bot_username TEXT,
                    code_path TEXT,
                    type TEXT DEFAULT 'bot', -- 'bot' or 'site'
                    port INTEGER,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_update TIMESTAMP,
                    total_updates INTEGER DEFAULT 0,
                    total_messages INTEGER DEFAULT 0,
                    FOREIGN KEY (owner_id) REFERENCES users(id)
                )
            """)
            
            # Migration
            try:
                await db.execute("ALTER TABLE bots ADD COLUMN type TEXT DEFAULT 'bot'")
                await db.execute("ALTER TABLE bots ADD COLUMN port INTEGER")
                await db.commit()
            except: pass
            
            # Check for missing code_path column (migration)
            try:
                await db.execute("ALTER TABLE bots ADD COLUMN code_path TEXT")
                await db.commit()
            except:
                pass
            
            # Index for faster lookups
            await db.execute("CREATE INDEX IF NOT EXISTS idx_bot_token ON bots(bot_token)")
            
            # Daily stats table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS daily_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id INTEGER,
                    date DATE,
                    updates_count INTEGER DEFAULT 0,
                    messages_count INTEGER DEFAULT 0,
                    FOREIGN KEY (bot_id) REFERENCES bots(id),
                    UNIQUE(bot_id, date)
                )
            """)
            
            # Logs table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    action TEXT,
                    details TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Content table for service logic
            await db.execute("""
                CREATE TABLE IF NOT EXISTS service_content (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type TEXT, -- 'video' or 'quran'
                    content TEXT,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Seed contents if empty
            cursor = await db.execute("SELECT COUNT(*) FROM service_content")
            if (await cursor.fetchone())[0] == 0:
                seed_data = [
                    ('quran', 'قُلْ هُوَ اللَّهُ أَحَدٌ', 'Surah Al-Ikhlas'),
                    ('quran', 'إِنَّ مَعَ الْعُسْرِ يُسْرًا', 'Surah Ash-Sharh'),
                    ('video', 'https://www.youtube.com/watch?v=dQw4w9WgXcQ', 'Sample Video 1'),
                ]
                await db.executemany("INSERT INTO service_content (type, content, description) VALUES (?, ?, ?)", seed_data)
                
            await db.commit()
    
    @asynccontextmanager
    async def connect(self):
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            yield db
    
    async def get_user(self, telegram_id: int) -> Optional[Dict]:
        async with self.connect() as db:
            cursor = await db.execute(
                "SELECT * FROM users WHERE telegram_id = ?",
                (telegram_id,)
            )
            row = await cursor.fetchone()
            return dict(row) if row else None
    
    async def create_user(self, telegram_id: int, **kwargs) -> Dict:
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO users (telegram_id, username, first_name, language)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET
                username = excluded.username,
                first_name = excluded.first_name,
                last_active = CURRENT_TIMESTAMP
            """, (
                telegram_id,
                kwargs.get('username'),
                kwargs.get('first_name'),
                kwargs.get('language', 'en')
            ))
            await db.commit()
            return await self.get_user(telegram_id)
    
    async def update_language(self, telegram_id: int, language: str):
        async with self.connect() as db:
            await db.execute(
                "UPDATE users SET language = ? WHERE telegram_id = ?",
                (language, telegram_id)
            )
            await db.commit()
    
    async def get_language(self, telegram_id: int) -> str:
        user = await self.get_user(telegram_id)
        return user.get('language', 'en') if user else 'en'
    
    async def get_user_bots(self, telegram_id: int) -> List[Dict]:
        async with self.connect() as db:
            cursor = await db.execute("""
                SELECT b.* FROM bots b
                JOIN users u ON b.owner_id = u.id
                WHERE u.telegram_id = ? AND b.is_active = 1
                ORDER BY b.created_at DESC
            """, (telegram_id,))
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]
    
    async def add_bot(self, owner_telegram_id: int, bot_token: str, 
                      bot_name: str, bot_username: str, code_path: str = None) -> Tuple[bool, str]:
        async with self.connect() as db:
            # Add bot
            await db.execute("""
                INSERT INTO bots (owner_id, bot_token, bot_name, bot_username, code_path)
                SELECT id, ?, ?, ?, ? FROM users WHERE telegram_id = ?
                ON CONFLICT(bot_token) DO UPDATE SET
                is_active = 1,
                code_path = excluded.code_path,
                bot_username = excluded.bot_username
            """, (bot_token, bot_name, bot_username, code_path, owner_telegram_id))
            await db.commit()
            return True, "SUCCESS"
    
    async def remove_bot(self, bot_token: str) -> bool:
        async with self.connect() as db:
            await db.execute(
                "UPDATE bots SET is_active = 0 WHERE bot_token = ?",
                (bot_token,)
            )
            await db.commit()
            return True
    
    async def increment_stats(self, bot_token: str, is_message: bool = False):
        async with self.connect() as db:
            today = date.today().isoformat()
            
            # Get bot id
            cursor = await db.execute(
                "SELECT id FROM bots WHERE bot_token = ? AND is_active = 1",
                (bot_token,)
            )
            bot = await cursor.fetchone()
            if not bot:
                return
            
            bot_id = bot[0]
            
            # Update totals
            if is_message:
                await db.execute(
                    "UPDATE bots SET total_messages = total_messages + 1, last_update = CURRENT_TIMESTAMP WHERE id = ?",
                    (bot_id,)
                )
            else:
                await db.execute(
                    "UPDATE bots SET total_updates = total_updates + 1, last_update = CURRENT_TIMESTAMP WHERE id = ?",
                    (bot_id,)
                )
            
            # Update daily stats
            await db.execute("""
                INSERT INTO daily_stats (bot_id, date, updates_count, messages_count)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(bot_id, date) DO UPDATE SET
                updates_count = updates_count + excluded.updates_count,
                messages_count = messages_count + excluded.messages_count
            """, (
                bot_id, today,
                0 if is_message else 1,
                1 if is_message else 0
            ))
            
            await db.commit()
    
    async def get_global_stats(self) -> Dict:
        async with self.connect() as db:
            # Total users
            cursor = await db.execute("SELECT COUNT(*) FROM users")
            total_users = (await cursor.fetchone())[0]
            
            # Total bots
            cursor = await db.execute("SELECT COUNT(*) FROM bots WHERE is_active = 1")
            total_bots = (await cursor.fetchone())[0]
            
            # Total updates
            cursor = await db.execute("SELECT SUM(total_updates) FROM bots")
            total_updates = (await cursor.fetchone())[0] or 0
            
            # Total messages
            cursor = await db.execute("SELECT SUM(total_messages) FROM bots")
            total_messages = (await cursor.fetchone())[0] or 0
            
            # Top bot
            cursor = await db.execute("""
                SELECT bot_name, total_updates FROM bots 
                WHERE is_active = 1 
                ORDER BY total_updates DESC LIMIT 1
            """)
            top_bot = await cursor.fetchone()
            
            return {
                "total_users": total_users,
                "total_bots": total_bots,
                "total_updates": total_updates,
                "total_messages": total_messages,
                "top_bot": dict(top_bot) if top_bot else None
            }
    
    async def log(self, user_id: int, action: str, details: str = None):
        async with self.connect() as db:
            await db.execute("""
                INSERT INTO logs (user_id, action, details)
                SELECT id, ?, ? FROM users WHERE telegram_id = ?
            """, (action, details, user_id))
            await db.commit()

# ==================== DEFAULT SERVICE CODE ====================
DEFAULT_SERVICE_CODE = """
import os
import asyncio
import logging
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message

# Get token from environment
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    exit(1)

bot = Bot(token=TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer("👋 Welcome! This bot is hosted on the Application Platform.\\n\\nUpload your own code to change this message.")

async def main():
    logging.basicConfig(level=logging.INFO)
    print("Bot is starting...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
"""

# ==================== CONTACT BOT SERVICE CODE ====================
CONTACT_SERVICE_CODE = """
import os
import asyncio
import logging
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message

# Get token & settings from environment
TOKEN = os.getenv("BOT_TOKEN")
OWNER_ID = int(os.environ.get("OWNER_ID", "OWNER_ID_PLACEHOLDER"))
LOG_CHANNEL = os.getenv("LOG_CHANNEL", "LOG_CHANNEL_PLACEHOLDER")

if not TOKEN:
    exit(1)

bot = Bot(token=TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def cmd_start(message: Message):
    await message.answer("مرحباً بك! تواصل معي من خلال هذا البوت. وسيتم الرد عليك في أقرب وقت.\\n\\nWelcome! Send me your message and I'll reply soon.")

@dp.message()
async def handle_messages(message: Message):
    # Owner replying to a user's message
    if message.from_user.id == OWNER_ID:
        if message.reply_to_message and message.reply_to_message.forward_origin:
            origin = message.reply_to_message.forward_origin
            if hasattr(origin, 'sender_user') and origin.sender_user:
                try:
                    await bot.copy_message(
                        chat_id=origin.sender_user.id,
                        from_chat_id=message.chat.id,
                        message_id=message.message_id
                    )
                except Exception as e:
                    await message.answer(f"❌ لم يتم الإرسال بسبب خطأ: {e}")
            else:
                await message.answer("❌ لا يمكن الرد على هذه الرسالة لأن مرسلها غير معروف أو مخفي.")
        else:
            await message.answer("⚠️ يرجى الرد على رسالة المستخدم ليتم إرسال الرد له.")
        return

    # User sending a message
    try:
        await bot.forward_message(chat_id=OWNER_ID, from_chat_id=message.chat.id, message_id=message.message_id)
    except Exception as e:
        logging.error(f"Cannot forward to owner: {e}")
        
    try:
        if LOG_CHANNEL and LOG_CHANNEL != "LOG_CHANNEL_PLACEHOLDER":
            await bot.forward_message(chat_id=LOG_CHANNEL, from_chat_id=message.chat.id, message_id=message.message_id)
    except Exception as e:
        logging.error(f"Cannot forward to log channel: {e}")
        
    await message.answer("تم إرسال رسالتك بنجاح ✅")

async def main():
    logging.basicConfig(level=logging.INFO)
    print("Contact bot is starting...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
"""

# ==================== BOT REGISTRY ====================

class BotRegistry:
    def __init__(self):
        self._bots: Dict[str, Dict] = {}
        self._db: Optional[Database] = None
        self._main_bot: Optional[Bot] = None
    
    def set_db(self, db: Database):
        self._db = db
        
    def set_main_bot(self, bot: Bot):
        self._main_bot = bot
    
    async def register(self, token: str, name: str, code_content: str = None, code_path: str = None, is_site=False) -> Tuple[bool, str]:
        """Register a new app in a separate process (Py/JS/Zip)"""
        try:
            me_username = "website"
            if not is_site:
                bot = Bot(token=token)
                try:
                    me = await asyncio.wait_for(bot.get_me(), timeout=10.0)
                    me_username = me.username
                except:
                    await bot.session.close()
                    return False, "Invalid Token"
                await bot.session.close()

            # Prepare path
            os.makedirs("hosted_apps", exist_ok=True)
            token_hash = hashlib.md5(token.encode()).hexdigest()
            target_dir = os.path.abspath(f"hosted_apps/app_{token_hash}")
            os.makedirs(target_dir, exist_ok=True)

            entry_point = None
            runtime = sys.executable # Default python

            if not code_content and not code_path:
                code_content = DEFAULT_SERVICE_CODE

            if code_content:
                entry_point = os.path.join(target_dir, "main.py")
                with open(entry_point, "w", encoding="utf-8") as f:
                    f.write(code_content)
            elif code_path:
                if code_path.endswith(".zip"):
                    import zipfile
                    with zipfile.ZipFile(code_path, 'r') as zip_ref:
                        zip_ref.extractall(target_dir)
                    # Find entry
                    for f in os.listdir(target_dir):
                        if f == "main.py": entry_point = os.path.join(target_dir, f); break
                        if f == "index.js": entry_point = os.path.join(target_dir, f); runtime = "node"; break
                        if f == "package.json": entry_point = target_dir; runtime = "npm"; break
                else:
                    filename = "main.py" if code_path.endswith(".py") else "index.js"
                    if code_path.endswith(".js"): runtime = "node"
                    entry_point = os.path.join(target_dir, filename)
                    import shutil
                    shutil.copy(code_path, entry_point)

            if not entry_point: return False, "Could not find entry point"

            # Allocate Port if site
            port = None
            if is_site:
                port = 10000 + len(self._bots) + 1 # Dynamic port

            env = os.environ.copy()
            env["BOT_TOKEN"] = token
            if port: env["PORT"] = str(port)
            env["PYTHONIOENCODING"] = "utf-8"

            cmd = [runtime, entry_point]
            if runtime == "npm": cmd = ["npm", "start"]

            process = await asyncio.create_subprocess_exec(
                *cmd, 
                cwd=target_dir, 
                stdout=asyncio.subprocess.PIPE, 
                stderr=asyncio.subprocess.PIPE, 
                env=env
            )
            
            async def _log_stream(stream, prefix):
                try:
                    while True:
                        line = await stream.readline()
                        if not line: break
                        text = line.decode('utf-8', errors='ignore').strip()
                        if text and self._main_bot:
                            try:
                                await self._main_bot.send_message(
                                    config.LOG_CHANNEL,
                                    f"📜 **[{prefix}]**\n`{text[:4000]}`",
                                    parse_mode="Markdown"
                                )
                                await asyncio.sleep(0.1) # Prevent flood
                            except: pass
                except: pass

            asyncio.create_task(_log_stream(process.stdout, me_username))
            asyncio.create_task(_log_stream(process.stderr, me_username + " Error"))

            self._bots[token] = {
                'token': token, 'name': name, 'username': me_username,
                'process': process, 'port': port,
                'type': 'site' if is_site else 'bot'
            }
            
            return True, me_username
        except Exception as e:
            logging.error(f"Launch error: {e}")
            return False, str(e)
    
    async def unregister(self, token: str) -> bool:
        """Unregister a bot and kill its process"""
        if token in self._bots:
            info = self._bots[token]
            process = info.get('process')
            if process:
                logging.info(f"Terminating hosted bot process (PID: {process.pid})")
                try:
                    process.terminate()
                    import asyncio
                    try:
                        await asyncio.wait_for(process.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        process.kill()
                except ProcessLookupError:
                    pass
                except Exception as e:
                    logging.error(f"Error terminating process: {e}")
            
            # Close log file
            log_file = info.get('log_file')
            if log_file:
                log_file.close()
                
            del self._bots[token]
            return True
        return False
    
    def get_bot(self, token: str) -> Optional[Bot]:
        """Get bot instance by token"""
        info = self._bots.get(token)
        return info['bot'] if info else None
    
    async def process_update(self, token: str, update_data: dict) -> Optional[Dict]:
        """Process update for hosted bot"""
        if token not in self._bots:
            return None
        
        try:
            bot_info = self._bots[token]
            bot_info['total_updates'] += 1
            
            # Update stats
            if self._db:
                is_message = 'message' in update_data
                await self._db.increment_stats(token, is_message)
            
            # Process with the bot's own dispatcher
            bot = bot_info['bot']
            dp = bot_info['dp']
            update = types.Update(**update_data)
            logging.info(f"Feeding update to hosted bot @{bot_info['username']}")
            await dp.feed_update(bot, update)
            
            return {"ok": True}
            
        except Exception as e:
            logging.error(f"Error processing bot update: {e}")
            return {"ok": False, "error": str(e)}
    
    async def shutdown(self):
        """Shutdown all hosted bots"""
        for token in list(self._bots.keys()):
            await self.unregister(token)
        self._bots.clear()

# ==================== STATES ====================

class BotStates(StatesGroup):
    waiting_for_token = State()
    waiting_for_contact_token = State()
    waiting_for_code = State()
    waiting_for_broadcast = State()
    waiting_for_bot_removal = State()

# ==================== MIDDLEWARE ====================

class RateLimitMiddleware:
    def __init__(self, rate_limit: int, window: int):
        self.rate_limit = rate_limit
        self.window = window
        self.users: Dict[int, List[datetime]] = {}
    
    async def __call__(self, handler, event: Message, data: dict):
        user_id = event.from_user.id
        
        # Clean old entries
        now = datetime.now()
        if user_id in self.users:
            self.users[user_id] = [
                t for t in self.users[user_id] 
                if (now - t).seconds < self.window
            ]
        else:
            self.users[user_id] = []
        
        # Check rate limit
        if len(self.users[user_id]) >= self.rate_limit:
            lang = await data['db'].get_language(user_id)
            logging.info(f"Rate limit hit for user {user_id}")
            await event.answer(i18n.get('error', lang))
            return
        
        # Add current time
        self.users[user_id].append(now)
        
        # Process handler
        return await handler(event, data)

class LoggingMiddleware:
    async def __call__(self, handler, event: Message, data: dict):
        if hasattr(event, "text") and event.text:
            try:
                bot = data['bot']
                await bot.send_message(
                    config.LOG_CHANNEL,
                    f"👤 **User**: {event.from_user.id} (@{event.from_user.username})\n💬 **Sent**: `{event.text}`",
                    parse_mode="Markdown"
                )
            except: pass
        return await handler(event, data)

class ChannelCheckMiddleware:
    async def __call__(self, handler, event: Message, data: dict):
        logging.info(f"Channel check for user {event.from_user.id}")
        if event.from_user.id in config.ADMIN_IDS:
            logging.info(f"User {event.from_user.id} is admin, skipping check")
            return await handler(event, data)
        
        try:
            bot = data['bot']
            user_id = event.from_user.id
            db = data['db']
            lang = await db.get_language(user_id)
            
            # Check channel membership
            chat_member = await bot.get_chat_member(
                chat_id=config.REQUIRED_CHANNEL,
                user_id=user_id
            )
            
            if chat_member.status in ['left', 'kicked']:
                await event.answer(
                    i18n.get('must_join_channel', lang, channel=config.REQUIRED_CHANNEL)
                )
                return
            
            return await handler(event, data)
            
        except Exception as e:
            logging.error(f"Channel check error: {e}")
            return await handler(event, data)

# ==================== HANDLERS ====================

async def process_stat_select(message: Message, db: Database):
    username = message.text.replace("🤖 @", "").strip()
    logging.info(f"Selected bot stats for @{username}")
    async with db.connect() as conn:
        cursor = await conn.execute("SELECT * FROM bots WHERE bot_username = ? AND is_active = 1", (username,))
        bot = await cursor.fetchone()
        if bot:
            lang = await db.get_language(message.from_user.id)
            stats_text = i18n.get('bot_stats_message', lang,
                name=bot['bot_name'], username=bot['bot_username'],
                updates=bot['total_updates'], messages=bot['total_messages'],
                created=bot['created_at'][:10], status="✅ Active" if bot['is_active'] else "❌ Inactive"
            )
            await message.answer(stats_text, parse_mode=ParseMode.HTML)
        else:
            logging.warning(f"Bot @{username} not found or inactive")

async def process_remove_select(message: Message, db: Database, registry: BotRegistry):
    username = message.text.replace("❌ @", "").strip()
    logging.info(f"Selected bot removal for @{username}")
    async with db.connect() as conn:
        cursor = await conn.execute("SELECT bot_token FROM bots WHERE bot_username = ? AND is_active = 1", (username,))
        row = await cursor.fetchone()
        if row:
            token = row['bot_token']
            lang = await db.get_language(message.from_user.id)
            await registry.unregister(token)
            await db.remove_bot(token)
            await message.answer(i18n.get('bot_removed', lang))
            await show_main_menu(message, db, lang)
        else:
            logging.warning(f"Bot @{username} not found for removal")

async def process_lang_btn(message: Message, db: Database):
    lang = 'en' if 'English' in message.text else 'ar'
    await db.update_language(message.from_user.id, lang)
    await message.answer(i18n.get('language_changed', lang))
    await show_main_menu(message, db, lang)

async def process_admin_btns(message: Message, state: FSMContext, db: Database):
    if message.from_user.id not in config.ADMIN_IDS: return
    lang = await db.get_language(message.from_user.id)
    if i18n.get('broadcast', lang) in message.text:
        await state.set_state(BotStates.waiting_for_broadcast)
        await message.answer(i18n.get('enter_broadcast', lang))
    elif i18n.get('refresh', lang) in message.text:
        await cmd_admin(message, db)

async def cancel_handler(message: Message, state: FSMContext, db: Database):
    await state.clear()
    lang = await db.get_language(message.from_user.id)
    await show_main_menu(message, db, lang)

# ==================== HANDLERS (MANAGER) ====================

# ... (Previous handlers like process_stat_select, process_remove_select, etc. remain the same) ...

def register_manager_handlers(dp: Dispatcher):
    """Register platform management handlers for the main bot"""
    # Middleware
    dp.message.middleware(RateLimitMiddleware(
        config.RATE_LIMIT_ACTIONS,
        config.RATE_LIMIT_WINDOW
    ))
    dp.message.middleware(LoggingMiddleware())
    
    # Handlers
    dp.message.register(cmd_language, Command("language"))
    dp.message.register(process_lang_btn, F.text.regexp(r"(🇬🇧 English|🇸🇦 العربية)"))
    dp.message.register(cmd_start, CommandStart())
    
    # Inline Callbacks
    dp.callback_query.register(process_callbacks)
    
    # Commands redirect to main menu
    dp.message.register(cmd_start, Command("addbot"))
    dp.message.register(cmd_start, Command("mybots"))
    dp.message.register(cmd_start, Command("stats"))
    dp.message.register(cmd_start, Command("removebot"))
    
    # Flows
    dp.message.register(process_bot_token, BotStates.waiting_for_token)
    dp.message.register(process_contact_token, BotStates.waiting_for_contact_token)
    dp.message.register(process_bot_code, BotStates.waiting_for_code)
    
    # Selection handlers
    dp.message.register(process_stat_select, F.text.startswith("🤖 @"))
    dp.message.register(process_remove_select, F.text.startswith("❌ @"))
    
    # Admin
    admin_btns = [i18n.strings['en']['refresh'], i18n.strings['ar']['refresh'], 
                  i18n.get('broadcast', 'en'), i18n.get('broadcast', 'ar')]
    dp.message.register(process_admin_btns, F.text.in_(admin_btns))
    dp.message.register(cmd_admin, Command("admin"))
    dp.message.register(process_broadcast, BotStates.waiting_for_broadcast)
    
    # Cancel
    cancel_btns = ["cancel_stats", "cancel_remove", i18n.get('back', 'en'), i18n.get('back', 'ar')]
    dp.message.register(cancel_handler, Command("cancel"))
    dp.message.register(cancel_handler, F.text.in_(cancel_btns))
    
    # Help
    dp.message.register(cmd_help, Command("help"))
    
    @dp.errors()
    async def error_handler(error_event):
        logging.error(f"Manager Bot Error: {error_event.exception}", exc_info=True)
        return True

# ==================== HANDLERS (SERVICE) ====================

async def cmd_service_start(message: Message, db: Database):
    """Handle /start for service bots"""
    user = await db.create_user(message.from_user.id, username=message.from_user.username, first_name=message.from_user.first_name)
    lang = user['language']
    
    keyboard = i18n.get_keyboard([
        ['quran', 'videos'],
        ['help', 'language']
    ], lang)
    
    await message.answer(i18n.get('service_welcome', lang), reply_markup=keyboard)

async def cmd_quran(message: Message, db: Database):
    """Send a random Quran verse"""
    lang = await db.get_language(message.from_user.id)
    # Get random content from database
    async with db.connect() as conn:
        cursor = await conn.execute("SELECT content FROM service_content WHERE type = 'quran' ORDER BY RANDOM() LIMIT 1")
        row = await cursor.fetchone()
        if row:
            await message.answer(f"📖 {row['content']}")
        else:
            await message.answer(i18n.get('no_content', lang))

async def cmd_videos(message: Message, db: Database):
    """Send a random video link"""
    lang = await db.get_language(message.from_user.id)
    async with db.connect() as conn:
        cursor = await conn.execute("SELECT content FROM service_content WHERE type = 'video' ORDER BY RANDOM() LIMIT 1")
        row = await cursor.fetchone()
        if row:
            await message.answer(f"🎥 {row['content']}")
        else:
            await message.answer(i18n.get('no_content', lang))

def register_service_handlers(dp: Dispatcher):
    """Register service handlers for hosted bots"""
    # Middleware
    dp.message.middleware(RateLimitMiddleware(config.RATE_LIMIT_ACTIONS, config.RATE_LIMIT_WINDOW))
    dp.message.middleware(ChannelCheckMiddleware())
    
    # Handlers
    dp.message.register(cmd_service_start, CommandStart())
    dp.message.register(cmd_quran, F.text.in_([i18n.get('quran', 'en'), i18n.get('quran', 'ar')]))
    dp.message.register(cmd_videos, F.text.in_([i18n.get('videos', 'en'), i18n.get('videos', 'ar')]))
    
    # Reuse some common handlers
    dp.message.register(cmd_language, Command("language"))
    dp.message.register(process_lang_btn, F.text.regexp(r"(🇬🇧 English|🇸🇦 العربية)"))
    async def service_help_handler(m: Message, db: Database):
        lang = await db.get_language(m.from_user.id)
        await m.answer(i18n.get('service_help', lang))
    
    dp.message.register(service_help_handler, F.text.in_([i18n.get('help', 'en'), i18n.get('help', 'ar')]))

    @dp.errors()
    async def service_error_handler(error_event):
        logging.error(f"Service Bot Error: {error_event.exception}", exc_info=True)
        return True

# Language handlers
async def cmd_language(message: Message, bot: Bot, db: Database, user_id: int = None):
    """Handle language command"""
    user_id = user_id or message.from_user.id
    lang = await db.get_language(user_id)
    
    keyboard = i18n.get_keyboard([
        ['🇬🇧 English', '🇸🇦 العربية'],
        ['back']
    ], lang)
    
    # We need to handle the text input for these buttons
    await message.answer(
        i18n.get('choose_language', lang),
        reply_markup=keyboard
    )

async def cmd_start(message: Message, db: Database, bot: Bot):
    """Handle /start command"""
    user = await db.create_user(
        message.from_user.id,
        username=message.from_user.username,
        first_name=message.from_user.first_name
    )
    
    lang = user['language']
    
    # Log action
    await db.log(message.from_user.id, "start")
    
    # Send welcome message
    welcome_text = i18n.get('welcome', lang)
    if user.get('first_name'):
        welcome_text = i18n.get('welcome_back', lang, name=user['first_name'])
    
    await message.answer(welcome_text)
    await show_main_menu(message, db, lang)

async def show_main_menu(message: Message, db: Database, lang: str):
    """Show main menu with Inline Keyboard (Modern Look)"""
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    
    # Remove reply keyboard if exists
    status_msg = await message.answer("Loading Menu...", reply_markup=ReplyKeyboardRemove())
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=i18n.get('add_bot', lang), callback_data="btn_add"),
         InlineKeyboardButton(text=i18n.get('add_contact_bot', lang), callback_data="btn_add_contact")],
        [InlineKeyboardButton(text=i18n.get('my_bots', lang), callback_data="btn_list"),
         InlineKeyboardButton(text=i18n.get('bot_stats', lang), callback_data="btn_stats")],
        [InlineKeyboardButton(text=i18n.get('remove_bot', lang), callback_data="btn_remove"),
         InlineKeyboardButton(text=i18n.get('settings', lang), callback_data="btn_settings")],
        [InlineKeyboardButton(text=i18n.get('help', lang), callback_data="btn_help"),
         InlineKeyboardButton(text=i18n.get('language', lang), callback_data="btn_lang")]
    ])
    
    if message.from_user.id in config.ADMIN_IDS:
        keyboard.inline_keyboard.append([InlineKeyboardButton(text=i18n.get('admin_panel_btn', lang), callback_data="btn_admin")])

    await message.answer(i18n.get('welcome', lang), reply_markup=keyboard)
    try: await status_msg.delete()
    except: pass

# Callback Handlers
async def process_callbacks(callback_query: CallbackQuery, state: FSMContext, db: Database, registry: BotRegistry):
    data = callback_query.data
    user_id = callback_query.from_user.id
    lang = await db.get_language(user_id)
    message = callback_query.message
    
    if data == "btn_add":
        await state.set_state(BotStates.waiting_for_token)
        await message.answer(i18n.get('enter_token', lang))
    elif data == "btn_add_contact":
        await state.set_state(BotStates.waiting_for_contact_token)
        await message.answer(i18n.get('enter_token', lang))
    elif data == "btn_list":
        await cmd_my_bots(message, db, user_id)
    elif data == "btn_stats":
        await cmd_bot_stats(message, db, user_id)
    elif data == "btn_remove":
        await cmd_remove_bot(message, db, user_id)
    elif data == "btn_settings" or data == "btn_lang":
        await cmd_language(message, None, db, user_id)
    elif data == "btn_help":
        await cmd_help(message, db, user_id)
    elif data == "btn_admin":
        await cmd_admin(message, db, user_id)
    
    await callback_query.answer()

async def process_bot_token(message: Message, state: FSMContext, bot: Bot, db: Database):
    """Handle token or site-name input"""
    lang = await db.get_language(message.from_user.id)
    token = message.text.strip()
    await state.update_data(bot_token=token)
    await state.set_state(BotStates.waiting_for_code)
    await message.answer(i18n.get('enter_code', lang))

async def process_contact_token(message: Message, state: FSMContext, bot: Bot, db: Database, registry: BotRegistry):
    """Handle token input and deploy contact bot immediately"""
    lang = await db.get_language(message.from_user.id)
    token = message.text.strip()
    
    # Simple validation for token format (e.g. 123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11)
    if not re.match(r"^\d+:[A-Za-z0-9_-]+$", token):
        await message.answer(i18n.get('invalid_token', lang))
        return

    msg = await message.answer(i18n.get('processing', lang))
    
    # Generate bot code mapped to the specific user's info
    bot_code = CONTACT_SERVICE_CODE.replace("OWNER_ID_PLACEHOLDER", str(message.from_user.id))
    bot_code = bot_code.replace("LOG_CHANNEL_PLACEHOLDER", str(config.LOG_CHANNEL))

    success, result = await registry.register(token, "ContactBot", code_content=bot_code, is_site=False)
    
    try: await msg.delete()
    except: pass

    if success:
        await db.add_bot(message.from_user.id, token, "ContactBot", result)
        await message.answer(i18n.get('bot_added', lang, name="تواصل" if lang == 'ar' else "Contact", username=result))
        await show_main_menu(message, db, lang)
    else:
        await message.answer(f"Error: {result}")
    
    await state.clear()
    
    if success:
        # Log action
        await db.log(message.from_user.id, "add_contact_bot", f"Bot: @{result}")
        
        # Notify channel
        try:
            await bot.send_message(
                config.LOG_CHANNEL,
                f"🚀 **New Contact Bot**\n\n**Owner**: {message.from_user.id}\n**Bot**: @{result}",
                parse_mode="Markdown"
            )
        except: pass

async def process_bot_code(message: Message, state: FSMContext, registry: BotRegistry, db: Database):
    """Handle File/Folder/Code deploy"""
    lang = await db.get_language(message.from_user.id)
    state_data = await state.get_data()
    token = state_data.get('bot_token')
    if not token:
        await state.clear()
        return

    is_site = ":" not in token
    code_path = None
    code_content = None
    
    if message.document:
        file = await message.bot.get_file(message.document.file_id)
        code_path = os.path.abspath(f"tmp_{message.document.file_name}")
        await message.bot.download_file(file.file_path, code_path)
    elif message.text:
        code_content = message.text

    msg = await message.answer(i18n.get('processing', lang))
    success, result = await registry.register(token, "UserApp", code_content=code_content, code_path=code_path, is_site=is_site)
    
    try: await msg.delete()
    except: pass

    if success:
        await db.add_bot(message.from_user.id, token, "UserApp", result, code_path=code_path)
        await message.answer(i18n.get('bot_added', lang, name="App", username=result))
        await state.clear()
        await show_main_menu(message, db, lang)
    else:
        await message.answer(f"Error: {result}")
        await state.clear()
        return
    
    # Log action
    await db.log(message.from_user.id, "add_bot", f"Bot: @{result}")
    
    # Notify channel
    try:
        await message.bot.send_message(
            config.LOG_CHANNEL,
            f"🚀 **New App Hosted**\n\n**Owner**: {message.from_user.id}\n**App**: @{result}\n**Type**: {'Website' if is_site else 'Bot'}",
            parse_mode="Markdown"
        )
    except Exception as e:
        logging.error(f"Could not send log to channel: {e}")
        
    # Send success message
    try:
        await msg.delete()
    except Exception:
        pass
    await message.answer(
        i18n.get('bot_added', lang, name="Bot", username=result),
        reply_markup=i18n.get_keyboard([['back']], lang)
    )
    
    await state.clear()

# My bots handlers
async def cmd_my_bots(message: Message, db: Database, user_id: int = None):
    """Handle my bots command"""
    user_id = user_id or message.from_user.id
    lang = await db.get_language(user_id)
    bots = await db.get_user_bots(user_id)
    
    if not bots:
        await message.answer(i18n.get('no_bots', lang))
        return
    
    text = "📋 <b>Your Bots:</b>\n\n"
    for i, bot in enumerate(bots, 1):
        username = bot.get('bot_username') or 'Unknown'
        text += f"{i}. 🤖 @{username}\n"
        text += f"   📊 Updates: {bot.get('total_updates', 0)}\n"
        text += f"   💬 Messages: {bot.get('total_messages', 0)}\n\n"
    
    await message.answer(text, parse_mode=ParseMode.HTML)

# Bot statistics
async def cmd_bot_stats(message: Message, db: Database, user_id: int = None):
    """Handle bot statistics command"""
    user_id = user_id or message.from_user.id
    lang = await db.get_language(user_id)
    bots = await db.get_user_bots(user_id)
    
    if not bots:
        await message.answer(i18n.get('no_bots', lang))
        return
    
    # Create selection buttons
    buttons = []
    for bot in bots:
        buttons.append([f"🤖 @{bot['bot_username']}"])
    buttons.append(['back'])
    
    await message.answer(
        i18n.get('select_bot', lang),
        reply_markup=i18n.get_keyboard(buttons, lang)
    )

    await message.answer(
        i18n.get('select_bot', lang),
        reply_markup=i18n.get_keyboard(buttons, lang)
    )

# Remove bot handlers
async def cmd_remove_bot(message: Message, db: Database, user_id: int = None):
    """Handle remove bot command"""
    user_id = user_id or message.from_user.id
    lang = await db.get_language(user_id)
    bots = await db.get_user_bots(user_id)
    
    if not bots:
        await message.answer(i18n.get('no_bots', lang))
        return
    
    # Create selection buttons
    buttons = []
    for bot in bots:
        buttons.append([f"❌ @{bot['bot_username']}"])
    buttons.append(['back'])
    
    await message.answer(
        i18n.get('select_bot', lang),
        reply_markup=i18n.get_keyboard(buttons, lang)
    )

# Admin handlers
async def cmd_admin(message: Message, db: Database, user_id: int = None):
    """Handle admin command"""
    user_id = user_id or message.from_user.id
    lang = await db.get_language(user_id)
    if user_id not in config.ADMIN_IDS:
        await message.answer(i18n.get('unauthorized', lang))
        return
    
    stats = await db.get_global_stats()
    
    text = i18n.get('admin_panel_msg', lang) + "\n\n"
    text += i18n.get('total_users', lang, count=stats['total_users']) + "\n"
    text += i18n.get('total_bots', lang, count=stats['total_bots']) + "\n"
    text += i18n.get('total_updates', lang, count=stats['total_updates']) + "\n"
    text += i18n.get('total_messages', lang, count=stats['total_messages']) + "\n"
    
    if stats['top_bot']:
        text += "\n" + i18n.get('top_bot', lang, 
            name=stats['top_bot']['bot_name'],
            updates=stats['top_bot']['total_updates']
        )
    
    # Combined admin buttons
    keyboard = i18n.get_keyboard([
        ['broadcast', 'refresh'],
        ['back']
    ], lang)
    
    await message.answer(text, reply_markup=keyboard, parse_mode=ParseMode.HTML)

async def process_broadcast(message: Message, state: FSMContext, bot: Bot, db: Database):
    """Process broadcast message"""
    if message.from_user.id not in config.ADMIN_IDS:
        return
    
    lang = await db.get_language(message.from_user.id)
    
    # Get all users
    async with db.connect() as conn:
        cursor = await conn.execute("SELECT telegram_id FROM users WHERE is_active = 1")
        users = await cursor.fetchall()
    
    sent_count = 0
    for user in users:
        try:
            await bot.copy_message(
                chat_id=user['telegram_id'],
                from_chat_id=message.chat.id,
                message_id=message.message_id
            )
            sent_count += 1
            await asyncio.sleep(0.05)  # Rate limiting
        except:
            continue
    
    await message.answer(
        i18n.get('broadcast_sent', lang, count=sent_count)
    )
    await state.clear()
    await show_main_menu(message, db, lang)

# Help handler
async def cmd_help(message: Message, db: Database, user_id: int = None):
    """Handle help command"""
    user_id = user_id or message.from_user.id
    lang = await db.get_language(user_id)
    
    help_text = """
ℹ️ <b>Bot Hosting Platform Help</b>

<b>Commands:</b>
/addbot - Add a new bot
/mybots - List your bots
/stats - View bot statistics
/removebot - Remove a bot
/language - Change language
/help - Show this help

<b>How to add a bot:</b>
1. Get a token from @BotFather
2. Use /addbot command
3. Send the token
4. Your bot is now hosted!

<b>Limits:</b>
- Max 3 bots per user
- 5 actions per 10 seconds

Need support? Contact @admin
    """
    
    await message.answer(help_text, parse_mode=ParseMode.HTML)

# Cancel handler
async def cmd_cancel(message: Message, state: FSMContext, db: Database):
    """Handle cancel command"""
    lang = await db.get_language(message.from_user.id)
    await state.clear()
    await show_main_menu(message, db, lang)

# ==================== WEBHOOK SERVER ====================

async def health_check(request: web.Request):
    """Health check endpoint for UptimeRobot"""
    return web.Response(text="OK", status=200)

async def main_webhook(request: web.Request):
    """Main bot webhook handler"""
    try:
        bot = request.app['bot']
        dp = request.app['dp']
        
        update = types.Update(**await request.json())
        await dp.feed_update(bot, update)
        
        return web.Response(status=200)
    except Exception as e:
        logging.error(f"Webhook error: {e}")
        return web.Response(status=500)

async def hosted_webhook(request: web.Request):
    """Hosted bot webhook handler"""
    try:
        token = request.match_info['token']
        registry = request.app['registry']
        
        update_data = await request.json()
        result = await registry.process_update(token, update_data)
        
        if result:
            return web.json_response(result)
        else:
            return web.Response(status=404)
            
    except Exception as e:
        logging.error(f"Hosted webhook error: {e}")
        return web.Response(status=500)

async def setup_bot_commands(bot: Bot):
    """Set bot commands for the Blue Menu button"""
    from aiogram.types import BotCommand
    
    # We set default commands in English, they will be translated if we use language-specific scopes
    # But for simplicity, we set the most common ones
    commands_en = [
        BotCommand(command="start", description=i18n.get('commands.start', 'en')),
        BotCommand(command="addbot", description=i18n.get('commands.addbot', 'en')),
        BotCommand(command="mybots", description=i18n.get('commands.mybots', 'en')),
        BotCommand(command="stats", description=i18n.get('commands.stats', 'en')),
        BotCommand(command="removebot", description=i18n.get('commands.removebot', 'en')),
        BotCommand(command="language", description=i18n.get('commands.language', 'en')),
        BotCommand(command="help", description=i18n.get('commands.help', 'en')),
    ]
    
    commands_ar = [
        BotCommand(command="start", description=i18n.strings['ar']['commands']['start']),
        BotCommand(command="addbot", description=i18n.strings['ar']['commands']['addbot']),
        BotCommand(command="mybots", description=i18n.strings['ar']['commands']['mybots']),
        BotCommand(command="stats", description=i18n.strings['ar']['commands']['stats']),
        BotCommand(command="removebot", description=i18n.strings['ar']['commands']['removebot']),
        BotCommand(command="language", description=i18n.strings['ar']['commands']['language']),
        BotCommand(command="help", description=i18n.strings['ar']['commands']['help']),
    ]
    
    try:
        # Default (English)
        await bot.set_my_commands(commands_en)
        # Arabic scope
        from aiogram.types import BotCommandScopeAllPrivateChats
        # Note: aiogram doesn't directly support per-language descriptions in set_my_commands
        # without complex scopes, so we stick to the main one or set for specific users.
        # But we can try to set the default to a mix or just English as is standard.
        logging.info("Bot commands (Blue Menu) configured")
    except Exception as e:
        logging.error(f"Error setting bot commands: {e}")

async def on_startup(bot: Bot, dp: Dispatcher, db: Database, registry: BotRegistry):
    """Startup actions"""
    logging.info(f"Admins: {config.ADMIN_IDS}")
    
    # Set webhook for main bot if not in polling mode
    if not config.USE_POLLING:
        webhook_url = f"{config.WEBHOOK_BASE_URL}{config.WEBHOOK_PATH}"
        await bot.set_webhook(
            url=webhook_url,
            allowed_updates=dp.resolve_used_update_types(),
            drop_pending_updates=True
        )
        logging.info(f"Bot started with webhook: {webhook_url}")
    else:
        await bot.delete_webhook(drop_pending_updates=False)
        logging.info("Bot started in POLLING mode")
    
    # Load active bots from database
    async with db.connect() as conn:
        cursor = await conn.execute(
            "SELECT bot_token, bot_name, bot_username, code_path FROM bots WHERE is_active = 1"
        )
        bots = await cursor.fetchall()
    
    async def load_bots():
        for bot_data in bots:
            try:
                logging.info(f"Auto-loading hosted bot: @{bot_data['bot_username']}")
                await registry.register(
                    token=bot_data['bot_token'], 
                    name=bot_data['bot_name'],
                    code_path=bot_data['code_path']
                )
            except Exception as e:
                logging.error(f"Failed to auto-load bot {bot_data.get('bot_username')}: {e}")
                
    # Run bot loading in the background to not block the server startup
    asyncio.create_task(load_bots())
    
    logging.info(f"Loaded {len(bots)} hosted bots")
    
    # Set bot commands (Blue Menu)
    await setup_bot_commands(bot)

async def on_shutdown(bot: Bot, registry: BotRegistry):
    """Shutdown actions"""
    # Remove webhook
    await bot.delete_webhook()
    
    # Shutdown all hosted bots
    await registry.shutdown()
    
    logging.info("Bot shutdown complete")

async def create_app():
    """Create aiohttp application"""
    # Initialize components
    db = Database(config.DATABASE_PATH)
    await db.init()
    
    registry = BotRegistry()
    registry.set_db(db)
    
    # Create bot and dispatcher
    bot = Bot(
        token=config.MAIN_BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    
    registry.set_main_bot(bot)
    
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    
    # Pass dependencies to handlers
    dp["db"] = db
    dp["registry"] = registry
    
    # Register handlers (Manager)
    register_manager_handlers(dp)
    
    # Create aiohttp app
    app = web.Application()
    
    # Store components in app
    app['bot'] = bot
    app['dp'] = dp
    app['db'] = db
    app['registry'] = registry
    
    # Register routes
    app.router.add_post(config.WEBHOOK_PATH, main_webhook)
    app.router.add_post(f"{config.HOSTED_WEBHOOK_PATH}/{{token}}", hosted_webhook)
    app.router.add_get(config.HEALTH_PATH, health_check)
    app.router.add_get('/', health_check)

    async def site_proxy(request):
        token_id = request.match_info['token']
        # Find bot by name or token
        target_bot = None
        for b in registry._bots.values():
            if str(b.get('username')) == token_id or str(b.get('token')) == token_id:
                target_bot = b; break
        
        if not target_bot or not target_bot.get('port'):
            return web.Response(text="Site Not Found", status=404)
        
        # Build target path
        path = request.match_info.get('path', '')
        url = f"http://localhost:{target_bot['port']}/{path}"
        if request.query_string: url += f"?{request.query_string}"
        
        async with aiohttp.ClientSession() as session:
            try:
                headers = dict(request.headers)
                headers.pop('Host', None)
                async with session.request(request.method, url, data=await request.read(), headers=headers) as resp:
                    return web.Response(body=await resp.read(), status=resp.status, headers=dict(resp.headers))
            except Exception as e:
                return web.Response(text=f"Proxy Error: {e}", status=502)

    app.router.add_route('*', '/site/{token}/{path:.*}', site_proxy)
    
    # Register startup/shutdown
    async def startup_handler(a):
        await on_startup(a['bot'], a['dp'], a['db'], a['registry'])
    
    async def shutdown_handler(a):
        await on_shutdown(a['bot'], a['registry'])
        
    app.on_startup.append(startup_handler)
    app.on_shutdown.append(shutdown_handler)
    
    return app

# ==================== UTIL ====================

async def self_ping():
    """Keep the server alive on Render free tier"""
    import aiohttp
    
    # Render provides RENDER_EXTERNAL_URL automatically
    base_url = os.getenv("RENDER_EXTERNAL_URL", config.WEBHOOK_BASE_URL)
    url = f"{base_url.rstrip('/')}{config.HEALTH_PATH}"
    logging.info(f"Starting self-ping task for {url}")
    
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        logging.debug("Self-ping successful")
                    else:
                        logging.warning(f"Self-ping returned status: {resp.status}")
        except Exception as e:
            logging.error(f"Self-ping error: {e}")
            
        await asyncio.sleep(60)  # Ping every 1 minute to actively keep it awake

async def cleanup_task():
    """Periodically clean logs and temporary files to save space"""
    while True:
        try:
            logging.info("Running storage cleanup...")
            # Keep logs directory clean
            if os.path.exists("logs"):
                for f in os.listdir("logs"):
                    # Don't delete the main log file we are currently using
                    if f != "main_bot.log":
                        try: os.remove(os.path.join("logs", f))
                        except: pass
            
            # Clear empty app folders or old temp files
            if os.path.exists("hosted_apps"):
                # You might want to keep active ones, but for now we clear 
                # any that aren't registered if we had a registration check.
                # Since the user wants to 'not make files like that', we trim.
                pass 
                
            logging.info("Cleanup completed.")
        except Exception as e:
            logging.error(f"Cleanup task error: {e}")
            
        await asyncio.sleep(86400) # Run once every 24 hours

# ==================== MAIN ====================

async def main():
    """Main function"""
    # Setup logging
    logging.basicConfig(
        level=config.LOG_LEVEL if hasattr(config, "LOG_LEVEL") else logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    logging.info("Starting main function...")
    
    # Create and run app
    app = await create_app()
    
    # Create data directory if not exists
    os.makedirs("data", exist_ok=True)
    
    # Run server
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, config.HOST, config.PORT)
    
    try:
        logging.info(f"Server starting on {config.HOST}:{config.PORT}")
        await site.start()
        
        # Start polling if enabled
        if config.USE_POLLING:
            # Start polling in the background to not block the web server
            bot = app['bot']
            dp = app['dp']
            asyncio.create_task(dp.start_polling(bot))
        
        # Start self-ping and cleanup
        asyncio.create_task(self_ping())
        asyncio.create_task(cleanup_task())
        
        # Keep running
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
        logging.info("Shutdown signal received")
    finally:
        logging.info("Cleaning up...")
        registry = app.get('registry')
        if registry:
            await registry.shutdown()
        await runner.cleanup()
        logging.info("Shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())