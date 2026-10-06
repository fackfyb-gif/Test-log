import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional

import aiosqlite
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.tl.types import (
    Channel,
    Chat,
    MessageMediaDocument,
    MessageMediaGeo,
    MessageMediaGeoLive,
    MessageMediaPhoto,
    User,
)

# ---------------------------------------------------------
# 1. ATROF-MUHIT VA LOGGING SOZLAMALARI
# ---------------------------------------------------------
load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_NAME = os.getenv("DB_NAME", "telelog_pro.db")

if not all([API_ID, API_HASH, BOT_TOKEN]):
    print(
        "❌ CRITICAL ERROR: .env faylida API_ID, API_HASH yoki BOT_TOKEN topilmadi!"
    )
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot_activity.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("TeleLogPro")


# ---------------------------------------------------------
# 2. BAZA BILAN ASINXRON ISHLASH SINFI (DatabaseManager)
# ---------------------------------------------------------
class DatabaseManager:
    """Asinxron SQLite ma'lumotlar bazasini boshqarish sinfi"""

    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init_db(self) -> None:
        """Jadvallarni va indekslarni yaratish"""
        async with aiosqlite.connect(self.db_path) as db:
            # 1. Usernamelar va Ismlar o'zgarishi tarixi jadvali
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS user_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    first_name TEXT,
                    last_name TEXT,
                    username TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # 2. Barcha xabarlar va media faoliyati jadvali
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS message_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    chat_name TEXT,
                    user_id INTEGER NOT NULL,
                    message_type TEXT NOT NULL,
                    content TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            )

            # Tezkor qidiruv uchun Indekslar yaratish
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_user_history ON user_history(user_id);"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_message_logs_user ON message_logs(user_id);"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_message_logs_type ON message_logs(message_type);"
            )

            await db.commit()
            logger.info("Database jadvallari va indekslari muvaffaqiyatli tayyorlandi.")

    async def log_user_info(self, user: User) -> None:
        """Foydalanuvchining ism va username o'zgarishini tekshirish va saqlash"""
        if not user or not hasattr(user, "id"):
            return

        first_name = user.first_name or ""
        last_name = user.last_name or ""
        username = user.username or ""

        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT first_name, last_name, username 
                FROM user_history 
                WHERE user_id = ? 
                ORDER BY updated_at DESC LIMIT 1
            """,
                (user.id,),
            ) as cursor:
                last_record = await cursor.fetchone()

            current_data = (first_name, last_name, username)

            if not last_record or last_record != current_data:
                await db.execute(
                    """
                    INSERT INTO user_history (user_id, first_name, last_name, username)
                    VALUES (?, ?, ?, ?)
                """,
                    (user.id, first_name, last_name, username),
                )
                await db.commit()
                logger.info(
                    f"📝 USER UPDATED: ID={user.id} | Name='{first_name} {last_name}' | Username=@{username}"
                )

    async def save_message_log(
        self,
        chat_id: int,
        chat_name: str,
        user_id: int,
        msg_type: str,
        content: str,
    ) -> None:
        """Xabar faoliyatini bazaga yozish"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO message_logs (chat_id, chat_name, user_id, message_type, content)
                VALUES (?, ?, ?, ?, ?)
            """,
                (chat_id, chat_name, user_id, msg_type, content),
            )
            await db.commit()

    async def fetch_user_report(self, target_id: int) -> Dict[str, Any]:
        """Foydalanuvchi haqida to'liq tahliliy hisobot yig'ish"""
        report = {}
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            # 1. Username tarixi
            async with db.execute(
                "SELECT first_name, last_name, username, updated_at FROM user_history WHERE user_id = ? ORDER BY updated_at ASC",
                (target_id,),
            ) as cursor:
                report["history"] = [dict(row) for row in await cursor.fetchall()]

            # 2. Faol bo'lgan chatlari
            async with db.execute(
                "SELECT DISTINCT chat_name, chat_id FROM message_logs WHERE user_id = ?",
                (target_id,),
            ) as cursor:
                report["chats"] = [dict(row) for row in await cursor.fetchall()]

            # 3. Media turlari bo'yicha saralash
            for m_type in ["golos", "rasm", "lokatsiya", "stiker"]:
                async with db.execute(
                    "SELECT chat_name, content, created_at FROM message_logs WHERE user_id = ? AND message_type = ? ORDER BY created_at DESC",
                    (target_id, m_type),
                ) as cursor:
                    report[m_type] = [
                        dict(row) for row in await cursor.fetchall()
                    ]

        return report


# ---------------------------------------------------------
# 3. MEDIA SHAKLLANTIRISH TIZIMI (MediaProcessor)
# ---------------------------------------------------------
class MediaProcessor:
    """Kelgan xabarlarning turini va kontentini tahlil qiluvchi servis"""

    @staticmethod
    def parse_event(event) -> tuple[str, str]:
        msg_type = "text"
        content = event.text[:150] if event.text else ""

        # Golos (Ovozli xabar)
        if event.voice:
            msg_type = "golos"
            duration = getattr(event.voice, "duration", 0)
            content = f"Ovozli xabar ({duration} soniya)"

        # Rasm
        elif isinstance(event.media, MessageMediaPhoto) or (
            event.file and event.file.ext in [".jpg", ".png", ".jpeg"]
        ):
            msg_type = "rasm"
            content = "Foto Rasm"

        # Lokatsiya
        elif isinstance(event.media, (MessageMediaGeo, MessageMediaGeoLive)):
            msg_type = "lokatsiya"
            geo = event.media.geo
            content = f"https://www.google.com/maps?q={geo.lat},{geo.long}"

        # Stiker
        elif event.file and event.file.ext == ".webp":
            msg_type = "stiker"
            sticker_pack = "Noma'lum"
            if isinstance(event.media, MessageMediaDocument):
                for attr in event.media.document.attributes:
                    if hasattr(attr, "stickerset") and hasattr(
                        attr.stickerset, "short_name"
                    ):
                        sticker_pack = attr.stickerset.short_name
            content = f"StickerPack: {sticker_pack}"

        return msg_type, content


# ---------------------------------------------------------
# 4. ASOSIY TELEGRAM BOT CORE (TeleLogBot)
# ---------------------------------------------------------
class TeleLogBot:
    """Kuzatuvchi va buyruqlarga javob beruvchi bot strukturasi"""

    def __init__(self, api_id: int, api_hash: str, bot_token: str, db_name: str):
        self.db = DatabaseManager(db_name)
        self.client = TelegramClient("bot_telelog_session", api_id, api_hash)
        self.bot_token = bot_token

    async def start(self):
        await self.db.init_db()
        await self.client.start(bot_token=self.bot_token)
        logger.info("🤖 TeleLog Enterprise Bot muvaffaqiyatli ishga tushdi!")

        self._register_handlers()
        
        # Konsol oyna paneli asinxron tarzda alohida ishlaydi
        asyncio.create_task(self._console_control_panel())

        await self.client.run_until_disconnected()

    def _register_handlers(self):
        """Voqealar va xabarlar handlerlarini ro'yxatga olish"""

        @self.client.on(events.NewMessage)
        async def on_new_message(event):
            if not event.sender_id:
                return

            try:
                # 1. Foydalanuvchini bazada yangilash
                sender = await event.get_sender()
                if isinstance(sender, User):
                    await self.db.log_user_info(sender)

                # 2. Chat ma'lumotlarini olish
                chat = await event.get_chat()
                chat_name = "Shaxsiy chat"
                if isinstance(chat, (Chat, Channel)):
                    chat_name = chat.title

                # 3. Mediani tahlil qilish va saqlash
                msg_type, content = MediaProcessor.parse_event(event)
                await self.db.save_message_log(
                    chat_id=event.chat_id,
                    chat_name=chat_name,
                    user_id=event.sender_id,
                    msg_type=msg_type,
                    content=content,
                )

            except Exception as e:
                logger.error(f"Xabarni ishlashda xatolik: {e}", exc_info=True)

        @self.client.on(events.NewMessage(pattern=r"/report (\d+)"))
        async def on_report_command(event):
            """Telegram'ning o'zida /report <user_id> buyrug'ini qabul qilish"""
            user_id = int(event.pattern_match.group(1))
            await event.reply(f"🔍 ID: `{user_id}` bo'yicha hisobot shakllantirilmoqda...")
            
            report_text = await self._generate_report_string(user_id)
            await event.reply(report_text, parse_mode="md")

    async def _generate_report_string(self, target_id: int) -> str:
        """Hisobotni matn ko'rinishiga keltiruvchi yordamchi funksiya"""
        data = await self.db.fetch_user_report(target_id)
        
        res = [f"📊 **FOYDALANUVCHI HISOBOTI (ID: `{target_id}`)**\n"]

        # Username tarixi
        res.append("🆔 **1. Username & Ism Tarixi:**")
        if data["history"]:
            for h in data["history"]:
                res.append(f"  • `[{h['updated_at']}]` {h['first_name']} {h['last_name']} | @{h['username']}")
        else:
            res.append("  _Ma'lumot topilmadi._")

        # Chatlar
        res.append("\n💬 **2. Faol bo'lgan guruhlari:**")
        if data["chats"]:
            for c in data["chats"]:
                res.append(f"  • {c['chat_name']} `(ID: {c['chat_id']})`")
        else:
            res.append("  _Guruhlar topilmadi._")

        # Goloslar
        res.append("\n🎙 **3. Ovozli xabarlar (Golos):**")
        if data["golos"]:
            for v in data["golos"][:5]: # Oxirgi 5 tasini ko'rsatish
                res.append(f"  • `[{v['created_at']}]` {v['chat_name']} -> {v['content']}")
        else:
            res.append("  _Goloslar topilmadi._")

        # Lokatsiyalar
        res.append("\n📍 **4. Lokatsiyalar:**")
        if data["lokatsiya"]:
            for l in data["lokatsiya"][:5]:
                res.append(f"  • `[{l['created_at']}]` {l['chat_name']} -> [Google Maps]({l['content']})")
        else:
            res.append("  _Lokatsiyalar topilmadi._")

        return "\n".join(res)

    async def _console_control_panel(self):
        """Asinxron konsol boshqaruv paneli"""
        await asyncio.sleep(3)
        print("\n" + "=" * 60)
        print("💻 Enterprise Control Panel Tayyor.")
        print("Qidiruv uchun User ID ni yozib ENTER bosing.")
        print("=" * 60 + "\n")

        loop = asyncio.get_event_loop()
        while True:
            try:
                user_input = await loop.run_in_executor(None, input, "ENTER USER ID > ")
                user_input = user_input.strip()

                if user_input.isdigit():
                    uid = int(user_input)
                    report_text = await self._generate_report_string(uid)
                    print("\n" + report_text + "\n")
                else:
                    print("⚠️ Iltimos, faqat musbat raqamlardan iborat ID kiriting!")
            except Exception as e:
                logger.error(f"Konsol panel xatosi: {e}")


# ---------------------------------------------------------
# 5. DASTURNI ISHGA TUSHIRISH (ENTRYPOINT)
# ---------------------------------------------------------
if __name__ == "__main__":
    try:
        bot = TeleLogBot(
            api_id=int(API_ID),
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            db_name=DB_NAME,
        )
        asyncio.run(bot.start())
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 Bot to'xtatildi.")
