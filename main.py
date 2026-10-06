import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict

import aiosqlite
from dotenv import load_dotenv
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
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
# 1. SOZLAMALAR VA BAZA MANZILI
# ---------------------------------------------------------
load_dotenv()

API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
USER_SESSION = os.getenv("USER_SESSION", "")  # Userbot uchun StringSession
DB_NAME = os.getenv("DB_NAME", "telelog_pro.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot_activity.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("TeleLogPro")

if not API_ID or not API_HASH or not BOT_TOKEN:
    logger.critical("❌ CRITICAL ERROR: API_ID, API_HASH yoki BOT_TOKEN topilmadi!")
    sys.exit(1)


# ---------------------------------------------------------
# 2. BAZA BO'LIMI (DatabaseManager)
# ---------------------------------------------------------
class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init_db(self) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS user_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    first_name TEXT,
                    last_name TEXT,
                    username TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS message_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    chat_name TEXT,
                    user_id INTEGER NOT NULL,
                    message_type TEXT NOT NULL,
                    content TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_user_history ON user_history(user_id);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_message_logs_user ON message_logs(user_id);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_message_logs_type ON message_logs(message_type);")
            await db.commit()
            logger.info("Database muvaffaqiyatli tayyorlandi.")

    async def log_user_info(self, user: User) -> None:
        if not user or not hasattr(user, "id"):
            return
        first_name = user.first_name or ""
        last_name = user.last_name or ""
        username = user.username or ""

        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT first_name, last_name, username FROM user_history WHERE user_id = ? ORDER BY updated_at DESC LIMIT 1",
                (user.id,),
            ) as cursor:
                last_record = await cursor.fetchone()

            if not last_record or last_record != (first_name, last_name, username):
                await db.execute(
                    "INSERT INTO user_history (user_id, first_name, last_name, username) VALUES (?, ?, ?, ?)",
                    (user.id, first_name, last_name, username),
                )
                await db.commit()

    async def save_message_log(self, chat_id: int, chat_name: str, user_id: int, msg_type: str, content: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO message_logs (chat_id, chat_name, user_id, message_type, content) VALUES (?, ?, ?, ?, ?)",
                (chat_id, chat_name, user_id, msg_type, content),
            )
            await db.commit()

    async def fetch_user_report(self, target_id: int) -> Dict[str, Any]:
        report = {}
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            # 1. Usernamelar tarixi
            async with db.execute("SELECT first_name, last_name, username, updated_at FROM user_history WHERE user_id = ? ORDER BY updated_at ASC", (target_id,)) as c:
                report["history"] = [dict(row) for row in await c.fetchall()]

            # 2. Faol bo'lgan barcha guruhlar/chatlar ro'yxati
            async with db.execute("SELECT DISTINCT chat_name, chat_id FROM message_logs WHERE user_id = ?", (target_id,)) as c:
                report["chats"] = [dict(row) for row in await c.fetchall()]

            # 3. Barcha media turlari (cheklanmagan to'liq ro'yxat)
            for m_type in ["golos", "rasm", "lokatsiya", "stiker"]:
                async with db.execute("SELECT chat_name, content, created_at FROM message_logs WHERE user_id = ? AND message_type = ? ORDER BY created_at DESC", (target_id, m_type)) as c:
                    report[m_type] = [dict(row) for row in await c.fetchall()]

            # 4. Umumiy statistika
            async with db.execute("SELECT COUNT(DISTINCT chat_id), COUNT(*) FROM message_logs WHERE user_id = ?", (target_id,)) as c:
                stats = await c.fetchone()
                report["group_count"] = stats[0] if stats else 0
                report["msg_count"] = stats[1] if stats else 0

        return report


# ---------------------------------------------------------
# 3. MEDIA PARSER
# ---------------------------------------------------------
class MediaProcessor:
    @staticmethod
    def parse_event(event) -> tuple[str, str]:
        msg_type = "text"
        content = event.text[:150] if event.text else ""

        if event.voice:
            msg_type = "golos"
            duration = getattr(event.voice, "duration", 0)
            content = f"Ovozli xabar ({duration} soniya)"
        elif isinstance(event.media, MessageMediaPhoto) or (event.file and event.file.ext in [".jpg", ".png", ".jpeg"]):
            msg_type = "rasm"
            content = "Foto Rasm"
        elif isinstance(event.media, (MessageMediaGeo, MessageMediaGeoLive)):
            msg_type = "lokatsiya"
            geo = event.media.geo
            content = f"https://www.google.com/maps?q={geo.lat},{geo.long}"
        elif event.file and event.file.ext == ".webp":
            msg_type = "stiker"
            sticker_pack = "Noma'lum"
            if isinstance(event.media, MessageMediaDocument):
                for attr in event.media.document.attributes:
                    if hasattr(attr, "stickerset") and hasattr(attr.stickerset, "short_name"):
                        sticker_pack = attr.stickerset.short_name
            content = f"StickerPack: {sticker_pack}"

        return msg_type, content


# ---------------------------------------------------------
# 4. TELELOG CORE
# ---------------------------------------------------------
class TeleLogBot:
    def __init__(self, api_id: int, api_hash: str, bot_token: str, user_session: str, db_name: str):
        self.db = DatabaseManager(db_name)
        self.bot_client = TelegramClient("bot_telelog_session", api_id, api_hash)
        self.bot_token = bot_token
        
        self.user_client = None
        if user_session:
            self.user_client = TelegramClient(StringSession(user_session), api_id, api_hash)

    def build_user_menu(self, user_id: int):
        return [
            [Button.inline("📊 Stats", data=f"stats_{user_id}"), Button.inline("🏷 Names", data=f"names_{user_id}")],
            [Button.inline("👥 Groups & Chats", data=f"groups_{user_id}"), Button.inline("🎙 Voice & Media", data=f"media_{user_id}")],
            [Button.inline("📍 All Locations", data=f"loc_{user_id}"), Button.inline("🎨 Stickers", data=f"stickers_{user_id}")]
        ]

    async def start(self):
        await self.db.init_db()
        await self.bot_client.start(bot_token=self.bot_token)
        
        tasks = [self.bot_client.run_until_disconnected()]

        if self.user_client:
            await self.user_client.start()
            logger.info("🚀 Userbot va Bot ikkalasi ham faollashtirildi!")
            tasks.append(self.user_client.run_until_disconnected())
        else:
            logger.warning("⚠️ USER_SESSION kiritilmagani uchun faqat Bot rejimida ishlamoqda.")

        self._register_handlers()
        await asyncio.gather(*tasks)

    def _register_handlers(self):
        # Userbot log xabarlar
        if self.user_client:
            @self.user_client.on(events.NewMessage)
            async def on_userbot_message(event):
                if not event.sender_id:
                    return
                try:
                    sender = await event.get_sender()
                    if isinstance(sender, User):
                        await self.db.log_user_info(sender)

                    chat = await event.get_chat()
                    chat_name = "Shaxsiy chat"
                    if isinstance(chat, (Chat, Channel)):
                        chat_name = chat.title

                    msg_type, content = MediaProcessor.parse_event(event)
                    await self.db.save_message_log(
                        chat_id=event.chat_id,
                        chat_name=chat_name,
                        user_id=event.sender_id,
                        msg_type=msg_type,
                        content=content,
                    )
                except Exception as e:
                    logger.error(f"Userbot log xatosi: {e}")

        # Bot Buyruqlari
        @self.bot_client.on(events.NewMessage(pattern=r"^/start$"))
        async def on_start(event):
            await event.reply("🔎 **TeleLog Botiga xush kelibsiz!**\nFoydalanuvchini tekshirish uchun **User ID** kiriting:")

        @self.bot_client.on(events.NewMessage)
        async def on_bot_message(event):
            if event.text and event.text.isdigit():
                user_id = int(event.text)
                report = await self.db.fetch_user_report(user_id)

                text = f"⚙️ **Target ID:** `{user_id}`\n"
                text += f"💬 **Jami guruh va chatlari:** {report['group_count']} ta\n"
                text += f"📩 **Jami xabarlari:** {report['msg_count']} ta\n\n"
                text += "Tugmalar orqali ma'lumotlarni ko'rishingiz mumkin:"

                await event.reply(text, buttons=self.build_user_menu(user_id))

        # Inline Button Callback
        @self.bot_client.on(events.CallbackQuery)
        async def on_callback(event):
            data = event.data.decode("utf-8")
            action, target_id = data.split("_")
            target_id = int(target_id)
            report = await self.db.fetch_user_report(target_id)

            if action == "stats":
                res = f"📊 **STATISTIKA (ID: `{target_id}`)**\n\n"
                res += f"• Faol guruh/chatlari: **{report['group_count']}** ta\n"
                res += f"• Jami xabarlari: **{report['msg_count']}** ta\n"
                res += f"• Ovozli xabarlari: **{len(report['golos'])}** ta\n"
                res += f"• Rasmlari: **{len(report['rasm'])}** ta\n"
                res += f"• Lokatsiyalari: **{len(report['lokatsiya'])}** ta\n"
                res += f"• Stikerlari: **{len(report['stiker'])}** ta\n"
                await event.answer()
                await event.edit(res, buttons=self.build_user_menu(target_id))

            elif action == "names":
                res = f"🏷 **USER TARIXI (ISM VA USERNAME) (ID: `{target_id}`)**\n\n"
                if report["history"]:
                    for h in report["history"]:
                        res += f"• `[{h['updated_at']}]` {h['first_name']} {h['last_name'] or ''} | @{h['username'] or 'yo-q'}\n"
                else:
                    res += "_Ism va username tarixi topilmadi._"
                await event.answer()
                await event.edit(res, buttons=self.build_user_menu(target_id))

            elif action == "groups":
                res = f"👥 **XABAR YOZGAN BARCHA GURUHLARI VA CHATLARI (ID: `{target_id}`)**\n\n"
                if report["chats"]:
                    for idx, c in enumerate(report["chats"], 1):
                        res += f"{idx}. **{c['chat_name']}** `(ID: {c['chat_id']})`\n"
                else:
                    res += "_Guruhlar va chatlar topilmadi._"
                await event.answer()
                await event.edit(res, buttons=self.build_user_menu(target_id))

            elif action == "media":
                res = f"🎙 **OVOZLI XABARLAR VA MEDIA (ID: `{target_id}`)**\n\n"
                if report["golos"]:
                    res += "**Ovozli xabarlar (Golos):**\n"
                    for v in report["golos"]:
                        res += f"• `[{v['created_at']}]` **{v['chat_name']}** -> {v['content']}\n"
                else:
                    res += "_Ovozli xabarlar topilmadi._\n"
                await event.answer()
                await event.edit(res, buttons=self.build_user_menu(target_id))

            elif action == "loc":
                res = f"📍 **BARCHA CHATLARGA YUBORILGAN LOKATSIYALAR LINKLARI (ID: `{target_id}`)**\n\n"
                if report["lokatsiya"]:
                    for idx, l in enumerate(report["lokatsiya"], 1):
                        res += f"{idx}. `[{l['created_at']}]` **{l['chat_name']}**:\n🔗 [Google Maps Xaritasi]({l['content']})\n\n"
                else:
                    res += "_Hech qaysi chatga lokatsiya yuborilmagan._"
                await event.answer()
                await event.edit(res, buttons=self.build_user_menu(target_id), link_preview=False)

            elif action == "stickers":
                res = f"🎨 **YUBORILGAN STIKERLAR (ID: `{target_id}`)**\n\n"
                if report["stiker"]:
                    for s in report["stiker"]:
                        res += f"• `[{s['created_at']}]` **{s['chat_name']}** -> {s['content']}\n"
                else:
                    res += "_Stikerlar topilmadi._"
                await event.answer()
                await event.edit(res, buttons=self.build_user_menu(target_id))


# ---------------------------------------------------------
# 5. RUN
# ---------------------------------------------------------
if __name__ == "__main__":
    try:
        bot = TeleLogBot(
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            user_session=USER_SESSION,
            db_name=DB_NAME,
        )
        asyncio.run(bot.start())
    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 Bot to'xtatildi.")
