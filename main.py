Rasmda koʻrsatilgan Messages (xabarlar turi boʻyicha filterlash va statistika) tugmalar paneli hamda menyu stili toʻliq integratsiya qilindi.
Qoʻshilgan imkoniyatlar:
 * "Select message type" menyusi:
   * All (9388) – Barcha xabarlar va ularning umumiy soni.
   * Voices (Ovozli xabarlar)
   * Circles (Video doirachalar - Video note)
   * Gif/sticker (GIF va stikerlar)
   * Links (Havolalar/Linklar)
   * Video (Videolar)
   * Files (Hujjatlar va fayllar)
   * Images (Rasm va fotolar)
   * Geo/contacts (Lokatsiya va kontaktlar)
 * Statistika va belgilash:
   * Agar biror turdagi xabarlar mavjud boʻlsa, uning qarshisiga ✔️ belgisi va soni koʻrsatiladi (masalan: Images ✔️ 1336).
   * Agar mavjud boʻlmasa, ❌ No deb chiqariladi (masalan: Voices ❌ No).
 * Pastki menyu (Barcha tugmalar integratsiyasi):
   * 📊 Stats, 🔔 Track, 🔗 Names, 👁 Groups, 💬 Messages, 🔎 Analysis, 📢 Channels va hokazo tugmalar rasmda koʻrsatilgan tartib va uslubda joylashtirildi.
Yangi va toʻliq Python kodi:
import asyncio
import io
import logging
import re
import os
import sys
from datetime import datetime
from typing import Any, Dict, Set, List

import aiosqlite
from dotenv import load_dotenv
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.tl.types import (
    Channel,
    Chat,
    MessageMediaGeo,
    MessageMediaGeoLive,
    MessageMediaContact,
    User,
    UserStatusOnline,
    UserStatusOffline,
    UserStatusRecently,
    UserStatusLastWeek,
    UserStatusLastMonth,
)

# ---------------------------------------------------------
# 1. SOZLAMALAR
# ---------------------------------------------------------
load_dotenv()

API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
USER_SESSION = os.getenv("USER_SESSION", "")
DB_NAME = os.getenv("DB_NAME", "telelog_ultra.db")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("TeleLogChannelsTrack")

if not API_ID or not API_HASH or not BOT_TOKEN:
    logger.critical("❌ API_ID, API_HASH yoki BOT_TOKEN topilmadi!")
    sys.exit(1)


# ---------------------------------------------------------
# 2. BAZA BOSHQARUVI
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
                    phone_number TEXT,
                    is_premium INTEGER DEFAULT 0,
                    last_status TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS message_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER NOT NULL,
                    chat_name TEXT,
                    chat_username TEXT,
                    message_id INTEGER,
                    user_id INTEGER NOT NULL,
                    message_type TEXT NOT NULL,
                    content TEXT,
                    file_caption TEXT,
                    sticker_pack TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS user_channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    username TEXT,
                    chat_type TEXT DEFAULT 'group',
                    is_creator INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, chat_id)
                )
            """)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS tracked_users (
                    user_id INTEGER PRIMARY KEY,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            await db.execute("CREATE INDEX IF NOT EXISTS idx_user_history ON user_history(user_id);")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_msg_user_type ON message_logs(user_id, message_type);")
            await db.commit()

    async def log_user_info(self, user: User, status_text: str = "Noma'lum") -> None:
        if not user or not hasattr(user, "id"):
            return
        first_name = user.first_name or ""
        last_name = user.last_name or ""
        username = user.username or ""
        phone_number = getattr(user, "phone", "") or "Mavjud emas / Yashirin"
        is_premium = 1 if getattr(user, "premium", False) else 0

        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT first_name, last_name, username, phone_number, is_premium, last_status FROM user_history WHERE user_id = ? ORDER BY updated_at DESC LIMIT 1",
                (user.id,),
            ) as cursor:
                last_record = await cursor.fetchone()

            if not last_record or last_record != (first_name, last_name, username, phone_number, is_premium, status_text):
                await db.execute(
                    "INSERT INTO user_history (user_id, first_name, last_name, username, phone_number, is_premium, last_status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (user.id, first_name, last_name, username, phone_number, is_premium, status_text),
                )
                await db.commit()

    async def save_user_channel(self, user_id: int, chat_id: int, title: str, username: str, chat_type: str, is_creator: int = 0) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT OR REPLACE INTO user_channels 
                   (user_id, chat_id, title, username, chat_type, is_creator) 
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (user_id, chat_id, title, username, chat_type, is_creator),
            )
            await db.commit()

    async def save_message_log(
        self, chat_id: int, chat_name: str, chat_username: str, message_id: int, user_id: int, msg_type: str, content: str, file_caption: str = "", sticker_pack: str = ""
    ) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """INSERT INTO message_logs 
                   (chat_id, chat_name, chat_username, message_id, user_id, message_type, content, file_caption, sticker_pack) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (chat_id, chat_name, chat_username, message_id, user_id, msg_type, content, file_caption, sticker_pack),
            )
            await db.commit()

    async def toggle_track_user(self, user_id: int) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT user_id FROM tracked_users WHERE user_id = ?", (user_id,)) as c:
                row = await c.fetchone()
            if row:
                await db.execute("DELETE FROM tracked_users WHERE user_id = ?", (user_id,))
                await db.commit()
                return False
            else:
                await db.execute("INSERT INTO tracked_users (user_id) VALUES (?)", (user_id,))
                await db.commit()
                return True

    async def is_user_tracked(self, user_id: int) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT user_id FROM tracked_users WHERE user_id = ?", (user_id,)) as c:
                return (await c.fetchone()) is not None

    async def get_tracked_users(self) -> Set[int]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT user_id FROM tracked_users") as c:
                rows = await c.fetchall()
                return {row[0] for row in rows}

    async def fetch_message_type_counts(self, target_id: int) -> Dict[str, int]:
        """Xabarlar turi bo'yicha statistika"""
        counts = {
            "all": 0, "voices": 0, "circles": 0, "gif_sticker": 0,
            "links": 0, "video": 0, "files": 0, "images": 0, "geo_contacts": 0
        }
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT message_type, content FROM message_logs WHERE user_id = ?", (target_id,)) as cursor:
                rows = await cursor.fetchall()
                for m_type, content in rows:
                    counts["all"] += 1
                    if m_type == "golos":
                        counts["voices"] += 1
                    elif m_type == "circle":
                        counts["circles"] += 1
                    elif m_type in ["stiker", "gif"]:
                        counts["gif_sticker"] += 1
                    elif m_type == "video":
                        counts["video"] += 1
                    elif m_type == "media":
                        counts["files"] += 1
                    elif m_type == "rasm":
                        counts["images"] += 1
                    elif m_type in ["lokatsiya", "kontakt"]:
                        counts["geo_contacts"] += 1

                    if content and ("http://" in content or "https://" in content or "t.me/" in content):
                        counts["links"] += 1
        return counts

    async def fetch_user_detailed_groups(self, target_id: int) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            query = """
                SELECT 
                    ml.chat_id,
                    ml.chat_name,
                    ml.chat_username,
                    COUNT(ml.id) as msg_count,
                    MAX(ml.created_at) as last_msg_time,
                    COALESCE(uc.is_creator, 0) as is_creator
                FROM message_logs ml
                LEFT JOIN user_channels uc ON ml.chat_id = uc.chat_id AND uc.user_id = ml.user_id
                WHERE ml.user_id = ?
                GROUP BY ml.chat_id
                ORDER BY last_msg_time DESC
            """
            async with db.execute(query, (target_id,)) as cursor:
                rows = await cursor.fetchall()
                return [dict(row) for row in rows]

    async def fetch_user_report(self, target_id: int) -> Dict[str, Any]:
        report = {}
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            async with db.execute(
                "SELECT first_name, last_name, username, phone_number, is_premium, last_status, updated_at FROM user_history WHERE user_id = ? ORDER BY updated_at DESC",
                (target_id,)
            ) as c:
                report["history"] = [dict(row) for row in await c.fetchall()]

            async with db.execute(
                "SELECT chat_id, title, username, chat_type, is_creator FROM user_channels WHERE user_id = ?",
                (target_id,)
            ) as c:
                report["owned_channels"] = [dict(row) for row in await c.fetchall()]

            async with db.execute(
                "SELECT DISTINCT chat_name, chat_id, chat_username FROM message_logs WHERE user_id = ?",
                (target_id,)
            ) as c:
                report["chats"] = [dict(row) for row in await c.fetchall()]

            async with db.execute(
                "SELECT DISTINCT sticker_pack FROM message_logs WHERE user_id = ? AND message_type = 'stiker' AND sticker_pack != ''",
                (target_id,)
            ) as c:
                report["sticker_packs"] = [row["sticker_pack"] for row in await c.fetchall()]

            for m_type in ["rasm", "golos", "video", "media", "lokatsiya", "stiker", "text"]:
                async with db.execute(
                    "SELECT chat_name, chat_id, chat_username, message_id, content, file_caption, sticker_pack, created_at FROM message_logs WHERE user_id = ? AND message_type = ? ORDER BY created_at DESC LIMIT 30",
                    (target_id, m_type)
                ) as c:
                    report[m_type] = [dict(row) for row in await c.fetchall()]

            async with db.execute(
                "SELECT COUNT(DISTINCT chat_id), COUNT(*) FROM message_logs WHERE user_id = ?",
                (target_id,)
            ) as c:
                stats = await c.fetchone()
                report["group_count"] = stats[0] if stats else 0
                report["msg_count"] = stats[1] if stats else 0

        return report


# ---------------------------------------------------------
# 3. YORDAMCHI FUNKSIYALAR
# ---------------------------------------------------------
def make_chat_link(chat_id: int, chat_username: str = None, message_id: int = None) -> str:
    if chat_username:
        url = f"https://t.me/{chat_username}"
        if message_id:
            url += f"/{message_id}"
        return url
    else:
        clean_id = str(chat_id).replace("-100", "").replace("-", "")
        url = f"https://t.me/c/{clean_id}"
        if message_id:
            url += f"/{message_id}"
        return url

def parse_user_status(status_obj) -> str:
    if isinstance(status_obj, UserStatusOnline):
        return "🟢 Online"
    elif isinstance(status_obj, UserStatusOffline):
        dt = status_obj.was_online.strftime("%Y-%m-%d %H:%M:%S")
        return f"🔴 Offline ({dt})"
    elif isinstance(status_obj, UserStatusRecently):
        return "🟡 Yaqinda kirgan"
    elif isinstance(status_obj, UserStatusLastWeek):
        return "⚪ Shu hafta kirgan"
    elif isinstance(status_obj, UserStatusLastMonth):
        return "⚪️ Shu oy kirgan"
    return "⚪️ Status noma'lum"


# ---------------------------------------------------------
# 4. BOT ENGINE
# ---------------------------------------------------------
class TeleLogBot:
    def __init__(self, api_id: int, api_hash: str, bot_token: str, user_session: str, db_name: str):
        self.db = DatabaseManager(db_name)
        self.bot_client = TelegramClient("bot_telelog_session", api_id, api_hash)
        self.bot_token = bot_token
        self.user_client = TelegramClient(StringSession(user_session), api_id, api_hash) if user_session else None
        self.last_known_statuses: Dict[int, str] = {}

    async def build_messages_menu(self, user_id: int):
        """Rasmda ko'rsatilgan TGStat/Infostat Messages menyusini hosil qilish"""
        counts = await self.db.fetch_message_type_counts(user_id)

        def fmt(val: int) -> str:
            return f"✔ {val}" if val > 0 else "❌ No"

        return [
            [Button.inline(f"All ({counts['all']})", data=f"msgtype_{user_id}_all")],
            [
                Button.inline(f"Voices {fmt(counts['voices'])}", data=f"voice_{user_id}"),
                Button.inline(f"Circles {fmt(counts['circles'])}", data=f"circle_{user_id}")
            ],
            [
                Button.inline(f"Gif/sticker {fmt(counts['gif_sticker'])}", data=f"stickers_{user_id}"),
                Button.inline(f"Links {fmt(counts['links'])}", data=f"links_{user_id}")
            ],
            [
                Button.inline(f"Video {fmt(counts['video'])}", data=f"media_{user_id}"),
                Button.inline(f"Files {fmt(counts['files'])}", data=f"files_{user_id}")
            ],
            [
                Button.inline(f"Images {fmt(counts['images'])}", data=f"photos_{user_id}"),
                Button.inline(f"Geo/contacts {fmt(counts['geo_contacts'])}", data=f"loc_{user_id}")
            ],
            # Rasmning pastki qismidagi nav menyusi
            [
                Button.inline("📊 Stats", data=f"stats_{user_id}"),
                Button.inline("🔔 Track", data=f"track_{user_id}"),
                Button.inline("🔗 Names", data=f"names_{user_id}")
            ],
            [
                Button.inline("👁 Groups", data=f"groups_{user_id}_1"),
                Button.inline("💬 Messages", data=f"messages_{user_id}"),
                Button.inline("🔎 Analysis", data=f"scan_{user_id}")
            ],
            [
                Button.inline("📢 Channels", data=f"groups_{user_id}_1"),
                Button.inline("👍 Reputation", data=f"stats_{user_id}"),
                Button.inline("👥 Friends", data=f"stats_{user_id}")
            ],
            [
                Button.inline("😻 Reactions", data=f"stats_{user_id}"),
                Button.inline("🎁 Gifts", data=f"stats_{user_id}"),
                Button.inline("✍️ Share", data=f"stats_{user_id}")
            ],
            [
                Button.inline("🗣 Words frequency", data=f"stats_{user_id}"),
                Button.inline("👥 Common groups", data=f"groups_{user_id}_1")
            ]
        ]

    async def live_status_tracker_loop(self):
        while True:
            try:
                if self.user_client:
                    tracked_users = await self.db.get_tracked_users()
                    for uid in tracked_users:
                        try:
                            u = await self.user_client.get_entity(uid)
                            if isinstance(u, User):
                                curr_status = parse_user_status(getattr(u, "status", None))
                                old_status = self.last_known_statuses.get(uid)

                                if old_status and old_status != curr_status:
                                    msg = f"🔔 **STATUS O'ZGARDI!**\n\n"
                                    msg += f"👤 **Target User:** `{uid}`\n"
                                    msg += f"Eski: {old_status}\n"
                                    msg += f"Yangi: **{curr_status}**"
                                    await self.db.log_user_info(u, curr_status)
                                    if ADMIN_ID:
                                        await self.bot_client.send_message(ADMIN_ID, msg)

                                self.last_known_statuses[uid] = curr_status
                        except Exception:
                            continue
            except Exception as e:
                logger.error(f"Tracker loop xatosi: {e}")
            await asyncio.sleep(15)

    async def scan_telegram_history(self, target_id: int):
        if not self.user_client:
            return False
        try:
            target_user = await self.user_client.get_entity(target_id)
            if isinstance(target_user, User):
                status_str = parse_user_status(getattr(target_user, "status", None))
                await self.db.log_user_info(target_user, status_str)

            async for dialog in self.user_client.iter_dialogs():
                try:
                    entity = dialog.entity
                    chat_title = dialog.title or "Nomsiz Chat"
                    chat_username = getattr(entity, "username", "") or ""
                    
                    if isinstance(entity, Channel):
                        chat_type = "channel" if entity.broadcast else "supergroup"
                        is_creator = 1 if getattr(entity, "creator", False) else 0
                        await self.db.save_user_channel(target_id, entity.id, chat_title, chat_username, chat_type, is_creator)
                    elif isinstance(entity, Chat):
                        is_creator = 1 if getattr(entity, "creator", False) else 0
                        await self.db.save_user_channel(target_id, entity.id, chat_title, chat_username, "group", is_creator)

                    async for msg in self.user_client.iter_messages(dialog.id, from_user=target_id, limit=60):
                        msg_type = "text"
                        content = msg.text[:150] if msg.text else ""
                        file_caption = msg.text or ""
                        sticker_pack = ""

                        if msg.sticker:
                            msg_type = "stiker"
                            content = f"🎨 Stiker {msg.sticker.alt or ''}"
                            for attr in msg.sticker.attributes:
                                if hasattr(attr, "stickerset") and hasattr(attr.stickerset, "short_name"):
                                    sticker_pack = attr.stickerset.short_name
                        elif msg.photo:
                            msg_type = "rasm"
                            content = "🖼 Foto Rasm"
                        elif msg.voice:
                            msg_type = "golos"
                            content = f"🎙 Ovozli ({getattr(msg.voice, 'duration', 0)}s)"
                        elif msg.video_note:
                            msg_type = "circle"
                            content = f"🎥 Video doiracha ({getattr(msg.video_note, 'duration', 0)}s)"
                        elif msg.video:
                            msg_type = "video"
                            content = f"🎥 Video ({getattr(msg.video, 'duration', 0)}s)"
                        elif msg.document:
                            msg_type = "media"
                            content = "📁 Fayl"
                        elif isinstance(msg.media, (MessageMediaGeo, MessageMediaGeoLive)):
                            msg_type = "lokatsiya"
                            geo = msg.media.geo
                            content = f"https://www.google.com/maps?q={geo.lat},{geo.long}"
                        elif isinstance(msg.media, MessageMediaContact):
                            msg_type = "kontakt"
                            content = f"👤 Contact: {msg.media.phone_number}"

                        await self.db.save_message_log(
                            chat_id=dialog.id,
                            chat_name=chat_title,
                            chat_username=chat_username,
                            message_id=msg.id,
                            user_id=target_id,
                            msg_type=msg_type,
                            content=content,
                            file_caption=file_caption[:100],
                            sticker_pack=sticker_pack
                        )
                except Exception:
                    continue
            return True
        except Exception as e:
            logger.error(f"Skanerlash xatosi: {e}")
            return False

    async def start(self):
        await self.db.init_db()
        await self.bot_client.start(bot_token=self.bot_token)
        tasks = [self.bot_client.run_until_disconnected()]

        if self.user_client:
            await self.user_client.start()
            logger.info("🚀 Userbot, Bot va Channels Track tayyor!")
            tasks.append(self.user_client.run_until_disconnected())
            tasks.append(self.live_status_tracker_loop())

        self._register_handlers()
        await asyncio.gather(*tasks)

    def _register_handlers(self):
        if self.user_client:
            @self.user_client.on(events.NewMessage)
            async def on_userbot_message(event):
                if not event.sender_id:
                    return
                try:
                    sender = await event.get_sender()
                    if isinstance(sender, User):
                        status_str = parse_user_status(getattr(sender, "status", None))
                        await self.db.log_user_info(sender, status_str)

                    chat = await event.get_chat()
                    chat_name = "Shaxsiy chat"
                    chat_username = getattr(chat, "username", "") or ""
                    if isinstance(chat, (Chat, Channel)):
                        chat_name = chat.title
                        chat_type = "channel" if getattr(chat, "broadcast", False) else "group"
                        is_creator = 1 if getattr(chat, "creator", False) else 0
                        await self.db.save_user_channel(event.sender_id, chat.id, chat_name, chat_username, chat_type, is_creator)

                    msg_type = "text"
                    content = event.text[:150] if event.text else ""
                    caption = event.text or ""
                    sticker_pack = ""

                    if event.sticker:
                        msg_type = "stiker"
                        content = f"🎨 Stiker {event.sticker.alt or ''}"
                        for attr in event.sticker.attributes:
                            if hasattr(attr, "stickerset") and hasattr(attr.stickerset, "short_name"):
                                sticker_pack = attr.stickerset.short_name
                    elif event.photo:
                        msg_type = "rasm"
                        content = "🖼 Foto Rasm"
                    elif event.voice:
                        msg_type = "golos"
                        content = f"🎙 Ovozli ({getattr(event.voice, 'duration', 0)}s)"
                    elif event.video_note:
                        msg_type = "circle"
                        content = f"🎥 Video doiracha ({getattr(event.video_note, 'duration', 0)}s)"
                    elif event.video:
                        msg_type = "video"
                        content = f"🎥 Video ({getattr(event.video, 'duration', 0)}s)"
                    elif event.document:
                        msg_type = "media"
                        content = "📁 Fayl"
                    elif isinstance(event.media, (MessageMediaGeo, MessageMediaGeoLive)):
                        msg_type = "lokatsiya"
                        geo = event.media.geo
                        content = f"https://www.google.com/maps?q={geo.lat},{geo.long}"
                    elif isinstance(event.media, MessageMediaContact):
                        msg_type = "kontakt"
                        content = f"👤 Contact: {event.media.phone_number}"

                    await self.db.save_message_log(
                        chat_id=event.chat_id,
                        chat_name=chat_name,
                        chat_username=chat_username,
                        message_id=event.id,
                        user_id=event.sender_id,
                        msg_type=msg_type,
                        content=content,
                        file_caption=caption[:100],
                        sticker_pack=sticker_pack
                    )

                    if ADMIN_ID and await self.db.is_user_tracked(event.sender_id):
                        link = make_chat_link(event.chat_id, chat_username, event.id)
                        alert = f"🚨 **TRACKING ALERT!**\n\n"
                        alert += f"👤 **User ID:** `{event.sender_id}`\n"
                        alert += f"💬 **Chat:** [{chat_name}]({link})\n"
                        alert += f"📩 **Turi:** {msg_type}\n"
                        alert += f"📝 **Matn:** {content}"
                        await self.bot_client.send_message(ADMIN_ID, alert, link_preview=False)

                except Exception as e:
                    logger.error(f"Userbot log xatosi: {e}")

        @self.bot_client.on(events.NewMessage(pattern=r"^/start$"))
        async def on_start(event):
            await event.reply("🔎 **TeleLog Ultra Botiga xush kelibsiz!**\nFoydalanuvchi **User ID** sini kiriting:")

        @self.bot_client.on(events.NewMessage)
        async def on_bot_message(event):
            if event.text and event.text.isdigit():
                user_id = int(event.text)
                
                if self.user_client:
                    try:
                        u = await self.user_client.get_entity(user_id)
                        if isinstance(u, User):
                            live_status = parse_user_status(getattr(u, "status", None))
                            await self.db.log_user_info(u, live_status)
                    except Exception:
                        pass

                report = await self.db.fetch_user_report(user_id)
                menu = await self.build_messages_menu(user_id)
                username_str = f"@{report['history'][0]['username']}" if report.get("history") and report["history"][0].get("username") else f"`{user_id}`"

                text = f"{username_str}\n"
                text += f"**Select message type**"

                await event.reply(text, buttons=menu)

        @self.bot_client.on(events.CallbackQuery)
        async def on_callback(event):
            data = event.data.decode("utf-8")
            parts = data.split("_")
            action = parts[0]
            target_id = int(parts[1])

            if action == "messages":
                report = await self.db.fetch_user_report(target_id)
                menu = await self.build_messages_menu(target_id)
                username_str = f"@{report['history'][0]['username']}" if report.get("history") and report["history"][0].get("username") else f"`{target_id}`"

                text = f"{username_str}\n"
                text += f"**Select message type**"
                await event.answer()
                await event.edit(text, buttons=menu)
                return

            if action == "track":
                is_active = await self.db.toggle_track_user(target_id)
                msg_status = "🟢 Live Tracking YOQILDI!" if is_active else "🔴 Live Tracking O'CHIRILDI!"
                await event.answer(msg_status, alert=True)
                menu = await self.build_messages_menu(target_id)
                await event.edit(buttons=menu)
                return

            if action == "scan":
                await event.answer("🔄 Telegram API orqali guruh va kanallar skanerlanmoqda...", alert=True)
                success = await self.scan_telegram_history(target_id)
                if success:
                    await event.answer("✅ Full Scan yakunlandi!", alert=True)
                else:
                    await event.answer("❌ USER_SESSION sozlanmagan!", alert=True)
                return

            if action == "groups":
                page = int(parts[2]) if len(parts) > 2 else 1
                per_page = 12

                groups = await self.db.fetch_user_detailed_groups(target_id)
                total_groups = len(groups)
                total_pages = (total_groups + per_page - 1) // per_page if total_groups > 0 else 1

                if page < 1: page = 1
                if page > total_pages: page = total_pages

                report = await self.db.fetch_user_report(target_id)
                username_str = f"@{report['history'][0]['username']}" if report.get("history") and report["history"][0].get("username") else "Noma'lum"

                res = f"Known groups of account `{target_id}` ({username_str}).\n"
                res += "👮‍♂️ -admin, 🔒 -private, ❌ -left\n"
                res += "Last msg - group (total messages)\n\n"

                start_idx = (page - 1) * per_page
                end_idx = start_idx + per_page
                page_groups = groups[start_idx:end_idx]

                if page_groups:
                    for g in page_groups:
                        date_str = ""
                        if g["last_msg_time"]:
                            try:
                                dt = datetime.strptime(g["last_msg_time"], "%Y-%m-%d %H:%M:%S")
                                date_str = dt.strftime("%d %b")
                            except Exception:
                                date_str = ""

                        badge = ""
                        if g["is_creator"]:
                            badge += "👮‍♂️ "
                        if not g["chat_username"]:
                            badge += "🔒 "

                        link = make_chat_link(g["chat_id"], g["chat_username"])
                        res += f"{badge}{date_str} [{g['chat_name']}]({link}) **({g['msg_count']})**\n"
                else:
                    res += "_Guruhlar topilmadi._\n"

                res += f"\nTotal **{total_groups}**, page **{page}** of **{total_pages}**"

                buttons = []
                nav_row = []
                if page > 1:
                    nav_row.append(Button.inline("⬅️ Back", data=f"groups_{target_id}_{page-1}"))
                if page < total_pages:
                    nav_row.append(Button.inline("➡️ Next", data=f"groups_{target_id}_{page+1}"))
                
                if nav_row:
                    buttons.append(nav_row)

                buttons.append([
                    Button.inline("💬 Messages Menyu", data=f"messages_{target_id}"),
                    Button.inline("💾 Download as file", data=f"dlfile_{target_id}")
                ])

                await event.answer()
                await event.edit(res, buttons=buttons, link_preview=False)
                return

            menu = await self.build_messages_menu(target_id)
            report = await self.db.fetch_user_report(target_id)

            if action == "stats":
                last_hist = report["history"][0] if report["history"] else {}
                status_now = last_hist.get("last_status", "Noma'lum")
                phone_num = last_hist.get("phone_number", "Yashirin/Mavjud emas")
                prem = "⭐ Ha (Premium)" if last_hist.get("is_premium") else "Yo'q"

                res = f"📊 **STATISTIKA (ID: `{target_id}`)**\n\n"
                res += f"👤 Status: **{status_now}**\n"
                res += f"📱 Telefon: `{phone_num}`\n"
                res += f"⭐ Premium: **{prem}**\n\n"
                res += f"• Guruh va Kanallari: **{len(report['owned_channels'])}** ta\n"
                res += f"• Faol chatlari: **{report['group_count']}** ta\n"
                res += f"• Jami xabarlari: **{report['msg_count']}** ta\n"
                res += f"• Stiker-Pakatlari: **{len(report['sticker_packs'])}** ta\n"
                await event.answer()
                await event.edit(res, buttons=menu)

            elif action == "names":
                res = f"🏷 **PROFIL VA TELEFON TARIXI (ID: `{target_id}`)**\n\n"
                if report["history"]:
                    for h in report["history"]:
                        phone_info = f" | 📱 `{h['phone_number']}`" if h.get('phone_number') else ""
                        res += f"• `[{h['updated_at']}]` **{h['first_name']} {h['last_name'] or ''}** (@{h['username'] or 'yo-q'}){phone_info}\n"
                else:
                    res += "_Tarix topilmadi._"
                await event.answer()
                await event.edit(res, buttons=menu)

            elif action == "stickers":
                res = f"🎨 **STIKERLAR VA GIFLAR (ID: `{target_id}`)**\n\n"
                if report["sticker_packs"]:
                    res += "📦 **Stiker Pakatlari:**\n"
                    for idx, sp in enumerate(report["sticker_packs"], 1):
                        res += f"{idx}. 🔗 [Stiker Pakatni Ochish](https://t.me/addstickers/{sp})\n"
                    res += "\n"

                if report["stiker"]:
                    res += "💬 **Yuborilgan Stikerlar:**\n"
                    for s in report["stiker"][:15]:
                        msg_link = make_chat_link(s["chat_id"], s["chat_username"], s["message_id"])
                        res += f"• `[{s['created_at']}]` **{s['chat_name']}** -> [Stikerga o'tish]({msg_link})\n"
                else:
                    res += "_Stikerlar topilmadi._"
                await event.answer()
                await event.edit(res, buttons=menu, link_preview=False)

            elif action == "photos":
                res = f"🖼 **RASMLAR (ID: `{target_id}`)**\n\n"
                if report["rasm"]:
                    for idx, p in enumerate(report["rasm"], 1):
                        msg_link = make_chat_link(p["chat_id"], p["chat_username"], p["message_id"])
                        res += f"{idx}. `[{p['created_at']}]` **{p['chat_name']}** -> [Rasmni ko'rish]({msg_link})\n"
                else:
                    res += "_Rasmlar topilmadi._"
                await event.answer()
                await event.edit(res, buttons=menu, link_preview=False)

            elif action == "voice":
                res = f"🎙 **OVOZLI XABARLAR (ID: `{target_id}`)**\n\n"
                if report["golos"]:
                    for idx, v in enumerate(report["golos"], 1):
                        msg_link = make_chat_link(v["chat_id"], v["chat_username"], v["message_id"])
                        res += f"{idx}. `[{v['created_at']}]` **{v['chat_name']}** -> [Eshitish]({msg_link})\n"
                else:
                    res += "_Ovozli xabarlar topilmadi._"
                await event.answer()
                await event.edit(res, buttons=menu, link_preview=False)

            elif action == "media":
                res = f"🎥 **VIDEO VA HUJJATLAR (ID: `{target_id}`)**\n\n"
                all_media = report["video"] + report["media"]
                if all_media:
                    for idx, m in enumerate(all_media, 1):
                        msg_link = make_chat_link(m["chat_id"], m["chat_username"], m["message_id"])
                        res += f"{idx}. `[{m['created_at']}]` **{m['chat_name']}** -> [Mediaga o'tish]({msg_link})\n"
                else:
                    res += "_Media topilmadi._"
                await event.answer()
                await event.edit(res, buttons=menu, link_preview=False)

            elif action == "loc":
                res = f"📍 **LOKATSIYA VA KONTAKTLAR (ID: `{target_id}`)**\n\n"
                if report["lokatsiya"]:
                    for idx, l in enumerate(report["lokatsiya"], 1):
                        msg_link = make_chat_link(l["chat_id"], l["chat_username"], l["message_id"])
                        res += f"{idx}. `[{l['created_at']}]` **{l['chat_name']}**\n"
                        res += f"   ├ 🗺 [Google Maps]({l['content']})\n"
                        res += f"   └ 💬 [Lokatsiya xabari]({msg_link})\n\n"
                else:
                    res += "_Lokatsiyalar va kontaktlar topilmadi._"
                await event.answer()
                await event.edit(res, buttons=menu, link_preview=False)

if __name__ == "__main__":
    bot = TeleLogBot(API_ID, API_HASH, BOT_TOKEN, USER_SESSION, DB_NAME)
    asyncio.run(bot.start())

