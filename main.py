import os
import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite
from aiohttp import web, ClientSession
from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    ChatJoinRequest, ChatMemberUpdated, Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.filters import Command
from aiogram.enums import ChatMemberStatus, ParseMode
from aiogram.client.default import DefaultBotProperties

BOT_TOKEN = os.environ["BOT_TOKEN"]
LINK_CHANNEL = int(os.environ["LINK_CHANNEL"])
SUB_CHANNEL = int(os.environ["SUB_CHANNEL"])
ADMIN = int(os.environ["ADMIN"])
PORT = int(os.getenv("PORT", "8080"))

DATA_DIR = Path("/app/data")
STATE_FILE = DATA_DIR / "state.txt"
DB_FILE = DATA_DIR / "stats.db"

db = None
titles = {LINK_CHANNEL: str(LINK_CHANNEL), SUB_CHANNEL: str(SUB_CHANNEL)}
admin_msgs = set()
last_stats_msg = None

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

KEYBOARD = InlineKeyboardMarkup(inline_keyboard=[
    [InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh")]
])


def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def read_state() -> str:
    try:
        content = STATE_FILE.read_text().strip()
    except Exception:
        content = ""
    if content not in ("0", "1"):
        try:
            STATE_FILE.write_text("1")
        except Exception:
            pass
        return "1"
    return content


def write_state(val: str) -> None:
    try:
        STATE_FILE.write_text(val)
    except Exception:
        pass


async def get_db():
    global db
    if db is None:
        db = await aiosqlite.connect(DB_FILE)
        for p in (
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=NORMAL",
            "PRAGMA temp_store=MEMORY",
            "PRAGMA cache_size=-2000",
        ):
            await db.execute(p)
        await db.execute(
            "CREATE TABLE IF NOT EXISTS events ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "type TEXT NOT NULL,"
            "user_id INTEGER NOT NULL,"
            "ts INTEGER NOT NULL)"
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_ts ON events(ts)")
        await db.commit()
    return db


async def log_event(kind: str, user_id: int) -> None:
    d = await get_db()
    cur = await d.execute(
        "INSERT INTO events (type, user_id, ts) VALUES (?, ?, ?)",
        (kind, user_id, int(time.time())),
    )
    await cur.close()
    await d.commit()


async def period_stats(since: int) -> tuple:
    d = await get_db()
    cur = await d.execute(
        "SELECT type, COUNT(*) FROM events WHERE ts >= ? GROUP BY type", (since,)
    )
    counts = dict(await cur.fetchall())
    await cur.close()
    cur = await d.execute(
        "SELECT COUNT(DISTINCT user_id) FROM events WHERE ts >= ?", (since,)
    )
    row = await cur.fetchone()
    await cur.close()
    return counts.get("accepted", 0), counts.get("kicked", 0), (row[0] if row else 0)


async def load_titles() -> None:
    for ch in (LINK_CHANNEL, SUB_CHANNEL):
        try:
            chat = await bot.get_chat(ch)
            t = chat.title or str(ch)
            if chat.username:
                t = f"{t} (@{chat.username})"
            titles[ch] = t
        except Exception:
            titles[ch] = str(ch)


async def build_stats() -> str:
    now = datetime.now(timezone.utc)
    day = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    mon = int(now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp())

    d = await period_stats(day)
    m = await period_stats(mon)
    a = await period_stats(0)

    status = "🟢 Работает" if read_state() == "1" else "🔴 Остановлен"
    parts = [
        "<b>📊 Статистика бота</b>",
        f"Статус: {status}",
        "",
        f"📥 <b>Авто заявки:</b> {titles[LINK_CHANNEL]}",
        f"🚫 <b>Авто кик:</b> {titles[SUB_CHANNEL]}",
        "",
    ]
    for title, (acc, kick, uniq) in (
        ("📅 За день", d), ("📆 За месяц", m), ("🗓 За всё время", a)
    ):
        parts.append(f"<b>{title}</b>")
        parts.append(f"✅ Принято: <code>{acc}</code>")
        parts.append(f"🚫 Кикнуто: <code>{kick}</code>")
        parts.append(f"👤 Уникальных: <code>{uniq}</code>")
        parts.append("")
    return "\n".join(parts)


async def cleanup_admin_msgs() -> None:
    for chat_id, mid in list(admin_msgs):
        try:
            await bot.delete_message(chat_id, mid)
        except Exception:
            pass
    admin_msgs.clear()


async def send_stats(chat_id: int) -> None:
    global last_stats_msg
    await cleanup_admin_msgs()
    msg = await bot.send_message(chat_id, await build_stats(), reply_markup=KEYBOARD)
    admin_msgs.add((chat_id, msg.message_id))
    last_stats_msg = (chat_id, msg.message_id)


async def reply_admin(chat_id: int, text: str) -> None:
    msg = await bot.send_message(chat_id, text)
    admin_msgs.add((chat_id, msg.message_id))


@dp.chat_join_request(F.chat.id == LINK_CHANNEL)
async def on_join_request(req: ChatJoinRequest) -> None:
    if read_state() != "1":
        return
    try:
        await req.approve()
    except Exception:
        return
    await log_event("accepted", req.from_user.id)


@dp.chat_member(F.chat.id == SUB_CHANNEL)
async def on_chat_member(ev: ChatMemberUpdated) -> None:
    if read_state() != "1":
        return
    if ev.new_chat_member.status != ChatMemberStatus.MEMBER:
        return
    if ev.old_chat_member.status not in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
        return
    uid = ev.new_chat_member.user.id
    try:
        await bot.ban_chat_member(SUB_CHANNEL, uid)
        await bot.unban_chat_member(SUB_CHANNEL, uid)
    except Exception:
        pass
    try:
        await bot.unban_chat_member(LINK_CHANNEL, uid)
    except Exception:
        pass
    await log_event("kicked", uid)


@dp.message(Command("start"), F.from_user.id == ADMIN)
async def cmd_start(msg: Message) -> None:
    admin_msgs.add((msg.chat.id, msg.message_id))
    await send_stats(msg.chat.id)


@dp.message(Command("play"), F.from_user.id == ADMIN)
async def cmd_play(msg: Message) -> None:
    write_state("1")
    admin_msgs.add((msg.chat.id, msg.message_id))
    await cleanup_admin_msgs()
    await reply_admin(msg.chat.id, "🟢 Бот включён")


@dp.message(Command("stop"), F.from_user.id == ADMIN)
async def cmd_stop(msg: Message) -> None:
    write_state("0")
    admin_msgs.add((msg.chat.id, msg.message_id))
    await cleanup_admin_msgs()
    await reply_admin(msg.chat.id, "🔴 Бот выключен")


@dp.message(F.from_user.id == ADMIN)
async def admin_any(msg: Message) -> None:
    await cleanup_admin_msgs()
    admin_msgs.add((msg.chat.id, msg.message_id))


@dp.callback_query(F.data == "refresh", F.from_user.id == ADMIN)
async def cb_refresh(cb: CallbackQuery) -> None:
    global last_stats_msg
    if cb.message:
        try:
            await bot.edit_message_text(
                text=await build_stats(),
                chat_id=cb.message.chat.id,
                message_id=cb.message.message_id,
                reply_markup=KEYBOARD,
            )
            last_stats_msg = (cb.message.chat.id, cb.message.message_id)
        except Exception:
            pass
    await cb.answer("Обновлено")


async def auto_refresh() -> None:
    while True:
        await asyncio.sleep(6 * 3600)
        if last_stats_msg and last_stats_msg in admin_msgs:
            try:
                await bot.edit_message_text(
                    text=await build_stats(),
                    chat_id=last_stats_msg[0],
                    message_id=last_stats_msg[1],
                    reply_markup=KEYBOARD,
                )
            except Exception:
                pass


async def health(_):
    return web.Response(text="ok")


async def self_ping() -> None:
    url = f"http://127.0.0.1:{PORT}/health"
    async with ClientSession() as s:
        while True:
            try:
                async with s.get(url, timeout=5) as r:
                    await r.read()
            except Exception:
                pass
            await asyncio.sleep(60)


async def start_web() -> None:
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()


async def main() -> None:
    ensure_dirs()
    read_state()
    await get_db()
    await load_titles()
    await start_web()
    asyncio.create_task(self_ping())
    asyncio.create_task(auto_refresh())
    try:
        await dp.start_polling(
            bot,
            allowed_updates=["message", "chat_join_request", "chat_member", "callback_query"],
        )
    finally:
        await bot.session.close()
        if db is not None:
            await db.close()


if __name__ == "__main__":
    asyncio.run(main())
