import os
import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path

import aiosqlite
from aiohttp import web, ClientSession
from aiogram import Bot, Dispatcher, F
from aiogram.types import ChatJoinRequest, ChatMemberUpdated, Message
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
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("PRAGMA temp_store=MEMORY")
        await db.execute("PRAGMA cache_size=-2000")
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


async def period_stats(since: int) -> dict:
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
    unique = row[0] if row else 0
    return {
        "accepted": counts.get("accepted", 0),
        "kicked": counts.get("kicked", 0),
        "unique": unique,
    }


async def build_stats() -> str:
    now = datetime.now(timezone.utc)
    day_start = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    month_start = int(now.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp())

    d = await period_stats(day_start)
    m = await period_stats(month_start)
    a = await period_stats(0)

    status = "🟢 Работает" if read_state() == "1" else "🔴 Остановлен"

    parts = [
        "<b>📊 Статистика бота</b>",
        f"Статус: {status}",
        "",
    ]
    for title, s in [("📅 За день", d), ("📆 За месяц", m), ("🗓 За всё время", a)]:
        parts.append(f"<b>{title}</b>")
        parts.append(f"✅ Принято заявок: <code>{s['accepted']}</code>")
        parts.append(f"🚫 Кикнуто подписчиков: <code>{s['kicked']}</code>")
        parts.append(f"👤 Уникальных пользователей: <code>{s['unique']}</code>")
        parts.append("")
    return "\n".join(parts)


bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()


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
    old = ev.old_chat_member.status
    new = ev.new_chat_member.status
    if new == ChatMemberStatus.MEMBER and old in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
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
    await msg.answer(await build_stats())


@dp.message(Command("play"), F.from_user.id == ADMIN)
async def cmd_play(msg: Message) -> None:
    write_state("1")
    await msg.answer("🟢 Бот включён")


@dp.message(Command("stop"), F.from_user.id == ADMIN)
async def cmd_stop(msg: Message) -> None:
    write_state("0")
    await msg.answer("🔴 Бот выключен")


async def health(_request):
    return web.Response(text="ok")


async def self_ping() -> None:
    url = f"http://127.0.0.1:{PORT}/health"
    async with ClientSession() as session:
        while True:
            try:
                async with session.get(url, timeout=5) as r:
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
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()


async def main() -> None:
    ensure_dirs()
    read_state()
    await get_db()
    await start_web()
    asyncio.create_task(self_ping())
    try:
        await dp.start_polling(
            bot,
            allowed_updates=["message", "chat_join_request", "chat_member"],
        )
    finally:
        await bot.session.close()
        if db is not None:
            await db.close()


if __name__ == "__main__":
    asyncio.run(main())
