import os
import asyncio
import time
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
from aiogram.exceptions import TelegramBadRequest


BOT_TOKEN = os.environ["BOT_TOKEN"]
LINK_CHANNEL = int(os.environ["LINK_CHANNEL"])
SUB_CHANNEL = int(os.environ["SUB_CHANNEL"])
ADMIN = int(os.environ["ADMIN"])
PORT = int(os.getenv("PORT", "8080"))

DATA_DIR = Path("/app/data")
STATE_FILE = DATA_DIR / "state.txt"
DB_FILE = DATA_DIR / "stats.db"

db = None

titles = {
    LINK_CHANNEL: str(LINK_CHANNEL),
    SUB_CHANNEL: str(SUB_CHANNEL),
}

admin_msgs = set()
last_stats_msg = None

request_queue = asyncio.Queue()
queued_requests = set()
processing_enabled = asyncio.Event()

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)

dp = Dispatcher()


# =========================
# KEYBOARD
# =========================

def stats_keyboard() -> InlineKeyboardMarkup:
    action = "Стоп" if read_state() == "1" else "Старт"

    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="🔄 Обновить",
                callback_data="refresh",
            )
        ],
        [
            InlineKeyboardButton(
                text=action,
                callback_data="toggle_state",
            )
        ],
    ])


# =========================
# STATE
# =========================

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

    if val == "1":
        processing_enabled.set()
    else:
        processing_enabled.clear()


# =========================
# DATABASE
# =========================

async def get_db():
    global db

    if db is None:
        db = await aiosqlite.connect(DB_FILE)

        pragmas = (
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=NORMAL",
            "PRAGMA temp_store=FILE",
            "PRAGMA cache_size=-1000",
            "PRAGMA mmap_size=0",
            "PRAGMA wal_autocheckpoint=1000",
        )

        for pragma in pragmas:
            await db.execute(pragma)

        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                ts INTEGER NOT NULL
            )
            """
        )

        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ts ON events(ts)"
        )

        await db.commit()

    return db


async def log_event(kind: str, user_id: int) -> None:
    d = await get_db()

    await d.execute(
        """
        INSERT INTO events (type, user_id, ts)
        VALUES (?, ?, ?)
        """,
        (kind, user_id, int(time.time())),
    )

    await d.commit()


# =========================
# REQUEST QUEUE
# =========================

async def enqueue_request(chat_id: int, user_id: int) -> None:
    key = (chat_id, user_id)

    if key in queued_requests:
        return

    queued_requests.add(key)
    request_queue.put_nowait(key)


def request_is_gone(exc: TelegramBadRequest) -> bool:
    error = str(exc).lower()

    return any(value in error for value in (
        "request not found",
        "join request not found",
        "hide_requester_missing",
        "user_already_participant",
        "already a participant",
        "already participant",
    ))


async def process_requests() -> None:
    while True:
        await processing_enabled.wait()

        try:
            key = await asyncio.wait_for(
                request_queue.get(),
                timeout=1,
            )
        except asyncio.TimeoutError:
            continue

        if not processing_enabled.is_set():
            request_queue.put_nowait(key)
            request_queue.task_done()
            continue

        chat_id, user_id = key

        try:
            await bot.approve_chat_join_request(
                chat_id,
                user_id,
            )

        except TelegramBadRequest as exc:
            if request_is_gone(exc):
                queued_requests.discard(key)

            else:
                await asyncio.sleep(5)

                if key in queued_requests:
                    request_queue.put_nowait(key)

        except asyncio.CancelledError:
            request_queue.put_nowait(key)
            request_queue.task_done()
            raise

        except Exception:
            await asyncio.sleep(5)

            if key in queued_requests:
                request_queue.put_nowait(key)

        else:
            queued_requests.discard(key)

            try:
                await log_event("accepted", user_id)
            except Exception:
                pass

        request_queue.task_done()

        await asyncio.sleep(0.2)


# =========================
# STATISTICS
# =========================

async def build_stats() -> str:
    d = await get_db()

    # Один запрос вместо нескольких отдельных запросов.
    cur = await d.execute(
        """
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT user_id) AS unique_users,

            COALESCE(
                SUM(CASE
                    WHEN type = 'accepted' THEN 1
                    ELSE 0
                END),
                0
            ) AS accepted,

            COALESCE(
                SUM(CASE
                    WHEN type = 'kicked' THEN 1
                    ELSE 0
                END),
                0
            ) AS kicked

        FROM events
        """
    )

    row = await cur.fetchone()
    await cur.close()

    total = row[0]
    unique = row[1]
    accepted = row[2]
    kicked = row[3]

    pending = request_queue.qsize()

    status = (
        "🟢 Работает"
        if read_state() == "1"
        else "🔴 Остановлен"
    )

    parts = [
        "<b>📊 Статистика бота</b>",
        f"Статус: {status}",
        "",
        f"📥 <b>Авто заявки:</b> {titles[LINK_CHANNEL]}",
        f"🚫 <b>Авто кик:</b> {titles[SUB_CHANNEL]}",
        "",
        f"✅ Принято: <code>{accepted}</code>",
        f"🚫 Кикнуто: <code>{kicked}</code>",
        f"👤 Уникальных: <code>{unique}</code>",
        "",
        f"📋 В очереди заявок: <code>{pending}</code>",
    ]

    return "\n".join(parts)


# =========================
# CHANNEL TITLES
# =========================

async def load_titles() -> None:
    for ch in (LINK_CHANNEL, SUB_CHANNEL):
        try:
            chat = await bot.get_chat(ch)

            title = chat.title or str(ch)

            if chat.username:
                title = f"{title} (@{chat.username})"

            titles[ch] = title

        except Exception:
            titles[ch] = str(ch)


# =========================
# ADMIN MESSAGES
# =========================

async def cleanup_admin_msgs() -> None:
    for chat_id, message_id in list(admin_msgs):
        try:
            await bot.delete_message(
                chat_id,
                message_id,
            )
        except Exception:
            pass

    admin_msgs.clear()


async def send_stats(chat_id: int) -> None:
    global last_stats_msg

    await cleanup_admin_msgs()

    msg = await bot.send_message(
        chat_id,
        await build_stats(),
        reply_markup=stats_keyboard(),
    )

    admin_msgs.add((chat_id, msg.message_id))

    last_stats_msg = (
        chat_id,
        msg.message_id,
    )


async def reply_admin(chat_id: int, text: str) -> None:
    msg = await bot.send_message(
        chat_id,
        text,
    )

    admin_msgs.add((chat_id, msg.message_id))


# =========================
# JOIN REQUEST HANDLER
# =========================

@dp.chat_join_request(F.chat.id == LINK_CHANNEL)
async def on_join_request(req: ChatJoinRequest) -> None:
    await enqueue_request(
        req.chat.id,
        req.from_user.id,
    )


# =========================
# CHAT MEMBER HANDLER
# =========================

@dp.chat_member(F.chat.id == SUB_CHANNEL)
async def on_chat_member(ev: ChatMemberUpdated) -> None:
    if read_state() != "1":
        return

    if ev.new_chat_member.status != ChatMemberStatus.MEMBER:
        return

    if ev.old_chat_member.status not in (
        ChatMemberStatus.LEFT,
        ChatMemberStatus.KICKED,
    ):
        return

    uid = ev.new_chat_member.user.id

    try:
        await bot.ban_chat_member(
            SUB_CHANNEL,
            uid,
        )

        await bot.unban_chat_member(
            SUB_CHANNEL,
            uid,
        )

    except Exception:
        pass

    try:
        await bot.unban_chat_member(
            LINK_CHANNEL,
            uid,
        )
    except Exception:
        pass

    try:
        await log_event("kicked", uid)
    except Exception:
        pass


# =========================
# ADMIN COMMANDS
# =========================

@dp.message(Command("start"), F.from_user.id == ADMIN)
async def cmd_start(msg: Message) -> None:
    admin_msgs.add((
        msg.chat.id,
        msg.message_id,
    ))

    await send_stats(msg.chat.id)


@dp.message(Command("play"), F.from_user.id == ADMIN)
async def cmd_play(msg: Message) -> None:
    write_state("1")

    admin_msgs.add((
        msg.chat.id,
        msg.message_id,
    ))

    await cleanup_admin_msgs()

    await reply_admin(
        msg.chat.id,
        "🟢 Бот включён",
    )


@dp.message(Command("stop"), F.from_user.id == ADMIN)
async def cmd_stop(msg: Message) -> None:
    write_state("0")

    admin_msgs.add((
        msg.chat.id,
        msg.message_id,
    ))

    await cleanup_admin_msgs()

    await reply_admin(
        msg.chat.id,
        "🔴 Бот выключен",
    )


@dp.message(F.from_user.id == ADMIN)
async def admin_any(msg: Message) -> None:
    await cleanup_admin_msgs()

    admin_msgs.add((
        msg.chat.id,
        msg.message_id,
    ))


# =========================
# CALLBACKS
# =========================

@dp.callback_query(
    F.data == "toggle_state",
    F.from_user.id == ADMIN,
)
async def cb_toggle_state(cb: CallbackQuery) -> None:
    global last_stats_msg

    new_state = (
        "0"
        if read_state() == "1"
        else "1"
    )

    write_state(new_state)

    if cb.message:
        try:
            await bot.edit_message_text(
                text=await build_stats(),
                chat_id=cb.message.chat.id,
                message_id=cb.message.message_id,
                reply_markup=stats_keyboard(),
            )

            last_stats_msg = (
                cb.message.chat.id,
                cb.message.message_id,
            )

        except Exception:
            pass

    await cb.answer(
        "Бот остановлен"
        if new_state == "0"
        else "Бот запущен"
    )


@dp.callback_query(
    F.data == "refresh",
    F.from_user.id == ADMIN,
)
async def cb_refresh(cb: CallbackQuery) -> None:
    global last_stats_msg

    if cb.message:
        try:
            await bot.edit_message_text(
                text=await build_stats(),
                chat_id=cb.message.chat.id,
                message_id=cb.message.message_id,
                reply_markup=stats_keyboard(),
            )

            last_stats_msg = (
                cb.message.chat.id,
                cb.message.message_id,
            )

        except Exception:
            pass

    await cb.answer("Обновлено")


# =========================
# AUTO REFRESH
# =========================

async def auto_refresh() -> None:
    while True:
        await asyncio.sleep(6 * 3600)

        if last_stats_msg and last_stats_msg in admin_msgs:
            try:
                await bot.edit_message_text(
                    text=await build_stats(),
                    chat_id=last_stats_msg[0],
                    message_id=last_stats_msg[1],
                    reply_markup=stats_keyboard(),
                )

            except Exception:
                pass


# =========================
# WEB HEALTH
# =========================

async def health(_):
    return web.Response(text="ok")


async def self_ping() -> None:
    url = f"http://127.0.0.1:{PORT}/health"

    async with ClientSession() as session:
        while True:
            try:
                async with session.get(
                    url,
                    timeout=5,
                ) as response:
                    await response.read()

            except Exception:
                pass

            await asyncio.sleep(60)


async def start_web() -> None:
    app = web.Application()

    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)

    await runner.setup()

    await web.TCPSite(
        runner,
        "0.0.0.0",
        PORT,
    ).start()


# =========================
# POLLING
# =========================

async def run_polling() -> None:
    while True:
        try:
            await dp.start_polling(
                bot,
                allowed_updates=[
                    "message",
                    "chat_join_request",
                    "chat_member",
                    "callback_query",
                ],
            )

            return

        except asyncio.CancelledError:
            raise

        except Exception:
            await asyncio.sleep(5)


# =========================
# MAIN
# =========================

async def main() -> None:
    ensure_dirs()

    await get_db()
    await load_titles()
    await start_web()

    if read_state() == "1":
        processing_enabled.set()

    asyncio.create_task(self_ping())
    asyncio.create_task(auto_refresh())
    asyncio.create_task(process_requests())

    try:
        await run_polling()

    finally:
        await bot.session.close()

        if db is not None:
            await db.close()


if __name__ == "__main__":
    asyncio.run(main())
