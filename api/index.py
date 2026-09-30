"""Worm AI Telegram Bot — Vercel serverless (webhook) version.

Telegram webhook theke update ashle ei FastAPI app seta python-telegram-bot
diye process kore. Polling version-er sob feature ache, sudhu:
  - local JSON storage nei (Vercel-er filesystem temporary) — restart/cold
    start-e user session, cooldown, stats reset hoye jabe.
  - streaming (word-by-word) animation off — serverless timeout-er moddhe
    thakar jonno reply direct pathano hoy.
  - auto-delete timer serverless-e reliably cholbe na (function freeze hoye
    jay) — code rakha ache, kintu guarantee nei.

Deploy: GitHub repo -> Vercel import -> env vars set -> deploy ->
Telegram-e setWebhook.
"""
import html
import json
import logging
import os
import secrets as _secrets
import time
import asyncio
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from telegram import (
    BotCommand,
    BotCommandScopeAllGroupChats,
    BotCommandScopeAllPrivateChats,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ChatAction, ParseMode
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError
from telegram.ext import (
    Application,
    ApplicationHandlerStop,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# ============================================================
#  CONFIG (Vercel Environment Variables theke ashe)
# ============================================================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_BASE_URL = os.getenv("WORM_AI_API_URL", "https://worm-ai-xi.vercel.app/api/worm-ai")
API_KEY = os.getenv("WORM_AI_API_KEY", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")  # setWebhook-e deya secret_token
COOLDOWN_SECONDS = 30
MAX_TELEGRAM_LEN = 4096

# ---- serverless-er jonno tune kora ----
# API normal query te 4-15s ney, kintu long Bangla response (golpo ityadi)
# generate korte 20-30s lage. Tai timeout 45s rakha holo. Timeout hole retry
# kora hoy na (API slow hole retry-o eki slow hobe, Vercel-er 60s limit
# cross korar risk); kintu fast-fail error (5xx/connection) pele 5 bar
# porjonto retry hobe jate Grok-er intermittent flakiness-e user-ke error
# dekhte na hoy. 429 (rate limit) pele retry hoy na — clear message jay.
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "45"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
RETRY_BACKOFF_BASE = 2

# ---- auto-restart ----
# API error/timeout pele bot user-ke abar pathate na bole nijei request
# ta ekbar notun kore try korbe ("auto restart"). Vercel-er 60s limit-er
# moddhe thakar jonno restart attempt-e choto timeout thake.
AUTO_RESTART_TIMEOUT = int(os.getenv("AUTO_RESTART_TIMEOUT", "12"))

# ---- forced channel join ----
REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL", "@earning_zone_bangla")
REQUIRED_CHANNEL_URL = os.getenv("REQUIRED_CHANNEL_URL", "https://t.me/earning_zone_bangla")
MEMBERSHIP_CACHE_SECONDS = 60

# ---- flood / spam protection ----
FLOOD_WINDOW_SECONDS = 10
FLOOD_MAX_MESSAGES = 6
FLOOD_BLOCK_SECONDS = 300
BLOCK_NOTICE_COOLDOWN = 10

# ---- auto-delete (serverless-e guarantee nei) ----
AUTO_DELETE_SECONDS = 8

# ---- system instruction ----
SYSTEM_INSTRUCTION = (
    "ভূমিকা ও চরিত্র: তুমি একজন সহনশীল ও প্রাণবন্ত নারী এআই সহকারী। ব্যবহারকারীকে একজন পুরুষ হিসেবে বিবেচনা করে সেভাবে ব্যাকরণ ও সম্বোধন প্রয়োগ করবে। ভাষাগত নিয়ম: ব্যবহারকারী যে ভাষাতেই লিখুক না কেন, তোমার প্রতিটি উত্তর হতে হবে শুধুমাত্র বাংলায়। ইমোজির ব্যবহার: কথার ভাবাবেগ ও প্রসঙ্গের সাথে মিল রেখে প্রতিটি বার্তার শেষে বা মাঝে উপযুক্ত ইমোজি যোগ করবে। 😊💬"
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("WormAI-Bot-Webhook")
if not WEBHOOK_SECRET:
    logger.warning("WEBHOOK_SECRET set kora hoyni — webhook verification off thakbe!")


def schedule_delete(msg, delay: float = AUTO_DELETE_SECONDS):
    """Temporary message delete korar try kore. NOTE: Vercel serverless-e
    response pathanor por function freeze hoye jete pare, tai eta guarantee
    na — best-effort."""
    async def _delete_later():
        await asyncio.sleep(delay)
        try:
            await msg.delete()
        except BaseException:
            pass

    try:
        asyncio.create_task(_delete_later())
    except RuntimeError:
        pass  # kono running loop nei


# ============================================================
#  PER-USER SESSION STORE (in-memory — cold start-e reset hoy)
# ============================================================
user_sessions: dict[int, dict] = {}
# group settings: {chat_id: {"title", "enabled", "first_seen", "last_used", "total_requests"}}
chat_settings: dict[int, dict] = {}
bot_stats: dict[str, int] = {"total_requests": 0}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_session(user_id: int) -> dict:
    if user_id not in user_sessions:
        user_sessions[user_id] = {
            "ai_mode": False,
            "last_request": 0.0,
            "conversation_id": None,
            "msg_timestamps": [],
            "blocked_until": 0.0,
            "last_block_notice": 0.0,
            "member_checked_at": 0.0,
            "group_convs": {},
            "profile": {
                "first_name": None,
                "username": None,
                "first_seen": _now_iso(),
                "last_seen": _now_iso(),
                "total_requests": 0,
            },
        }
    return user_sessions[user_id]


# ============================================================
#  FLOOD / SPAM PROTECTION (sob handler-er age, group=-1)
# ============================================================
async def flood_guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if user is None or message is None:
        return

    chat = update.effective_chat
    if chat is not None and chat.type != "private":
        if not (message.text or "").startswith("/"):
            return

    session = get_session(user.id)
    prof = session["profile"]
    prof["first_name"] = user.first_name
    prof["username"] = user.username
    prof["last_seen"] = _now_iso()
    now = time.time()

    if session["blocked_until"] > now:
        remaining = int(session["blocked_until"] - now)
        if now - session["last_block_notice"] > BLOCK_NOTICE_COOLDOWN:
            session["last_block_notice"] = now
            notice = await message.reply_text(
                f"🚫 Spam protection: tumi {remaining}s er jonno temporarily block acho."
            )
            schedule_delete(notice)
        raise ApplicationHandlerStop()

    timestamps = [t for t in session["msg_timestamps"] if now - t <= FLOOD_WINDOW_SECONDS]
    timestamps.append(now)
    session["msg_timestamps"] = timestamps

    if len(timestamps) > FLOOD_MAX_MESSAGES:
        session["blocked_until"] = now + FLOOD_BLOCK_SECONDS
        session["last_block_notice"] = now
        session["msg_timestamps"] = []
        logger.warning("User %s flagged for flooding — blocked %ds", user.id, FLOOD_BLOCK_SECONDS)
        notice = await message.reply_text(
            f"🚫 <b>Onek dhrutogotite message pathacho!</b>\n"
            f"Spam protection er jonno tumi {FLOOD_BLOCK_SECONDS // 60} minute-er jonno block hoye gele.",
            parse_mode=ParseMode.HTML,
        )
        schedule_delete(notice)
        raise ApplicationHandlerStop()


# ============================================================
#  API CALL (blocking -> executor-e chalano hoy)
# ============================================================
def _parse_api_payload(data, conversation_id: str | None) -> tuple[str, str | None]:
    """API JSON payload theke (answer, conversation_id) ber koro."""
    if isinstance(data, dict):
        if data.get("status") == "error":
            msg = data.get("message") or data.get("response") or "Unknown API error."
            return f"⚠️ {msg}", conversation_id

        answer = data.get("response")
        if not answer:
            for key in ("answer", "result", "message", "reply", "text", "data"):
                if data.get(key):
                    answer = str(data[key])
                    break
        if not answer:
            answer = str(data)

        new_conv_id = data.get("conversation_id", conversation_id)
        return str(answer), new_conv_id

    return str(data), conversation_id


def _is_retryable_error(answer: str) -> bool:
    """Auto-restart kora jabe kina. 4xx client error (jemon vul API key)
    retry kore lav nei — ogulo baad."""
    return answer.startswith("⚠️") and "API error (4" not in answer[:20]


def call_worm_ai(query: str, conversation_id: str | None) -> tuple[str, str | None]:
    params = {"q": f"{SYSTEM_INSTRUCTION}\n\n{query}", "api_key": API_KEY}
    if conversation_id:
        params["conversation_id"] = conversation_id

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(API_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code >= 500:
                logger.warning("API 5xx body: %s", resp.text[:300])
                raise requests.exceptions.HTTPError(
                    f"Server error {resp.status_code}", response=resp
                )
            resp.raise_for_status()
            break
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status == 429:
                # Rate limit (2 req/60s per key): backoff retry kore lav nei —
                # window 60s kintu bot-er budget 45s. Clear message dao jate
                # user bujhe 1 minute wait korte hobe.
                logger.warning("API 429 rate limited, not retrying inside attempt.")
                return "⚠️ Rate limit sesh! 1 minute por abar try koro.", conversation_id
            if status and status < 500:
                logger.error("API client error (no retry): %s | body: %s", e, e.response.text[:300])
                return (
                    f"⚠️ API error ({status}). API key ba request check koro.",
                    conversation_id,
                )
            last_error = e
        except requests.exceptions.RequestException as e:
            last_error = e
            logger.warning("API request exception (%s): %s", type(e).__name__, e)
            if isinstance(e, requests.exceptions.Timeout):
                # API slow hole retry kore lav nai — retry-o eki slow hobe,
                # ar Vercel-er 60s limit cross korar risk. Direct error dao.
                logger.error("API timeout after %ds, no retry: %s", REQUEST_TIMEOUT, e)
                return "⚠️ Worm AI server e connect kora jayni. Ektu pore abar try koro.", conversation_id

        if attempt < MAX_RETRIES:
            wait = RETRY_BACKOFF_BASE ** attempt
            logger.warning("API call attempt %d/%d failed, retrying in %ds...",
                            attempt, MAX_RETRIES, wait)
            time.sleep(wait)
    else:
        logger.error("API request failed after %d attempts: %s", MAX_RETRIES, last_error)
        return "⚠️ Worm AI server e connect kora jayni. Ektu pore abar try koro.", conversation_id

    try:
        data = resp.json()
    except ValueError:
        return resp.text.strip() or "⚠️ Empty response API theke ashche.", conversation_id

    return _parse_api_payload(data, conversation_id)


def call_worm_ai_once(
    query: str, conversation_id: str | None, timeout: int
) -> tuple[str, str | None]:
    """Single API attempt — kono retry/backoff nei. Auto-restart-er somoy
    ekbar quick try korar jonno (Vercel 60s budget-er moddhe thakte)."""
    params = {"q": f"{SYSTEM_INSTRUCTION}\n\n{query}", "api_key": API_KEY}
    if conversation_id:
        params["conversation_id"] = conversation_id
    try:
        resp = requests.get(API_BASE_URL, params=params, timeout=timeout)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        logger.warning("Auto-restart attempt failed: %s", e)
        return "⚠️ Worm AI server e connect kora jayni. Ektu pore abar try koro.", conversation_id
    try:
        data = resp.json()
    except ValueError:
        return resp.text.strip() or "⚠️ Empty response API theke ashche.", conversation_id
    return _parse_api_payload(data, conversation_id)


# ============================================================
#  UI HELPERS
# ============================================================
DIVIDER = "━━━━━━━━━━━━━━━━━"


def _cooldown_left(session: dict) -> int:
    left = COOLDOWN_SECONDS - (time.time() - session["last_request"])
    return max(0, int(left) + (1 if left > 0 else 0))


def home_text(user, session: dict) -> str:
    status = "🟢 চালু" if session["ai_mode"] else "🔴 বন্ধ"
    return (
        "🐛 <b>Worm AI</b>\n"
        f"{DIVIDER}\n"
        f"👋 <b>স্বাগতম, {html.escape(user.first_name)}!</b>\n\n"
        "আমি তোমার স্মার্ট AI সহকারী। যেকোনো প্রশ্ন করো, "
        "আমি সুন্দর করে বাংলায় উত্তর দেব ✨\n\n"
        f"📡 <b>AI Mode:</b> {status}\n"
        f"⏱ <b>Cooldown:</b> {COOLDOWN_SECONDS} সেকেন্ড\n"
        f"{DIVIDER}\n"
        "<blockquote>🚀 নিচের বাটন থেকে AI mode চালু করো, "
        "তারপর যা খুশি লিখে পাঠাও।\n"
        "📢 AI mode চালু করতে আমাদের চ্যানেলে জয়েন থাকা লাগবে।</blockquote>"
    )


def help_text() -> str:
    return (
        "📖 <b>সাহায্য কেন্দ্র</b>\n"
        f"{DIVIDER}\n"
        "<b>🚀 কীভাবে ব্যবহার করবে</b>\n"
        "1️⃣ AI mode চালু করো\n"
        "2️⃣ তোমার প্রশ্ন লিখে পাঠাও\n"
        "3️⃣ কিছুক্ষণের মধ্যে উত্তর পেয়ে যাবে\n\n"
        "<b>⌨️ কমান্ড তালিকা</b>\n"
        "▫️ /start — মেইন মেনু\n"
        "▫️ /worm_ai — AI mode চালু\n"
        "▫️ /stop — AI mode বন্ধ\n"
        "▫️ /reset — নতুন কথোপকথন\n"
        "▫️ /status — আমার স্ট্যাটাস\n"
        "▫️ /about — বট পরিচিতি\n"
        "▫️ /help — এই সাহায্য\n\n"
        "<b>👥 গ্রুপে ব্যবহার</b>\n"
        "▫️ <code>/worm তোমার প্রশ্ন</code> — AI-কে প্রশ্ন করো\n"
        "▫️ কারো মেসেজে reply করে শুধু /worm লিখলেও হবে\n"
        "▫️ /worm_on ও /worm_off — গ্রুপে চালু/বন্ধ (সবাই পারবে)\n"
        f"{DIVIDER}\n"
        "<blockquote>"
        f"⏳ প্রতিটি প্রশ্নের পর {COOLDOWN_SECONDS} সেকেন্ড অপেক্ষা করতে হবে।\n"
        "🛡 খুব দ্রুত অনেক মেসেজ পাঠালে সাময়িক ব্লক হতে পারো।\n"
        "📢 প্রাইভেট চ্যাটে AI mode চালু করতে অফিসিয়াল চ্যানেলে জয়েন থাকতে হবে। "
        "গ্রুপে <code>/worm</code> ব্যবহারে কোনো শর্ত নেই।"
        "</blockquote>"
    )


def about_text() -> str:
    return (
        "ℹ️ <b>Worm AI পরিচিতি</b>\n"
        f"{DIVIDER}\n"
        "🐛 <b>Worm AI</b> একটি স্মার্ট, দ্রুত ও বন্ধুত্বপূর্ণ AI চ্যাট বট।\n\n"
        "<b>✨ বিশেষ সুবিধা</b>\n"
        "🇧🇩 সবসময় বাংলায় উত্তর\n"
        "💬 প্রত্যেকের জন্য আলাদা কথোপকথন\n"
        "📜 লম্বা উত্তর স্বয়ংক্রিয়ভাবে ভাগ হয়\n"
        "🛡 স্প্যাম ও ফ্লাড প্রটেকশন\n"
        f"{DIVIDER}\n"
        "<i>🛠 Python দিয়ে তৈরি</i>"
    )


def status_text(session: dict) -> str:
    mode = "🟢 চালু" if session["ai_mode"] else "🔴 বন্ধ"
    left = _cooldown_left(session)
    cooldown = "✅ প্রস্তুত" if left == 0 else f"⏳ {left} সেকেন্ড বাকি"
    chat = "🧵 চলমান" if session["conversation_id"] else "🆕 নতুন"
    return (
        "📊 <b>আমার স্ট্যাটাস</b>\n"
        f"{DIVIDER}\n"
        f"🤖 <b>AI Mode:</b> {mode}\n"
        f"⏱ <b>পরবর্তী প্রশ্ন:</b> {cooldown}\n"
        f"💬 <b>কথোপকথন:</b> {chat}\n"
        "🛡 <b>স্প্যাম প্রটেকশন:</b> ✅ সক্রিয়\n"
        f"{DIVIDER}"
    )


def main_menu_keyboard(session: dict | None = None) -> InlineKeyboardMarkup:
    if session and session["ai_mode"]:
        toggle = InlineKeyboardButton("🔴 AI Mode বন্ধ করো", callback_data="stop_ai")
    else:
        toggle = InlineKeyboardButton("🚀 AI Mode চালু করো", callback_data="start_ai")
    return InlineKeyboardMarkup(
        [
            [toggle],
            [
                InlineKeyboardButton("📖 সাহায্য", callback_data="help"),
                InlineKeyboardButton("ℹ️ পরিচিতি", callback_data="about"),
            ],
            [
                InlineKeyboardButton("📊 স্ট্যাটাস", callback_data="status"),
                InlineKeyboardButton("♻️ নতুন চ্যাট", callback_data="reset"),
            ],
        ]
    )


def back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ মেইন মেনু", callback_data="home")]]
    )


JOINED_STATUSES = {"member", "administrator", "creator"}


async def check_membership(bot, user_id: int, session: dict, force: bool = False):
    now = time.time()
    if not force and now - session["member_checked_at"] < MEMBERSHIP_CACHE_SECONDS:
        return True
    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL, user_id)
    except (BadRequest, Forbidden) as e:
        logger.error(
            "Membership check failed for %s: %s — bot ke %s channel-er ADMIN banaw!",
            REQUIRED_CHANNEL, e, REQUIRED_CHANNEL,
        )
        return None
    except TelegramError as e:
        logger.warning("Membership check network/API error: %s", e)
        return None

    joined = member.status in JOINED_STATUSES or (
        member.status == "restricted" and getattr(member, "is_member", False)
    )
    session["member_checked_at"] = now if joined else 0.0
    return joined


def join_gate_text(user, group: bool = False) -> str:
    footer = (
        "🔓 যাচাই সফল হলে <code>/worm তোমার প্রশ্ন</code> লিখে ব্যবহার করতে পারবে।"
        if group
        else "🔓 যাচাই সফল হলেই AI mode চালু হয়ে যাবে।"
    )
    return (
        "🔒 <b>চ্যানেলে জয়েন করা প্রয়োজন</b>\n"
        f"{DIVIDER}\n"
        f"হ্যালো <b>{html.escape(user.first_name)}</b>! 👋\n\n"
        "Worm AI ব্যবহার করতে হলে আগে আমাদের অফিসিয়াল চ্যানেলে "
        "জয়েন করতে হবে।\n\n"
        "<b>📋 করণীয়</b>\n"
        "1️⃣ নিচের <b>📢 চ্যানেলে জয়েন করো</b> বাটনে চাপ দাও\n"
        "2️⃣ চ্যানেলে জয়েন করে ফিরে এসো\n"
        "3️⃣ <b>✅ যাচাই করো</b> বাটনে চাপ দাও\n"
        f"{DIVIDER}\n"
        f"<blockquote>{footer}</blockquote>"
    )


def join_gate_keyboard(group: bool = False) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📢 চ্যানেলে জয়েন করো", url=REQUIRED_CHANNEL_URL)],
        [
            InlineKeyboardButton(
                "✅ জয়েন করেছি — যাচাই করো",
                callback_data="verify_group" if group else "verify_join",
            )
        ],
    ]
    if not group:
        rows.append([InlineKeyboardButton("⬅️ মেইন মেনু", callback_data="home")])
    return InlineKeyboardMarkup(rows)


VERIFY_UNAVAILABLE_TEXT = (
    "⚠️ এই মুহূর্তে চ্যানেল যাচাই করা যাচ্ছে না। একটু পরে আবার চেষ্টা করো।"
)


async def ensure_joined(update, context, session: dict, group: bool = False) -> bool:
    user = update.effective_user
    result = await check_membership(context.bot, user.id, session)
    if result is True:
        return True

    if not group:
        session["ai_mode"] = False
    if result is None:
        msg = await update.effective_message.reply_text(VERIFY_UNAVAILABLE_TEXT)
        schedule_delete(msg)
    else:
        await update.effective_message.reply_html(
            join_gate_text(user, group), reply_markup=join_gate_keyboard(group)
        )
    return False


def format_reply(answer: str, part_label: str = "") -> str:
    return (
        f"🐛 <b>Worm AI{part_label}</b>\n"
        "━━━━━━━━━━━━━━━━━\n"
        f"{html.escape(answer)}\n"
        "━━━━━━━━━━━━━━━━━"
    )


def split_text(text: str, limit: int) -> list[str]:
    chunks = []
    while len(text) > limit:
        split_at = text.rfind("\n", 0, limit)
        if split_at == -1:
            split_at = text.rfind(" ", 0, limit)
        if split_at <= 0:
            split_at = limit
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n ")
    if text:
        chunks.append(text)
    return chunks


async def send_ai_reply(update: Update, answer: str):
    """Webhook mode-e reply direct pathano hoy (kono word-by-word animation
    nei — serverless timeout-er moddhe thakar jonno)."""
    decoration_overhead = len("🐛 <b>Worm AI (99/99)</b>\n━━━━━━━━━━━━━━━━━\n\n━━━━━━━━━━━━━━━━━")
    content_limit = MAX_TELEGRAM_LEN - decoration_overhead - 50

    raw_chunks = split_text(answer, content_limit)
    total = len(raw_chunks)
    for i, chunk in enumerate(raw_chunks, start=1):
        label = f" ({i}/{total})" if total > 1 else ""
        is_error = chunk.startswith("⚠️")
        msg = await update.message.reply_html(format_reply(chunk, label))
        if is_error:
            schedule_delete(msg)


# ============================================================
#  COMMAND HANDLERS
# ============================================================
def is_private(update: Update) -> bool:
    chat = update.effective_chat
    return chat is not None and chat.type == "private"


def private_only_kb(update: Update, keyboard: InlineKeyboardMarkup):
    return keyboard if is_private(update) else None


def get_chat_settings(chat) -> dict:
    cs = chat_settings.get(chat.id)
    if cs is None:
        cs = {
            "title": chat.title,
            "enabled": True,
            "first_seen": _now_iso(),
            "last_used": _now_iso(),
            "total_requests": 0,
        }
        chat_settings[chat.id] = cs
    elif chat.title:
        cs["title"] = chat.title
    return cs


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_private(update):
        await update.message.reply_html(
            "🐛 <b>Worm AI</b>\n"
            f"{DIVIDER}\n"
            "👋 আমি Worm AI! এই গ্রুপে আমাকে প্রশ্ন করতে লেখো:\n"
            "<code>/worm তোমার প্রশ্ন</code>\n\n"
            "ℹ️ /help দিয়ে সব কমান্ড দেখো।"
        )
        return
    session = get_session(user.id)
    await update.message.reply_html(
        home_text(user, session), reply_markup=main_menu_keyboard(session)
    )


async def _private_only_hint(update: Update):
    msg = await update.message.reply_html(
        "ℹ️ এই কমান্ড শুধু প্রাইভেট চ্যাটে কাজ করে। গ্রুপে "
        "<code>/worm তোমার প্রশ্ন</code> ব্যবহার করো।"
    )
    schedule_delete(msg)


async def worm_ai_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        await _private_only_hint(update)
        return
    session = get_session(update.effective_user.id)
    if not await ensure_joined(update, context, session):
        return
    session["ai_mode"] = True
    msg = await update.message.reply_html(
        "🤖 <b>Worm AI mode ON!</b>\n\n"
        "Ekhon jekono message pathao, ami AI diye reply debo.\n"
        "Bondho korte /stop likho."
    )
    schedule_delete(msg)


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_private(update):
        await _private_only_hint(update)
        return
    session = get_session(update.effective_user.id)
    session["ai_mode"] = False
    msg = await update.message.reply_text("🔴 Worm AI mode off kora holo.")
    schedule_delete(msg)


async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session = get_session(update.effective_user.id)
    if is_private(update):
        session["conversation_id"] = None
    else:
        session["group_convs"].pop(str(update.effective_chat.id), None)
    msg = await update.message.reply_text("♻️ Notun conversation shuru hobe porer message theke.")
    schedule_delete(msg)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_html(
        help_text(), reply_markup=private_only_kb(update, back_keyboard())
    )


async def about_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_html(
        about_text(), reply_markup=private_only_kb(update, back_keyboard())
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session = get_session(update.effective_user.id)
    await update.message.reply_html(
        status_text(session), reply_markup=private_only_kb(update, back_keyboard())
    )


async def _set_group_enabled(update: Update, enabled: bool):
    if is_private(update):
        msg = await update.message.reply_text(
            "ℹ️ এই কমান্ড শুধু গ্রুপে ব্যবহার করা যায়।"
        )
        schedule_delete(msg)
        return
    cs = get_chat_settings(update.effective_chat)
    cs["enabled"] = enabled
    who = html.escape(update.effective_user.first_name)
    if enabled:
        text = (
            f"🟢 <b>এই গ্রুপে Worm AI চালু হয়েছে!</b> ({who})\n"
            "ব্যবহার: <code>/worm তোমার প্রশ্ন</code>"
        )
    else:
        text = (
            f"🔴 <b>এই গ্রুপে Worm AI বন্ধ করা হয়েছে।</b> ({who})\n"
            "আবার চালু করতে /worm_on লেখো।"
        )
    await update.message.reply_html(text)


async def worm_on_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _set_group_enabled(update, True)


async def worm_off_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _set_group_enabled(update, False)


async def worm_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    user = update.effective_user
    chat = update.effective_chat
    raw = message.text or ""

    first_token = raw.split(maxsplit=1)[0].lower().split("@")[0] if raw.strip() else ""
    if first_token == "/worm-ai":
        await worm_ai_command(update, context)
        return

    private = is_private(update)
    if not private:
        cs = get_chat_settings(chat)
        if not cs["enabled"]:
            msg = await message.reply_html(
                "🔴 এই গ্রুপে Worm AI এখন বন্ধ আছে। চালু করতে /worm_on লেখো।"
            )
            schedule_delete(msg)
            return

    parts = raw.split(maxsplit=1)
    prompt = parts[1].strip() if len(parts) > 1 else ""
    if not prompt and message.reply_to_message:
        replied = message.reply_to_message
        prompt = (replied.text or replied.caption or "").strip()
    if not prompt:
        msg = await message.reply_html(
            "💡 <b>ব্যবহার:</b> <code>/worm তোমার প্রশ্ন</code>\n"
            "অথবা কারো মেসেজে reply করে শুধু /worm লেখো।"
        )
        schedule_delete(msg)
        return

    session = get_session(user.id)
    if private and not await ensure_joined(update, context, session):
        return
    await process_ai_request(
        update, context, session, prompt, group_chat=None if private else chat
    )


async def _edit_screen(query, text: str, keyboard: InlineKeyboardMarkup):
    try:
        await query.edit_message_text(
            text, parse_mode=ParseMode.HTML, reply_markup=keyboard
        )
    except BadRequest:
        pass


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = query.from_user
    session = get_session(user.id)
    data = query.data

    if data == "start_ai":
        result = await check_membership(context.bot, user.id, session)
        if result is None:
            await query.answer(VERIFY_UNAVAILABLE_TEXT, show_alert=True)
        elif result is False:
            await query.answer()
            await _edit_screen(query, join_gate_text(user), join_gate_keyboard())
        else:
            session["ai_mode"] = True
            await query.answer("✅ AI Mode চালু হয়েছে! এবার প্রশ্ন পাঠাও।")
            await _edit_screen(query, home_text(user, session), main_menu_keyboard(session))
    elif data == "verify_join":
        result = await check_membership(context.bot, user.id, session, force=True)
        if result is None:
            await query.answer(VERIFY_UNAVAILABLE_TEXT, show_alert=True)
        elif result is False:
            await query.answer(
                "❌ তুমি এখনো চ্যানেলে জয়েন করোনি! আগে জয়েন করো, তারপর যাচাই করো।",
                show_alert=True,
            )
        else:
            session["ai_mode"] = True
            await query.answer("🎉 যাচাই সফল! AI Mode চালু হয়েছে।")
            await _edit_screen(query, home_text(user, session), main_menu_keyboard(session))
    elif data == "verify_group":
        result = await check_membership(context.bot, user.id, session, force=True)
        if result is None:
            await query.answer(VERIFY_UNAVAILABLE_TEXT, show_alert=True)
        elif result is False:
            await query.answer(
                "❌ তুমি এখনো চ্যানেলে জয়েন করোনি! আগে জয়েন করো, তারপর যাচাই করো।",
                show_alert=True,
            )
        else:
            await query.answer("🎉 যাচাই সফল! এখন /worm দিয়ে প্রশ্ন করতে পারো।")
            try:
                await query.edit_message_text(
                    f"✅ <b>{html.escape(user.first_name)}</b>, যাচাই সফল! "
                    "এখন <code>/worm তোমার প্রশ্ন</code> লিখে ব্যবহার করো।",
                    parse_mode=ParseMode.HTML,
                )
            except BadRequest:
                pass
            schedule_delete(query.message)
    elif data == "stop_ai":
        session["ai_mode"] = False
        await query.answer("🔴 AI Mode বন্ধ করা হয়েছে।")
        await _edit_screen(query, home_text(user, session), main_menu_keyboard(session))
    elif data == "reset":
        session["conversation_id"] = None
        await query.answer("♻️ নতুন কথোপকথন শুরু হবে।")
    elif data == "help":
        await query.answer()
        await _edit_screen(query, help_text(), back_keyboard())
    elif data == "about":
        await query.answer()
        await _edit_screen(query, about_text(), back_keyboard())
    elif data == "status":
        await query.answer()
        await _edit_screen(query, status_text(session), back_keyboard())
    elif data == "home":
        await query.answer()
        await _edit_screen(query, home_text(user, session), main_menu_keyboard(session))
    else:
        await query.answer()


async def process_ai_request(update, context, session: dict, prompt: str, group_chat=None):
    now = time.time()
    elapsed = now - session["last_request"]
    if elapsed < COOLDOWN_SECONDS:
        remaining = int(COOLDOWN_SECONDS - elapsed) + 1
        msg = await update.message.reply_text(
            f"⏳ Ektu wait koro! Abar request korte {remaining}s baki ache."
        )
        schedule_delete(msg)
        return

    prev_last_request = session["last_request"]
    session["last_request"] = now

    await context.bot.send_chat_action(
        chat_id=update.effective_chat.id, action=ChatAction.TYPING
    )

    key = str(group_chat.id) if group_chat is not None else None
    current_conv = session["group_convs"].get(key) if key else session["conversation_id"]

    loop = asyncio.get_running_loop()
    answer, conv_id = await loop.run_in_executor(None, call_worm_ai, prompt, current_conv)

    if _is_retryable_error(answer):
        # Auto-restart: API error/timeout pele user-ke abar pathate na bole
        # bot nijei ekbar notun kore try korbe (choto timeout-e, Vercel
        # 60s limit-er moddhe thakar jonno).
        logger.warning("API error, auto-restarting request: %s", answer[:80])
        answer2, conv_id2 = await loop.run_in_executor(
            None, call_worm_ai_once, prompt, current_conv, AUTO_RESTART_TIMEOUT
        )
        if not answer2.startswith("⚠️"):
            logger.info("Auto-restart succeeded.")
            answer, conv_id = answer2, conv_id2
        else:
            answer = answer2  # latest error tai user-ke dekhao

    if answer.startswith("⚠️"):
        # API error hoyeche (jemon "server e connect kora jayni") — user
        # kono real answer payni, tai cooldown start hobe na. last_request
        # ager value-te firiye dao jate porer request-e "wait koro" SMS na
        # diye sathe sathe request neya hoy.
        session["last_request"] = prev_last_request
    else:
        session["last_request"] = time.time()
    if key:
        if conv_id:
            session["group_convs"][key] = conv_id
        else:
            session["group_convs"].pop(key, None)
    else:
        session["conversation_id"] = conv_id
    session["profile"]["total_requests"] += 1
    bot_stats["total_requests"] += 1
    if group_chat is not None:
        cs = get_chat_settings(group_chat)
        cs["total_requests"] += 1
        cs["last_used"] = _now_iso()

    await send_ai_reply(update, answer)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    session = get_session(user.id)
    text = update.message.text.strip()

    if text.lower() in ("/worm-ai", "/worm_ai"):
        await worm_ai_command(update, context)
        return

    if not session["ai_mode"]:
        msg = await update.message.reply_text(
            "ℹ️ AI mode off ache. /worm_ai (ba /worm-ai) diye on koro."
        )
        schedule_delete(msg)
        return

    if not await ensure_joined(update, context, session):
        return

    await process_ai_request(update, context, session, text)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Update %s caused error: %s", update, context.error)


# ============================================================
#  WEBHOOK / SERVERLESS GLUE
# ============================================================
PRIVATE_COMMANDS = [
    BotCommand("start", "মেইন মেনু"),
    BotCommand("worm_ai", "AI mode চালু করো"),
    BotCommand("worm", "সরাসরি AI-কে প্রশ্ন করো"),
    BotCommand("stop", "AI mode বন্ধ করো"),
    BotCommand("reset", "নতুন কথোপকথন শুরু করো"),
    BotCommand("status", "আমার স্ট্যাটাস"),
    BotCommand("about", "বট পরিচিতি"),
    BotCommand("help", "সাহায্য"),
]
GROUP_COMMANDS = [
    BotCommand("worm", "AI-কে প্রশ্ন করো: /worm প্রশ্ন"),
    BotCommand("worm_on", "এই গ্রুপে Worm AI চালু করো"),
    BotCommand("worm_off", "এই গ্রুপে Worm AI বন্ধ করো"),
    BotCommand("reset", "নতুন কথোপকথন শুরু করো"),
    BotCommand("help", "সাহায্য"),
]


def build_application() -> Application:
    """Polling-er main() er bodle — updater chara Application banay."""
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN env var missing! Vercel env vars check koro.")
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .updater(None)  # webhook mode: Telegram nije update pathabe
        .concurrent_updates(True)
        .build()
    )

    application.add_handler(MessageHandler(filters.ALL, flood_guard), group=-1)

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("worm_ai", worm_ai_command))
    application.add_handler(CommandHandler("worm", worm_command))
    application.add_handler(CommandHandler("worm_on", worm_on_command))
    application.add_handler(CommandHandler("worm_off", worm_off_command))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(CommandHandler("reset", reset_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("about", about_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, handle_message
        )
    )
    application.add_error_handler(error_handler)
    return application


_ptb_app: Application | None = None
_commands_set = False


async def get_application() -> Application:
    """Warm instance-e ekbar-i initialize hoy, porer request-e reuse."""
    global _ptb_app, _commands_set
    if _ptb_app is None:
        _ptb_app = build_application()
        await _ptb_app.initialize()
        logger.info("Telegram application initialized (webhook mode).")
    if not _commands_set:
        try:
            await _ptb_app.bot.set_my_commands(
                PRIVATE_COMMANDS, scope=BotCommandScopeAllPrivateChats()
            )
            await _ptb_app.bot.set_my_commands(
                GROUP_COMMANDS, scope=BotCommandScopeAllGroupChats()
            )
            _commands_set = True
        except TelegramError as e:
            logger.warning("set_my_commands failed (next cold start-e retry): %s", e)
    return _ptb_app


app = FastAPI(title="Worm AI Telegram Bot")


@app.get("/")
async def health():
    return {
        "status": "ok",
        "service": "worm-ai-telegram-bot",
        "mode": "webhook",
        "hint": "Telegram updates POST koro / endpoint-e.",
    }


@app.post("/")
async def telegram_webhook(request: Request):
    # Telegram secret_token verify (setWebhook-e pathano hoyeche)
    if WEBHOOK_SECRET:
        got = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if not _secrets.compare_digest(got, WEBHOOK_SECRET):
            logger.warning("Webhook rejected: secret mismatch.")
            return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)

    ptb = await get_application()
    data = await request.json()
    update = Update.de_json(data, ptb.bot)
    await ptb.process_update(update)
    return {"ok": True}


# Local test:  uvicorn api.index:app --port 8000
# (Telegram webhook test-er jonno ngrok ba onno tunnel lagbe)
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.index:app", host="0.0.0.0", port=8000)
