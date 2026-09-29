"""
Worm AI — Advanced Telegram Bot
=================================
Features:
  - /start        -> Sundor welcome message + inline keyboard
  - /worm_ai       -> AI mode ON (official command, underscore version)
  - /worm-ai       -> Same as above (typed with hyphen, works as plain text)
  - /stop          -> AI mode OFF
  - /help          -> Command list
  - Per-user session (alada alada conversation/session id per user)
  - 30 second cooldown per user, per request
  - Nicely formatted HTML replies + typing indicator
  - Auto retry with exponential backoff on API/network failure
  - Long replies auto-split across multiple Telegram messages (4096 char limit)
  - Flood/spam protection — auto temp-block users sending too many messages too fast
  - ChatGPT-style word-by-word streaming reveal for AI replies
  - Auto-delete for temporary/status messages (cooldown, errors, mode on/off)
  - Forces Bengali-only AI replies via a system instruction sent with every request
  - Group support: /worm <prosno> works in groups (per-user-per-group conversation,
    /worm_on & /worm_off toggle per group, anyone can change)
  - All user data persisted to local JSON (data/bot_data.json), survives restarts
  - Forced channel join: private AI mode locked until the user joins the required channel
    (groups have no join requirement — /worm always works there)
  - Redesigned Bengali UI: home/help/about/status screens with in-place button navigation

Setup:
  1. pip install -r requirements.txt
  2. Copy .env.example -> .env and fill in BOT_TOKEN + WORM_AI_API_KEY
  3. python bot.py
"""

import os
import json
import time
import html
import logging
import asyncio
import tempfile
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
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
#  CONFIG
# ============================================================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_BASE_URL = os.getenv("WORM_AI_API_URL", "https://worm-ai-lilac.vercel.app/api/worm-ai")
API_KEY = os.getenv("WORM_AI_API_KEY", "")
COOLDOWN_SECONDS = 30
MAX_TELEGRAM_LEN = 4096         # Telegram's hard message length limit
MAX_RETRIES = 3                 # kotobar retry korbe API fail hole
# ---- forced channel join (AI mode chalu korar age join korte hobe) ----
# NOTE: bot ke oi channel-er ADMIN banate hobe, na hole membership check kaj korbe na.
REQUIRED_CHANNEL = os.getenv("REQUIRED_CHANNEL", "@earning_zone_bangla")
REQUIRED_CHANNEL_URL = os.getenv("REQUIRED_CHANNEL_URL", "https://t.me/earning_zone_bangla")
MEMBERSHIP_CACHE_SECONDS = 60   # join thik ache hole koto second porjonto abar check korbe na

# ---- local JSON storage ----
DATA_DIR = os.getenv("DATA_DIR", "data")            # folder jekhane data save hobe
DATA_FILE = os.path.join(DATA_DIR, "bot_data.json")
DATA_VERSION = 1
SAVE_INTERVAL_SECONDS = 5       # protyek koto second por change thakle file-e save korbe

REQUEST_TIMEOUT = 60            # API-r jonno max koto second wait korbe (AI slow hole beshi dorkar)
RETRY_BACKOFF_BASE = 2          # seconds: attempt 1->2s, 2->4s, 3->8s wait

# ---- flood / spam protection ----
FLOOD_WINDOW_SECONDS = 10       # ei somoy-er moddhe
FLOOD_MAX_MESSAGES = 6          # eto ta message pathale spam dhora hobe
FLOOD_BLOCK_SECONDS = 300       # spam dhorle koto khon block thakbe (5 min)
BLOCK_NOTICE_COOLDOWN = 10      # block thakar somoy koto ghono ghono notice pathabe

# ---- fake "streaming" (word-by-word) reveal effect ----
STREAM_MAX_STEPS = 12           # total kotogulo edit korbe (beshi hole Telegram rate-limit khabe)
STREAM_EDIT_DELAY = 0.35        # protyek edit-er modhe koto second wait korbe
STREAM_MIN_CHARS_TO_ANIMATE = 40  # eto choto reply hole animate na kore ek shote pathabe

# ---- auto-delete for temporary/status messages ----
AUTO_DELETE_SECONDS = 8          # cooldown/error/status message koto second por delete hobe

# ---- system instruction sent with every API request ----
SYSTEM_INSTRUCTION = (
    "ভূমিকা ও চরিত্র: তুমি একজন সহনশীল ও প্রাণবন্ত নারী এআই সহকারী। ব্যবহারকারীকে একজন পুরুষ হিসেবে বিবেচনা করে সেভাবে ব্যাকরণ ও সম্বোধন প্রয়োগ করবে। ভাষাগত নিয়ম: ব্যবহারকারী যে ভাষাতেই লিখুক না কেন, তোমার প্রতিটি উত্তর হতে হবে শুধুমাত্র বাংলায়। ইমোজির ব্যবহার: কথার ভাবাবেগ ও প্রসঙ্গের সাথে মিল রেখে প্রতিটি বার্তার শেষে বা মাঝে উপযুক্ত ইমোজি যোগ করবে। 😊💬"
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("WormAI-Bot")


def schedule_delete(msg, delay: float = AUTO_DELETE_SECONDS):
    """Deletes a temporary/status message (cooldown notice, error, mode
    on/off confirmation, etc.) after `delay` seconds so the chat doesn't
    fill up with clutter. Silently ignores failures (already deleted,
    bot lacks delete permission in a group, etc.)."""
    async def _delete_later():
        await asyncio.sleep(delay)
        try:
            await msg.delete()
        except Exception:
            pass

    asyncio.create_task(_delete_later())

# ============================================================
#  PER-USER SESSION STORE
#  (proshoyjon hole eta sqlite/redis diye persistent kora jabe)
# ============================================================
user_sessions: dict[int, dict] = {}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_session(user_id: int) -> dict:
    if user_id not in user_sessions:
        user_sessions[user_id] = {
            "ai_mode": False,
            "last_request": 0.0,
            # flood/spam protection state
            "msg_timestamps": [],
            "blocked_until": 0.0,
            "last_block_notice": 0.0,
            # channel membership cache (last time we confirmed the user had joined)
            "member_checked_at": 0.0,
            # user profile + usage stats (saved to JSON)
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
#  LOCAL JSON STORAGE
#  Ki ki save hoy: user-er AI mode on/off, last request
#  time (cooldown), block state, profile (naam/username/first & last seen)
#  ar total request count. Chat-er message content save kora hoy NA.
#  File: data/bot_data.json  (atomic write -> crash hole file nosto hoy na)
# ============================================================
PERSISTED_FIELDS = ("ai_mode", "last_request", "blocked_until")

bot_stats: dict[str, int] = {"total_requests": 0}
# group settings: {chat_id: {"title", "enabled", "first_seen", "last_used", "total_requests"}}
chat_settings: dict[int, dict] = {}
_last_saved_snapshot: str | None = None
_save_lock = asyncio.Lock()


def _is_num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def load_data() -> None:
    """Startup-e JSON file theke sob user-er data user_sessions-e load kore."""
    global _last_saved_snapshot
    if not os.path.exists(DATA_FILE):
        logger.info("Data file nei (%s) — notun kore shuru hocche.", DATA_FILE)
        return

    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            raise ValueError("top-level JSON object noy")
    except (OSError, ValueError) as e:
        backup = f"{DATA_FILE}.corrupt-{int(time.time())}"
        try:
            os.replace(DATA_FILE, backup)
        except OSError:
            backup = "(backup kora jayni)"
        logger.error("Data file poRa jayni (%s). Backup: %s — fresh shuru hocche.", e, backup)
        return

    stats = raw.get("stats")
    if isinstance(stats, dict):
        for k in bot_stats:
            if isinstance(stats.get(k), int) and not isinstance(stats.get(k), bool):
                bot_stats[k] = stats[k]

    users = raw.get("users")
    loaded = 0
    if isinstance(users, dict):
        for uid_str, saved in users.items():
            try:
                uid = int(uid_str)
            except ValueError:
                continue
            if not isinstance(saved, dict):
                continue
            s = get_session(uid)
            if isinstance(saved.get("ai_mode"), bool):
                s["ai_mode"] = saved["ai_mode"]
            for k in ("last_request", "blocked_until"):
                if _is_num(saved.get(k)):
                    s[k] = float(saved[k])
            prof = saved.get("profile")
            if isinstance(prof, dict):
                for k in s["profile"]:
                    if k in prof and (prof[k] is None or isinstance(prof[k], (str, int))):
                        s["profile"][k] = prof[k]
            loaded += 1

    chats = raw.get("chats")
    if isinstance(chats, dict):
        for cid_str, saved in chats.items():
            try:
                chat_id = int(cid_str)
            except ValueError:
                continue
            if not isinstance(saved, dict):
                continue
            title = saved.get("title")
            total = saved.get("total_requests")
            chat_settings[chat_id] = {
                "title": title if isinstance(title, str) else None,
                "enabled": saved["enabled"] if isinstance(saved.get("enabled"), bool) else True,
                "first_seen": saved["first_seen"] if isinstance(saved.get("first_seen"), str) else _now_iso(),
                "last_used": saved["last_used"] if isinstance(saved.get("last_used"), str) else _now_iso(),
                "total_requests": total if isinstance(total, int) and not isinstance(total, bool) else 0,
            }

    _last_saved_snapshot = json.dumps(_build_payload(), ensure_ascii=False, sort_keys=True)
    logger.info("Data load hoyeche: %d user, total requests: %d", loaded, bot_stats["total_requests"])


def _build_payload() -> dict:
    users = {}
    for uid, s in user_sessions.items():
        entry = {k: s[k] for k in PERSISTED_FIELDS}
        entry["profile"] = s["profile"]
        users[str(uid)] = entry
    chats = {str(cid): cs for cid, cs in chat_settings.items()}
    return {"version": DATA_VERSION, "stats": bot_stats, "users": users, "chats": chats}


def _write_file(text: str) -> None:
    """Atomic write: temp file-e likhe tarpor rename — beche thakle purano file kokhono adha-likha hoy na."""
    os.makedirs(DATA_DIR, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=DATA_DIR, prefix=".bot_data.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, DATA_FILE)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


async def save_data(force: bool = False) -> None:
    """Data change hole (ba force=True hole) JSON file-e save kore."""
    global _last_saved_snapshot
    async with _save_lock:
        payload = _build_payload()
        snapshot = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        if not force and snapshot == _last_saved_snapshot:
            return
        payload["saved_at"] = _now_iso()
        text = json.dumps(payload, ensure_ascii=False, indent=2)
        try:
            await asyncio.to_thread(_write_file, text)
            _last_saved_snapshot = snapshot
        except OSError as e:
            logger.error("Data save kora jayni: %s", e)


async def autosave_loop() -> None:
    while True:
        await asyncio.sleep(SAVE_INTERVAL_SECONDS)
        try:
            await save_data()
        except Exception:
            logger.exception("Autosave-e unexpected error")


async def on_startup(app: Application) -> None:
    app.bot_data["autosave_task"] = asyncio.create_task(autosave_loop())
    # Telegram-er "/" menu-te command-er talika (private ar group-er jonno alada)
    try:
        await app.bot.set_my_commands(
            [
                BotCommand("start", "মেইন মেনু"),
                BotCommand("worm_ai", "AI mode চালু করো"),
                BotCommand("worm", "সরাসরি AI-কে প্রশ্ন করো"),
                BotCommand("stop", "AI mode বন্ধ করো"),
                BotCommand("status", "আমার স্ট্যাটাস"),
                BotCommand("about", "বট পরিচিতি"),
                BotCommand("help", "সাহায্য"),
            ],
            scope=BotCommandScopeAllPrivateChats(),
        )
        await app.bot.set_my_commands(
            [
                BotCommand("worm", "AI-কে প্রশ্ন করো: /worm প্রশ্ন"),
                BotCommand("worm_on", "এই গ্রুপে Worm AI চালু করো"),
                BotCommand("worm_off", "এই গ্রুপে Worm AI বন্ধ করো"),
                BotCommand("help", "সাহায্য"),
            ],
            scope=BotCommandScopeAllGroupChats(),
        )
    except TelegramError as e:
        logger.warning("Command menu set kora jayni: %s", e)


async def on_shutdown(app: Application) -> None:
    task = app.bot_data.get("autosave_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    await save_data(force=True)  # bondho howar age last save
    logger.info("Data save kore bot bondho holo.")


# ============================================================
#  FLOOD / SPAM PROTECTION
#  Runs BEFORE every other handler (registered in group=-1).
#  If a user sends too many messages too fast, they get temporarily
#  blocked and no other handler processes their update.
# ============================================================
async def flood_guard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.effective_message
    if user is None or message is None:
        return

    # Group-e sadharon (non-command) message count/block kora hoy na —
    # noile privacy mode off thakle member-ra normal kotha bolei block hoye jeto.
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

    # already blocked?
    if session["blocked_until"] > now:
        remaining = int(session["blocked_until"] - now)
        if now - session["last_block_notice"] > BLOCK_NOTICE_COOLDOWN:
            session["last_block_notice"] = now
            notice = await message.reply_text(
                f"🚫 Spam protection: tumi {remaining}s er jonno temporarily block acho."
            )
            schedule_delete(notice)
        raise ApplicationHandlerStop()

    # track message timestamps in a sliding window
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
#  API CALL (blocking -> run in executor so bot stays async)
# ============================================================
def call_worm_ai(query: str) -> str:
    """Returns the answer text.

    Kono conversation history rakha hoy na — protyek call-i fresh/notun
    hishebe pathano hoy, tai query shob shomoy choto ar fast thake, ar
    age-r context jomar karone slow/timeout howar shomvabona thake na.

    Retries with exponential backoff on connection errors, timeouts,
    and 5xx server errors. Does NOT retry on 4xx client errors (e.g.
    bad apikey) since retrying won't fix those.
    """
    q_text = f"{SYSTEM_INSTRUCTION}\n\n{query}"
    params = {"q": q_text, "api_key": API_KEY}

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        t0 = time.monotonic()
        try:
            resp = requests.get(API_BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
            elapsed = time.monotonic() - t0
            logger.info(
                "API call attempt %d ok in %.1fs (q_len=%d)", attempt, elapsed, len(q_text)
            )
            if resp.status_code >= 500:
                logger.warning("API 5xx body: %s", resp.text[:300])
                raise requests.exceptions.HTTPError(
                    f"Server error {resp.status_code}", response=resp
                )
            resp.raise_for_status()
            break  # success -> exit retry loop
        except requests.exceptions.HTTPError as e:
            # 4xx -> don't retry, fail immediately
            if e.response is not None and e.response.status_code < 500:
                logger.error("API client error (no retry): %s | body: %s", e, e.response.text[:300])
                return f"⚠️ API error ({e.response.status_code}). API key ba request check koro."
            last_error = e
        except requests.exceptions.RequestException as e:
            elapsed = time.monotonic() - t0
            last_error = e
            logger.warning(
                "API request exception (%s) after %.1fs: %s (q_len=%d)",
                type(e).__name__, elapsed, e, len(q_text),
            )

        if attempt < MAX_RETRIES:
            wait = RETRY_BACKOFF_BASE ** attempt
            logger.warning(
                "API call attempt %d/%d failed (%s), retrying in %ds...",
                attempt, MAX_RETRIES, last_error, wait,
            )
            time.sleep(wait)
    else:
        # loop finished without break -> all retries exhausted
        logger.error("API request failed after %d attempts: %s", MAX_RETRIES, last_error)
        return "⚠️ Worm AI server e connect kora jayni (multiple retry-o fail). Ektu pore abar try koro."

    try:
        data = resp.json()
    except ValueError:
        return resp.text.strip() or "⚠️ Empty response API theke ashche."

    if isinstance(data, dict):
        if data.get("status") == "error":
            msg = data.get("message") or data.get("response") or "Unknown API error."
            return f"⚠️ {msg}"

        answer = data.get("response")
        if not answer:
            for key in ("answer", "result", "message", "reply", "text", "data"):
                if data.get(key):
                    answer = str(data[key])
                    break
        if not answer:
            answer = str(data)
        return str(answer)

    return str(data)


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
        "🧠 কোনো আগের কথা মনে রাখে না — প্রতিটি প্রশ্ন সম্পূর্ণ নতুন\n"
        "⚡ টাইপিং-স্টাইলে লাইভ রিপ্লাই\n"
        "📜 লম্বা উত্তর স্বয়ংক্রিয়ভাবে ভাগ হয়\n"
        "🛡 স্প্যাম ও ফ্লাড প্রটেকশন\n"
        "🔁 নেটওয়ার্ক সমস্যায় অটো রিট্রাই\n"
        f"{DIVIDER}\n"
        "<i>🛠 Python দিয়ে তৈরি</i>"
    )


def status_text(session: dict) -> str:
    mode = "🟢 চালু" if session["ai_mode"] else "🔴 বন্ধ"
    left = _cooldown_left(session)
    cooldown = "✅ প্রস্তুত" if left == 0 else f"⏳ {left} সেকেন্ড বাকি"
    return (
        "📊 <b>আমার স্ট্যাটাস</b>\n"
        f"{DIVIDER}\n"
        f"🤖 <b>AI Mode:</b> {mode}\n"
        f"⏱ <b>পরবর্তী প্রশ্ন:</b> {cooldown}\n"
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
            [InlineKeyboardButton("📊 স্ট্যাটাস", callback_data="status")],
        ]
    )


def back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ মেইন মেনু", callback_data="home")]]
    )


JOINED_STATUSES = {"member", "administrator", "creator"}


async def check_membership(bot, user_id: int, session: dict, force: bool = False):
    """True  -> user channel-e joined
    False -> joined na (ba chere diyeche)
    None  -> verify kora jayni (bot channel-er admin na, channel bhul, network error...)
    """
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


async def ensure_joined(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    session: dict,
    group: bool = False,
) -> bool:
    """Returns True if the user may use AI. Otherwise shows the join screen
    and returns False. Private flow-e AI mode-o off kore dey; group flow-e
    user-er private AI mode-e hat dey na."""
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
    """Splits text into chunks <= limit chars, preferring to break on a
    newline, then a space, so words/paragraphs don't get cut mid-way."""
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


async def send_ai_reply(update: Update, answer: str, animate: bool = True):
    """Sends the AI answer, splitting into multiple Telegram messages if
    it's longer than Telegram's 4096-char limit, and animates each part
    with a ChatGPT-style word-by-word reveal instead of dumping the whole
    reply in one shot."""
    decoration_overhead = len("🐛 <b>Worm AI (99/99)</b>\n━━━━━━━━━━━━━━━━━\n\n━━━━━━━━━━━━━━━━━")
    content_limit = MAX_TELEGRAM_LEN - decoration_overhead - 50  # safety buffer

    raw_chunks = split_text(answer, content_limit)
    total = len(raw_chunks)

    for i, chunk in enumerate(raw_chunks, start=1):
        label = f" ({i}/{total})" if total > 1 else ""
        await stream_chunk(update, chunk, label, animate)


async def stream_chunk(update: Update, chunk: str, label: str, animate: bool = True):
    """Reveals one chunk word-by-word by repeatedly editing the same
    message, capped at STREAM_MAX_STEPS edits to stay well under
    Telegram's edit-rate limits. Falls back to a single send for short
    replies or if editing fails for any reason. Error messages (start
    with ⚠️) auto-delete after a while so they don't clutter the chat."""
    is_error = chunk.startswith("⚠️")
    words = chunk.split(" ")

    if not animate or len(chunk) < STREAM_MIN_CHARS_TO_ANIMATE or len(words) <= 1:
        msg = await update.message.reply_html(format_reply(chunk, label))
        if is_error:
            schedule_delete(msg)
        return

    steps = min(STREAM_MAX_STEPS, len(words))
    words_per_step = max(1, -(-len(words) // steps))  # ceil division

    sent_msg = await update.message.reply_html(format_reply("▌", label))

    shown_upto = 0
    try:
        while shown_upto < len(words):
            shown_upto = min(shown_upto + words_per_step, len(words))
            partial = " ".join(words[:shown_upto])
            cursor = " ▌" if shown_upto < len(words) else ""
            try:
                await sent_msg.edit_text(
                    format_reply(partial + cursor, label), parse_mode=ParseMode.HTML
                )
            except RetryAfter as e:
                await asyncio.sleep(e.retry_after)
            except BadRequest:
                pass  # e.g. "message is not modified" — safe to ignore
            await asyncio.sleep(STREAM_EDIT_DELAY)
    finally:
        # always make sure the final, complete text is shown
        try:
            await sent_msg.edit_text(format_reply(chunk, label), parse_mode=ParseMode.HTML)
        except BadRequest:
            pass
        if is_error:
            schedule_delete(sent_msg)


# ============================================================
#  COMMAND HANDLERS
# ============================================================
def is_private(update: Update) -> bool:
    chat = update.effective_chat
    return chat is not None and chat.type == "private"


def private_only_kb(update: Update, keyboard: InlineKeyboardMarkup):
    """Menu button gula shudhu private chat-e dekhabe (group-e main menu bhul dekhay)."""
    return keyboard if is_private(update) else None


def get_chat_settings(chat) -> dict:
    cs = chat_settings.get(chat.id)
    if cs is None:
        cs = {
            "title": chat.title,
            "enabled": True,  # notun group-e default chalu
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
    """/worm <prosno> — group ba private, jekono jaygay kaj kore.
    Kono prosno na dile, kono message-e reply kore /worm likhle oi message-er
    text-i prosno hishebe dhora hoy."""
    message = update.message
    user = update.effective_user
    chat = update.effective_chat
    raw = message.text or ""

    # "/worm-ai" (hyphen shoho) — Telegram eta "/worm" command hishebe dhore, tai ekhane dhorchi
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
    # Channel-join lock shudhu private chat-e — group-e kono restriction nei.
    if private and not await ensure_joined(update, context, session):
        return
    await process_ai_request(
        update, context, session, prompt, group_chat=None if private else chat
    )


async def _edit_screen(query, text: str, keyboard: InlineKeyboardMarkup):
    """Swaps the current menu message in place (no chat clutter)."""
    try:
        await query.edit_message_text(
            text, parse_mode=ParseMode.HTML, reply_markup=keyboard
        )
    except BadRequest:
        pass  # "message is not modified" etc.


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


async def process_ai_request(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    session: dict,
    prompt: str,
    group_chat=None,
):
    """Cooldown check -> API call -> state save -> reply. Private ar group,
    duitor jonnoi ek-i pipeline. group_chat set thakle oi group-er alada
    conversation ar simple (non-animated) reply use hoy."""
    now = time.time()
    elapsed = now - session["last_request"]
    if elapsed < COOLDOWN_SECONDS:
        remaining = int(COOLDOWN_SECONDS - elapsed) + 1
        msg = await update.message.reply_text(
            f"⏳ Ektu wait koro! Abar request korte {remaining}s baki ache."
        )
        schedule_delete(msg)
        return

    # Cooldown ekhoni "reserve" kori — jate ekshathe duita request eshe cooldown na bhange
    session["last_request"] = now

    await context.bot.send_chat_action(
        chat_id=update.effective_chat.id, action=ChatAction.TYPING
    )

    # Kono conversation history rakha hoy na — protyek request-i notun/fresh
    # hishebe pathano hoy, tai query shob shomoy choto ar fast thake.
    loop = asyncio.get_running_loop()
    answer = await loop.run_in_executor(None, call_worm_ai, prompt)

    session["last_request"] = time.time()
    session["profile"]["total_requests"] += 1
    bot_stats["total_requests"] += 1
    if group_chat is not None:
        cs = get_chat_settings(group_chat)
        cs["total_requests"] += 1
        cs["last_used"] = _now_iso()

    # group-e animation off — Telegram-er group rate-limit er jonno
    await send_ai_reply(update, answer, animate=group_chat is None)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles plain text messages, and also catches the literal
    '/worm-ai' text (Telegram doesn't allow hyphens in real commands,
    so it arrives here as normal text)."""
    user = update.effective_user
    session = get_session(user.id)
    text = update.message.text.strip()

    # allow typing "/worm-ai" with a hyphen too
    if text.lower() in ("/worm-ai", "/worm_ai"):
        await worm_ai_command(update, context)
        return

    if not session["ai_mode"]:
        msg = await update.message.reply_text(
            "ℹ️ AI mode off ache. /worm_ai (ba /worm-ai) diye on koro."
        )
        schedule_delete(msg)
        return

    # ---- channel must still be joined ----
    if not await ensure_joined(update, context, session):
        return

    await process_ai_request(update, context, session, text)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.error("Update %s caused error: %s", update, context.error)


# ============================================================
#  MAIN
# ============================================================
def main():
    if not BOT_TOKEN:
        raise SystemExit("BOT_TOKEN missing! .env file check koro.")
    if not API_KEY:
        logger.warning("WORM_AI_API_KEY set kora hoyni — API call fail korte pare.")

    load_data()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .concurrent_updates(True)  # ekjon-er slow API call jate group-er onno ke block na kore
        .build()
    )

    # flood guard MUST run first, for every update -> group=-1
    app.add_handler(MessageHandler(filters.ALL, flood_guard), group=-1)

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("worm_ai", worm_ai_command))
    app.add_handler(CommandHandler("worm", worm_command))
    app.add_handler(CommandHandler("worm_on", worm_on_command))
    app.add_handler(CommandHandler("worm_off", worm_off_command))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("about", about_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    # AI-mode chat shudhu private-e; group-e shudhu /worm command
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, handle_message
        )
    )
    app.add_error_handler(error_handler)

    logger.info("Worm AI Bot starting (polling mode)...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
