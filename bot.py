#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# ─── Standard Library ───────────────────────────────────────────────────────
import os
import json
import asyncio
import glob
import html as _html
import time
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Callable, Awaitable, Any

# ─── Third-party ────────────────────────────────────────────────────────────
import aiohttp
import pytz
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    MessageEntity, User, ChatMember,
    InputMediaPhoto, InputMediaVideo, InputMediaDocument, InputMediaAudio,
    CopyTextButton,
)
from telegram.ext import (
    ApplicationBuilder, Application,
    MessageHandler, CommandHandler,
    CallbackQueryHandler, ConversationHandler,
    filters, ContextTypes,
)
from telegram.error import Forbidden, RetryAfter, TimedOut

# ─── Bot Config ─────────────────────────────────────────────────────────────
BOT_TOKEN        = os.environ.get("BOT_TOKEN", "8772542821:AAFuF6-bH_PoFfOzOSjAzgx2uErihcomB-U")
OWNER_IDS        = {1995454152, 6382706640}
OWNER_ID         = 1995454152
TIMEZONE         = pytz.timezone("Asia/Baghdad")

# ─── Paths ──────────────────────────────────────────────────────────────────
DOWNLOAD_DIR     = os.path.expanduser("~/instagram_downloads")
DATA_FILE        = "bot_data.json"
USERS_FILE       = "users.json"
DB_FILE          = "scheduler.db"

# ─── Limits & Intervals ─────────────────────────────────────────────────────
MAX_FILE_SIZE_MB          = 50
MAX_CONCURRENT            = 10
RATE_LIMIT_COUNT          = 5
RATE_LIMIT_WIN            = 60
CLEANUP_INTERVAL          = 120
FILE_MAX_AGE              = 300
STATE_TTL_SECONDS         = 3600
STATE_CLEANUP_INTERVAL    = 600
SEND_RETRY_MAX_ATTEMPTS   = 5
RETRY_AFTER_GRACE_SECONDS = 1.0

# ─── Cobalt API ─────────────────────────────────────────────────────────────
COBALT_API_URL = os.environ.get("COBALT_API_URL", "http://localhost:9000")
COBALT_TIMEOUT = 30
COBALT_RETRIES = 3

ALLOWED_STYLES = {"primary", "success", "danger", None}
BTN_STYLES = {
    "🔵 أساسي":   "primary",
    "🟢 نجاح":    "success",
    "🔴 خطر":     "danger",
    "⬛ افتراضي": None,
}
ALL_PERMISSIONS = {
    "edit_welcome":  "✏️ تعديل رسالة البداية",
    "manage_btns":   "🔘 إدارة الأزرار",
    "force_sub":     "📢 الاشتراك الإجباري",
    "perm_channels": "📌 القنوات الثابتة",
    "broadcast":     "📣 بث رسالة",
    "ban_users":     "🚫 حظر المستخدمين",
    "stats":         "📊 الإحصائيات",
}

(S_MSG, S_CAP_POS, S_INTERVAL, S_TIME, S_DAYS,
 S_BTN_CONFIRM, S_BTN_TEXT, S_BTN_TYPE, S_BTN_URL, S_BTN_COLOR) = range(10)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("bot.log", encoding="utf-8")],
)
logger = logging.getLogger(__name__)
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
_executor = ThreadPoolExecutor(max_workers=20)
data_file_lock = asyncio.Lock()
users_file_lock = asyncio.Lock()
send_rate_limit_lock = asyncio.Lock()
send_rate_limit_until = 0.0

class Database:
    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as exc:
            conn.rollback()
            logger.error(f"DB: {exc}")
            raise
        finally:
            conn.close()

    def setup(self):
        with self._conn() as c:
            cur = c.cursor()
            cur.execute("""CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT, task_type TEXT NOT NULL,
                interval_minutes INTEGER, schedule_time TEXT, schedule_days TEXT,
                content_type TEXT, file_id TEXT, text_content TEXT, caption TEXT,
                entities TEXT, media_group_id TEXT, caption_position TEXT DEFAULT 'below',
                active INTEGER DEFAULT 1, last_sent TIMESTAMP, execution_date DATE,
                last_message_ids TEXT, delete_after_send INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
            cur.execute("""CREATE TABLE IF NOT EXISTS buttons (
                id INTEGER PRIMARY KEY AUTOINCREMENT, task_id INTEGER NOT NULL,
                text TEXT NOT NULL, url TEXT NOT NULL, btn_type TEXT DEFAULT 'url',
                btn_content TEXT, style TEXT DEFAULT 'secondary', emoji_id TEXT,
                row_number INTEGER DEFAULT 0, order_index INTEGER DEFAULT 0,
                FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE)""")
            cur.execute("""CREATE TABLE IF NOT EXISTS media_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT, task_id INTEGER NOT NULL,
                media_type TEXT NOT NULL, file_id TEXT NOT NULL, caption TEXT,
                order_index INTEGER DEFAULT 0,
                FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE)""")
            for tbl, col, defn in [
                ("tasks", "last_message_ids", "TEXT"),
                ("tasks", "delete_after_send", "INTEGER DEFAULT 0"),
                ("buttons", "emoji_id", "TEXT"),
            ]:
                try:
                    cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {defn}")
                except sqlite3.OperationalError:
                    pass

    def create_task(self, d):
        with self._conn() as c:
            cur = c.cursor()
            cur.execute("""INSERT INTO tasks (task_type,interval_minutes,schedule_time,
                schedule_days,content_type,file_id,text_content,caption,entities,
                media_group_id,caption_position,active) VALUES (?,?,?,?,?,?,?,?,?,?,?,1)""",
                (d['task_type'],d.get('interval_minutes'),d.get('schedule_time'),
                 d.get('schedule_days'),d['content_type'],d.get('file_id'),
                 d.get('text_content'),d.get('caption'),d.get('entities'),
                 d.get('media_group_id'),d.get('caption_position','below')))
            return cur.lastrowid

    def get_tasks(self, task_type=None):
        with self._conn() as c:
            cur = c.cursor()
            if task_type:
                cur.execute(
                    "SELECT * FROM tasks WHERE task_type=? AND active=1 ORDER BY created_at DESC",
                    (task_type,),
                )
            else:
                cur.execute("SELECT * FROM tasks WHERE active=1 ORDER BY created_at DESC")
            return [dict(r) for r in cur.fetchall()]

    def get_all_tasks(self):
        with self._conn() as c:
            cur = c.cursor()
            cur.execute("SELECT * FROM tasks ORDER BY created_at DESC")
            return [dict(r) for r in cur.fetchall()]

    def get_task(self, tid):
        with self._conn() as c:
            cur = c.cursor()
            cur.execute("SELECT * FROM tasks WHERE id=?", (tid,))
            row = cur.fetchone()
            if row:
                return dict(row)
            return None

    def update_task(self, tid, updates):
        fields = ", ".join(f"{k}=?" for k in updates)
        with self._conn() as c:
            cur = c.cursor()
            cur.execute(
                f"UPDATE tasks SET {fields} WHERE id=?",
                list(updates.values()) + [tid],
            )

    def delete_task(self, tid):
        with self._conn() as c:
            cur = c.cursor()
            cur.execute("DELETE FROM tasks WHERE id=?",(tid,))
            cur.execute("DELETE FROM buttons WHERE task_id=?",(tid,))
            cur.execute("DELETE FROM media_items WHERE task_id=?",(tid,))

    def toggle_task(self, tid):
        t = self.get_task(tid)
        if t:
            ns = 0 if t['active'] else 1
            self.update_task(tid, {'active': ns})
            return ns

    def update_last_sent(self, tid):
        self.update_task(tid,{'last_sent':datetime.now(TIMEZONE).isoformat()})

    def add_button(self, task_id, text, url, style='secondary',
                   btn_type='url', btn_content=None, emoji_id=None, row=0, order=0):
        with self._conn() as c:
            cur = c.cursor()
            cur.execute(
                "INSERT INTO buttons (task_id,text,url,btn_type,btn_content,style,emoji_id,row_number,order_index) VALUES (?,?,?,?,?,?,?,?,?)",
                (task_id, text, url, btn_type, btn_content, style, emoji_id, row, order),
            )

    def get_buttons(self, task_id):
        with self._conn() as c:
            cur = c.cursor()
            cur.execute(
                "SELECT * FROM buttons WHERE task_id=? ORDER BY row_number,order_index",
                (task_id,),
            )
            return [dict(r) for r in cur.fetchall()]

    def delete_button(self, btn_id):
        with self._conn() as c:
            cur = c.cursor()
            cur.execute("SELECT task_id FROM buttons WHERE id=?", (btn_id,))
            row = cur.fetchone()
            if row:
                cur.execute("DELETE FROM buttons WHERE id=?", (btn_id,))
                return row['task_id']
        return None

    def add_media(self, task_id, mtype, file_id, caption=None, order=0):
        with self._conn() as c:
            cur = c.cursor()
            cur.execute(
                "INSERT INTO media_items (task_id,media_type,file_id,caption,order_index) VALUES (?,?,?,?,?)",
                (task_id, mtype, file_id, caption, order),
            )

    def get_media(self, task_id):
        with self._conn() as c:
            cur = c.cursor()
            cur.execute("SELECT * FROM media_items WHERE task_id=? ORDER BY order_index", (task_id,))
            return [dict(r) for r in cur.fetchall()]

db = Database()

def _default_data():
    return {
        "welcome_text": ("أهلاً بك عزيزي #userlink 👋\n\nيمكنك تحميل من Instagram بأعلى جودة ✨\n\n"
                         "⚡️ أرسل رابط ريلز، كاروسيل، ستوري، أو هايلايت\n⬆️ ولا تنسَ مشاركة البوت لأصدقائك 🙌"),
        "welcome_entities": None,
        "welcome_buttons": [{"text":"⚡️ VidsTech","type":"url","value":"https://t.me/VidsTech","style":None,"emoji_id":None}],
        "how_to_use_text": "📱 كيفية استخدام البوت\n\n1️⃣ افتح تطبيق Instagram\n2️⃣ اختر المحتوى (ريلز، كاروسيل، ستوري، هايلايت)\n3️⃣ اضغط مشاركة ثم نسخ الرابط\n4️⃣ أرسل الرابط هنا مباشرةً\n\n✅ يدعم البوت: ريلز 🎬 | كاروسيل 🖼️ | ستوري 👁️ | هايلايت ⭐",
        "force_channels":[],"perm_channels":[],"banned_users":{},"admins":{},
    }

def _clean_buttons(btns):
    out = []
    for b in btns:
        if not isinstance(b,dict): continue
        style = b.get("style")
        if style not in ALLOWED_STYLES: style = None
        eid = b.get("emoji_id")
        if eid: eid = str(eid) if str(eid).isdigit() else None
        out.append({"text":str(b.get("text","زر")),"type":b.get("type","url"),"value":str(b.get("value","")),"style":style,"emoji_id":eid})
    return out

_data_cache: dict = {}
_data_cache_ts: float = 0.0

def _invalidate_cache():
    global _data_cache_ts
    _data_cache_ts = 0.0

def _atomic_write_json(path, payload):
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)

def load_data():
    global _data_cache, _data_cache_ts
    now = time.monotonic()
    if _data_cache and (now - _data_cache_ts) < 2.0:
        return _data_cache

    defaults = _default_data()
    if not Path(DATA_FILE).exists():
        _write_data(defaults)
        _data_cache = defaults
        _data_cache_ts = now
        return defaults

    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = _default_data()

    changed = False
    for k, v in defaults.items():
        if k not in data:
            data[k] = v
            changed = True

    if "welcome_buttons" in data:
        cleaned = _clean_buttons(data["welcome_buttons"])
        if cleaned != data["welcome_buttons"]:
            data["welcome_buttons"] = cleaned
            changed = True

    if changed:
        _write_data(data)

    _data_cache = data
    _data_cache_ts = now
    return data

def _write_data(data):
    if "welcome_buttons" in data:
        data["welcome_buttons"] = _clean_buttons(data["welcome_buttons"])
    _atomic_write_json(DATA_FILE, data)

def save_data(data):
    _write_data(data)
    _invalidate_cache()

async def save_data_locked(data):
    async with data_file_lock:
        save_data(data)

def is_owner(uid): return uid in OWNER_IDS
def get_admin(uid): return load_data().get("admins",{}).get(str(uid))
def has_perm(uid,perm):
    if is_owner(uid): return True
    adm = get_admin(uid); return bool(adm and adm.get("permissions",{}).get(perm))
def is_banned(uid): return str(uid) in load_data().get("banned_users",{})
def can_use_panel(uid): return is_owner(uid) or bool(get_admin(uid))

def entities_to_json(entities):
    if not entities: return None
    out = []
    for e in entities:
        d = {"type":e.type,"offset":e.offset,"length":e.length}
        if e.url: d["url"] = e.url
        if e.language: d["language"] = e.language
        if e.custom_emoji_id: d["custom_emoji_id"] = e.custom_emoji_id
        if e.user: d["user"] = {"id":e.user.id,"first_name":e.user.first_name,"username":e.user.username}
        out.append(d)
    return out

def json_to_entities(data):
    if not data: return None
    out = []
    for d in data:
        user = None
        if "user" in d: user = User(id=d["user"]["id"],first_name=d["user"]["first_name"],is_bot=False)
        out.append(MessageEntity(type=d["type"],offset=d["offset"],length=d["length"],
            url=d.get("url"),language=d.get("language"),custom_emoji_id=d.get("custom_emoji_id"),user=user))
    return out

def entities_to_html(text,entities):
    if not entities: return _html.escape(text)
    se = sorted(entities,key=lambda e:e.offset); res=[]; prev=0; tb=text.encode("utf-16-le")
    def s(a,b): return tb[a*2:b*2].decode("utf-16-le")
    for e in se:
        res.append(_html.escape(s(prev,e.offset))); inn=s(e.offset,e.offset+e.length); t=e.type
        if   t=="bold":          res.append(f"<b>{_html.escape(inn)}</b>")
        elif t=="italic":        res.append(f"<i>{_html.escape(inn)}</i>")
        elif t=="underline":     res.append(f"<u>{_html.escape(inn)}</u>")
        elif t=="strikethrough": res.append(f"<s>{_html.escape(inn)}</s>")
        elif t=="spoiler":       res.append(f"<tg-spoiler>{_html.escape(inn)}</tg-spoiler>")
        elif t=="code":          res.append(f"<code>{_html.escape(inn)}</code>")
        elif t=="pre":
            lang = f' language="{_html.escape(e.language)}"' if e.language else ""
            res.append(f"<pre{lang}>{_html.escape(inn)}</pre>")
        elif t=="text_link":    res.append(f'<a href="{_html.escape(e.url or "")}">{_html.escape(inn)}</a>')
        elif t=="text_mention": res.append(f'<a href="tg://user?id={e.user.id if e.user else 0}">{_html.escape(inn)}</a>')
        elif t=="custom_emoji": res.append(f'<tg-emoji emoji-id="{e.custom_emoji_id}">{_html.escape(inn)}</tg-emoji>')
        else: res.append(_html.escape(inn))
        prev = e.offset+e.length
    res.append(_html.escape(s(prev,len(text)))); return "".join(res)

class _SafeButton(InlineKeyboardButton):
    __slots__ = ("_style","_icon_custom_emoji_id")
    def __init__(self,text,style=None,icon_custom_emoji_id=None,**kw):
        super().__init__(text,**kw)
        self._style = style if style in ALLOWED_STYLES else None
        eid = str(icon_custom_emoji_id) if icon_custom_emoji_id else None
        self._icon_custom_emoji_id = eid if (eid and eid.isdigit()) else None
    def to_dict(self,**kw):
        d = super().to_dict(**kw)
        api = {}
        if self._style: api["style"] = self._style
        if self._icon_custom_emoji_id: api["icon_custom_emoji_id"] = self._icon_custom_emoji_id
        if api: d["api_kwargs"] = api
        return d

def _make_btn(b):
    style = b.get("style"); eid = b.get("emoji_id")
    style = style if style in ALLOWED_STYLES else None
    eid = str(eid) if eid and str(eid).isdigit() else None
    if b.get("type")=="url": return _SafeButton(b.get("text","زر"),url=b.get("value",""),style=style,icon_custom_emoji_id=eid)
    return _SafeButton(b.get("text","زر"),callback_data=b.get("value","noop"),style=style,icon_custom_emoji_id=eid)

HOW_TO_USE_BTN_TEXT = "كيفية الاستخدام"
HOW_TO_USE_BTN_EMOJI = "5327982530702359565"
HOW_TO_USE_CALLBACK = "how_to_use"

def build_welcome_keyboard(data):
    btns = data.get("welcome_buttons",[])
    rows = [[_make_btn(b)] for b in btns]
    how_btn = _SafeButton(
        HOW_TO_USE_BTN_TEXT,
        callback_data=HOW_TO_USE_CALLBACK,
        style="primary",
    )
    rows.append([how_btn])
    return InlineKeyboardMarkup(rows)

def _build_welcome(raw_text,raw_entities,user):
    name=user.first_name or "صديقي"; PH="#userlink"
    def u16(s): return len(s.encode("utf-16-le"))//2
    ph_u16=u16(PH); men_u16=u16(name); diff=men_u16-ph_u16
    tb=raw_text.encode("utf-16-le"); phb=PH.encode("utf-16-le"); pos=tb.find(phb)
    if pos==-1: return raw_text,json_to_entities(raw_entities)
    ins=pos//2
    new_text=tb[:pos].decode("utf-16-le")+name+tb[pos+len(phb):].decode("utf-16-le")
    me=MessageEntity(type=MessageEntity.TEXT_MENTION,offset=ins,length=men_u16,user=User(id=user.id,first_name=name,is_bot=False))
    fixed=[me]
    for d in (raw_entities or []):
        o,l=d["offset"],d["length"]
        if o+l<=ins: fixed.append(MessageEntity(type=d["type"],offset=o,length=l,url=d.get("url"),language=d.get("language"),custom_emoji_id=d.get("custom_emoji_id")))
        elif o>=ins+ph_u16: fixed.append(MessageEntity(type=d["type"],offset=o+diff,length=l,url=d.get("url"),language=d.get("language"),custom_emoji_id=d.get("custom_emoji_id")))
    return new_text,fixed or None

async def send_welcome(message,data,user):
    raw_text=data.get("welcome_text",""); raw_ents=data.get("welcome_entities"); kb=build_welcome_keyboard(data)
    if raw_ents:
        text,ents=_build_welcome(raw_text,raw_ents,user)
        try: await message.reply_text(text,entities=ents,reply_markup=kb); return
        except Exception as e: logger.warning(f"send_welcome: {e}")
    mention=f"[{user.first_name or 'صديقي'}](tg://user?id={user.id})"
    text=raw_text.replace("#userlink",mention)
    try: await message.reply_text(text,parse_mode="Markdown",reply_markup=kb)
    except Exception: await message.reply_text(text,reply_markup=kb)

def load_users():
    if not Path(USERS_FILE).exists():
        return {}
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_users(u):
    _atomic_write_json(USERS_FILE, u)

async def register_user(user):
    async with users_file_lock:
        users = load_users()
        uid = str(user.id)
        if uid in users:
            return False
        users[uid] = {
            "id": user.id,
            "first_name": user.first_name or "",
            "username": user.username or "",
            "joined": time.strftime("%Y-%m-%d %H:%M"),
        }
        save_users(users)
        return True

async def _check_member(bot,username,uid):
    try:
        m = await asyncio.wait_for(bot.get_chat_member(username,uid),timeout=6.0)
        return m.status not in ("left","kicked","banned")
    except asyncio.TimeoutError:
        logger.warning(f"force_sub timeout: {username}")
        return True
    except Exception as e:
        logger.warning(f"force_sub error {username}: {e}")
        return True

async def check_force_sub(update,context):
    data=load_data(); uid=update.effective_user.id
    active_dyn=[c for c in data.get("force_channels",[]) if c.get("active")]
    active_perm=data.get("perm_channels",[])
    all_ch=active_dyn+active_perm
    if not all_ch: return True
    results=await asyncio.gather(*[_check_member(context.bot,ch["username"],uid) for ch in all_ch],return_exceptions=True)
    not_joined=[ch for ch,ok in zip(all_ch,results) if ok is not True]
    if not not_joined: return True
    rows=[[InlineKeyboardButton(f"📢 {ch.get('title',ch.get('username','قناة'))}",url=ch["link"])] for ch in not_joined]
    rows.append([InlineKeyboardButton("✅ تحقق",callback_data="check_sub")])
    msg=update.message or (update.callback_query and update.callback_query.message)
    if msg:
        sent=await msg.reply_text("⚠️ عذراً يجب الاشتراك في هذه القنوات أولاً:",reply_markup=InlineKeyboardMarkup(rows))
        context.user_data["force_sub_msg_id"]=sent.message_id; context.user_data["force_sub_chat_id"]=sent.chat_id
    return False

async def _update_force_sub_counter(context,user_id):
    data=load_data(); changed=False
    for ch in data.get("force_channels",[]):
        if not ch.get("active"): continue
        try:
            m=await asyncio.wait_for(context.bot.get_chat_member(ch["username"],user_id),timeout=5.0)
            if m.status not in ("left","kicked","banned"):
                ch["added"]=ch.get("added",0)+1; changed=True
                if ch.get("target",0)>0 and ch["added"]>=ch["target"]: ch["active"]=False
        except Exception: pass
    if changed:
        await save_data_locked(data)

semaphore: asyncio.Semaphore = None
user_requests: Dict[int, List[float]] = {}
active_downloads: Dict[int, int] = {}
user_last_seen: Dict[int, float] = {}

def _touch_runtime_state(uid: int, now: Optional[float] = None):
    now = now if now is not None else time.monotonic()
    user_last_seen[uid] = now

def _cleanup_runtime_state(now: Optional[float] = None):
    now = now if now is not None else time.monotonic()
    for uid, stamps in list(user_requests.items()):
        filtered = [ts for ts in stamps if now - ts < RATE_LIMIT_WIN]
        if filtered:
            user_requests[uid] = filtered
        else:
            user_requests.pop(uid, None)
    stale_uids = [
        uid for uid, last in user_last_seen.items()
        if now - last > STATE_TTL_SECONDS and active_downloads.get(uid, 0) <= 0
    ]
    for uid in stale_uids:
        user_last_seen.pop(uid, None)
        user_requests.pop(uid, None)
        active_downloads.pop(uid, None)

def _retry_after_to_seconds(exc: RetryAfter) -> int:
    value = getattr(exc, "retry_after", 1)
    if isinstance(value, timedelta):
        wait_seconds = value.total_seconds()
    else:
        try:
            wait_seconds = float(value)
        except Exception:
            wait_seconds = 1
    return max(1, int(wait_seconds))

async def _wait_for_rate_limit_pause():
    while True:
        async with send_rate_limit_lock:
            remaining = send_rate_limit_until - time.monotonic()
        if remaining <= 0:
            return
        await asyncio.sleep(remaining)

async def _apply_retry_after_pause(wait_seconds: int):
    global send_rate_limit_until
    target = time.monotonic() + wait_seconds + RETRY_AFTER_GRACE_SECONDS
    async with send_rate_limit_lock:
        if target > send_rate_limit_until:
            send_rate_limit_until = target

async def _send_with_retry(
    send_coro_factory: Callable[[], Awaitable[Any]],
    log_label: str,
    max_attempts: int = SEND_RETRY_MAX_ATTEMPTS,
):
    for attempt in range(1, max_attempts + 1):
        await _wait_for_rate_limit_pause()
        try:
            return await send_coro_factory()
        except RetryAfter as exc:
            wait_seconds = _retry_after_to_seconds(exc)
            await _apply_retry_after_pause(wait_seconds)
            logger.warning(
                f"{log_label}: RetryAfter {wait_seconds}s (attempt {attempt}/{max_attempts})"
            )
        except Forbidden:
            raise
    raise RuntimeError(f"{log_label}: exceeded retry attempts")

def is_rate_limited(uid):
    now = time.monotonic()
    _touch_runtime_state(uid, now)
    recent = [t for t in user_requests.get(uid, []) if now - t < RATE_LIMIT_WIN]
    user_requests[uid] = recent
    if len(recent) >= RATE_LIMIT_COUNT:
        return True
    recent.append(now)
    user_requests[uid] = recent
    return False

# ─── Instagram URL Helpers ────────────────────────────────────────────────────
INSTAGRAM_DOMAINS = ("instagram.com", "instagr.am", "ig.me", "i.instagram.com")
TIKTOK_DOMAINS    = ("tiktok.com", "vm.tiktok.com", "vt.tiktok.com", "m.tiktok.com")

CONTENT_TYPE_EMOJI = {
    "reel":      "🎬",
    "carousel":  "🖼️",
    "story":     "👁️",
    "highlight": "⭐",
    "video":     "📱",
}

TIKTOK_BOT_USERNAME_KEY = "tiktok_bot_username"

def _is_instagram_url(url: str) -> bool:
    return any(d in url for d in INSTAGRAM_DOMAINS)

def _is_tiktok_url(url: str) -> bool:
    return any(d in url for d in TIKTOK_DOMAINS)

def _is_valid_instagram_content_url(url: str) -> bool:
    import re as _re
    valid_patterns = [
        r'instagram\.com/p/',
        r'instagram\.com/reel/',
        r'instagram\.com/reels/',
        r'instagram\.com/stories/',
        r'instagram\.com/highlights/',
        r'instagr\.am/',
        r'ig\.me/',
        r'i\.instagram\.com/',
    ]
    return any(_re.search(p, url) for p in valid_patterns)

def _detect_instagram_type(url: str) -> str:
    url_lower = url.lower()
    if "/reel/" in url_lower or "/reels/" in url_lower:
        return "reel"
    elif "/stories/" in url_lower or "/story/" in url_lower:
        return "story"
    elif "/highlights/" in url_lower:
        return "highlight"
    elif "/p/" in url_lower:
        return "carousel"
    return "video"

# ─── yt-dlp — منطق التحميل ───────────────────────────────────────────────────

# ─── Instagram Auth ──────────────────────────────────────────────────────────
INSTAGRAM_COOKIES_FILE = os.environ.get("INSTAGRAM_COOKIES", "")
INSTAGRAM_SESSION_ID   = os.environ.get("INSTAGRAM_SESSION_ID", "")
INSTAGRAM_USERNAME     = os.environ.get("INSTAGRAM_USERNAME", "")
INSTAGRAM_PASSWORD     = os.environ.get("INSTAGRAM_PASSWORD", "")

class DownloadError(Exception):
    """خطأ عام أثناء التحميل"""

class PrivateContentError(DownloadError):
    """المحتوى خاص أو يتطلب تسجيل دخول"""

class FileSizeError(DownloadError):
    """الملف أكبر من الحد المسموح"""

CobaltError        = DownloadError
CobaltPrivateError = PrivateContentError
CobaltSizeError    = FileSizeError


def _collect_files(download_dir: str, prefix: str, max_bytes: int) -> list[str]:
    result = []
    for fp in sorted(glob.glob(os.path.join(download_dir, f"{prefix}*"))):
        if not os.path.exists(fp):
            continue
        sz = os.path.getsize(fp)
        if sz == 0:
            os.remove(fp)
            continue
        if sz > max_bytes:
            logger.warning(f"⚠️ تخطي ملف كبير: {sz/1024/1024:.1f}MB")
            os.remove(fp)
            continue
        result.append(fp)
    return result


async def _cobalt_fetch(url: str) -> dict:
    """يرسل الرابط لـ Cobalt API الخاص ويرجع الرد"""
    import re as _re
    clean_url = _re.sub(r'[?&]img_index=\d+', '', url)
    clean_url = _re.sub(r'\?&', '?', clean_url).rstrip('?&')

    payload = {
        "url":           clean_url,
        "videoQuality":  "1080",
        "audioFormat":   "mp3",
        "filenameStyle": "basic",
        "downloadMode":  "auto",
    }
    headers = {
        "Accept":       "application/json",
        "Content-Type": "application/json",
    }

    last_exc = None
    for attempt in range(1, COBALT_RETRIES + 1):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{COBALT_API_URL}/",
                    json=payload,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=COBALT_TIMEOUT),
                ) as resp:
                    if resp.status == 429:
                        wait = int(resp.headers.get("Retry-After", 5))
                        logger.warning(f"Cobalt rate limit، ننتظر {wait}s")
                        await asyncio.sleep(wait)
                        continue
                    if resp.status != 200:
                        body = await resp.text()
                        raise DownloadError(f"Cobalt HTTP {resp.status}: {body[:200]}")
                    return await resp.json(content_type=None)
        except aiohttp.ClientError as e:
            last_exc = e
            logger.warning(f"Cobalt اتصال فاشل (محاولة {attempt}/{COBALT_RETRIES}): {e}")
            if attempt < COBALT_RETRIES:
                await asyncio.sleep(2 * attempt)

    raise DownloadError(f"Cobalt فشل بعد {COBALT_RETRIES} محاولات: {last_exc}")


async def _download_file(session: aiohttp.ClientSession, url: str, dest: str) -> int:
    max_bytes = MAX_FILE_SIZE_MB * 1024 * 1024
    downloaded = 0
    async with session.get(
        url,
        timeout=aiohttp.ClientTimeout(total=180, connect=15),
        headers={"User-Agent": "Mozilla/5.0"},
    ) as resp:
        if resp.status != 200:
            raise DownloadError(f"فشل تحميل الملف: HTTP {resp.status}")
        with open(dest, "wb") as f:
            async for chunk in resp.content.iter_chunked(512 * 1024):
                downloaded += len(chunk)
                if downloaded > max_bytes:
                    raise FileSizeError(f"الملف تجاوز {MAX_FILE_SIZE_MB}MB")
                f.write(chunk)
    return downloaded


def _ext_from(url: str, fname: str = "") -> str:
    for src in (fname, url):
        if not src:
            continue
        clean = src.split("?")[0].split("#")[0]
        for ext in (".mp4", ".mov", ".jpg", ".jpeg", ".png", ".webp", ".mp3", ".m4a"):
            if clean.lower().endswith(ext):
                return ext
    return ".mp4"


async def _ytdlp_download(url: str, user_id: int) -> tuple[list[str], str, str]:
    """التحميل عبر Cobalt self-hosted"""
    content_type = _detect_instagram_type(url)
    ts           = int(time.time())
    prefix       = f"{user_id}_{ts}"
    file_paths: list[str] = []

    data   = await _cobalt_fetch(url)
    status = data.get("status", "")

    if status == "error":
        err  = data.get("error", {})
        code = err.get("code", "") if isinstance(err, dict) else str(err)
        if any(k in code.lower() for k in ("private", "login", "auth")):
            raise PrivateContentError("المحتوى خاص أو يتطلب تسجيل دخول")
        raise DownloadError(f"Cobalt خطأ: {code}")

    download_urls: list[dict] = []

    if status in ("tunnel", "redirect"):
        media_url = data.get("url")
        if not media_url:
            raise DownloadError("Cobalt لم يُرجع رابط تحميل")
        download_urls.append({"url": media_url, "ext": _ext_from(media_url, data.get("filename", ""))})

    elif status == "picker":
        for item in (data.get("picker") or []):
            if not isinstance(item, dict):
                continue
            mu = item.get("url")
            if mu:
                download_urls.append({"url": mu, "ext": _ext_from(mu, item.get("filename", ""))})

    else:
        raise DownloadError(f"Cobalt رد غير متوقع: status={status}")

    if not download_urls:
        raise DownloadError("لم يُعثر على روابط في رد Cobalt")

    async with aiohttp.ClientSession() as session:
        for i, item in enumerate(download_urls):
            dest = os.path.join(DOWNLOAD_DIR, f"{prefix}_{i}{item['ext']}")
            try:
                size = await _download_file(session, item["url"], dest)
                logger.info(f"✅ ملف {i+1}/{len(download_urls)} — {size/1024/1024:.1f}MB")
                file_paths.append(dest)
            except FileSizeError:
                if os.path.exists(dest): os.remove(dest)
                if len(download_urls) == 1: raise
                logger.warning(f"⚠️ ملف {i+1} تجاوز الحد، تم تخطيه")
            except Exception as e:
                if os.path.exists(dest): os.remove(dest)
                logger.error(f"❌ فشل ملف {i+1}: {e}")
                if len(download_urls) == 1:
                    raise DownloadError(f"فشل التحميل: {e}")

    if not file_paths:
        raise DownloadError("فشل تحميل جميع الملفات")

    logger.info(f"✅ Cobalt حمّل {len(file_paths)} ملف | {content_type}")
    return file_paths, "", content_type

class DownloadError(Exception):
    """خطأ عام أثناء التحميل"""

class PrivateContentError(DownloadError):
    """المحتوى خاص أو يتطلب تسجيل دخول"""

class FileSizeError(DownloadError):
    """الملف أكبر من الحد المسموح"""

# للتوافق مع الكود القديم
CobaltError        = DownloadError
CobaltPrivateError = PrivateContentError
CobaltSizeError    = FileSizeError






# ─── التحميل الهجين: yt-dlp للفيديو + gallery-dl للألبومات ──────────────────

async def _ytdlp_download(url: str, user_id: int) -> tuple[list[str], str, str]:
    """
    منطق التحميل الكامل:
    - الريلز/الفيديو المفرد  → yt-dlp
    - الكاروسيل/الستوريز/الهايلايت → gallery-dl أولاً، yt-dlp كـ fallback
    يرجع (file_paths, title, content_type)
    """
    import yt_dlp

    content_type = _detect_instagram_type(url)
    ts           = int(time.time())
    prefix       = f"{user_id}_{ts}"
    max_bytes    = MAX_FILE_SIZE_MB * 1024 * 1024
    file_paths: list[str] = []
    title        = ""

    loop = asyncio.get_event_loop()

    # ─── مسار 1: gallery-dl للكاروسيل / ستوري / هايلايت ────────────────────
    if content_type in ("carousel", "story", "highlight"):
        logger.info(f"📥 gallery-dl | {content_type} | {url}")
        try:
            file_paths = await loop.run_in_executor(
                _executor, _gallery_dl_download, url, prefix
            )
            if file_paths:
                logger.info(f"✅ gallery-dl حمّل {len(file_paths)} ملف")
                return file_paths, "", content_type
            logger.warning("⚠️ gallery-dl لم يُنتج ملفات، جرّب yt-dlp")
        except Exception as e:
            logger.warning(f"⚠️ gallery-dl فشل ({e})، جرّب yt-dlp")

    # ─── مسار 2: yt-dlp (ريلز + fallback للباقي) ────────────────────────────
    logger.info(f"📥 yt-dlp | {content_type} | {url}")
    ytdlp_tmpl = os.path.join(DOWNLOAD_DIR, f"{prefix}_%(autonumber)s.%(ext)s")

    def _run_ytdlp():
        opts = _ytdlp_opts(ytdlp_tmpl)
        # للكاروسيل نسمح بالـ playlist
        if content_type in ("carousel", "story", "highlight"):
            opts["noplaylist"] = False
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=True)
            return info

    try:
        info = await loop.run_in_executor(_executor, _run_ytdlp)
    except Exception as exc:
        err_str = str(exc).lower()
        if any(k in err_str for k in ("private", "login", "not available",
                                       "requires login", "age", "account")):
            raise PrivateContentError("المحتوى خاص أو يتطلب تسجيل دخول")
        if "filesize" in err_str or "too large" in err_str:
            raise FileSizeError(f"الملف تجاوز {MAX_FILE_SIZE_MB}MB")
        raise DownloadError(f"فشل التحميل: {exc}")

    if info:
        title = info.get("title", "") or ""
        # جمع الملفات من entries أو مباشرة
        entries = info.get("entries") or ([info] if info.get("id") else [])
        for entry in entries:
            if not entry:
                continue
            for rd in (entry.get("requested_downloads") or []):
                fp = rd.get("filepath") or rd.get("filename") or ""
                if fp and os.path.exists(fp) and fp not in file_paths:
                    sz = os.path.getsize(fp)
                    if sz <= max_bytes:
                        file_paths.append(fp)
                    else:
                        os.remove(fp)

    # fallback: ابحث بالـ prefix
    if not file_paths:
        file_paths = _collect_files(DOWNLOAD_DIR, prefix, max_bytes)

    if not file_paths:
        raise DownloadError("لم يتم العثور على ملفات بعد التحميل")

    logger.info(f"✅ yt-dlp حمّل {len(file_paths)} ملف | {content_type}")
    return file_paths, title, content_type


# ─── Cleanup ──────────────────────────────────────────────────────────────────

async def cleanup_task():
    while True:
        try:
            now = time.time()
            deleted = 0
            for f in glob.glob(os.path.join(DOWNLOAD_DIR, "*")):
                if now - os.path.getmtime(f) > FILE_MAX_AGE:
                    os.remove(f)
                    deleted += 1
            if deleted:
                logger.info(f"🧹 حُذف {deleted} ملف")
        except Exception as e:
            logger.error(f"cleanup: {e}")
        await asyncio.sleep(CLEANUP_INTERVAL)

async def runtime_state_cleanup_task():
    while True:
        await asyncio.sleep(STATE_CLEANUP_INTERVAL)
        try:
            _cleanup_runtime_state()
        except Exception as e:
            logger.error(f"runtime_state_cleanup: {e}")

# ─── Handlers ─────────────────────────────────────────────────────────────────

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user=update.effective_user; uid=user.id
    if update.effective_chat.type in ("group","supergroup"): return
    if is_banned(uid): await update.message.reply_text("🚫 أنت محظور."); return
    await register_user(user)
    if not await check_force_sub(update,context): return
    await send_welcome(update.message,load_data(),user)

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid=update.effective_user.id
    if update.effective_chat.type!="private": await update.message.reply_text("⚙️ افتح لوحة التحكم في الخاص"); return
    if not can_use_panel(uid): await update.message.reply_text("⛔ ليس لديك صلاحية."); return
    await _show_main_panel(update.message,uid)

async def _show_main_panel(msg,uid,edit=False):
    data=load_data(); uc=len(load_users())
    ad=sum(1 for c in data.get("force_channels",[]) if c.get("active"))
    pc=len(data.get("perm_channels",[])); bc=len(data.get("banned_users",{}))
    ac=len(data.get("admins",{})); tc=len(db.get_tasks())
    rows=[]
    if has_perm(uid,"edit_welcome"): rows.append([InlineKeyboardButton("✏️ تعديل رسالة البداية",callback_data="admin_edit_welcome")])
    if has_perm(uid,"edit_welcome"):
        tk_user = data.get(TIKTOK_BOT_USERNAME_KEY, "غير محدد")
        rows.append([InlineKeyboardButton(f"🤖 بوت تيك توك: @{tk_user}",callback_data="admin_set_tiktok_bot")])
    if has_perm(uid,"edit_welcome"): rows.append([InlineKeyboardButton("📱 تعديل شرح الاستخدام",callback_data="admin_edit_how_to_use")])
    if has_perm(uid,"manage_btns"): rows.append([InlineKeyboardButton("➕ إضافة زر",callback_data="admin_add_button"),InlineKeyboardButton("🗑️ حذف زر",callback_data="admin_del_button")])
    if has_perm(uid,"force_sub"): rows.append([InlineKeyboardButton(f"📢 الاشتراك الإجباري › {ad} نشط",callback_data="admin_force_sub")])
    if has_perm(uid,"perm_channels"): rows.append([InlineKeyboardButton(f"📌 القنوات الثابتة › {pc}",callback_data="admin_perm_channels")])
    if has_perm(uid,"ban_users"): rows.append([InlineKeyboardButton(f"🚫 المحظورون › {bc}",callback_data="admin_ban_panel")])
    if has_perm(uid,"broadcast"): rows.append([InlineKeyboardButton("📣 بث رسالة",callback_data="admin_broadcast")])
    if has_perm(uid,"stats"): rows.append([InlineKeyboardButton("📊 إحصائيات",callback_data="admin_stats")])
    if is_owner(uid): rows.append([InlineKeyboardButton(f"👮 إدارة المشرفين › {ac}",callback_data="admin_admins_panel")])
    rows.append([InlineKeyboardButton("─────────────────────",callback_data="noop")])
    rows.append([InlineKeyboardButton(f"📅 الدوريات › {tc} مهمة",callback_data="sched_main")])
    header=(
        "╔══════════════════════╗\n║   ⚙️  لوحة التحكم   ║\n╚══════════════════════╝\n\n"
        f"👥 المستخدمون:     `{uc}`\n"
        f"📢 اشتراك إجباري: `{ad}` نشط\n"
        f"📌 قنوات ثابتة:   `{pc}`\n"
        f"🚫 محظورون:       `{bc}`\n"
        f"👮 مشرفون:        `{ac}`\n"
        f"📅 مهام دورية:    `{tc}`"
    )
    kb=InlineKeyboardMarkup(rows)
    if edit:
        try: await msg.edit_text(header,parse_mode="Markdown",reply_markup=kb); return
        except Exception: pass
    await msg.reply_text(header,parse_mode="Markdown",reply_markup=kb)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type in ("group", "supergroup"): return
    user = update.effective_user; uid = user.id
    if is_banned(uid): await update.message.reply_text("🚫 أنت محظور."); return
    await register_user(user)
    if can_use_panel(uid) and context.user_data.get("admin_action"):
        await handle_admin_input(update, context); return

    url = update.message.text.strip()

    # TikTok redirect
    if _is_tiktok_url(url):
        data = load_data()
        tiktok_username = data.get(TIKTOK_BOT_USERNAME_KEY, "")
        tiktok_line = f"👉 @{tiktok_username}" if tiktok_username else "👉 تواصل مع الأدمن للحصول على البوت"
        await update.message.reply_text(
            "⚠️ حدث خطأ أثناء تحميل الفيديو\n\n"
            "لتنزيل مقاطع فيديو من TikTok، استخدم برنامجنا الجديد، فهو مجاني تمامًا:\n"
            f"{tiktok_line}",
        )
        return

    if not _is_instagram_url(url):
        await update.message.reply_text(
            "<tg-emoji emoji-id='5334544901428229844'>ℹ️</tg-emoji> يرجى إرسال رابط إنستقرام صحيح "
            "(ريلز، كاروسيل، ستوري، هايلايت).",
            parse_mode="HTML"
        )
        return

    if not _is_valid_instagram_content_url(url):
        await update.message.reply_text(
            "<tg-emoji emoji-id='5334544901428229844'>ℹ️</tg-emoji> هذا رابط بروفايل وليس منشوراً.\n\n"
            "أرسل رابط <b>ريلز</b> أو <b>صورة/كاروسيل</b> أو <b>ستوري</b>.\n\n"
            "مثال:\n"
            "• <code>instagram.com/reel/ABC123</code>\n"
            "• <code>instagram.com/p/ABC123</code>",
            parse_mode="HTML"
        )
        return

    if not await check_force_sub(update, context): return
    if is_rate_limited(uid):
        recent = user_requests.get(uid, [])
        remaining = RATE_LIMIT_WIN - (time.monotonic() - min(recent)) if recent else RATE_LIMIT_WIN
        await update.message.reply_text(f"⏳ كتير! استنى {int(remaining)} ثانية."); return
    if active_downloads.get(uid, 0) >= 2:
        await update.message.reply_text("⏳ عندك تحميلات شغالة."); return

    uname = user.username or str(uid)
    logger.info(f"📥 تحميل | @{uname} ({uid}) | {url}")
    msg = await update.message.reply_text("⏳ جاري التحميل...")
    active_downloads[uid] = active_downloads.get(uid, 0) + 1
    _touch_runtime_state(uid)
    file_paths = []
    try:
        async with semaphore:
            await msg.edit_text(
                "<tg-emoji emoji-id='5256220713882241579'>⬜️</tg-emoji>جاري تحميل المحتوى ...",
                parse_mode="HTML"
            )
            file_paths, video_title, content_type = await _ytdlp_download(url, uid)

        bot_info = await context.bot.get_me()
        bot_username = bot_info.username or ""
        caption = (
            f"<tg-emoji emoji-id='5357438326951389066'>📱</tg-emoji> {video_title}\n\n@{bot_username}"
            if video_title
            else f"<tg-emoji emoji-id='5357438326951389066'>📱</tg-emoji>\n\n@{bot_username}"
        )

        await msg.edit_text(
            "<tg-emoji emoji-id='6334699770845595635'>📤</tg-emoji>جاري الإرسال...",
            parse_mode="HTML"
        )

        if len(file_paths) == 1:
            fp = file_paths[0]
            size_mb = os.path.getsize(fp) / (1024 * 1024)
            if size_mb > MAX_FILE_SIZE_MB:
                await msg.edit_text(f"❌ الملف كبير ({size_mb:.1f}MB)"); return
            ext = os.path.splitext(fp)[1].lower()
            for attempt in range(3):
                try:
                    with open(fp, "rb") as f:
                        if ext in (".jpg", ".jpeg", ".png", ".webp"):
                            await update.message.reply_photo(
                                photo=f, caption=caption, parse_mode="HTML",
                                read_timeout=300, write_timeout=300,
                            )
                        else:
                            await update.message.reply_video(
                                video=f, caption=caption, parse_mode="HTML",
                                read_timeout=300, write_timeout=300,
                            )
                    break
                except TimedOut:
                    if attempt == 2: raise
                    await asyncio.sleep(3)
            logger.info(f"✅ أُرسل | @{uname} | {size_mb:.1f}MB")

        else:
            media_items = []
            for fp in file_paths:
                size_mb = os.path.getsize(fp) / (1024 * 1024)
                if size_mb > MAX_FILE_SIZE_MB:
                    continue
                ext = os.path.splitext(fp)[1].lower()
                if ext in (".jpg", ".jpeg", ".png", ".webp"):
                    media_items.append(("photo", fp))
                else:
                    media_items.append(("video", fp))

            if not media_items:
                await msg.edit_text("❌ جميع الملفات تتجاوز الحد المسموح."); return

            BATCH = 10
            for i in range(0, len(media_items), BATCH):
                batch = media_items[i:i + BATCH]
                group = []
                handles = []
                for idx, (mtype, fp) in enumerate(batch):
                    cap = caption if i == 0 and idx == 0 else None
                    h = open(fp, "rb")
                    handles.append(h)
                    if mtype == "photo":
                        group.append(InputMediaPhoto(media=h, caption=cap, parse_mode="HTML" if cap else None))
                    else:
                        group.append(InputMediaVideo(media=h, caption=cap, parse_mode="HTML" if cap else None))
                try:
                    for attempt in range(3):
                        try:
                            await update.message.reply_media_group(
                                media=group, read_timeout=300, write_timeout=300
                            )
                            break
                        except TimedOut:
                            if attempt == 2: raise
                            await asyncio.sleep(3)
                finally:
                    for h in handles:
                        h.close()
                if i + BATCH < len(media_items):
                    await asyncio.sleep(0.5)

            logger.info(f"✅ أُرسل {len(media_items)} ملف | @{uname}")

        await msg.delete()
        await _update_force_sub_counter(context, uid)

    except (TimedOut, asyncio.TimeoutError):
        logger.error(f"⏱ Timeout أثناء الإرسال | @{uname}")
        await msg.edit_text(
            "<tg-emoji emoji-id='5440660757194744323'>⚠️</tg-emoji> انتهت مهلة الإرسال، الملف كبير أو الاتصال بطيء. حاول مرة ثانية.",
            parse_mode="HTML"
        )
    except PrivateContentError:
        await msg.edit_text(
            "<tg-emoji emoji-id='5440660757194744323'>⚠️</tg-emoji> الحساب خاص أو يتطلب تسجيل دخول.",
            parse_mode="HTML"
        )
    except FileSizeError:
        await msg.edit_text(
            f"<tg-emoji emoji-id='5440660757194744323'>⚠️</tg-emoji> حجم الملف يتجاوز {MAX_FILE_SIZE_MB}MB",
            parse_mode="HTML"
        )
    except DownloadError as e:
        logger.error(f"❌ yt-dlp | @{uname} | {e}")
        await msg.edit_text(
            "<tg-emoji emoji-id='5440660757194744323'>⚠️</tg-emoji> حدث خطأ أثناء التحميل، يرجى المحاولة مرة أخرى.",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"❌ | @{uname} | {e}", exc_info=True)
        await msg.edit_text(
            "<tg-emoji emoji-id='5440660757194744323'>⚠️</tg-emoji> حدث خطأ أثناء التحميل، يرجى المحاولة مرة أخرى.",
            parse_mode="HTML"
        )
    finally:
        current_downloads = active_downloads.get(uid, 0) - 1
        if current_downloads > 0:
            active_downloads[uid] = current_downloads
        else:
            active_downloads.pop(uid, None)
        _touch_runtime_state(uid)
        for fp in file_paths:
            if fp and os.path.exists(fp):
                try: os.remove(fp)
                except Exception: pass

async def _extract_channel(msg,context):
    uname=title=channel_id=None
    if getattr(msg,"forward_origin",None):
        chat=getattr(msg.forward_origin,"chat",None)
        if chat: channel_id=chat.id; title=chat.title or ""; uname=getattr(chat,"username",None)
    elif getattr(msg,"forward_from_chat",None):
        chat=msg.forward_from_chat; channel_id=chat.id; title=chat.title or ""; uname=getattr(chat,"username",None)
    if not channel_id or not uname: return None,None,None
    chat_username=f"@{uname}"; link=f"https://t.me/{uname}"
    try:
        bm=await asyncio.wait_for(context.bot.get_chat_member(chat_username,context.bot.id),timeout=8.0)
        if bm.status not in (ChatMember.ADMINISTRATOR,ChatMember.OWNER): return "NOT_ADMIN",chat_username,link
    except asyncio.TimeoutError: return "ERROR","انتهت مهلة الاتصال",link
    except Exception as e: return "ERROR",str(e),link
    return title or uname,chat_username,link

async def _show_force_sub_panel(msg):
    data=load_data(); chs=data.get("force_channels",[])
    rows=[]
    for i,ch in enumerate(chs):
        icon="🟢" if ch.get("active") else "🔴"; tgt=ch.get("target",0); tgt_s=f"{tgt}" if tgt>0 else "∞"
        rows.append([InlineKeyboardButton(f"{icon} {ch.get('title',ch.get('username','قناة'))}  ({ch.get('added',0)}/{tgt_s})",callback_data=f"adm_del_ch_{i}")])
    rows.append([InlineKeyboardButton("➕ إضافة قناة",callback_data="admin_add_channel")])
    rows.append([InlineKeyboardButton("🔙 رجوع",callback_data="admin_panel")])
    await msg.reply_text("📢 *الاشتراك الإجباري*\n\n• اضغط قناة لحذفها\n• أرسل رسالة محوَّلة بعد ➕",parse_mode="Markdown",reply_markup=InlineKeyboardMarkup(rows))

async def _show_perm_channels_panel(msg):
    data=load_data(); chs=data.get("perm_channels",[])
    rows=[[InlineKeyboardButton(f"📌 {ch.get('title',ch.get('username','قناة'))}  {ch.get('username','')}",callback_data=f"adm_del_perm_{i}")] for i,ch in enumerate(chs)]
    rows.append([InlineKeyboardButton("➕ إضافة قناة ثابتة",callback_data="admin_add_perm_channel")])
    rows.append([InlineKeyboardButton("🔙 رجوع",callback_data="admin_panel")])
    await msg.reply_text("📌 *القنوات الثابتة*\n\n• اضغط قناة لحذفها",parse_mode="Markdown",reply_markup=InlineKeyboardMarkup(rows))

async def _show_ban_panel(msg):
    data=load_data(); banned=data.get("banned_users",{})
    rows=[[InlineKeyboardButton(f"🔓 {info.get('name',bid)}  ({bid})",callback_data=f"unban_{bid}")] for bid,info in list(banned.items())[:20]]
    rows.append([InlineKeyboardButton("➕ حظر بالـ ID",callback_data="ban_add_id")])
    rows.append([InlineKeyboardButton("🔙 رجوع",callback_data="admin_panel")])
    await msg.reply_text(f"🚫 *المحظورون*  ({len(banned)})\n\nاضغط للرفع:",parse_mode="Markdown",reply_markup=InlineKeyboardMarkup(rows))

async def _show_admins_panel(msg):
    data=load_data(); admins=data.get("admins",{})
    rows=[[InlineKeyboardButton(f"👮 {info.get('name',aid)}  ({aid})",callback_data=f"admin_view_{aid}")] for aid,info in admins.items()]
    rows.append([InlineKeyboardButton("➕ إضافة مشرف بالـ ID",callback_data="add_admin_id")])
    rows.append([InlineKeyboardButton("🔙 رجوع",callback_data="admin_panel")])
    await msg.reply_text(f"👮 *إدارة المشرفين*  ({len(admins)})",parse_mode="Markdown",reply_markup=InlineKeyboardMarkup(rows))

async def _show_admin_perms(msg,admin_id):
    data=load_data(); admin=data.get("admins",{}).get(admin_id,{}); perms=admin.get("permissions",{})
    rows=[[InlineKeyboardButton(f"{'✅' if perms.get(pk) else '❌'} {plabel}",callback_data=f"tperm_{admin_id}_{pk}")] for pk,plabel in ALL_PERMISSIONS.items()]
    rows.append([InlineKeyboardButton("🗑️ حذف المشرف",callback_data=f"del_admin_{admin_id}")])
    rows.append([InlineKeyboardButton("🔙 رجوع للمشرفين",callback_data="admin_admins_panel")])
    await msg.reply_text(f"👮 *صلاحيات:* {admin.get('name',admin_id)}\n`{admin_id}`",parse_mode="Markdown",reply_markup=InlineKeyboardMarkup(rows))

async def handle_admin_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid=update.effective_user.id; action=context.user_data.get("admin_action"); msg=update.message

    if action=="set_tiktok_bot" and has_perm(uid,"edit_welcome"):
        context.user_data.pop("admin_action", None)
        raw = (msg.text or "").strip().lstrip("@")
        if not raw:
            await msg.reply_text("❌ يوزر فاضي، حاول مرة ثانية.")
            return
        data = load_data()
        data[TIKTOK_BOT_USERNAME_KEY] = raw
        await save_data_locked(data)
        await msg.reply_text(f"✅ تم تعيين بوت تيك توك: @{raw}")
        return

    if action=="edit_welcome" and has_perm(uid,"edit_welcome"):
        data=load_data(); data["welcome_text"]=msg.text or ""; data["welcome_entities"]=entities_to_json(msg.entities or msg.caption_entities)
        await save_data_locked(data); await msg.reply_text("✅ تم تحديث رسالة البداية!"); context.user_data.pop("admin_action",None)

    elif action=="edit_how_to_use" and has_perm(uid,"edit_welcome"):
        data=load_data()
        data["how_to_use_text"] = msg.text or ""
        data["how_to_use_entities"] = entities_to_json(msg.entities or [])
        await save_data_locked(data); await msg.reply_text("✅ تم تحديث شرح الاستخدام!"); context.user_data.pop("admin_action",None)

    elif action=="add_btn_name" and has_perm(uid,"manage_btns"):
        raw=msg.text or ""; ents=msg.entities or []; emoji_id=None
        for e in ents:
            if e.type==MessageEntity.CUSTOM_EMOJI and e.custom_emoji_id: emoji_id=str(e.custom_emoji_id); break
        if emoji_id:
            tb=raw.encode("utf-16-le"); res=bytearray(); prev=0
            for e in ents:
                if e.type==MessageEntity.CUSTOM_EMOJI: res+=tb[prev*2:e.offset*2]; prev=e.offset+e.length
            res+=tb[prev*2:]; text=res.decode("utf-16-le").strip()
        else: text=raw
        context.user_data.update({"new_btn_text":text,"new_btn_emoji":emoji_id,"admin_action":None})
        await msg.reply_text("🔧 اختر نوع الزر:",reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔗 رابط",callback_data="set_btn_type_url")],
            [InlineKeyboardButton("📋 نسخ",callback_data="set_btn_type_copy")],
            [InlineKeyboardButton("⚡ Callback",callback_data="set_btn_type_callback")]]))

    elif action=="add_btn_value" and has_perm(uid,"manage_btns"):
        context.user_data["new_btn_value"]=msg.text; context.user_data["admin_action"]=None
        await msg.reply_text("🎨 اختر لون الزر:",reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton(lbl,callback_data=f"set_btn_style_{st or 'none'}")] for lbl,st in BTN_STYLES.items()]))

    elif action=="broadcast" and has_perm(uid,"broadcast"):
        context.user_data.pop("admin_action",None)
        users=load_users(); sent=failed=0
        total=len(users)
        prg=await msg.reply_text(f"📣 جاري البث لـ {total} مستخدم...")
        text=msg.text or msg.caption or ""; ents=list(msg.entities or msg.caption_entities or [])
        NEED_HTML={"custom_emoji","text_link","text_mention","spoiler"}; needs_html=any(e.type in NEED_HTML for e in ents)
        BATCH_SIZE=30; user_list=list(users.keys()); processed=0
        for i in range(0,len(user_list),BATCH_SIZE):
            batch_uids=user_list[i:i+BATCH_SIZE]
            async def _send_one(us):
                try:
                    if needs_html and text:
                        await _send_with_retry(
                            lambda: context.bot.send_message(int(us),entities_to_html(text,ents),parse_mode="HTML"),
                            log_label=f"broadcast {us}",
                        )
                    else:
                        await _send_with_retry(
                            lambda us=us: context.bot.copy_message(chat_id=int(us),from_chat_id=msg.chat_id,message_id=msg.message_id),
                            log_label=f"broadcast {us}",
                        )
                    return True
                except Forbidden: return False
                except Exception as ex: logger.warning(f"broadcast fail {us}: {ex}"); return False
            results=await asyncio.gather(*[_send_one(u) for u in batch_uids],return_exceptions=True)
            for r in results:
                if r is True: sent+=1
                else: failed+=1
            processed+=len(batch_uids)
            if processed%100==0 or processed==total:
                try: await prg.edit_text(f"📣 جاري البث...\n✅ {sent} | ❌ {failed} | 📊 {processed}/{total}")
                except Exception: pass
            await asyncio.sleep(0.5)
        await prg.edit_text(f"✅ *تم البث!*\n\n📤 أُرسل: `{sent}`\n❌ فشل: `{failed}`",parse_mode="Markdown")

    elif action=="add_channel_forward" and has_perm(uid,"force_sub"):
        title,username,link=await _extract_channel(msg,context)
        if   title=="NOT_ADMIN": await msg.reply_text(f"⚠️ البوت ليس أدمن في {username}!"); return
        elif title=="ERROR":     await msg.reply_text(f"⚠️ خطأ: {username}"); return
        elif not title:          await msg.reply_text("❌ لم أتعرف على القناة.\n• أرسل رسالة محوَّلة من قناة عامة"); return
        context.user_data.update({"new_channel_link":link,"new_channel_username":username,"new_channel_title":title,"admin_action":"add_channel_target"})
        await msg.reply_text(f"✅ *{title}*  `{username}`\n\n🔢 أرسل عدد الأعضاء كهدف أو اضغط «بدون هدف»:",parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🎯 بدون هدف (دائم)",callback_data="channel_target_0")]]))

    elif action=="add_channel_target" and has_perm(uid,"force_sub"):
        try: target=int((msg.text or "").strip()); assert target>=0
        except (ValueError,AssertionError): await msg.reply_text("❌ أرسل رقم ≥ 0"); return
        await _save_force_channel(msg,context,target)

    elif action=="add_perm_channel_forward" and has_perm(uid,"perm_channels"):
        title,username,link=await _extract_channel(msg,context)
        if   title=="NOT_ADMIN": await msg.reply_text(f"⚠️ البوت ليس أدمن في {username}!"); return
        elif title=="ERROR":     await msg.reply_text(f"⚠️ خطأ: {username}"); return
        elif not title:          await msg.reply_text("❌ لم أتعرف على القناة."); return
        context.user_data.pop("admin_action",None)
        data=load_data(); data.setdefault("perm_channels",[]).append({"link":link,"username":username,"title":title})
        await save_data_locked(data); await msg.reply_text(f"📌 تمت الإضافة!\n*{title}*  `{username}`",parse_mode="Markdown")

    elif action=="ban_by_id" and has_perm(uid,"ban_users"):
        context.user_data.pop("admin_action",None)
        try: target_id=int((msg.text or "").strip())
        except ValueError: await msg.reply_text("❌ أرسل ID رقمي صحيح."); return
        data=load_data()
        data.setdefault("banned_users",{})[str(target_id)]={"name":str(target_id),"banned_at":time.strftime("%Y-%m-%d %H:%M"),"banned_by":uid}
        await save_data_locked(data); await msg.reply_text(f"🚫 تم حظر `{target_id}`!",parse_mode="Markdown")

    elif action=="add_admin_by_id" and is_owner(uid):
        context.user_data.pop("admin_action",None)
        try: admin_id=int((msg.text or "").strip())
        except ValueError: await msg.reply_text("❌ أرسل ID رقمي صحيح."); return
        if admin_id in OWNER_IDS: await msg.reply_text("⚠️ هذا المستخدم مالك بالفعل."); return
        data=load_data()
        data.setdefault("admins",{})[str(admin_id)]={"name":str(admin_id),"added_at":time.strftime("%Y-%m-%d %H:%M"),
            "added_by":uid,"permissions":{p:False for p in ALL_PERMISSIONS}}
        await save_data_locked(data); await msg.reply_text(f"✅ تم إضافة المشرف `{admin_id}`",parse_mode="Markdown")

async def _save_force_channel(msg,context,target):
    link=context.user_data.pop("new_channel_link",""); uname=context.user_data.pop("new_channel_username","")
    title=context.user_data.pop("new_channel_title",uname); context.user_data.pop("admin_action",None)
    data=load_data()
    data.setdefault("force_channels",[]).append({"link":link,"username":uname,"title":title,"target":target,"added":0,"active":True})
    await save_data_locked(data); tgt_s=f"`{target}` عضو" if target>0 else "دائم (∞)"
    await msg.reply_text(f"✅ تمت إضافة القناة!\n📢 *{title}*  {uname}\n🎯 الهدف: {tgt_s}",parse_mode="Markdown")

async def _finish_add_button(target,context):
    new_btn={"text":context.user_data.get("new_btn_text","زر"),"type":context.user_data.get("new_btn_type","callback"),
              "value":context.user_data.get("new_btn_value","noop"),"style":context.user_data.get("new_btn_style"),"emoji_id":context.user_data.get("new_btn_emoji")}
    if new_btn["style"] not in ALLOWED_STYLES: new_btn["style"]=None
    eid=str(new_btn["emoji_id"]) if new_btn["emoji_id"] else None; new_btn["emoji_id"]=eid if (eid and eid.isdigit()) else None
    data=load_data(); data.setdefault("welcome_buttons",[]).append(new_btn); await save_data_locked(data)
    await target.reply_text(f"✅ تم إضافة الزر: {new_btn['text']}")
    for k in ["new_btn_text","new_btn_type","new_btn_value","new_btn_style","new_btn_emoji","admin_action"]: context.user_data.pop(k,None)

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query
    try: await q.answer()
    except Exception: pass
    d=q.data; uid=q.from_user.id
    if d=="noop": return

    if d==HOW_TO_USE_CALLBACK:
        data=load_data()
        how_text = data.get("how_to_use_text", "لم يتم إعداد شرح الاستخدام بعد.")
        how_ents = json_to_entities(data.get("how_to_use_entities"))
        back_btn = _SafeButton("العودة للرئيسية",callback_data="how_to_use_back",style="primary",icon_custom_emoji_id="5352759161945867747")
        kb = InlineKeyboardMarkup([[back_btn]])
        try:
            await q.message.edit_text(how_text, entities=how_ents, reply_markup=kb)
        except Exception:
            await q.message.reply_text(how_text, entities=how_ents, reply_markup=kb)
        return

    if d=="how_to_use_back":
        data=load_data()
        raw_text=data.get("welcome_text",""); raw_ents=data.get("welcome_entities"); kb=build_welcome_keyboard(data)
        try:
            if raw_ents:
                text,ents=_build_welcome(raw_text,raw_ents,q.from_user)
                await q.message.edit_text(text, entities=ents, reply_markup=kb)
            else:
                mention=f"[{q.from_user.first_name or 'صديقي'}](tg://user?id={q.from_user.id})"
                text=raw_text.replace("#userlink",mention)
                await q.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)
        except Exception:
            pass
        return

    if d=="check_sub":
        msg_id=context.user_data.pop("force_sub_msg_id",None); chat_id=context.user_data.pop("force_sub_chat_id",None)
        if await check_force_sub(update,context):
            try:
                if msg_id and chat_id: await context.bot.delete_message(chat_id=chat_id,message_id=msg_id)
            except Exception: pass
            await q.message.reply_text("✅ تم التحقق! أرسل رابط إنستقرام الآن.")
        return

    if d.startswith("popup_"):
        try:
            bid=int(d[len("popup_"):])
            with db._conn() as conn:
                cur=conn.cursor(); cur.execute("SELECT btn_content FROM buttons WHERE id=?",(bid,)); row=cur.fetchone()
            if row and row["btn_content"]: await q.answer(row["btn_content"],show_alert=True)
            else: await q.answer("—")
        except Exception: await q.answer()
        return

    if not can_use_panel(uid): await q.answer("❌ غير مصرح",show_alert=True); return
    if q.message.chat.type!="private": await q.answer("⚙️ افتح لوحة التحكم في الخاص",show_alert=True); return

    if d in ("admin_panel","back_to_main"): await _show_main_panel(q.message,uid,edit=True); return

    if d=="admin_stats" and has_perm(uid,"stats"):
        data=load_data(); users=load_users(); tasks=db.get_tasks()
        lines=["📊 *الإحصائيات*\n",f"👥 المستخدمون:  `{len(users)}`",
               f"🚫 المحظورون:   `{len(data.get('banned_users',{}))}`",
               f"👮 المشرفون:    `{len(data.get('admins',{}))}`",
               f"📅 مهام الدوريات: `{len(tasks)}`\n","📢 *قنوات الاشتراك:*"]
        for ch in data.get("force_channels",[]):
            st="🟢" if ch.get("active") else "🔴"; tgt=ch.get("target",0)
            lines.append(f"  • {ch.get('title',ch.get('username'))} {st} ({ch.get('added',0)}/{'∞' if not tgt else tgt})")
        lines.append("\n📌 *القنوات الثابتة:*")
        for ch in data.get("perm_channels",[]): lines.append(f"  • {ch.get('title',ch.get('username'))}")
        await q.message.reply_text("\n".join(lines),parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 رجوع",callback_data="admin_panel")]])); return

    if d=="admin_set_tiktok_bot" and has_perm(uid,"edit_welcome"):
        data=load_data()
        cur=data.get(TIKTOK_BOT_USERNAME_KEY,"غير محدد")
        await q.message.reply_text(f"🤖 اليوزر الحالي لبوت تيك توك: @{cur}\n\nأرسل اليوزر الجديد (بدون @):")
        context.user_data["admin_action"]="set_tiktok_bot"; return

    if d=="admin_edit_welcome" and has_perm(uid,"edit_welcome"):
        await q.message.reply_text("✏️ أرسل رسالة البداية الجديدة.\n💡 استخدم `#userlink` لاسم المستخدم.",parse_mode="Markdown")
        context.user_data["admin_action"]="edit_welcome"; return
    if d=="admin_edit_how_to_use" and has_perm(uid,"edit_welcome"):
        data=load_data()
        current=data.get("how_to_use_text","")
        await q.message.reply_text(f"📱 أرسل نص شرح الاستخدام الجديد:\n\nالحالي:\n{current[:200]}{'...' if len(current)>200 else ''}")
        context.user_data["admin_action"]="edit_how_to_use"; return
    if d=="admin_add_button" and has_perm(uid,"manage_btns"):
        await q.message.reply_text("🔤 أرسل اسم الزر:"); context.user_data["admin_action"]="add_btn_name"; return
    if d.startswith("set_btn_type_"):
        context.user_data["new_btn_type"]=d[len("set_btn_type_"):]
        prompts={"url":"🔗 أرسل رابط URL:","copy":"📋 أرسل نص النسخ:","callback":"⚡ أرسل Callback Data:"}
        await q.message.reply_text(prompts.get(context.user_data["new_btn_type"],"أرسل القيمة:"))
        context.user_data["admin_action"]="add_btn_value"; return
    if d.startswith("set_btn_style_"):
        raw=d[len("set_btn_style_"):]; context.user_data["new_btn_style"]=None if raw=="none" else raw
        await _finish_add_button(q.message,context); return
    if d=="admin_del_button" and has_perm(uid,"manage_btns"):
        btns=[b for b in load_data().get("welcome_buttons",[]) if isinstance(b,dict)]
        if not btns: await q.message.reply_text("لا توجد أزرار."); return
        await q.message.reply_text("اختر الزر للحذف:",reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton(f"🗑️ {b['text']}",callback_data=f"del_btn_{i}")] for i,b in enumerate(btns)])); return
    if d.startswith("del_btn_"):
        idx=int(d[len("del_btn_"):]); data=load_data()
        if 0<=idx<len(data.get("welcome_buttons",[])):
            removed=data["welcome_buttons"].pop(idx)
            await save_data_locked(data)
            await q.message.edit_text(f"🗑️ تم حذف: {removed.get('text','')}")
            return
    if d=="admin_broadcast" and has_perm(uid,"broadcast"):
        await q.message.reply_text("📣 أرسل الرسالة للبث:"); context.user_data["admin_action"]="broadcast"; return
    if d=="admin_force_sub" and has_perm(uid,"force_sub"): await _show_force_sub_panel(q.message); return
    if d=="admin_add_channel" and has_perm(uid,"force_sub"):
        await q.message.reply_text("📢 أرسل رسالة *محوَّلة* من القناة:\n⚠️ تأكد أن البوت أدمن فيها أولاً",parse_mode="Markdown")
        context.user_data["admin_action"]="add_channel_forward"; return
    if d.startswith("channel_target_"):
        await _save_force_channel(q.message,context,int(d[len("channel_target_"):])); return
    if d.startswith("adm_del_ch_"):
        idx=int(d[len("adm_del_ch_"):]); data=load_data(); chs=data.get("force_channels",[])
        if 0<=idx<len(chs):
            removed=chs.pop(idx)
            await save_data_locked(data)
            await q.message.edit_text(f"🗑️ تم حذف: {removed.get('title',removed.get('username',''))}")
            return
    if d=="admin_perm_channels" and has_perm(uid,"perm_channels"): await _show_perm_channels_panel(q.message); return
    if d=="admin_add_perm_channel" and has_perm(uid,"perm_channels"):
        await q.message.reply_text("📌 أرسل رسالة *محوَّلة* من القناة:",parse_mode="Markdown")
        context.user_data["admin_action"]="add_perm_channel_forward"; return
    if d.startswith("adm_del_perm_"):
        idx=int(d[len("adm_del_perm_"):]); data=load_data(); chs=data.get("perm_channels",[])
        if 0<=idx<len(chs):
            removed=chs.pop(idx)
            await save_data_locked(data)
            await q.message.edit_text(f"🗑️ تم حذف: {removed.get('title',removed.get('username',''))}")
            return
    if d=="admin_ban_panel" and has_perm(uid,"ban_users"): await _show_ban_panel(q.message); return
    if d=="ban_add_id" and has_perm(uid,"ban_users"):
        await q.message.reply_text("🚫 أرسل الـ ID الرقمي:"); context.user_data["admin_action"]="ban_by_id"; return
    if d.startswith("unban_") and has_perm(uid,"ban_users"):
        tid=d[len("unban_"):]; data=load_data()
        if tid in data.get("banned_users",{}):
            del data["banned_users"][tid]
            await save_data_locked(data)
            await q.message.edit_text(f"✅ تم رفع الحظر عن `{tid}`",parse_mode="Markdown")
        else: await q.answer("غير موجود.",show_alert=True)
        return
    if d=="admin_admins_panel" and is_owner(uid): await _show_admins_panel(q.message); return
    if d=="add_admin_id" and is_owner(uid):
        await q.message.reply_text("👮 أرسل الـ ID الرقمي للمشرف الجديد:"); context.user_data["admin_action"]="add_admin_by_id"; return
    if d.startswith("admin_view_") and is_owner(uid): await _show_admin_perms(q.message,d[len("admin_view_"):]); return
    if d.startswith("tperm_") and is_owner(uid):
        rest=d[len("tperm_"):]; admin_id=perm_key=None
        for pk in ALL_PERMISSIONS:
            if rest.endswith(f"_{pk}"): perm_key=pk; admin_id=rest[:-len(pk)-1]; break
        if not admin_id or not perm_key: await q.answer("خطأ.",show_alert=True); return
        data=load_data(); admin=data.get("admins",{}).get(admin_id)
        if not admin: await q.answer("المشرف غير موجود.",show_alert=True); return
        admin.setdefault("permissions",{}); admin["permissions"][perm_key]=not admin["permissions"].get(perm_key,False)
        await save_data_locked(data); await _show_admin_perms(q.message,admin_id); return
    if d.startswith("del_admin_") and is_owner(uid):
        aid=d[len("del_admin_"):]; data=load_data()
        if aid in data.get("admins",{}):
            del data["admins"][aid]
            await save_data_locked(data)
            await q.message.edit_text(f"🗑️ تم حذف المشرف `{aid}`",parse_mode="Markdown")
        else: await q.answer("المشرف غير موجود.",show_alert=True)
        return

    await _sched_callback(update,context,q,d,uid)

def _ttn(tt): return {"recurring":"🔁 رسائل دورية","scheduled":"⏰ رسائل مجدولة","weekly":"📅 رسائل أسبوعية"}.get(tt,tt)
def _tprev(t):
    c=t.get('text_content') or t.get('caption') or ""
    if not c: return {"photo":"🖼","video":"🎥","animation":"🎞","document":"📄","audio":"🎵","media_group":"🗂","sticker":"🎭"}.get(t.get('content_type',''),"رسالة")
    return " ".join(c.split()[:2])[:20] or "رسالة"
def _tinf(t):
    if t['task_type']=='recurring': return f"كل {t['interval_minutes']} د"
    elif t['task_type']=='scheduled': return t['schedule_time']
    elif t['task_type']=='weekly': return f"{t['schedule_days']} - {t['schedule_time']}"
    return "مهمة"
def _clr(v): return {"#r":"danger","#g":"success","#b":"primary","#w":"secondary"}.get(v.lower().strip(),"secondary")

def _build_task_keyboard(buttons_data):
    if not buttons_data: return None
    rows={}
    for btn in buttons_data:
        r=btn['row_number']
        if r not in rows: rows[r]=[]
        style=btn.get('style','secondary'); eid=btn.get('emoji_id')
        api_kw={}
        if style and style!='secondary': api_kw['style']=style
        if eid and str(eid).isdigit(): api_kw['icon_custom_emoji_id']=eid
        kw={"api_kwargs":api_kw} if api_kw else {}
        bt=btn.get('btn_type','url')
        if bt=='popup': ib=InlineKeyboardButton(btn['text'],callback_data=f"popup_{btn['id']}",**kw)
        elif bt=='copy': ib=InlineKeyboardButton(btn['text'],copy_text=CopyTextButton(btn.get('btn_content') or btn['url']),**kw)
        else: ib=InlineKeyboardButton(btn['text'],url=btn['url'],**kw)
        rows[r].append(ib)
    return InlineKeyboardMarkup([rows[r] for r in sorted(rows)])

async def _sched_callback(update,context,q,d,uid):
    if d=="sched_main": await _show_sched_main(q.message); return
    if d.startswith("sched_list_"): await _show_tasks_list(q.message,d[len("sched_list_"):]); return
    if d.startswith("sched_task_"): await _show_task_details(q.message,context,int(d[len("sched_task_"):])); return
    if d.startswith("sched_toggle_") and not d.startswith("sched_toggle_del_"):
        tid=int(d[len("sched_toggle_"):]); ns=db.toggle_task(tid)
        if ns==1: await schedule_task(context.application,tid)
        else:
            for job in context.application.job_queue.get_jobs_by_name(f"task_{tid}"): job.schedule_removal()
        await q.answer("✅ تفعيل" if ns else "⏸ تعطيل"); await _show_task_details(q.message,context,tid); return
    if d.startswith("sched_toggle_del_"):
        tid=int(d[len("sched_toggle_del_"):]); task=db.get_task(tid)
        if task: db.update_task(tid,{'delete_after_send':0 if task.get('delete_after_send') else 1})
        await _show_task_details(q.message,context,tid); return
    if d.startswith("sched_btns_") and not d.startswith(("sched_btns_yes","sched_btns_no")):
        await _show_buttons_manager(q.message,int(d[len("sched_btns_"):])); return
    if d.startswith("sched_del_btn_"):
        tid=db.delete_button(int(d[len("sched_del_btn_"):]))
        if tid: await _show_buttons_manager(q.message,tid)
        return
    if d.startswith("sched_pos_"):
        tid=int(d[len("sched_pos_"):]); task=db.get_task(tid)
        if task: db.update_task(tid,{'caption_position':'above' if task['caption_position']=='below' else 'below'})
        await _show_task_details(q.message,context,tid); return
    if d.startswith("sched_del_"):
        tid=int(d[len("sched_del_"):]); task=db.get_task(tid)
        if task:
            for job in context.application.job_queue.get_jobs_by_name(f"task_{tid}"): job.schedule_removal()
            db.delete_task(tid)
        await q.answer("🗑 تم الحذف"); await _show_sched_main(q.message); return
    if d.startswith("sched_delall_"):
        tt=d[len("sched_delall_"):]
        for t in db.get_tasks(tt):
            for job in context.application.job_queue.get_jobs_by_name(f"task_{t['id']}"): job.schedule_removal()
            db.delete_task(t['id'])
        await q.answer("🗑 تم حذف الكل"); await _show_sched_main(q.message); return

async def _show_sched_main(msg):
    tasks=db.get_all_tasks()
    rc=sum(1 for t in tasks if t['task_type']=='recurring')
    sc=sum(1 for t in tasks if t['task_type']=='scheduled')
    wc=sum(1 for t in tasks if t['task_type']=='weekly')
    kb=InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🔁 دورية ({rc})",callback_data="sched_list_recurring"),
         InlineKeyboardButton(f"⏰ مجدولة ({sc})",callback_data="sched_list_scheduled")],
        [InlineKeyboardButton(f"📅 أسبوعية ({wc})",callback_data="sched_list_weekly")],
        [InlineKeyboardButton("➕ إضافة دورية",callback_data="sched_add_recurring"),
         InlineKeyboardButton("➕ إضافة مجدولة",callback_data="sched_add_scheduled")],
        [InlineKeyboardButton("➕ إضافة أسبوعية",callback_data="sched_add_weekly")],
        [InlineKeyboardButton("🔙 لوحة التحكم",callback_data="admin_panel")],
    ])
    await msg.reply_text(
        f"📅 *لوحة الدوريات*\n\n👥 المستخدمون: `{len(load_users())}`\n"
        f"🔁 دورية: `{rc}` | ⏰ مجدولة: `{sc}` | 📅 أسبوعية: `{wc}`\n\n"
        "📌 الرسائل تُرسَل في الخاص لكل المستخدمين",
        parse_mode="Markdown",reply_markup=kb)

async def _show_tasks_list(msg,task_type):
    tasks=db.get_tasks(task_type)
    if not tasks:
        await msg.reply_text(f"📭 لا توجد {_ttn(task_type)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 رجوع",callback_data="sched_main")]])); return
    rows=[[InlineKeyboardButton(f"{'✅' if t['active'] else '⏸'} {_tprev(t)} | {_tinf(t)}",callback_data=f"sched_task_{t['id']}")] for t in tasks]
    rows.append([InlineKeyboardButton("🗑 حذف الكل",callback_data=f"sched_delall_{task_type}")])
    rows.append([InlineKeyboardButton("🔙 رجوع",callback_data="sched_main")])
    await msg.reply_text(f"📋 {_ttn(task_type)}",reply_markup=InlineKeyboardMarkup(rows))

async def _show_task_details(msg,context,task_id):
    task=db.get_task(task_id)
    if not task: await msg.reply_text("❌ المهمة غير موجودة"); return
    status="✅ مفعلة" if task['active'] else "⏸ معطلة"
    text=f"📝 مهمة #{task['id']}\n\n{status}\n{_ttn(task['task_type'])}\n🎯 {task['content_type']}\n"
    if task['task_type']=='recurring': text+=f"⏱ كل {task['interval_minutes']} دقيقة\n"
    elif task['task_type']=='scheduled': text+=f"🕐 {task['schedule_time']}\n"
    elif task['task_type']=='weekly': text+=f"📅 {task['schedule_days']}\n🕐 {task['schedule_time']}\n"
    if task.get('caption'): text+=f"\n📝 {task['caption'][:40]}{'...' if len(task['caption'])>40 else ''}\n"
    if task.get('last_sent'): text+=f"\n⏰ آخر إرسال: {task['last_sent'][:19]}\n"
    text+=f"🔄 حذف القديمة: {'🗑 مفعّل' if task.get('delete_after_send') else '🗑 معطّل'}\n"
    btns=db.get_buttons(task_id)
    if btns: text+=f"\n🔘 أزرار: {len(btns)}\n"
    toggle="⏸ تعطيل" if task['active'] else "✅ تفعيل"
    del_toggle="🔄 إيقاف الحذف التلقائي" if task.get('delete_after_send') else "🔄 تفعيل الحذف التلقائي"
    keyboard=[
        [InlineKeyboardButton(toggle,callback_data=f"sched_toggle_{task_id}")],
        [InlineKeyboardButton(del_toggle,callback_data=f"sched_toggle_del_{task_id}")],
        [InlineKeyboardButton("🔘 إدارة الأزرار",callback_data=f"sched_btns_{task_id}")],
    ]
    if task.get('caption') and task['content_type'] in ['photo','video','animation']:
        pos="⬆️ فوق" if task['caption_position']=='above' else "⬇️ تحت"
        keyboard.append([InlineKeyboardButton(f"📍 الموضع: {pos}",callback_data=f"sched_pos_{task_id}")])
    keyboard.append([InlineKeyboardButton("🗑 حذف المهمة",callback_data=f"sched_del_{task_id}")])
    keyboard.append([InlineKeyboardButton("🔙 رجوع",callback_data=f"sched_list_{task['task_type']}")])
    await msg.reply_text(text,reply_markup=InlineKeyboardMarkup(keyboard))

async def _show_buttons_manager(msg,task_id):
    buttons=db.get_buttons(task_id)
    text=f"🔘 إدارة أزرار المهمة #{task_id}\n\n"
    for btn in buttons: text+=f"▪️ {btn['text']}  ({btn.get('btn_type','url')})\n"
    if not buttons: text+="لا توجد أزرار\n"
    rows=[[InlineKeyboardButton(f"🗑 {btn['text']}",callback_data=f"sched_del_btn_{btn['id']}")] for btn in buttons]
    rows.append([InlineKeyboardButton("➕ إضافة زر",callback_data=f"sched_add_btn_{task_id}")])
    rows.append([InlineKeyboardButton("🔙 رجوع",callback_data=f"sched_task_{task_id}")])
    await msg.reply_text(text,reply_markup=InlineKeyboardMarkup(rows))

async def _sched_clear(ctx):
    for k in list(ctx.user_data.keys()):
        if k.startswith('sc_'): ctx.user_data.pop(k,None)

async def sched_start_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    if q.from_user.id not in OWNER_IDS: await q.answer("❌ للمالك فقط",show_alert=True); return ConversationHandler.END
    tt=q.data[len("sched_add_"):]; await _sched_clear(context)
    context.user_data['sc_tt']=tt; context.user_data['sc_btns']=[]
    await q.message.reply_text(f"📝 أرسل الرسالة لـ {_ttn(tt)}\n\nيمكنك إرسال: نص، صورة، فيديو، ملف، صوت، ألبوم\n\n/cancel للإلغاء")
    return S_MSG

async def sched_add_btn_to_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    context.user_data['sc_editing_task']=int(q.data[len("sched_add_btn_"):])
    await q.message.reply_text("🔤 أرسل نص الزر:\n/cancel للإلغاء"); return S_BTN_TEXT

async def sched_receive_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg=update.message
    if msg.media_group_id:
        mgid=msg.media_group_id
        if 'sc_album_id' not in context.user_data:
            context.user_data['sc_album_id']=mgid; context.user_data['sc_album_items']=[]
        item={}
        if msg.photo: item={'type':'photo','file_id':msg.photo[-1].file_id,'caption':msg.caption,'cap_ent':json.dumps([e.to_dict() for e in msg.caption_entities]) if msg.caption_entities else None}
        elif msg.video: item={'type':'video','file_id':msg.video.file_id,'caption':msg.caption,'cap_ent':json.dumps([e.to_dict() for e in msg.caption_entities]) if msg.caption_entities else None}
        elif msg.document: item={'type':'document','file_id':msg.document.file_id,'caption':msg.caption,'cap_ent':json.dumps([e.to_dict() for e in msg.caption_entities]) if msg.caption_entities else None}
        if item: context.user_data['sc_album_items'].append(item)
        if 'sc_album_job' in context.user_data: context.user_data['sc_album_job'].schedule_removal()
        job=context.application.job_queue.run_once(_sched_process_album,when=2.0,
            data={'chat_id':msg.chat_id,'user_id':msg.from_user.id},name=f"sched_album_{mgid}")
        context.user_data['sc_album_job']=job; context.user_data['sc_ct']='media_group'; return S_MSG

    def _save(ct,fid=None,txt=None,cap=None,ents=None):
        context.user_data['sc_ct']=ct
        if fid: context.user_data['sc_fid']=fid
        if txt: context.user_data['sc_txt']=txt
        if cap: context.user_data['sc_cap']=cap
        if ents: context.user_data['sc_ent']=json.dumps([e.to_dict() for e in ents])

    if msg.text:
        _save('text',txt=msg.text)
        if msg.entities: context.user_data['sc_ent']=json.dumps([e.to_dict() for e in msg.entities])
    elif msg.photo:     _save('photo',    fid=msg.photo[-1].file_id,cap=msg.caption,ents=msg.caption_entities)
    elif msg.video:     _save('video',    fid=msg.video.file_id,    cap=msg.caption,ents=msg.caption_entities)
    elif msg.animation: _save('animation',fid=msg.animation.file_id,cap=msg.caption,ents=msg.caption_entities)
    elif msg.document:  _save('document', fid=msg.document.file_id, cap=msg.caption,ents=msg.caption_entities)
    elif msg.audio:     _save('audio',    fid=msg.audio.file_id,    cap=msg.caption,ents=msg.caption_entities)
    elif msg.sticker:   _save('sticker',  fid=msg.sticker.file_id)
    else: await msg.reply_text("❌ نوع غير مدعوم"); return S_MSG

    ct=context.user_data.get('sc_ct')
    if context.user_data.get('sc_cap') and ct in ['photo','video','animation']:
        kb=InlineKeyboardMarkup([[InlineKeyboardButton("⬆️ فوق",callback_data="sc_cap_above")],[InlineKeyboardButton("⬇️ تحت",callback_data="sc_cap_below")]])
        await msg.reply_text("📍 أين تريد النص؟",reply_markup=kb); return S_CAP_POS
    return await _sched_ask_schedule(update,context)

async def _sched_process_album(context: ContextTypes.DEFAULT_TYPE):
    jd=context.job.data; cid=jd['chat_id']; uid=jd['user_id']
    ud=context.application.user_data.get(uid)
    if not ud or 'sc_album_items' not in ud: return
    items=ud['sc_album_items']
    if not items: await context.bot.send_message(cid,"❌ لم يتم استلام ألبوم"); return
    await context.bot.send_message(cid,f"✅ تم استلام الألبوم ({len(items)} عنصر)")
    try:
        mg=[]
        for i in items[:10]:
            if i['type']=='photo': mg.append(InputMediaPhoto(media=i['file_id'],caption=i.get('caption','')[:1024] if not mg else None))
            elif i['type']=='video': mg.append(InputMediaVideo(media=i['file_id'],caption=i.get('caption','')[:1024] if not mg else None))
        if mg: await context.bot.send_media_group(cid,mg)
    except Exception as e: logger.error(f"album preview: {e}")
    await context.bot.send_message(cid,"📅 هل تريد المتابعة؟",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ متابعة للجدولة",callback_data="sc_album_continue")]]))

async def sched_handle_cap_pos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    context.user_data['sc_cap_pos']='above' if q.data=='sc_cap_above' else 'below'
    await q.message.reply_text("✅ "+(("⬆️ فوق") if context.user_data['sc_cap_pos']=='above' else "⬇️ تحت"))
    return await _sched_ask_schedule(update,context)

async def sched_album_continue(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    try: await q.message.delete()
    except Exception: pass
    return await _sched_ask_schedule(update,context)

async def _sched_ask_schedule(update,context):
    tt=context.user_data.get('sc_tt')
    _send=update.callback_query.message.reply_text if update.callback_query else update.message.reply_text
    if tt=='recurring':
        await _send("⏱ أدخل الفاصل بالدقائق (مثال: 60):\n/cancel للإلغاء"); return S_INTERVAL
    elif tt=='scheduled':
        await _send("🕐 أدخل الوقت HH:MM — توقيت العراق (مثال: 14:30):\n/cancel للإلغاء"); return S_TIME
    elif tt=='weekly':
        days_map={"Saturday":"السبت","Sunday":"الأحد","Monday":"الاثنين","Tuesday":"الثلاثاء","Wednesday":"الأربعاء","Thursday":"الخميس","Friday":"الجمعة"}
        context.user_data['sc_sel_days']=[]
        rows=[[InlineKeyboardButton(dn,callback_data=f"sc_day_{dk}")] for dk,dn in days_map.items()]
        rows.append([InlineKeyboardButton("✅ تأكيد",callback_data="sc_days_confirm")])
        await _send("📅 اختر الأيام:",reply_markup=InlineKeyboardMarkup(rows)); return S_DAYS

async def sched_receive_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        iv=int(update.message.text)
        if iv<1: raise ValueError
        context.user_data['sc_interval']=iv
    except ValueError: await update.message.reply_text("❌ رقم غير صحيح"); return S_INTERVAL
    await update.message.reply_text("🔘 إضافة أزرار؟",reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ إضافة أزرار",callback_data="sched_btns_yes")],
        [InlineKeyboardButton("❌ بدون أزرار",callback_data="sched_btns_no")]]))
    return S_BTN_CONFIRM

async def sched_receive_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    t=update.message.text.strip()
    try:
        h,m=map(int,t.split(":")); assert 0<=h<=23 and 0<=m<=59
        context.user_data['sc_time']=t
    except Exception: await update.message.reply_text("❌ صيغة خاطئة\nمثال: 14:30"); return S_TIME
    await update.message.reply_text("🔘 إضافة أزرار؟",reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ إضافة أزرار",callback_data="sched_btns_yes")],
        [InlineKeyboardButton("❌ بدون أزرار",callback_data="sched_btns_no")]]))
    return S_BTN_CONFIRM

async def sched_handle_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    days_map={"Saturday":"السبت","Sunday":"الأحد","Monday":"الاثنين","Tuesday":"الثلاثاء","Wednesday":"الأربعاء","Thursday":"الخميس","Friday":"الجمعة"}
    if q.data=='sc_days_confirm':
        sel=context.user_data.get('sc_sel_days',[])
        if not sel: await q.answer("❌ اختر يوماً على الأقل",show_alert=True); return S_DAYS
        context.user_data['sc_days']=','.join(sel)
        await q.message.reply_text("🕐 أدخل الوقت HH:MM — توقيت العراق:\n/cancel للإلغاء"); return S_TIME
    day=q.data[len("sc_day_"):]; sel=context.user_data.get('sc_sel_days',[])
    if day in sel: sel.remove(day)
    else: sel.append(day)
    context.user_data['sc_sel_days']=sel
    rows=[[InlineKeyboardButton(f"{'✅ ' if dk in sel else ''}{dn}",callback_data=f"sc_day_{dk}")] for dk,dn in days_map.items()]
    rows.append([InlineKeyboardButton("✅ تأكيد",callback_data="sc_days_confirm")])
    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(rows)); return S_DAYS

async def sched_handle_btn_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    if q.data in ("sched_btns_yes",):
        await q.message.reply_text("🔤 أرسل نص الزر:\n/cancel للإلغاء"); return S_BTN_TEXT
    if q.data=="sched_another_btn":
        await q.message.reply_text("🔤 أرسل نص الزر التالي:"); return S_BTN_TEXT
    return await _sched_save_task(update,context)

async def sched_receive_btn_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text=="/done": return await _sched_save_task(update,context)
    raw=update.message.text or ""; ents=update.message.entities or []; emoji_id=None
    for e in ents:
        if e.type==MessageEntity.CUSTOM_EMOJI and e.custom_emoji_id: emoji_id=str(e.custom_emoji_id); break
    if emoji_id:
        tb=raw.encode("utf-16-le"); res=bytearray(); prev=0
        for e in ents:
            if e.type==MessageEntity.CUSTOM_EMOJI: res+=tb[prev*2:e.offset*2]; prev=e.offset+e.length
        res+=tb[prev*2:]; text=res.decode("utf-16-le").strip()
    else: text=raw
    context.user_data['sc_cur_btn_text']=text; context.user_data['sc_cur_btn_emoji']=emoji_id
    await update.message.reply_text(f"🔘 نص: {text}\n\nاختر نوع الزر:",reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("🔗 رابط URL",callback_data="sc_btntype_url")],
        [InlineKeyboardButton("💬 نافذة Popup",callback_data="sc_btntype_popup")],
        [InlineKeyboardButton("📋 نسخ نص",callback_data="sc_btntype_copy")]]))
    return S_BTN_TYPE

async def sched_handle_btn_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q=update.callback_query; await q.answer()
    bt=q.data[len("sc_btntype_"):]; context.user_data['sc_cur_btn_type']=bt
    prompts={"url":"🔗 أرسل الرابط:\nمثال: https://example.com","popup":"💬 أرسل النص الذي سيظهر في النافذة:","copy":"📋 أرسل النص الذي سيتم نسخه:"}
    await q.message.reply_text(prompts.get(bt,"أرسل القيمة:")); return S_BTN_URL

async def sched_receive_btn_url(update: Update, context: ContextTypes.DEFAULT_TYPE):
    content=update.message.text; bt=context.user_data.get('sc_cur_btn_type','url')
    if bt=='url':
        if not content.startswith(('http://','https://','t.me/')): await update.message.reply_text("❌ رابط غير صحيح\nيجب أن يبدأ بـ http:// أو https://"); return S_BTN_URL
        context.user_data['sc_cur_btn_url']=content; context.user_data['sc_cur_btn_content']=None
    else: context.user_data['sc_cur_btn_url']='none'; context.user_data['sc_cur_btn_content']=content
    await update.message.reply_text("🎨 اختر لون الزر:\n\n#r 🔴 أحمر\n#g 🟢 أخضر\n#b 🔵 أزرق\n#w ⬜ افتراضي\n\nأرسل الرمز:")
    return S_BTN_COLOR

async def sched_receive_btn_color(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ci=update.message.text.strip()
    if ci.lower() not in ['#r','#g','#b','#w']: await update.message.reply_text("❌ رمز غير صحيح\n#r 🔴  #g 🟢  #b 🔵  #w ⬜"); return S_BTN_COLOR
    style=_clr(ci); bt=context.user_data.get('sc_cur_btn_type','url')
    btn_text=context.user_data.get('sc_cur_btn_text','زر'); btn_url=context.user_data.get('sc_cur_btn_url','none')
    btn_con=context.user_data.get('sc_cur_btn_content'); btn_eid=context.user_data.get('sc_cur_btn_emoji')
    editing=context.user_data.get('sc_editing_task')
    if editing:
        existing=db.get_buttons(editing)
        db.add_button(editing,btn_text,btn_url,style,bt,btn_con,btn_eid,len(existing))
        await update.message.reply_text("✅ تم إضافة الزر!")
        context.user_data.pop('sc_editing_task',None); return ConversationHandler.END
    btns=context.user_data.get('sc_btns',[])
    btns.append({'text':btn_text,'url':btn_url,'type':bt,'content':btn_con,'emoji_id':btn_eid,'style':style,'row':len(btns)})
    context.user_data['sc_btns']=btns
    await update.message.reply_text(f"✅ تم إضافة الزر  |  إجمالي: {len(btns)}",reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ زر آخر",callback_data="sched_another_btn")],
        [InlineKeyboardButton("✅ إنهاء",callback_data="sched_btns_no")]]))
    return S_BTN_CONFIRM

async def sched_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _sched_clear(context); await update.message.reply_text("❌ تم الإلغاء\n/start للرجوع"); return ConversationHandler.END

async def _sched_save_task(update,context):
    try:
        tt=context.user_data.get('sc_tt')
        task_data={'task_type':tt,'content_type':context.user_data.get('sc_ct'),
                   'file_id':context.user_data.get('sc_fid'),'text_content':context.user_data.get('sc_txt'),
                   'caption':context.user_data.get('sc_cap'),'entities':context.user_data.get('sc_ent'),
                   'caption_position':context.user_data.get('sc_cap_pos','below'),'media_group_id':context.user_data.get('sc_album_id')}
        if tt=='recurring': task_data['interval_minutes']=context.user_data.get('sc_interval')
        elif tt=='scheduled': task_data['schedule_time']=context.user_data.get('sc_time')
        elif tt=='weekly': task_data['schedule_days']=context.user_data.get('sc_days'); task_data['schedule_time']=context.user_data.get('sc_time')
        tid=db.create_task(task_data)
        for i,item in enumerate(context.user_data.get('sc_album_items',[])): db.add_media(tid,item['type'],item['file_id'],item.get('caption'),i)
        for btn in context.user_data.get('sc_btns',[]): db.add_button(tid,btn['text'],btn['url'],btn['style'],btn['type'],btn.get('content'),btn.get('emoji_id'),btn['row'])
        await schedule_task(context.application,tid)
        _send=update.callback_query.message.reply_text if update.callback_query else update.message.reply_text
        await _send(f"✅ تم الحفظ!\n\n🆔 #{tid}\n{_ttn(tt)}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📅 لوحة الدوريات",callback_data="sched_main")]]))
        await _sched_clear(context); return ConversationHandler.END
    except Exception as e:
        logger.error(f"save_task: {e}")
        _send=update.callback_query.message.reply_text if update.callback_query else update.message.reply_text
        await _send(f"❌ خطأ: {e}"); return ConversationHandler.END

async def schedule_task(application: Application, task_id: int):
    task=db.get_task(task_id)
    if not task or not task['active']: return
    jq=application.job_queue; jname=f"task_{task_id}"
    for job in jq.get_jobs_by_name(jname): job.schedule_removal()

    if task['task_type']=='recurring':
        jq.run_repeating(send_to_all_users,interval=timedelta(minutes=task['interval_minutes']),first=10,data=task_id,name=jname)
        logger.info(f"✅ دورية #{task_id} كل {task['interval_minutes']} د")

    elif task['task_type']=='scheduled':
        t=datetime.strptime(task['schedule_time'],'%H:%M').time()
        local_dt=TIMEZONE.localize(datetime.combine(datetime.today(),t))
        utc_t=local_dt.astimezone(pytz.utc).time().replace(tzinfo=pytz.utc)
        jq.run_daily(send_to_all_users,time=utc_t,days=(0,1,2,3,4,5,6),data=task_id,name=jname)
        logger.info(f"✅ مجدولة #{task_id} {task['schedule_time']} Baghdad")

    elif task['task_type']=='weekly':
        dmap={'Sunday':0,'Monday':1,'Tuesday':2,'Wednesday':3,'Thursday':4,'Friday':5,'Saturday':6}
        t=datetime.strptime(task['schedule_time'],'%H:%M').time()
        local_dt=TIMEZONE.localize(datetime.combine(datetime.today(),t))
        utc_dt=local_dt.astimezone(pytz.utc); utc_t=utc_dt.time().replace(tzinfo=pytz.utc)
        day_shift=utc_dt.date().day-local_dt.date().day
        adjusted=tuple(set((dmap[d.strip()]+day_shift)%7 for d in task['schedule_days'].split(',') if d.strip() in dmap))
        if not adjusted: logger.error(f"❌ أيام فارغة #{task_id}"); return
        jq.run_daily(send_to_all_users,time=utc_t,days=adjusted,data=task_id,name=jname)
        logger.info(f"✅ أسبوعية #{task_id} {task['schedule_days']} {task['schedule_time']} Baghdad")

async def send_to_all_users(context: ContextTypes.DEFAULT_TYPE):
    task_id=context.job.data; task=db.get_task(task_id)
    if not task or not task['active']: context.job.schedule_removal(); return

    if task['task_type']=='scheduled':
        today=datetime.now(TIMEZONE).date()
        if task.get('execution_date') and datetime.fromisoformat(task['execution_date']).date()==today: return

    users=load_users()
    data_cfg=load_data()
    excluded={str(oid) for oid in OWNER_IDS} | set(data_cfg.get("admins",{}).keys())
    users={uid:udata for uid,udata in users.items() if uid not in excluded}

    buttons = db.get_buttons(task_id)
    reply_markup = _build_task_keyboard(buttons) if buttons else None
    entities = None
    if task.get('entities'):
        try:
            entities = [MessageEntity(**e) for e in json.loads(task['entities'])]
        except Exception:
            pass
    show_above = None
    if task.get('caption') and task['content_type'] in ['photo', 'video', 'animation']:
        show_above = (task['caption_position'] == 'above')

    old_ids = {}
    if task.get('delete_after_send') and task.get('last_message_ids'):
        try:
            old_ids = json.loads(task['last_message_ids'])
        except Exception:
            pass

    new_ids = {}
    sent = 0
    failed = 0
    user_items = list(users.items())
    BATCH = 25

    async def _do(uid_str):
        _uid = int(uid_str)
        if old_ids.get(uid_str):
            try:
                await context.bot.delete_message(chat_id=_uid, message_id=old_ids[uid_str])
            except Exception:
                pass
        try:
            sent_msg = await _send_with_retry(
                lambda uid=_uid: _send_to_user(context.bot, uid, task, reply_markup, entities, show_above),
                log_label=f"task {task_id} user {_uid}",
            )
            return uid_str, sent_msg.message_id if sent_msg else None, True
        except Forbidden:
            return uid_str, None, False
        except Exception as e:
            logger.warning(f"send to {_uid}: {e}")
            return uid_str, None, False

    for i in range(0, len(user_items), BATCH):
        batch = user_items[i:i + BATCH]
        results = await asyncio.gather(*[_do(u) for u, _ in batch], return_exceptions=True)
        for r in results:
            if isinstance(r, tuple):
                uid_s, mid, ok = r
                if ok:
                    sent += 1
                    if mid:
                        new_ids[uid_s] = mid
                else:
                    failed += 1
        await asyncio.sleep(0.3)

    db.update_last_sent(task_id)
    if new_ids:
        db.update_task(task_id, {'last_message_ids': json.dumps(new_ids)})
    if task['task_type'] == 'scheduled':
        db.update_task(task_id, {'execution_date': datetime.now(TIMEZONE).date().isoformat()})
    logger.info(f"✅ مهمة #{task_id} → {sent} مستخدم، فشل {failed}")

async def _send_to_user(bot, uid, task, reply_markup, entities, show_above):
    ct = task['content_type']
    if ct == 'text':
        return await bot.send_message(uid, task['text_content'], entities=entities, reply_markup=reply_markup)
    elif ct == 'photo':
        return await bot.send_photo(uid, task['file_id'], caption=task.get('caption'), caption_entities=entities,
            reply_markup=reply_markup, show_caption_above_media=show_above)
    elif ct == 'video':
        return await bot.send_video(uid, task['file_id'], caption=task.get('caption'), caption_entities=entities,
            reply_markup=reply_markup, show_caption_above_media=show_above)
    elif ct == 'animation':
        return await bot.send_animation(uid, task['file_id'], caption=task.get('caption'), caption_entities=entities,
            reply_markup=reply_markup, show_caption_above_media=show_above)
    elif ct == 'document':
        return await bot.send_document(uid, task['file_id'], caption=task.get('caption'), caption_entities=entities, reply_markup=reply_markup)
    elif ct == 'audio':
        return await bot.send_audio(uid, task['file_id'], caption=task.get('caption'), caption_entities=entities, reply_markup=reply_markup)
    elif ct == 'sticker':
        return await bot.send_sticker(uid, task['file_id'])
    elif ct == 'media_group':
        items = db.get_media(task['id'])
        if not items: return None
        mg = []
        for i, item in enumerate(items[:10]):
            cap = item.get('caption') if i == 0 else None
            if item['media_type'] == 'photo': mg.append(InputMediaPhoto(item['file_id'], caption=cap))
            elif item['media_type'] == 'video': mg.append(InputMediaVideo(item['file_id'], caption=cap))
            elif item['media_type'] == 'document': mg.append(InputMediaDocument(item['file_id'], caption=cap))
            elif item['media_type'] == 'audio': mg.append(InputMediaAudio(item['file_id'], caption=cap))
        if not mg: return None
        msgs = await bot.send_media_group(uid, mg)
        if reply_markup and msgs:
            return await bot.send_message(uid, "⬆️", reply_markup=reply_markup)
        return msgs[0] if msgs else None

async def load_all_tasks(application: Application):
    tasks=db.get_tasks(); count=0
    for task in tasks:
        if task['active']: await schedule_task(application,task['id']); count+=1
    logger.info(f"✅ تم تحميل {count} مهمة")

async def post_init(application):
    global semaphore
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    asyncio.create_task(cleanup_task())
    asyncio.create_task(runtime_state_cleanup_task())
    await load_all_tasks(application)
    logger.info("🚀 البوت شغال! — يستخدم Cobalt API للتحميل")

def main():
    if not BOT_TOKEN or BOT_TOKEN=="ضع_التوكن_هنا":
        logger.error("❌ يرجى تعيين BOT_TOKEN في متغيرات البيئة"); return
    db.setup()
    app=(
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .connect_timeout(60)
        .read_timeout(300)
        .write_timeout(300)
        .media_write_timeout(300)
        .build()
    )

    sched_conv=ConversationHandler(
        entry_points=[
            CallbackQueryHandler(sched_start_add,    pattern=r"^sched_add_(recurring|scheduled|weekly)$"),
            CallbackQueryHandler(sched_add_btn_to_task, pattern=r"^sched_add_btn_\d+$"),
        ],
        states={
            S_MSG:       [MessageHandler(filters.ALL & ~filters.COMMAND, sched_receive_message),
                          CallbackQueryHandler(sched_album_continue,pattern="^sc_album_continue$")],
            S_CAP_POS:   [CallbackQueryHandler(sched_handle_cap_pos, pattern="^sc_cap_(above|below)$")],
            S_INTERVAL:  [MessageHandler(filters.TEXT & ~filters.COMMAND, sched_receive_interval)],
            S_TIME:      [MessageHandler(filters.TEXT & ~filters.COMMAND, sched_receive_time)],
            S_DAYS:      [CallbackQueryHandler(sched_handle_days,pattern="^sc_day_|^sc_days_confirm$")],
            S_BTN_CONFIRM:[CallbackQueryHandler(sched_handle_btn_confirm,pattern="^sched_btns_(yes|no|done)$|^sched_another_btn$")],
            S_BTN_TEXT:  [MessageHandler(filters.TEXT & ~filters.COMMAND, sched_receive_btn_text)],
            S_BTN_TYPE:  [CallbackQueryHandler(sched_handle_btn_type,pattern="^sc_btntype_")],
            S_BTN_URL:   [MessageHandler(filters.TEXT & ~filters.COMMAND, sched_receive_btn_url)],
            S_BTN_COLOR: [MessageHandler(filters.TEXT & ~filters.COMMAND, sched_receive_btn_color)],
        },
        fallbacks=[CommandHandler("cancel",sched_cancel)],
        per_user=True, per_chat=True,
    )

    app.add_handler(CommandHandler("start",start_command))
    app.add_handler(CommandHandler("admin",cmd_admin))
    app.add_handler(sched_conv)
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND & filters.ChatType.PRIVATE, handle_message))

    logger.info("✅ البوت جاهز")
    app.run_polling(drop_pending_updates=True, allowed_updates=["message","callback_query"])

if __name__=="__main__":
    main()
