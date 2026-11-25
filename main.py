import os
import asyncio
import json
import logging
import re
import threading
import unicodedata
import uuid
import urllib.request
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from urllib.parse import parse_qs

import psycopg
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row

# 連線池為選用：requirements.txt 沒有 psycopg_pool 時自動退回「每次查詢新建連線」。
try:
    from psycopg_pool import ConnectionPool
except ImportError:  # pragma: no cover
    ConnectionPool = None

from fastapi import FastAPI, HTTPException, Request
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent,
    PostbackEvent,
    JoinEvent,
    FollowEvent,
    TextMessage,
    TextSendMessage,
    FlexSendMessage,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("emt-bot")

# ============================================================
# LINE 機器人設定
# ============================================================
CHANNEL_ACCESS_TOKEN = os.environ.get("CHANNEL_ACCESS_TOKEN", "")
CHANNEL_SECRET = os.environ.get("CHANNEL_SECRET", "")

# 唯一最高管理者：LINE Developers > Basic settings > Your user ID
# 可用環境變數 ADMIN_USER_ID 覆蓋；未設定時沿用原本的值，部署設定不需變更。
ADMIN_USER_ID = os.environ.get("ADMIN_USER_ID", "").strip() or "Uf2f7fe99986289b57f9da61494cfcd81"

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(CHANNEL_SECRET)

TAIWAN_TZ = timezone(timedelta(hours=8))
WAIT_SECONDS = 5 * 60

# 救護事件於啟動滿 1 小時自動結束
INCIDENT_AUTO_CLOSE_SECONDS = 60 * 60

# 事故地點長度上限（避免超長文字導致 LINE 卡片或推播失敗）
MAX_LOCATION_LENGTH = 300

# Webhook 去重：保留最近處理過的事件識別碼數量
DEDUP_CACHE_SIZE = 1000

# 員工私訊通報：救護通知要推播到的群組。
# 優先使用環境變數 RESCUE_GROUP_ID；未設定時，使用管理者在群組輸入「設定通報群組」所存的值。
RESCUE_GROUP_ID_ENV = os.environ.get("RESCUE_GROUP_ID", "").strip()

# 員工欄位格式限制
EMP_NO_PATTERN = re.compile(r"^[A-Za-z0-9\-]{1,20}$")
EXT_PATTERN = re.compile(r"^[0-9\-#]{1,10}$")
MAX_NAME_LENGTH = 20

# 員工申請填寫步驟的有效時間（通報地點步驟沿用 WAIT_SECONDS = 5 分鐘）
REGISTRATION_STEP_TIMEOUT_SECONDS = 30 * 60

# ============================================================
# 暫存資料
# ============================================================
# 等待中的5分鐘輸入流程僅暫存於記憶體；正式事件及回應資料保存於 PostgreSQL。
pending_locations = {}
pending_corrections = {}
active_incidents = {}
pending_closures = {}
timeout_tasks = {}

# 每個群組各自的救護事件定時任務
incident_timer_tasks = {}

# 舊人員名單資料表保留於 PostgreSQL，不再載入或使用。
# 每日事件流水號由 PostgreSQL 永久保存；格式 YYYYMMDD01。

# ------------------------------------------------------------
# 執行緒 / 事件迴圈協調
# ------------------------------------------------------------
# 所有 LINE API、PostgreSQL 呼叫皆為同步操作，改在執行緒中執行，避免卡住事件迴圈。
# state_lock 保護上述記憶體狀態，讓 Webhook 處理與計時器不會同時修改。
state_lock = threading.RLock()
main_loop = None          # 由 lifespan 設定
event_queue = None        # 由 lifespan 建立；依到達順序逐批處理 Webhook 事件
_processed_ids = OrderedDict()
_processed_lock = threading.Lock()

_db_pool = None


def _spawn(coro):
    """可由任何執行緒呼叫：把協程交給主事件迴圈，回傳具備 done()/cancel() 的 Future。"""
    if main_loop is None:
        coro.close()
        raise RuntimeError("事件迴圈尚未就緒")
    return asyncio.run_coroutine_threadsafe(coro, main_loop)


def _cancel_future(future):
    if future is not None and not future.done():
        future.cancel()


# ============================================================
# 資料庫
# ============================================================
def _database_url():
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("Render 尚未設定 DATABASE_URL，服務停止啟動以避免資料僅存在記憶體。")
    return database_url


def open_db_pool():
    """啟動時建立連線池；套件不存在時沿用逐次連線。"""
    global _db_pool
    database_url = _database_url()
    if ConnectionPool is None:
        logger.warning("未安裝 psycopg_pool，使用逐次連線模式（功能相同，效能較低）。")
        return
    pool_kwargs = {
        "conninfo": database_url,
        "min_size": 1,
        "max_size": int(os.environ.get("DB_POOL_MAX_SIZE", "5")),
        # prepare_threshold=None：避免長連線在 Neon pooler（PgBouncer）下產生 prepared statement 衝突。
        "kwargs": {"row_factory": dict_row, "connect_timeout": 10, "prepare_threshold": None},
        "timeout": 10,
        "open": False,
    }
    # Neon 閒置會斷線；psycopg_pool >= 3.2 可在取用前檢查連線是否仍有效。
    if hasattr(ConnectionPool, "check_connection"):
        pool_kwargs["check"] = ConnectionPool.check_connection
    pool = ConnectionPool(**pool_kwargs)
    pool.open(wait=True, timeout=15)
    _db_pool = pool
    logger.info("PostgreSQL 連線池已啟用。")


def close_db_pool():
    global _db_pool
    pool, _db_pool = _db_pool, None
    if pool is not None:
        try:
            pool.close()
        except Exception:
            logger.exception("關閉連線池失敗")


def get_db_connection():
    """回傳可用於 `with` 的連線；離開 with 時自動 commit（例外則 rollback）。"""
    if _db_pool is not None:
        return _db_pool.connection()
    return psycopg.connect(_database_url(), row_factory=dict_row, connect_timeout=10)


def init_database():
    """建立所需資料表；資料庫無法使用時直接讓服務啟動失敗。
    資料表結構與原版完全相同；只是已完成的遷移不再於每次啟動重複執行。"""
    statements = [
        """CREATE TABLE IF NOT EXISTS app_settings (
            setting_key TEXT PRIMARY KEY,
            setting_value BOOLEAN NOT NULL DEFAULT FALSE,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS nurse_roster (
            slot SMALLINT PRIMARY KEY CHECK (slot BETWEEN 1 AND 3),
            user_id TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS safety_roster (
            user_id TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""",
        """CREATE TABLE IF NOT EXISTS event_counters (
            event_date DATE PRIMARY KEY,
            last_number INTEGER NOT NULL CHECK (last_number >= 0)
        )""",
        """CREATE TABLE IF NOT EXISTS incidents (
            incident_id TEXT PRIMARY KEY,
            group_id TEXT NOT NULL,
            location TEXT NOT NULL,
            initiator TEXT NOT NULL,
            initiator_name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL,
            ended_at TIMESTAMPTZ,
            status TEXT NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'closed', 'auto_closed'))
        )""",
        """CREATE UNIQUE INDEX IF NOT EXISTS one_active_incident_per_group
            ON incidents(group_id) WHERE status = 'active'""",
        """CREATE TABLE IF NOT EXISTS incident_responses (
            incident_id TEXT NOT NULL REFERENCES incidents(incident_id),
            user_id TEXT NOT NULL,
            role TEXT NOT NULL CHECK (role IN ('nurse', 'safety')),
            status TEXT NOT NULL CHECK (status IN ('available', 'unavailable')),
            responded_at TIMESTAMPTZ NOT NULL,
            display_name TEXT NOT NULL,
            PRIMARY KEY (incident_id, user_id)
        )""",
        """CREATE TABLE IF NOT EXISTS incident_response_history (
            id BIGSERIAL PRIMARY KEY,
            incident_id TEXT NOT NULL REFERENCES incidents(incident_id),
            user_id TEXT NOT NULL,
            display_name TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('available', 'unavailable')),
            changed_at TIMESTAMPTZ NOT NULL,
            previous_status TEXT
        )""",
        """CREATE TABLE IF NOT EXISTS incident_location_history (
            id BIGSERIAL PRIMARY KEY,
            incident_id TEXT NOT NULL REFERENCES incidents(incident_id),
            old_location TEXT NOT NULL,
            new_location TEXT NOT NULL,
            changed_by TEXT NOT NULL,
            changed_at TIMESTAMPTZ NOT NULL
        )""",
        """ALTER TABLE incidents ADD COLUMN IF NOT EXISTS end_type TEXT""",
        # 員工私訊通報：員工資料、申請狀態與對話步驟（存資料庫，Render 重啟或休眠後不會遺失）。
        """CREATE TABLE IF NOT EXISTS employees (
            line_user_id TEXT PRIMARY KEY,
            emp_no TEXT,
            name TEXT,
            ext TEXT,
            status TEXT NOT NULL DEFAULT 'draft'
                CHECK (status IN ('draft', 'pending', 'approved', 'rejected', 'disabled')),
            step TEXT,
            step_at TIMESTAMPTZ,
            applied_at TIMESTAMPTZ,
            reviewed_at TIMESTAMPTZ,
            reviewed_by TEXT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""",
        # 同一工號同時只能綁定一個「待核准或已核准」的 LINE 帳號。
        """CREATE UNIQUE INDEX IF NOT EXISTS one_live_emp_no
            ON employees(emp_no) WHERE status IN ('pending', 'approved')""",
        """CREATE TABLE IF NOT EXISTS bot_config (
            config_key TEXT PRIMARY KEY,
            config_value TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )""",
    ]
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # 交易層級諮詢鎖：若日後有多個 instance 同時啟動，遷移會依序執行而不互相鎖死。
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (74110501,))
            for statement in statements:
                cur.execute(statement)

            # role 限制：只有在目前限制尚未包含 'member' 時才調整，避免每次啟動都重建限制而鎖表。
            cur.execute(
                "SELECT pg_get_constraintdef(oid) AS definition FROM pg_constraint "
                "WHERE conname = 'incident_responses_role_check' "
                "AND conrelid = 'incident_responses'::regclass"
            )
            row = cur.fetchone()
            if not row or "member" not in row["definition"]:
                cur.execute("ALTER TABLE incident_responses DROP CONSTRAINT IF EXISTS incident_responses_role_check")
                cur.execute("ALTER TABLE incident_responses ADD CONSTRAINT incident_responses_role_check CHECK (role IN ('nurse', 'safety', 'member'))")

            # 舊資料歷程補寫（冪等；已有歷程的回應不會重複寫入）。
            cur.execute(
                "INSERT INTO incident_response_history "
                "(incident_id, user_id, display_name, status, changed_at, previous_status) "
                "SELECT r.incident_id, r.user_id, r.display_name, r.status, r.responded_at, NULL "
                "FROM incident_responses r WHERE NOT EXISTS ("
                "SELECT 1 FROM incident_response_history h "
                "WHERE h.incident_id=r.incident_id AND h.user_id=r.user_id)"
            )


def create_incident_record(incident, group_id):
    with get_db_connection() as conn:
        conn.execute(
            "INSERT INTO incidents (incident_id, group_id, location, initiator, "
            "initiator_name, created_at, status) VALUES (%s, %s, %s, %s, %s, %s, 'active')",
            (incident["id"], group_id, incident["location"], incident["initiator"],
             incident["initiator_name"], incident["created_at"]),
        )


def save_incident_response(incident_id, user_id, record):
    """更新目前狀態，並永久保存每次回應或修改歷程（同一交易內完成）。"""
    display_name = record.get("display_name") or "群組成員"
    with get_db_connection() as conn:
        with conn.transaction():
            old = conn.execute(
                "SELECT status FROM incident_responses "
                "WHERE incident_id=%s AND user_id=%s FOR UPDATE",
                (incident_id, user_id)
            ).fetchone()
            previous_status = old["status"] if old else None
            conn.execute(
                "INSERT INTO incident_responses "
                "(incident_id, user_id, role, status, responded_at, display_name) "
                "VALUES (%s, %s, 'member', %s, %s, %s) "
                "ON CONFLICT (incident_id, user_id) DO UPDATE SET role='member', "
                "status=EXCLUDED.status, responded_at=EXCLUDED.responded_at, "
                "display_name=EXCLUDED.display_name",
                (incident_id, user_id, record["status"], record["time"], display_name)
            )
            conn.execute(
                "INSERT INTO incident_response_history "
                "(incident_id, user_id, display_name, status, changed_at, previous_status) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (incident_id, user_id, display_name,
                 record["status"], record["time"], previous_status)
            )
    return previous_status


def load_response_history(incident_id):
    with get_db_connection() as conn:
        return conn.execute(
            "SELECT display_name, status, changed_at, previous_status "
            "FROM incident_response_history WHERE incident_id=%s ORDER BY id",
            (incident_id,)
        ).fetchall()


def load_location_history(incident_id):
    with get_db_connection() as conn:
        return conn.execute(
            "SELECT old_location, new_location, changed_by, changed_at "
            "FROM incident_location_history WHERE incident_id=%s ORDER BY id",
            (incident_id,)
        ).fetchall()


def _fetch_incident(conn, incident_id):
    """以既有連線讀取單一事件及其回應；供單筆查詢與清單查詢共用。"""
    row = conn.execute(
        "SELECT incident_id, group_id, location, initiator, initiator_name, "
        "created_at, ended_at, status, end_type FROM incidents WHERE incident_id=%s",
        (incident_id,)
    ).fetchone()
    if not row:
        return None
    responses = conn.execute(
        "SELECT user_id, status, responded_at, display_name FROM incident_responses "
        "WHERE incident_id=%s ORDER BY responded_at", (incident_id,)
    ).fetchall()
    return {
        "id": row["incident_id"], "group_id": row["group_id"],
        "location": row["location"], "initiator": row["initiator"],
        "initiator_name": row["initiator_name"], "created_at": row["created_at"],
        "ended_at": row["ended_at"], "status": row["status"],
        "end_type": row.get("end_type"),
        "responses": {r["user_id"]: {"status": r["status"], "time": r["responded_at"],
                       "display_name": r["display_name"]} for r in responses}
    }


def get_incident_by_id(incident_id):
    with get_db_connection() as conn:
        return _fetch_incident(conn, incident_id)


def update_incident_location(incident_id, location, changed_by):
    with get_db_connection() as conn:
        with conn.transaction():
            row = conn.execute(
                "SELECT location FROM incidents WHERE incident_id=%s AND status='active' FOR UPDATE",
                (incident_id,)
            ).fetchone()
            if not row:
                return False
            conn.execute("UPDATE incidents SET location=%s WHERE incident_id=%s", (location, incident_id))
            conn.execute(
                "INSERT INTO incident_location_history "
                "(incident_id, old_location, new_location, changed_by, changed_at) "
                "VALUES (%s, %s, %s, %s, NOW())",
                (incident_id, row["location"], location, changed_by)
            )
            return True


def close_incident_record(incident_id, status, end_type):
    with get_db_connection() as conn:
        cur = conn.execute(
            "UPDATE incidents SET status=%s, ended_at=NOW(), end_type=%s "
            "WHERE incident_id=%s AND status='active'",
            (status, end_type, incident_id),
        )
        return cur.rowcount == 1


def load_persistent_state():
    """從 PostgreSQL 還原所有進行中事件及其目前回應狀態。"""
    with get_db_connection() as conn:
        incidents = conn.execute(
            "SELECT incident_id, group_id, location, initiator, initiator_name, created_at "
            "FROM incidents WHERE status = 'active' ORDER BY created_at"
        ).fetchall()
        responses = conn.execute(
            "SELECT r.incident_id, r.user_id, r.status, r.responded_at, r.display_name "
            "FROM incident_responses r JOIN incidents i ON i.incident_id = r.incident_id "
            "WHERE i.status = 'active' ORDER BY r.responded_at"
        ).fetchall()

    active_incidents.clear()
    for row in incidents:
        active_incidents[row["group_id"]] = {
            "id": row["incident_id"], "location": row["location"],
            "initiator": row["initiator"], "initiator_name": row["initiator_name"],
            "created_at": row["created_at"], "responses": {},
        }
    for row in responses:
        incident = next((item for item in active_incidents.values() if item["id"] == row["incident_id"]), None)
        if incident:
            incident["responses"][row["user_id"]] = {
                "status": row["status"],
                "time": row["responded_at"], "display_name": row["display_name"],
            }


def now_tw():
    return datetime.now(TAIWAN_TZ)


def next_incident_id():
    """以資料庫原子遞增方式產生 YYYYMMDD01；已取號即永不回收。"""
    today = now_tw().date()
    with get_db_connection() as conn:
        row = conn.execute(
            "INSERT INTO event_counters (event_date, last_number) VALUES (%s, 1) "
            "ON CONFLICT (event_date) DO UPDATE SET "
            "last_number = event_counters.last_number + 1 RETURNING last_number",
            (today,),
        ).fetchone()
    return f"{today.strftime('%Y%m%d')}{row['last_number']:02d}"


def time_text(value):
    return value.astimezone(TAIWAN_TZ).strftime("%Y-%m-%d %H:%M:%S")


def normalize_command(text):
    """指令比對用：全形轉半形（例如「１１９」）並去除前後空白。"""
    return unicodedata.normalize("NFKC", text).strip()


def clip_location(text):
    text = text.strip()
    if len(text) > MAX_LOCATION_LENGTH:
        return text[:MAX_LOCATION_LENGTH] + "…"
    return text


def get_group_id(event):
    source = getattr(event, "source", None)
    if getattr(source, "type", None) == "group":
        return getattr(source, "group_id", None)
    return None


def get_user_id(event):
    return getattr(getattr(event, "source", None), "user_id", None)


def get_display_name(group_id, user_id):
    if not group_id or not user_id:
        return "群組成員"
    try:
        return line_bot_api.get_group_member_profile(
            group_id, user_id
        ).display_name
    except Exception:
        return "群組成員"


def is_admin(user_id):
    """確認是否為唯一最高管理者。"""
    return bool(user_id) and user_id == ADMIN_USER_ID


def alert_admin(text):
    """重要失敗通知：私訊最高管理者；本身失敗只記錄，不再往外拋。"""
    try:
        line_bot_api.push_message(ADMIN_USER_ID, TextSendMessage(text=text))
    except Exception:
        logger.exception("最高管理者告警私訊發送失敗")


def flex_message(alt_text, contents):
    return FlexSendMessage(alt_text=alt_text, contents=contents)


def postback(label, data, display_text=None):
    return {"type": "postback", "label": label, "data": data,
            "displayText": display_text or label}


def panel_message():
    contents = {
        "type": "bubble",
        "size": "kilo",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
            {"type": "text", "text": "廠區緊急救護面板", "weight": "bold", "size": "lg"},
            {"type": "text", "text": "發生緊急狀況時，請在群組輸入 119 啟動救護流程。", "wrap": True, "size": "sm"},
            {"type": "button", "style": "primary", "color": "#D93025", "height": "md",
             "action": postback("🚨 緊急救護", "action=start_emergency")}
        ]}
    }
    return flex_message("廠區緊急救護面板", contents)


def pending_location_message(token):
    """啟動等待提示；取消按鈕使用本次流程專屬 Token，且不在群組顯示按鈕文字。"""
    contents = {
        "type": "bubble",
        "size": "kilo",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {
                    "type": "text",
                    "text": "🚨【緊急救護啟動】",
                    "weight": "bold",
                    "size": "lg",
                    "color": "#B42318",
                    "wrap": True
                },
                {
                    "type": "text",
                    "text": "請直接輸入事故地點；收到地點後將立即建立救護事件並通知全群。等待時間為 5 分鐘。",
                    "size": "sm",
                    "wrap": True
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "height": "md",
                    "action": {
                        "type": "postback",
                        "label": "取消啟動",
                        "data": f"action=cancel_start&token={token}"
                    }
                }
            ]
        }
    }
    return flex_message("緊急救護啟動等待中", contents)


# ============================================================
# 5 分鐘等待流程逾時
# ============================================================
def _expire_pending_wait(kind, group_id, created_at, key):
    """逾時處理（在執行緒中執行）：清除等待狀態並通知群組。"""
    with state_lock:
        pending = pending_locations if kind == "location" else pending_corrections
        item = pending.get(group_id)
        if not (item and item["created_at"] == created_at):
            return
        pending.pop(group_id, None)
        timeout_tasks.pop(key, None)
    notice = ("⌛ 已超過 5 分鐘未收到事故地點，緊急救護啟動流程已自動取消。"
              if kind == "location" else
              "⌛ 已超過 5 分鐘未收到新的事故地點，本次修正已自動取消，維持原事故地點。")
    try:
        line_bot_api.push_message(group_id, TextSendMessage(text=notice))
    except Exception as error:
        logger.error("等待流程逾時通知失敗：%s", error)


def schedule_timeout(kind, group_id, created_at):
    key = (kind, group_id)
    _cancel_future(timeout_tasks.get(key))

    async def wait_and_expire():
        try:
            await asyncio.sleep(WAIT_SECONDS)
            await asyncio.to_thread(_expire_pending_wait, kind, group_id, created_at, key)
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("等待流程逾時處理錯誤：%s", key)

    timeout_tasks[key] = _spawn(wait_and_expire())


def expire_if_needed(kind, group_id):
    pending = pending_locations if kind == "location" else pending_corrections
    item = pending.get(group_id)
    if not item:
        return False
    if (now_tw() - item["created_at"]).total_seconds() >= WAIT_SECONDS:
        pending.pop(group_id, None)
        _cancel_future(timeout_tasks.pop((kind, group_id), None))
        return True
    return False


def clear_pending(kind, group_id):
    """取消並移除某群組的等待流程計時器。"""
    _cancel_future(timeout_tasks.pop((kind, group_id), None))


def response_message_contents(incident_id):
    # 由進行中事件取得目前事故地點（含已修正後的最新地點）；不更動函式參數與呼叫端。
    location = next(
        (item["location"] for item in active_incidents.values() if item["id"] == incident_id),
        "請參閱上方緊急救護通知"
    )
    incident = next((item for item in active_incidents.values() if item["id"] == incident_id), None)
    reporter = incident.get("initiator_name") if incident else None
    contents = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box", "layout": "vertical", "spacing": "xs",
            "backgroundColor": "#B42318", "paddingAll": "16px",
            "contents": [
                {"type": "text", "text": "🚨 緊急救護通知", "weight": "bold",
                 "size": "xl", "color": "#FFFFFF", "wrap": True},
                {"type": "text", "text": "請所有群組成員立即回報支援狀況",
                 "size": "sm", "color": "#FFD9D6", "wrap": True}
            ]
        },
        "body": {
            "type": "box", "layout": "vertical", "spacing": "md",
            "backgroundColor": "#FFFFFF", "paddingAll": "16px",
            "contents": [
                {"type": "text", "text": "事故地點", "size": "sm",
                 "color": "#6B7280", "weight": "bold"},
                {
                    "type": "box", "layout": "vertical",
                    "backgroundColor": "#FDECEA", "cornerRadius": "10px",
                    "paddingAll": "14px",
                    "contents": [
                        {"type": "text", "text": location, "weight": "bold",
                         "size": "xl", "color": "#B42318", "wrap": True}
                    ]
                },
                {"type": "text", "text": "請依目前狀況選擇是否可前往支援，之後可再次修改。",
                 "wrap": True, "size": "sm", "color": "#374151"},
                {"type": "button", "style": "primary", "color": "#16834A", "height": "md",
                 "action": postback("可前往", f"action=ack_available&incident_id={incident_id}")},
                {"type": "button", "style": "primary", "color": "#64748B", "height": "md",
                 "action": postback("無法前往", f"action=ack_unavailable&incident_id={incident_id}")}
            ]
        },
        "footer": {
            "type": "box", "layout": "vertical",
            "backgroundColor": "#F3F4F6", "paddingAll": "12px",
            "contents": [
                {"type": "text", "text": "被洗版請輸入「救護回報」重新叫出",
                 "size": "xs", "color": "#6B7280", "wrap": True, "align": "center"}
            ]
        }
    }
    # 顯示通報人（員工私訊通報時含工號、分機，方便現場聯絡）。
    if reporter:
        contents["body"]["contents"].insert(2, {
            "type": "text", "text": f"通報人：{reporter}", "size": "sm",
            "weight": "bold", "color": "#374151", "wrap": True
        })
    return contents


def response_message(incident_id):
    return flex_message(
        "請回報是否可以前往支援",
        response_message_contents(incident_id)
    )


# ============================================================
# LINE 群組真正的 @All 通知
# ============================================================
def reply_emergency_notice(reply_token, notice, incident_id, group_id):
    """以同一次 Reply 發送真正 @All 文字通知及 Flex 回報卡片（不耗推播額度）。
    Reply 失敗時才改用 Push 補送；若 @All 文字或回報卡片最終都送不出去，私訊最高管理者告警。"""
    def send(url, payload):
        request = urllib.request.Request(
            url,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"
            },
            method="POST"
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()

    # 第一則訊息：使用 Reply Token 發送真正的 LINE 全員提及。
    # 第二則訊息：Flex 回報卡片放在同一次 Reply 中一併送出（Reply 不耗推播額度）。
    text_payload = {
        "replyToken": reply_token,
        "messages": [{
            "type": "textV2",
            "text": "{everyone}\n" + notice,
            "substitution": {
                "everyone": {
                    "type": "mention",
                    "mentionee": {"type": "all"}
                }
            }
        }, {
            "type": "flex",
            "altText": "請回報是否可以前往支援",
            "contents": response_message_contents(incident_id)
        }]
    }

    text_sent = False
    card_sent = False
    try:
        send("https://api.line.me/v2/bot/message/reply", text_payload)
        text_sent = True
        card_sent = True
        logger.info("真正 @All 文字通知及 Flex 回報卡片已透過 Reply 發送。")
    except Exception as error:
        body = error.read().decode("utf-8", errors="replace") if hasattr(error, "read") else str(error)
        logger.error("LINE @All Reply 發送失敗：%s；%s", getattr(error, "code", "未知"), body)

        # Reply 失敗時，改用 Push 補送真正 @All 文字通知。
        push_text_payload = {
            "to": group_id,
            "messages": [{
                "type": "textV2",
                "text": "{everyone}\n" + notice,
                "substitution": {
                    "everyone": {
                        "type": "mention",
                        "mentionee": {"type": "all"}
                    }
                }
            }]
        }
        try:
            send("https://api.line.me/v2/bot/message/push", push_text_payload)
            text_sent = True
            logger.info("真正 @All 文字通知已透過 Push 補送。")
        except Exception as push_error:
            body = push_error.read().decode("utf-8", errors="replace") if hasattr(push_error, "read") else str(push_error)
            logger.error("Push @All 文字通知失敗：%s；%s", getattr(push_error, "code", "未知"), body)

    # 備援：Reply 失敗時，才改用 Push 補送 Flex 回報卡片。
    if not card_sent:
        try:
            line_bot_api.push_message(
                group_id,
                response_message(incident_id)
            )
            card_sent = True
            logger.info("緊急救護 Flex 回報卡片已透過 Push 補送。")
        except Exception as error:
            logger.error("Flex 回報卡片 Push 發送失敗：%s", error)

    if not (text_sent and card_sent):
        problems = []
        if not text_sent:
            problems.append("@All 文字通知")
        if not card_sent:
            problems.append("Flex 回報卡片")
        alert_admin(
            "⚠️【救護通知發送異常】\n"
            f"事件編號：{incident_id}\n"
            f"群組 ID：{group_id}\n"
            f"未成功發送：{'、'.join(problems)}\n"
            "事件已建立並進行中，請立即以其他方式確認群組是否已收到通知。"
        )


# ============================================================
# 救護事件定時自動結束（啟動滿 1 小時）
# ============================================================
def cancel_incident_timer(group_id):
    _cancel_future(incident_timer_tasks.pop(group_id, None))


def finish_incident(group_id, incident_id, end_type, notify_group=True, cancel_timer=True):
    """結束事件；完整報告只私訊最高管理者，群組僅發簡短結束通知。
    cancel_timer=False 供自動結束計時器自己呼叫（避免計時器取消自己）。"""
    with state_lock:
        incident = active_incidents.get(group_id)
        if not incident or incident["id"] != incident_id:
            return False

        status = "auto_closed" if end_type == "系統自動結束" else "closed"
        if not close_incident_record(incident_id, status, end_type):
            return False

        # 資料庫已完成結束後，再停止計時及移除記憶體中的進行中事件。
        incident["ended_at"] = now_tw()
        if cancel_timer:
            cancel_incident_timer(group_id)
        active_incidents.pop(group_id, None)
        pending_closures.pop(group_id, None)

    # 完整結束報告只傳送給最高管理者，不在救護群組公開完整回報名單及歷程。
    try:
        line_bot_api.push_message(
            ADMIN_USER_ID,
            TextSendMessage(text=build_final_report(incident, end_type))
        )
        logger.info("完整救護事件結束報告已私訊最高管理者：%s", incident_id)
    except Exception as error:
        logger.error("最高管理者私訊結束報告發送失敗：%s；%s", incident_id, error)

    # 群組只通知事件已結束，不顯示完整報告。
    if notify_group:
        group_notice = (
            "✅【救護事件已結束】\n"
            f"事件編號：{incident_id}\n"
            f"結束方式：{end_type}"
        )
        try:
            line_bot_api.push_message(group_id, TextSendMessage(text=group_notice))
        except Exception as error:
            logger.error("群組事件結束簡短通知發送失敗：%s；%s", incident_id, error)

    return True


def schedule_incident_timer(group_id, incident):
    cancel_incident_timer(group_id)
    incident_id = incident["id"]
    created_at = incident["created_at"]
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    holder = {}

    async def wait_and_close():
        try:
            elapsed = (now_tw() - created_at.astimezone(TAIWAN_TZ)).total_seconds()
            await asyncio.sleep(max(0, INCIDENT_AUTO_CLOSE_SECONDS - elapsed))
            closed = await asyncio.to_thread(
                finish_incident, group_id, incident_id, "系統自動結束", True, False
            )
            if closed:
                logger.info("救護事件滿 1 小時自動結束：%s", incident_id)
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("自動結束計時錯誤：%s", incident_id)
        finally:
            # 不取 state_lock，避免事件迴圈被正在處理事件的執行緒擋住；dict 單一操作在 GIL 下是安全的。
            if incident_timer_tasks.get(group_id) is holder.get("future"):
                incident_timer_tasks.pop(group_id, None)

    future = _spawn(wait_and_close())
    holder["future"] = future
    incident_timer_tasks[group_id] = future


# ============================================================
# 救護事件狀態與結束報告
# ============================================================
def incident_status_text(incident, include_unavailable=False):
    responses = list(incident.get("responses", {}).values())
    available = [r for r in responses if r["status"] == "available"]
    unavailable = [r for r in responses if r["status"] == "unavailable"]
    return "\n".join([
        "📋【緊急救護即時回報狀況】",
        f"事件編號：{incident['id']}",
        f"事故地點：{incident['location']}",
        f"可前往：{len(available)} 人",
        "可前往名單：" + ("、".join(r.get("display_name", "群組成員") for r in available) or "目前無"),
        f"無法前往：{len(unavailable)} 人",
        "無法前往名單：" + ("、".join(r.get("display_name", "群組成員") for r in unavailable) or "目前無"),
        f"已回應：{len(responses)} 人",
        "尚未回應人數：無法由 LINE API 可靠取得"
    ])


def build_final_report(incident, end_type):
    end_time = incident.get("ended_at") or now_tw()
    lines = [
        "✅【救護事件結束報告】",
        f"事件編號：{incident['id']}",
        f"事故地點：{incident['location']}",
        f"啟動人員：{incident.get('initiator_name', '未知')}",
        f"啟動時間：{time_text(incident['created_at'])}",
        f"結束時間：{time_text(end_time)}",
        f"結束方式：{end_type}",
        "",
        "【目前回應狀態】"
    ]
    responses = incident.get("responses", {})
    for record in responses.values():
        label = "可前往" if record["status"] == "available" else "無法前往"
        lines.append(f"{record.get('display_name', '群組成員')}：{label}（{time_text(record['time'])}）")
    if not responses:
        lines.append("尚無人回應")
    lines.extend(["", "【回應變更歷程】"])
    for row in load_response_history(incident["id"]):
        current = "可前往" if row["status"] == "available" else "無法前往"
        if not row["previous_status"]:
            change = "首次回應"
        elif row["previous_status"] == "available" and row["status"] == "unavailable":
            change = "由可前往改為無法前往"
        else:
            change = "由無法前往改為可前往"
        lines.append(f"{time_text(row['changed_at'])}｜{row['display_name']}｜{change}｜{current}")
    lines.extend(["", "【事故地點修改歷程】"])
    location_rows = load_location_history(incident["id"])
    if location_rows:
        for row in location_rows:
            lines.append(f"{time_text(row['changed_at'])}｜{row['old_location']} → {row['new_location']}｜修改者：{row['changed_by']}")
    else:
        lines.append("事故地點未曾修改")
    return "\n".join(lines)


def build_admin_menu():
    return (
        "【救護機器人最高管理選單】\n"
        "管理選單：顯示本選單\n"
        "查詢目前事件：查詢所有群組進行中事件\n"
        "查詢救護事件 YYYYMMDD01：查詢指定事件完整資料\n"
        "結束救護事件：結束目前進行中的事件（可於私訊或救護群組操作）\n"
        "系統狀態：查詢機器人及資料庫狀態\n"
        "待核准名單：顯示待核准的員工申請（附核准／拒絕按鈕）\n"
        "員工名單：查詢員工申請與核准名單\n"
        "停用員工 工號：停用已核准員工的通報權限\n"
        "通報群組：查詢目前的救護通報群組\n"
        "（在要接收通報的群組輸入「設定通報群組」即可設定）"
    )


def query_active_incidents_text():
    # 單一連線完成清單與各事件明細，避免每個事件各開一條連線。
    with get_db_connection() as conn:
        rows = conn.execute(
            "SELECT incident_id, group_id, location, initiator_name, created_at "
            "FROM incidents WHERE status='active' ORDER BY created_at"
        ).fetchall()
        if not rows:
            return "目前沒有進行中的救護事件。"
        details = {row["incident_id"]: _fetch_incident(conn, row["incident_id"]) for row in rows}
    lines = ["【目前進行中的救護事件】"]
    for row in rows:
        incident = details.get(row["incident_id"])
        lines.extend([
            f"事件編號：{row['incident_id']}",
            f"群組：{row['group_id']}",
            f"事故地點：{row['location']}",
            f"啟動人員：{row['initiator_name']}",
            f"啟動時間：{time_text(row['created_at'])}",
            incident_status_text(incident) if incident else "無法取得回報資料",
            "--------------------"
        ])
    return "\n".join(lines)


def build_historical_report(incident):
    if incident.get("status") == "active":
        return (
            "【進行中救護事件查詢】\n"
            f"事件編號：{incident['id']}\n"
            f"事故地點：{incident['location']}\n"
            f"啟動人員：{incident.get('initiator_name', '未知')}\n"
            f"啟動時間：{time_text(incident['created_at'])}\n"
            f"事件群組：{incident.get('group_id')}\n\n"
            f"{incident_status_text(incident)}"
        )
    return (build_final_report(incident, incident.get("end_type") or incident.get("status") or "歷史事件")
            + f"\n資料庫狀態：{incident.get('status')}\n事件群組：{incident.get('group_id')}")


def reply_text(reply_token, text):
    line_bot_api.reply_message(
        reply_token,
        TextSendMessage(text=text)
    )


def start_emergency_flow(reply_token, group_id, user_id, already_pending_text):
    """119 文字指令與「🚨 緊急救護」按鈕共用的啟動流程。"""
    if group_id in active_incidents:
        reply_text(reply_token, "⚠️ 目前已有進行中的救護事件，請先由最高管理者結束現有事件。")
    elif group_id in pending_locations:
        reply_text(reply_token, already_pending_text)
    else:
        pending = {
            "initiator": user_id,
            "initiator_name": get_display_name(group_id, user_id),
            "created_at": now_tw(),
            "token": uuid.uuid4().hex
        }
        pending_locations[group_id] = pending
        schedule_timeout("location", group_id, pending["created_at"])
        line_bot_api.reply_message(
            reply_token,
            pending_location_message(pending["token"])
        )


# ============================================================
# 員工申請、核准與私訊通報
# ============================================================
EMP_HELP_TEXT = (
    "【廠區救護通報機器人】\n"
    "申請：申請通報權限（需填工號、姓名、分機，並經管理員核准）\n"
    "119：緊急通報（限已核准員工），再輸入事故地點即可發出\n"
    "我的資料：查詢申請狀態\n"
    "取消：取消目前進行中的操作"
)

EMP_STATUS_TEXT = {
    "draft": "尚未完成申請，請輸入「申請」重新填寫。",
    "pending": "申請已送出，正在等待管理員核准。",
    "approved": "已核准，可使用「119」通報。",
    "rejected": "申請未獲核准，如有疑問請洽管理員；可輸入「申請」重新申請。",
    "disabled": "通報權限已被停用，請洽管理員；可輸入「申請」重新申請。",
}


# ---------------- 資料庫 ----------------
def get_config(key):
    with get_db_connection() as conn:
        row = conn.execute("SELECT config_value FROM bot_config WHERE config_key=%s", (key,)).fetchone()
    return row["config_value"] if row else None


def set_config(key, value):
    with get_db_connection() as conn:
        conn.execute(
            "INSERT INTO bot_config (config_key, config_value) VALUES (%s, %s) "
            "ON CONFLICT (config_key) DO UPDATE SET config_value=EXCLUDED.config_value, updated_at=NOW()",
            (key, value),
        )


def get_rescue_group_id():
    """通報群組：環境變數優先，其次為管理者在群組設定的值。"""
    return RESCUE_GROUP_ID_ENV or get_config("rescue_group_id")


def get_employee(user_id):
    with get_db_connection() as conn:
        return conn.execute("SELECT * FROM employees WHERE line_user_id=%s", (user_id,)).fetchone()


def employee_update(user_id, **cols):
    allowed = {"emp_no", "name", "ext", "status", "step", "step_at"}
    if not cols or set(cols) - allowed:
        raise ValueError("employee_update 欄位不合法")
    sets = ", ".join(f"{key}=%s" for key in cols) + ", updated_at=NOW()"
    with get_db_connection() as conn:
        conn.execute(f"UPDATE employees SET {sets} WHERE line_user_id=%s", (*cols.values(), user_id))


def employee_begin_application(user_id):
    """開始（或重新）申請；僅 draft / rejected / disabled 或新帳號可開始。"""
    with get_db_connection() as conn:
        row = conn.execute(
            "INSERT INTO employees (line_user_id, status, step, step_at) "
            "VALUES (%s, 'draft', 'emp_no', NOW()) "
            "ON CONFLICT (line_user_id) DO UPDATE SET status='draft', step='emp_no', step_at=NOW(), "
            "emp_no=NULL, name=NULL, ext=NULL, applied_at=NULL, reviewed_at=NULL, "
            "reviewed_by=NULL, updated_at=NOW() "
            "WHERE employees.status IN ('draft', 'rejected', 'disabled') RETURNING line_user_id",
            (user_id,),
        ).fetchone()
    return row is not None


def emp_no_in_use(emp_no, user_id):
    with get_db_connection() as conn:
        row = conn.execute(
            "SELECT 1 AS hit FROM employees WHERE emp_no=%s AND line_user_id<>%s "
            "AND status IN ('pending', 'approved')",
            (emp_no, user_id),
        ).fetchone()
    return row is not None


def employee_submit(user_id):
    """draft → pending；工號若被他人搶先使用，資料庫唯一索引會拋出 UniqueViolation。"""
    with get_db_connection() as conn:
        return conn.execute(
            "UPDATE employees SET status='pending', step=NULL, step_at=NULL, "
            "applied_at=NOW(), updated_at=NOW() "
            "WHERE line_user_id=%s AND status='draft' "
            "AND emp_no IS NOT NULL AND name IS NOT NULL AND ext IS NOT NULL RETURNING *",
            (user_id,),
        ).fetchone()


def employee_review(user_id, new_status, reviewer):
    """pending → approved / rejected；已處理過的申請回傳 None。"""
    with get_db_connection() as conn:
        return conn.execute(
            "UPDATE employees SET status=%s, reviewed_at=NOW(), reviewed_by=%s, updated_at=NOW() "
            "WHERE line_user_id=%s AND status='pending' RETURNING *",
            (new_status, reviewer, user_id),
        ).fetchone()


def employee_disable_by_emp_no(emp_no):
    with get_db_connection() as conn:
        return conn.execute(
            "UPDATE employees SET status='disabled', step=NULL, step_at=NULL, "
            "reviewed_at=NOW(), reviewed_by=%s, updated_at=NOW() "
            "WHERE emp_no=%s AND status='approved' RETURNING *",
            (ADMIN_USER_ID, emp_no),
        ).fetchone()


def list_pending_employees(limit):
    with get_db_connection() as conn:
        return conn.execute(
            "SELECT * FROM employees WHERE status='pending' ORDER BY applied_at LIMIT %s", (limit,)
        ).fetchall()


def list_employees_text():
    with get_db_connection() as conn:
        rows = conn.execute(
            "SELECT emp_no, name, ext, status FROM employees "
            "WHERE status IN ('approved', 'pending', 'disabled') ORDER BY status, emp_no"
        ).fetchall()
    if not rows:
        return "目前沒有員工資料。"
    label = {"approved": "已核准", "pending": "待核准", "disabled": "已停用"}
    lines = ["【員工名單】"]
    lines.extend(f"{r['emp_no']}｜{r['name']}｜分機 {r['ext']}｜{label.get(r['status'], r['status'])}" for r in rows)
    text = "\n".join(lines)
    return text if len(text) <= 4500 else text[:4500] + "\n…（名單過長，已截斷）"


# ---------------- 訊息 ----------------
def employee_review_card(emp):
    uid = emp["line_user_id"]
    applied = time_text(emp["applied_at"]) if emp.get("applied_at") else "-"
    contents = {
        "type": "bubble", "size": "kilo",
        "body": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": [
            {"type": "text", "text": "📝 救護通報權限申請", "weight": "bold", "size": "lg", "wrap": True},
            {"type": "text", "text": f"工號：{emp['emp_no']}", "size": "sm", "wrap": True},
            {"type": "text", "text": f"姓名：{emp['name']}", "size": "sm", "wrap": True},
            {"type": "text", "text": f"分機：{emp['ext']}", "size": "sm", "wrap": True},
            {"type": "text", "text": f"申請時間：{applied}", "size": "xs", "color": "#6B7280", "wrap": True},
        ]},
        "footer": {"type": "box", "layout": "horizontal", "spacing": "md", "contents": [
            {"type": "button", "style": "primary", "color": "#16834A", "height": "sm",
             "action": {"type": "postback", "label": "核准",
                        "data": f"action=emp_review&decision=approve&uid={uid}"}},
            {"type": "button", "style": "secondary", "height": "sm",
             "action": {"type": "postback", "label": "拒絕",
                        "data": f"action=emp_review&decision=reject&uid={uid}"}},
        ]},
    }
    return flex_message("有新的員工通報申請", contents)


def safe_notice_text(value):
    """textV2 以 {key} 作為替換標記，使用者輸入的大括號需轉成全形，避免通知被誤判而發送失敗。"""
    return str(value).replace("{", "｛").replace("}", "｝")


def _line_post(url, payload):
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        response.read()


def push_emergency_notice(group_id, notice, incident_id):
    """員工私訊通報用：以 Push 將真正 @All 文字及 Flex 回報卡片一次推播到群組。
    （私訊的 Reply Token 不能回覆到群組，所以只能 Push；群組推播依群組人數計入訊息額度。）"""
    payload = {
        "to": group_id,
        "messages": [{
            "type": "textV2",
            "text": "{everyone}\n" + notice,
            "substitution": {"everyone": {"type": "mention", "mentionee": {"type": "all"}}},
        }, {
            "type": "flex",
            "altText": "請回報是否可以前往支援",
            "contents": response_message_contents(incident_id),
        }],
    }
    try:
        _line_post("https://api.line.me/v2/bot/message/push", payload)
        logger.info("私訊通報：@All 通知及回報卡片已推播到群組：%s", incident_id)
        return True
    except Exception as error:
        body = error.read().decode("utf-8", errors="replace") if hasattr(error, "read") else str(error)
        logger.error("私訊通報 @All Push 失敗：%s；%s", getattr(error, "code", "未知"), body)

    # 備援：不含 @All 的一般推播（至少讓群組看到地點與回報卡片）。
    try:
        line_bot_api.push_message(
            group_id,
            [TextSendMessage(text="🚨 " + notice.replace("{", "").replace("}", "")),
             response_message(incident_id)],
        )
        logger.info("私訊通報：改用一般 Push 補送成功：%s", incident_id)
        return True
    except Exception as error:
        logger.error("私訊通報：備援 Push 也失敗：%s", error)

    alert_admin(
        "⚠️【私訊通報：群組通知發送失敗】\n"
        f"事件編號：{incident_id}\n"
        f"群組 ID：{group_id}\n"
        "事件已建立並進行中，請立即以其他方式通知現場人員。\n"
        "常見原因：LINE 官方帳號當月訊息額度已用完。"
    )
    return False


# ---------------- 流程 ----------------
def _employee_summary(emp_no, name, ext):
    return f"工號：{emp_no}\n姓名：{name}\n分機：{ext}"


def start_application(token, user_id, emp):
    status = emp["status"] if emp else None
    if status == "approved":
        reply_text(token, "您已通過核准，可直接使用「119」通報。\n"
                          f"{_employee_summary(emp['emp_no'], emp['name'], emp['ext'])}\n"
                          "如需更改資料，請洽管理員。")
    elif status == "pending":
        reply_text(token, "您的申請已送出，正在等待管理員核准。")
    elif employee_begin_application(user_id):
        reply_text(token, "請輸入您的工號（英數字，例如 A12345）。\n輸入「取消」可中止申請。")
    else:
        reply_text(token, "目前無法申請，請洽管理員。")


def submit_application(token, user_id):
    try:
        row = employee_submit(user_id)
    except UniqueViolation:
        employee_update(user_id, step=None, step_at=None)
        reply_text(token, "此工號剛被其他 LINE 帳號申請或核准，請輸入「申請」重新填寫並確認工號。")
        return
    if not row:
        reply_text(token, "申請狀態已變更，請輸入「我的資料」查看。")
        return
    notified = True
    try:
        line_bot_api.push_message(ADMIN_USER_ID, employee_review_card(row))
    except Exception:
        notified = False
        logger.exception("新員工申請通知管理員失敗：%s", row["emp_no"])
    reply_text(token, "✅ 申請已送出，待管理員核准後會通知您。"
                      + ("" if notified else "\n（系統通知管理員失敗，請另行聯絡管理員。）"))


def handle_application_step(token, user_id, emp, step, cmd, text):
    if step == "emp_no":
        emp_no = cmd.upper()
        if not EMP_NO_PATTERN.match(emp_no):
            reply_text(token, "工號格式不正確，請輸入英數字（可含 -），最長 20 字元。")
        elif emp_no_in_use(emp_no, user_id):
            reply_text(token, "此工號已有其他 LINE 帳號申請或使用中，請確認工號是否正確。\n"
                              "若是更換 LINE 帳號，請洽管理員停用舊帳號後再申請。")
        else:
            employee_update(user_id, emp_no=emp_no, step="name", step_at=now_tw())
            reply_text(token, "請輸入您的姓名。")
    elif step == "name":
        name = " ".join(text.split())
        if not name or len(name) > MAX_NAME_LENGTH:
            reply_text(token, f"姓名請輸入 1～{MAX_NAME_LENGTH} 個字。")
        else:
            employee_update(user_id, name=name, step="ext", step_at=now_tw())
            reply_text(token, "請輸入您的分機號碼（數字，可含 - 或 #）。")
    elif step == "ext":
        if not EXT_PATTERN.match(cmd):
            reply_text(token, "分機格式不正確，請輸入數字（可含 - 或 #），最長 10 字元。")
        else:
            employee_update(user_id, ext=cmd, step="confirm", step_at=now_tw())
            reply_text(token, "請確認申請資料：\n"
                              f"{_employee_summary(emp['emp_no'], emp['name'], cmd)}\n\n"
                              "資料正確請回覆「確認」送出申請，要中止請回覆「取消」。")
    elif step == "confirm":
        if cmd in ("確認", "確認送出"):
            submit_application(token, user_id)
        else:
            reply_text(token, "請確認申請資料：\n"
                              f"{_employee_summary(emp['emp_no'], emp['name'], emp['ext'])}\n\n"
                              "資料正確請回覆「確認」送出申請，要中止請回覆「取消」。")


def begin_report(token, user_id, emp):
    status = emp["status"] if emp else None
    if status != "approved":
        reply_text(token, "您尚未通過通報權限核准。\n"
                          + (EMP_STATUS_TEXT.get(status, "") if status else "請先輸入「申請」。"))
        return
    group_id = get_rescue_group_id()
    if not group_id:
        reply_text(token, "⚠️ 尚未設定救護通報群組，請立即改用電話通知，並聯絡管理員設定。")
    elif group_id in active_incidents:
        reply_text(token, "⚠️ 目前已有進行中的救護事件，群組已收到通知，請至群組查看並回報。")
    elif group_id in pending_locations:
        reply_text(token, "⚠️ 群組內正在進行救護啟動流程，請至群組輸入事故地點。")
    else:
        employee_update(user_id, step="await_location", step_at=now_tw())
        reply_text(token, "🚨 請直接輸入事故地點，收到後將立即通知救護群組（5 分鐘內有效）。\n輸入「取消」可取消。")


def submit_report(token, user_id, emp, text):
    employee_update(user_id, step=None, step_at=None)
    if emp["status"] != "approved":
        reply_text(token, "您的通報權限已變更，無法通報。請洽管理員。")
        return
    group_id = get_rescue_group_id()
    if not group_id:
        reply_text(token, "⚠️ 尚未設定救護通報群組，請立即改用電話通知，並聯絡管理員設定。")
        return
    if group_id in active_incidents:
        reply_text(token, "⚠️ 目前已有進行中的救護事件，群組已收到通知，請至群組查看並回報。")
        return
    if group_id in pending_locations:
        reply_text(token, "⚠️ 群組內正在進行救護啟動流程，請至群組輸入事故地點。")
        return

    location = clip_location(text)
    reporter = f"{emp['name']}（工號 {emp['emp_no']}／分機 {emp['ext']}）"
    incident_id = next_incident_id()
    incident = {
        "id": incident_id, "location": location,
        "initiator": user_id, "initiator_name": reporter,
        "created_at": now_tw(), "responses": {},
    }
    try:
        create_incident_record(incident, group_id)
    except Exception as error:
        logger.error("私訊通報：救護事件寫入資料庫失敗：%s", error)
        reply_text(token, "⚠️ 救護事件資料保存失敗，未發送通知！請立即改用電話通知，或稍後重新輸入「119」。")
        return

    active_incidents[group_id] = incident
    schedule_incident_timer(group_id, incident)
    notice = (f"🚨 緊急救護通知，請回報支援狀況。\n"
              f"通報人：{safe_notice_text(reporter)}\n"
              f"事故地點：{safe_notice_text(location)}")
    if push_emergency_notice(group_id, notice, incident_id):
        reply_text(token, f"✅ 已通知救護群組。\n事件編號：{incident_id}\n事故地點：{location}")
    else:
        reply_text(token, f"⚠️ 事件 {incident_id} 已建立，但群組通知發送失敗（可能是訊息額度已用完）。\n"
                          "請立即改用電話或其他方式通知現場人員！")


def handle_employee_dm(event, user_id, cmd, text):
    """員工私訊：申請與通報流程。回傳 True 表示已處理（並已回覆）。"""
    if not user_id:
        return False
    token = event.reply_token
    emp = get_employee(user_id)
    expired_notice = ""
    if emp and emp["step"]:
        limit = WAIT_SECONDS if emp["step"] == "await_location" else REGISTRATION_STEP_TIMEOUT_SECONDS
        started = emp["step_at"]
        if started is None or (now_tw() - started.astimezone(TAIWAN_TZ)).total_seconds() > limit:
            employee_update(user_id, step=None, step_at=None)
            emp = {**emp, "step": None}
            expired_notice = "⌛ 上一個操作已逾時取消。\n"
    step = emp["step"] if emp else None

    if cmd == "取消":
        if step:
            employee_update(user_id, step=None, step_at=None)
            reply_text(token, "已取消目前操作。")
            return True
    elif step == "await_location":
        submit_report(token, user_id, emp, text)
        return True
    elif step in ("emp_no", "name", "ext", "confirm"):
        handle_application_step(token, user_id, emp, step, cmd, text)
        return True

    if cmd in ("申請", "註冊"):
        start_application(token, user_id, emp)
        return True
    if cmd in ("119", "通報", "緊急通報"):
        begin_report(token, user_id, emp)
        return True
    if cmd in ("我的資料", "查詢申請", "申請狀態"):
        if not emp:
            reply_text(token, "您尚未申請，請輸入「申請」。")
        else:
            info = (_employee_summary(emp["emp_no"], emp["name"], emp["ext"]) + "\n") if emp.get("emp_no") else ""
            reply_text(token, info + EMP_STATUS_TEXT.get(emp["status"], emp["status"]))
        return True

    if is_admin(user_id):
        return False  # 交給管理者私訊模式處理
    reply_text(token, expired_notice + EMP_HELP_TEXT)
    return True


def is_admin_dm_command(cmd):
    return (cmd in ("管理選單", "查詢目前事件", "結束救護事件", "系統狀態", "待核准名單", "員工名單", "通報群組")
            or cmd.startswith(("查詢救護事件 ", "停用員工 ")))


def handle_dm_postback(event, user_id):
    """私訊中的按鈕：管理者核准或拒絕員工申請。"""
    data = parse_qs(getattr(event.postback, "data", ""))
    if data.get("action", [""])[0] != "emp_review":
        return
    token = event.reply_token
    if not is_admin(user_id):
        reply_text(token, "⛔ 僅最高管理者可以核准或拒絕申請。")
        return
    decision = data.get("decision", [""])[0]
    uid = data.get("uid", [""])[0]
    if decision not in ("approve", "reject") or not uid:
        reply_text(token, "按鈕資料不正確。")
        return
    approved = decision == "approve"
    try:
        row = employee_review(uid, "approved" if approved else "rejected", user_id)
    except UniqueViolation:
        reply_text(token, "核准失敗：此工號已有其他帳號核准使用中。")
        return
    if not row:
        reply_text(token, "此申請已處理過或不存在。")
        return
    reply_text(token, f"已{'核准' if approved else '拒絕'}：{row['name']}"
                      f"（工號 {row['emp_no']}／分機 {row['ext']}）")
    message = ("✅ 您的救護通報申請已核准。\n緊急時請私訊本機器人輸入「119」，再輸入事故地點即可通報。"
               if approved else "❌ 您的救護通報申請未獲核准，如有疑問請洽管理員。")
    try:
        line_bot_api.push_message(uid, TextSendMessage(text=message))
    except Exception as error:
        logger.error("申請結果通知員工失敗：%s；%s", row["emp_no"], error)


# ============================================================
# Webhook 事件處理（同步；於背景執行緒依序執行）
# ============================================================
def handle_event(event):
    group_id = get_group_id(event)
    source_type = getattr(getattr(event, "source", None), "type", None)
    user_id = get_user_id(event)

    if isinstance(event, JoinEvent) and group_id:
        line_bot_api.reply_message(event.reply_token, panel_message())
        return

    # 員工加機器人為好友：回覆使用說明（Reply 不耗額度）。
    if isinstance(event, FollowEvent) and source_type == "user":
        reply_text(event.reply_token, "歡迎使用廠區救護通報機器人。\n\n" + EMP_HELP_TEXT)
        return

    # 私訊按鈕：管理者核准／拒絕員工申請。
    if isinstance(event, PostbackEvent) and not group_id:
        if source_type == "user":
            handle_dm_postback(event, user_id)
        return

    is_text = isinstance(event, MessageEvent) and isinstance(event.message, TextMessage)
    text = event.message.text.strip() if is_text else ""
    cmd = normalize_command(text) if is_text else ""

    # ----------------------------------------------------
    # 最高管理者私訊管理：所有管理指令僅接受管理者 DM
    # ----------------------------------------------------
    if is_text:
        if not group_id:
            if source_type != "user":
                reply_text(event.reply_token, "管理功能僅能在與救護機器人的一對一私訊中執行。")
                return
            # 員工申請／通報流程（管理者也可申請與通報）；管理指令優先，不會被申請步驟吃掉。
            if not (is_admin(user_id) and is_admin_dm_command(cmd)):
                if handle_employee_dm(event, user_id, cmd, text):
                    return
            if is_admin(user_id):
                if cmd == "待核准名單":
                    pending_rows = list_pending_employees(6)
                    if not pending_rows:
                        reply_text(event.reply_token, "目前沒有待核准的申請。")
                    else:
                        if len(pending_rows) > 5:
                            messages = [employee_review_card(r) for r in pending_rows[:4]]
                            messages.append(TextSendMessage(text="尚有更多待核准申請，處理完畢後請再輸入「待核准名單」。"))
                        else:
                            messages = [employee_review_card(r) for r in pending_rows]
                        line_bot_api.reply_message(event.reply_token, messages)
                elif cmd == "員工名單":
                    reply_text(event.reply_token, list_employees_text())
                elif cmd.startswith("停用員工 "):
                    emp_no = cmd.replace("停用員工 ", "", 1).strip().upper()
                    disabled = employee_disable_by_emp_no(emp_no)
                    reply_text(event.reply_token,
                               f"已停用：{disabled['name']}（工號 {disabled['emp_no']}），該員工將無法再通報。" if disabled
                               else "查無此工號的已核准員工。")
                elif cmd == "通報群組":
                    current_group = get_rescue_group_id()
                    reply_text(event.reply_token,
                               f"目前救護通報群組：{current_group}" if current_group
                               else "尚未設定通報群組。請管理者在要接收通知的群組輸入「設定通報群組」。")
                elif cmd == "管理選單":
                    reply_text(event.reply_token, build_admin_menu())
                elif cmd == "查詢目前事件":
                    reply_text(event.reply_token, query_active_incidents_text())
                elif cmd.startswith("查詢救護事件 "):
                    incident_id = cmd.replace("查詢救護事件 ", "", 1).strip()
                    incident = get_incident_by_id(incident_id)
                    reply_text(event.reply_token, build_historical_report(incident) if incident
                               else "查無此救護事件，請確認事件編號。")
                elif cmd == "結束救護事件":
                    current = list(active_incidents.items())
                    if not current:
                        reply_text(event.reply_token, "目前沒有進行中的救護事件。")
                    elif len(current) > 1:
                        reply_text(
                            event.reply_token,
                            "目前有多件救護事件進行中，請至欲結束事件所屬的群組輸入「結束救護事件」。"
                        )
                    else:
                        target_group_id, incident = current[0]
                        incident_id = incident["id"]
                        if finish_incident(target_group_id, incident_id, "最高管理者人工結束"):
                            reply_text(event.reply_token, f"已立即結束救護事件 {incident_id}。")
                        else:
                            reply_text(event.reply_token, "事件結束失敗，資料庫狀態未變更，請稍後再試。")
                elif cmd == "系統狀態":
                    try:
                        with get_db_connection() as conn:
                            conn.execute("SELECT 1")
                        db_status = "正常"
                    except Exception:
                        logger.exception("系統狀態：資料庫檢查失敗")
                        db_status = "異常（詳細原因請查看伺服器日誌）"
                    reply_text(event.reply_token,
                               f"救護機器人：運作中\nNeon PostgreSQL：{db_status}\n目前記憶體內進行中事件：{len(active_incidents)} 件")
                else:
                    reply_text(event.reply_token, "最高管理者私訊管理模式。輸入「管理選單」查看可用指令。")
            else:
                reply_text(event.reply_token, "本機器人僅接受指定群組救護流程；管理功能僅限最高管理者私訊操作。")
            return

        # 管理者在群組輸入管理指令，一律不執行。
        if is_admin(user_id) and (
            cmd in ("管理選單", "查詢目前事件", "系統狀態")
            or cmd.startswith("查詢救護事件 ")
            or cmd in ("人員管理", "查詢人員名單", "啟動護理人員登記", "關閉護理人員登記",
                       "啟動工安人員登記", "關閉工安人員登記", "重設護理人員名單", "重設工安人員名單")
        ):
            reply_text(event.reply_token, "⛔ 最高管理者管理指令僅能在與救護機器人的一對一私訊中執行。")
            return

        # 在群組輸入「設定通報群組」：把本群組設為員工私訊通報的推播目標（僅最高管理者）。
        if group_id and cmd == "設定通報群組":
            if not is_admin(user_id):
                reply_text(event.reply_token, "⛔ 僅最高管理者可以設定通報群組。")
            else:
                set_config("rescue_group_id", group_id)
                note = ""
                if RESCUE_GROUP_ID_ENV and RESCUE_GROUP_ID_ENV != group_id:
                    note = "\n⚠️ 注意：Render 已設定環境變數 RESCUE_GROUP_ID 且與本群組不同，實際仍以環境變數為準。"
                reply_text(event.reply_token, "✅ 已將本群組設為救護通報群組，員工私訊通報時會通知本群組。" + note)
            return

    if not group_id:
        return

    # ----------------------------------------------------
    # 群組回應按鈕：任何成員可回應，並可隨時修改
    # ----------------------------------------------------
    if isinstance(event, PostbackEvent):
        data = parse_qs(getattr(event.postback, "data", ""))
        action = data.get("action", [""])[0]
        incident_id = data.get("incident_id", [""])[0]

        if action in ("ack_available", "ack_unavailable"):
            incident = active_incidents.get(group_id)
            if not incident or incident["id"] != incident_id:
                reply_text(event.reply_token, "此救護事件已結束或通知已失效。")
                return
            if not user_id:
                reply_text(event.reply_token, "無法辨識回報帳號，請以個人 LINE 帳號操作。")
                return

            status = "available" if action == "ack_available" else "unavailable"
            display_name = get_display_name(group_id, user_id)
            timestamp = now_tw()
            previous = incident["responses"].get(user_id)
            old_status = previous.get("status") if previous else None
            record = {"status": status, "time": timestamp, "display_name": display_name}
            try:
                save_incident_response(incident_id, user_id, record)
            except Exception as error:
                logger.error("回應資料保存失敗：%s；%s", incident_id, error)
                reply_text(event.reply_token, "回應保存失敗，請稍後重新操作。")
                return
            incident["responses"][user_id] = record

            # 成功回報不另外回覆群組；無法前往僅保存狀態與歷程。
            # 可前往時才依原有規則通知群組。
            # 從無法前往改為可前往，也會再次通知最新狀態。
            # 通知文字不變，改用按鈕事件的 Reply 發送（不耗推播額度）；Reply 失敗才改用 Push。
            if status == "available" and old_status != "available":
                notice_message = TextSendMessage(text=f"【救護回應】\n{display_name}：可前往")
                try:
                    line_bot_api.reply_message(event.reply_token, notice_message)
                except Exception as reply_error:
                    logger.warning("可前往群組通知 Reply 失敗，改用 Push：%s", reply_error)
                    try:
                        line_bot_api.push_message(group_id, notice_message)
                    except Exception as error:
                        logger.error("可前往群組通知失敗：%s", error)
            return

        if action == "start_emergency":
            start_emergency_flow(event.reply_token, group_id, user_id,
                                 "目前已有啟動流程正在等待事故地點。")
            return

        # 停用舊版護理/工安登記及結束確認按鈕。
        if action.startswith(("admin_", "register_", "ack_nurse_", "ack_safety_", "confirm_close", "cancel_close")):
            reply_text(event.reply_token, "此為舊版功能按鈕，已停用。請使用最新救護事件流程。")
            return

        if action == "cancel_start":
            pending = pending_locations.get(group_id)
            token = data.get("token", [""])[0]

            if not pending:
                reply_text(event.reply_token, "此取消按鈕已失效，沒有等待中的啟動流程。")
                return

            if not token or token != pending.get("token"):
                reply_text(event.reply_token, "此取消按鈕已失效，可能屬於先前的啟動流程。")
                return

            if user_id != pending.get("initiator") and not is_admin(user_id):
                reply_text(event.reply_token, "⛔ 僅原始啟動者或最高管理者可以取消啟動流程。")
                return

            initiator_name = pending.get("initiator_name", "未知")
            operator_name = get_display_name(group_id, user_id)
            cancelled_at = now_tw()

            # 先清除等待狀態並停止逾時計時；取消不建立事件、不產生流水號。
            pending_locations.pop(group_id, None)
            clear_pending("location", group_id)

            try:
                group_name = line_bot_api.get_group_summary(group_id).group_name
            except Exception:
                group_name = "無法取得群組名稱"

            cancel_notice = (
                "❎【緊急救護啟動已取消】\n"
                f"群組名稱：{group_name}\n"
                f"群組 ID：{group_id}\n"
                f"原始啟動者：{initiator_name}\n"
                f"取消操作者：{operator_name}\n"
                f"取消時間：{time_text(cancelled_at)}\n"
                "事件編號：未建立"
            )
            try:
                line_bot_api.push_message(
                    ADMIN_USER_ID,
                    TextSendMessage(text=cancel_notice)
                )
                logger.info("救護啟動取消通知已私訊最高管理者；群組：%s", group_id)
            except Exception as error:
                logger.error("救護啟動已取消，但最高管理者私訊通知失敗；群組：%s；錯誤：%s", group_id, error)

            # 成功取消不回覆群組；取消按鈕未設定 displayText，避免產生群組文字。
            return

        if action == "cancel_correction":
            pending_corrections.pop(group_id, None)
            clear_pending("correction", group_id)
            reply_text(event.reply_token, "已取消目前事故地點修正流程。")
            return

    if not is_text:
        return

    # 結束目前群組的救護事件：僅最高管理者或該事件啟動者可操作。
    if cmd == "結束救護事件":
        incident = active_incidents.get(group_id)
        if not incident:
            reply_text(event.reply_token, "目前此群組沒有進行中的救護事件。")
        elif is_admin(user_id):
            if finish_incident(group_id, incident["id"], "最高管理者人工結束", notify_group=False):
                reply_text(event.reply_token, f"已立即結束救護事件 {incident['id']}。")
            else:
                reply_text(event.reply_token, "事件結束失敗，資料庫狀態未變更，請稍後再試。")
        elif user_id and user_id == incident.get("initiator"):
            if finish_incident(group_id, incident["id"], "事件啟動者人工結束", notify_group=False):
                reply_text(event.reply_token, f"已立即結束救護事件 {incident['id']}。")
            else:
                reply_text(event.reply_token, "事件結束失敗，資料庫狀態未變更，請稍後再試。")
        else:
            reply_text(event.reply_token, "⛔ 僅最高管理者或本次救護事件啟動者可以結束事件。")
        return

    # 119：直接啟動，要求輸入事故地點
    if cmd == "119":
        start_emergency_flow(event.reply_token, group_id, user_id,
                             "目前已有啟動流程正在等待事故地點，請直接輸入事故地點。")
        return

    # 隨時重新叫出目前事件的救護回報卡片，避免原卡片被群組訊息洗版。
    if cmd == "救護回報":
        incident = active_incidents.get(group_id)
        if not incident:
            reply_text(event.reply_token, "目前沒有進行中的救護事件，無法提供回報按鈕。")
        else:
            line_bot_api.reply_message(
                event.reply_token,
                response_message(incident["id"])
            )
        return

    # 回報目前狀況
    if cmd == "回報狀況":
        incident = active_incidents.get(group_id)
        reply_text(event.reply_token, incident_status_text(incident) if incident else "目前沒有進行中的救護事件。")
        return

    # 事故地點修正流程
    if cmd == "修改事故地點":
        incident = active_incidents.get(group_id)
        if not incident:
            reply_text(event.reply_token, "目前沒有進行中的救護事件，無法修改事故地點。")
        elif group_id in pending_corrections:
            reply_text(event.reply_token, "目前已有事故地點修正流程，請先完成或取消。")
        else:
            pending = {"created_at": now_tw(), "incident_id": incident["id"]}
            pending_corrections[group_id] = pending
            schedule_timeout("correction", group_id, pending["created_at"])
            reply_text(event.reply_token, "請直接輸入新的事故地點；5 分鐘內未收到內容將自動取消。")
        return

    if group_id in pending_corrections:
        if expire_if_needed("correction", group_id):
            reply_text(event.reply_token, "事故地點修正已逾時取消，維持原事故地點。")
            return
        pending = pending_corrections.pop(group_id)
        clear_pending("correction", group_id)
        new_location = clip_location(text)
        incident = active_incidents.get(group_id)
        if incident and incident["id"] == pending["incident_id"]:
            if not update_incident_location(incident["id"], new_location, get_display_name(group_id, user_id)):
                reply_text(event.reply_token, "事故地點更新失敗，原地點維持不變，請稍後再試。")
            else:
                old_location = incident["location"]
                incident["location"] = new_location
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text=f"📍【事故地點更正】\n原事故地點：{old_location}\n最新事故地點：{new_location}\n請所有救護人員以最新地點為準。")
                )
        else:
            reply_text(event.reply_token, "原救護事件已不存在，本次地點修正未套用。")
        return

    # 等待初始事故地點：第一筆文字直接建立事件
    if group_id in pending_locations:
        if expire_if_needed("location", group_id):
            reply_text(event.reply_token, "緊急救護啟動流程已逾時取消，請重新輸入 119。")
            return
        pending = pending_locations.pop(group_id)
        clear_pending("location", group_id)

        incident_id = next_incident_id()
        incident = {
            "id": incident_id, "location": clip_location(text),
            "initiator": pending["initiator"],
            "initiator_name": pending["initiator_name"],
            "created_at": now_tw(), "responses": {}
        }
        try:
            create_incident_record(incident, group_id)
        except Exception as error:
            logger.error("救護事件寫入資料庫失敗：%s", error)
            reply_text(event.reply_token, "⚠️ 救護事件資料保存失敗，未發送正式通知；請稍後重新啟動。")
            return

        active_incidents[group_id] = incident
        schedule_incident_timer(group_id, incident)
        notice = "🚨 緊急救護通知，請回報支援狀況。"
        reply_emergency_notice(event.reply_token, notice, incident_id, group_id)
        return


def process_events(events):
    """依序處理一批事件；單一事件失敗不影響其餘事件。"""
    for event in events:
        try:
            with state_lock:
                handle_event(event)
        except Exception:
            logger.exception("事件處理失敗")


def already_processed(event):
    """Webhook 去重：LINE 重送（redelivery）或重複送達的同一事件只處理一次。"""
    key = (getattr(event, "webhook_event_id", None)
           or getattr(event, "webhookEventId", None)
           or getattr(event, "reply_token", None))
    if not key:
        return False
    with _processed_lock:
        if key in _processed_ids:
            return True
        _processed_ids[key] = True
        while len(_processed_ids) > DEDUP_CACHE_SIZE:
            _processed_ids.popitem(last=False)
    return False


async def event_worker():
    """依 Webhook 到達順序逐批處理事件，確保「119」與其後的事故地點不會被亂序處理。"""
    while True:
        events = await event_queue.get()
        try:
            await asyncio.to_thread(process_events, events)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("事件批次處理失敗")
        finally:
            event_queue.task_done()


# ============================================================
# PostgreSQL 啟動初始化及狀態還原
# ============================================================
def restore_state():
    """（同步）初始化資料庫、還原進行中事件並重新排程計時器。"""
    init_database()
    load_persistent_state()
    logger.info("PostgreSQL 已連線；已還原進行中救護事件 %d 件。", len(active_incidents))
    for group_id, incident in list(active_incidents.items()):
        created_at = incident["created_at"]
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        elapsed = (now_tw() - created_at.astimezone(TAIWAN_TZ)).total_seconds()
        if elapsed >= INCIDENT_AUTO_CLOSE_SECONDS:
            finish_incident(group_id, incident["id"], "系統自動結束")
        else:
            schedule_incident_timer(group_id, incident)


@asynccontextmanager
async def lifespan(_app):
    global main_loop, event_queue
    main_loop = asyncio.get_running_loop()
    event_queue = asyncio.Queue()
    # 若 DATABASE_URL、資料庫連線或資料表初始化失敗，讓 Render 明確顯示啟動失敗。
    await asyncio.to_thread(open_db_pool)
    await asyncio.to_thread(restore_state)
    worker = asyncio.create_task(event_worker())
    try:
        yield
    finally:
        worker.cancel()
        for future in list(timeout_tasks.values()) + list(incident_timer_tasks.values()):
            _cancel_future(future)
        await asyncio.to_thread(close_db_pool)


app = FastAPI(lifespan=lifespan)


# ============================================================
# 網頁及 LINE Webhook
# ============================================================
@app.get("/")
def home():
    return {"status": "Line Bot is running!"}


@app.post("/callback")
async def callback(request: Request):
    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()
    try:
        events = parser.parse(body.decode("utf-8"), signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature.")

    # 驗簽通過後立即放入佇列並回應 200，實際處理由背景工作依序執行，
    # 避免 LINE 因等待逾時而重送 Webhook。
    fresh = [event for event in events if not already_processed(event)]
    if fresh:
        event_queue.put_nowait(fresh)
    return "OK"
