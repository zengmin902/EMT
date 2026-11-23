import os
import asyncio
import json
import urllib.request
from datetime import datetime, timezone, timedelta
from urllib.parse import parse_qs

import psycopg
from psycopg.rows import dict_row

from fastapi import FastAPI, HTTPException, Request
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent,
    PostbackEvent,
    JoinEvent,
    TextMessage,
    TextSendMessage,
    FlexSendMessage,
)

app = FastAPI()

# ============================================================
# LINE 機器人設定
# ============================================================
CHANNEL_ACCESS_TOKEN = os.environ.get("CHANNEL_ACCESS_TOKEN", "")
CHANNEL_SECRET = os.environ.get("CHANNEL_SECRET", "")

# 唯一最高管理者：LINE Developers > Basic settings > Your user ID
ADMIN_USER_ID = "Uf2f7fe99986289b57f9da61494cfcd81"

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(CHANNEL_SECRET)

TAIWAN_TZ = timezone(timedelta(hours=8))
WAIT_SECONDS = 5 * 60

# 救護事件提醒設定
INCIDENT_REMINDER_SECONDS = 60 * 60       # 1小時提醒
INCIDENT_AUTO_CLOSE_SECONDS = 60 * 60      # 提醒後再等1小時，自動結束

# ============================================================
# 暫存資料
# ============================================================
# 等待中的5分鐘輸入流程僅暫存於記憶體；正式事件及名單均保存於 PostgreSQL。
pending_locations = {}
pending_corrections = {}
active_incidents = {}
pending_closures = {}
timeout_tasks = {}

# 每個群組各自的救護事件定時任務
incident_timer_tasks = {}

# ============================================================
# 人員登記資料由 PostgreSQL 還原至記憶體快取；所有群組共用同一份名單。
# ============================================================
# nurse_users[1..3] = {"user_id": "...", "display_name": "..."}
nurse_users = {}
nurse_registration_open = False

# safety_users[user_id] = {"user_id": "...", "display_name": "..."}
safety_users = {}
safety_registration_open = False

# 每日事件流水號由 PostgreSQL 永久保存；格式 YYYYMMDD01。


def get_db_connection():
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("Render 尚未設定 DATABASE_URL，服務停止啟動以避免資料僅存在記憶體。")
    return psycopg.connect(database_url, row_factory=dict_row, connect_timeout=10)


def init_database():
    """建立所需資料表；資料庫無法使用時直接讓服務啟動失敗。"""
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
    ]
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
            cur.execute(
                "INSERT INTO app_settings (setting_key, setting_value) VALUES "
                "('nurse_registration_open', FALSE), ('safety_registration_open', FALSE) "
                "ON CONFLICT (setting_key) DO NOTHING"
            )


def set_app_setting(key, value):
    with get_db_connection() as conn:
        conn.execute(
            "INSERT INTO app_settings (setting_key, setting_value, updated_at) "
            "VALUES (%s, %s, NOW()) ON CONFLICT (setting_key) DO UPDATE "
            "SET setting_value = EXCLUDED.setting_value, updated_at = NOW()",
            (key, bool(value)),
        )


def save_nurse_record(slot, record):
    with get_db_connection() as conn:
        conn.execute(
            "INSERT INTO nurse_roster (slot, user_id, display_name, updated_at) "
            "VALUES (%s, %s, %s, NOW()) ON CONFLICT (slot) DO UPDATE SET "
            "user_id = EXCLUDED.user_id, display_name = EXCLUDED.display_name, updated_at = NOW()",
            (slot, record["user_id"], record.get("display_name") or "群組成員"),
        )


def delete_nurse_record(slot):
    with get_db_connection() as conn:
        conn.execute("DELETE FROM nurse_roster WHERE slot = %s", (slot,))


def clear_nurse_records():
    with get_db_connection() as conn:
        conn.execute("DELETE FROM nurse_roster")


def save_safety_record(user_id, record):
    with get_db_connection() as conn:
        conn.execute(
            "INSERT INTO safety_roster (user_id, display_name, updated_at) "
            "VALUES (%s, %s, NOW()) ON CONFLICT (user_id) DO UPDATE SET "
            "display_name = EXCLUDED.display_name, updated_at = NOW()",
            (user_id, record.get("display_name") or "工安人員"),
        )


def delete_safety_record(user_id):
    with get_db_connection() as conn:
        conn.execute("DELETE FROM safety_roster WHERE user_id = %s", (user_id,))


def clear_safety_records():
    with get_db_connection() as conn:
        conn.execute("DELETE FROM safety_roster")


def create_incident_record(incident, group_id):
    with get_db_connection() as conn:
        conn.execute(
            "INSERT INTO incidents (incident_id, group_id, location, initiator, "
            "initiator_name, created_at, status) VALUES (%s, %s, %s, %s, %s, %s, 'active')",
            (incident["id"], group_id, incident["location"], incident["initiator"],
             incident["initiator_name"], incident["created_at"]),
        )


def save_incident_response(incident_id, user_id, record):
    with get_db_connection() as conn:
        row = conn.execute(
            "INSERT INTO incident_responses "
            "(incident_id, user_id, role, status, responded_at, display_name) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (incident_id, user_id) DO NOTHING "
            "RETURNING user_id",
            (incident_id, user_id, record["role"], record["status"],
             record["time"], record.get("display_name") or "群組成員"),
        ).fetchone()
        return row is not None


def update_incident_location(incident_id, location):
    with get_db_connection() as conn:
        cur = conn.execute(
            "UPDATE incidents SET location = %s WHERE incident_id = %s AND status = 'active'",
            (location, incident_id),
        )
        return cur.rowcount == 1


def close_incident_record(incident_id, status):
    with get_db_connection() as conn:
        cur = conn.execute(
            "UPDATE incidents SET status = %s, ended_at = NOW() "
            "WHERE incident_id = %s AND status = 'active'",
            (status, incident_id),
        )
        return cur.rowcount == 1


def load_persistent_state():
    """載入全域人員名單、登記開關及所有進行中事件與回報。"""
    global nurse_registration_open, safety_registration_open
    with get_db_connection() as conn:
        settings = {r["setting_key"]: r["setting_value"] for r in conn.execute(
            "SELECT setting_key, setting_value FROM app_settings"
        ).fetchall()}
        nurses = conn.execute("SELECT slot, user_id, display_name FROM nurse_roster ORDER BY slot").fetchall()
        safety = conn.execute("SELECT user_id, display_name FROM safety_roster ORDER BY updated_at, user_id").fetchall()
        incidents = conn.execute(
            "SELECT incident_id, group_id, location, initiator, initiator_name, created_at "
            "FROM incidents WHERE status = 'active' ORDER BY created_at"
        ).fetchall()
        responses = conn.execute(
            "SELECT r.incident_id, r.user_id, r.role, r.status, r.responded_at, r.display_name "
            "FROM incident_responses r JOIN incidents i ON i.incident_id = r.incident_id "
            "WHERE i.status = 'active' ORDER BY r.responded_at"
        ).fetchall()

    nurse_users.clear()
    for row in nurses:
        nurse_users[int(row["slot"])] = {"user_id": row["user_id"], "display_name": row["display_name"]}
    safety_users.clear()
    for row in safety:
        safety_users[row["user_id"]] = {"user_id": row["user_id"], "display_name": row["display_name"]}
    nurse_registration_open = bool(settings.get("nurse_registration_open", False))
    safety_registration_open = bool(settings.get("safety_registration_open", False))

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
                "role": row["role"], "status": row["status"],
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


def get_group_nurses(group_id=None):
    """取得全域護理人員名單，依護理人員 1、2、3 排序。"""
    return [
        (slot, nurse_users[slot])
        for slot in (1, 2, 3)
        if slot in nurse_users
    ]


def get_next_nurse_slot():
    """回傳第一個空缺名額；沒有空缺時回傳 None。"""
    for slot in (1, 2, 3):
        if slot not in nurse_users:
            return slot
    return None


def is_nurse(user_id):
    return any(record.get("user_id") == user_id for record in nurse_users.values())


def is_safety_staff(user_id):
    return bool(user_id) and user_id in safety_users


def get_response_role(user_id):
    """護理人員優先；同時具兩種資格者只視為護理人員。"""
    if is_nurse(user_id):
        return "nurse"
    if is_safety_staff(user_id):
        return "safety"
    return None


def nurse_registration_status_text():
    lines = ["📋【護理人員登記狀況】"]
    for slot in (1, 2, 3):
        record = nurse_users.get(slot)
        lines.append(
            f"護理人員{slot}：{record.get('display_name') or '已登記'}"
            if record else f"護理人員{slot}：尚未登記"
        )
    lines.append("登記模式：開啟" if nurse_registration_open else "登記模式：關閉")
    if nurse_registration_open:
        next_slot = get_next_nurse_slot()
        lines.append(f"下一個登記名額：護理人員{next_slot}" if next_slot else "三名額滿")
    return "\n".join(lines)


def safety_registration_status_text():
    lines = ["📋【工安人員登記狀況】", f"已登記人數：{len(safety_users)} 人"]
    lines.append("登記模式：開啟" if safety_registration_open else "登記模式：關閉")
    if safety_users:
        for index, record in enumerate(safety_users.values(), 1):
            lines.append(f"{index}. {record.get('display_name') or '工安人員'}")
    else:
        lines.append("目前尚無登記人員")
    return "\n".join(lines)


# ============================================================
# 一般等待逾時處理
# ============================================================
def schedule_timeout(kind, group_id, created_at):
    key = (kind, group_id)
    old_task = timeout_tasks.get(key)

    if old_task and not old_task.done():
        old_task.cancel()

    async def wait_and_expire():
        try:
            await asyncio.sleep(WAIT_SECONDS)

            current = (
                pending_locations
                if kind == "location"
                else pending_corrections
            )

            item = current.get(group_id)

            if item and item["created_at"] == created_at:
                current.pop(group_id, None)
                timeout_tasks.pop(key, None)

                try:
                    if kind == "location":
                        line_bot_api.push_message(
                            group_id,
                            TextSendMessage(
                                text=(
                                    "⌛ 已超過 5 分鐘未收到事故地點，"
                                    "緊急救護啟動流程已自動取消。"
                                )
                            )
                        )
                    else:
                        line_bot_api.push_message(
                            group_id,
                            TextSendMessage(
                                text=(
                                    "⌛ 已超過 5 分鐘未收到新的事故地點，"
                                    "本次修正已自動取消，維持原事故地點。"
                                )
                            )
                        )
                except Exception as error:
                    print(f"等待流程逾時通知失敗：{error}")

        except asyncio.CancelledError:
            return

    try:
        timeout_tasks[key] = asyncio.create_task(wait_and_expire())
    except RuntimeError:
        pass


def expire_if_needed(kind, group_id):
    current = (
        pending_locations
        if kind == "location"
        else pending_corrections
    )

    item = current.get(group_id)

    if not item:
        return False

    if (now_tw() - item["created_at"]).total_seconds() >= WAIT_SECONDS:
        current.pop(group_id, None)

        task = timeout_tasks.pop((kind, group_id), None)

        if task and not task.done():
            task.cancel()

        return True

    return False


# ============================================================
# LINE 訊息工具
# ============================================================
def flex_message(alt_text, contents):
    return FlexSendMessage(
        alt_text=alt_text,
        contents=contents
    )


def postback(label, data, display_text=None):
    return {
        "type": "postback",
        "label": label,
        "data": data,
        "displayText": display_text or label,
    }


def panel_message():
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
                    "text": "廠區緊急救護面板",
                    "weight": "bold",
                    "size": "lg"
                },
                {
                    "type": "text",
                    "text": "發生緊急狀況時，請點選下方按鈕啟動救護通知。",
                    "wrap": True,
                    "size": "sm",
                    "color": "#666666"
                },
                {
                    "type": "button",
                    "style": "primary",
                    "color": "#D93025",
                    "height": "md",
                    "action": postback(
                        "🚨 緊急救護",
                        "action=start_emergency"
                    )
                }
            ]
        }
    }

    return flex_message("廠區緊急救護面板", contents)


def registration_prompt_message(role):
    if role == "nurse":
        title = "護理人員登記"
        description = "目前開放護理人員登記，名額依序安排護理人員1、2、3。登記後立即生效。"
        label = "我要登記為護理人員"
        action = "register_nurse"
    else:
        title = "工安人員登記"
        description = "目前開放工安人員登記。登記後立即生效，僅限實際工安人員操作。"
        label = "我要登記為工安人員"
        action = "register_safety"
    contents = {
        "type": "bubble",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
            {"type": "text", "text": title, "weight": "bold", "size": "lg"},
            {"type": "text", "text": description, "wrap": True, "size": "sm"},
            {"type": "button", "style": "primary", "height": "md", "action": postback(label, f"action={action}")}
        ]}
    }
    return flex_message(title, contents)


def admin_roster_message():
    contents = [
        {"type": "text", "text": "人員登記管理", "weight": "bold", "size": "lg"},
        {"type": "text", "text": "最高管理者專用；護理人員與工安人員名單全域共用。", "wrap": True, "size": "xs", "color": "#666666"},
        {"type": "separator", "margin": "md"},
        {"type": "text", "text": "護理人員登記：" + ("開啟" if nurse_registration_open else "關閉"), "weight": "bold", "size": "sm"},
        {"type": "button", "style": "primary", "action": postback("開啟護理人員登記", "action=admin_nurse_open")},
        {"type": "button", "style": "secondary", "action": postback("關閉護理人員登記", "action=admin_nurse_close")},
        {"type": "text", "text": "工安人員登記：" + ("開啟" if safety_registration_open else "關閉"), "weight": "bold", "size": "sm", "margin": "md"},
        {"type": "button", "style": "primary", "action": postback("開啟工安人員登記", "action=admin_safety_open")},
        {"type": "button", "style": "secondary", "action": postback("關閉工安人員登記", "action=admin_safety_close")},
        {"type": "separator", "margin": "md"},
        {"type": "text", "text": "護理人員名單", "weight": "bold", "size": "sm"},
    ]
    for slot in (1, 2, 3):
        rec = nurse_users.get(slot)
        label = f"護理人員{slot}：{rec.get('display_name')}" if rec else f"護理人員{slot}：尚未登記"
        contents.append({"type": "text", "text": label, "size": "sm", "wrap": True})
        if rec:
            contents.append({"type": "button", "style": "secondary", "height": "sm", "action": postback(f"刪除護理人員{slot}", f"action=admin_delete_nurse&slot={slot}")})
    contents.extend([
        {"type": "separator", "margin": "md"},
        {"type": "text", "text": f"工安人員名單（{len(safety_users)}人）", "weight": "bold", "size": "sm"},
    ])
    for index, (uid, rec) in enumerate(safety_users.items()):
        if index >= 10:
            contents.append({"type": "text", "text": "其餘工安人員請使用「查詢工安人員」及「刪除工安人員 顯示名稱」指令管理。", "size": "xs", "wrap": True, "color": "#666666"})
            break
        contents.append({"type": "text", "text": rec.get("display_name") or "工安人員", "size": "sm", "wrap": True})
        contents.append({"type": "button", "style": "secondary", "height": "sm", "action": postback("刪除此工安人員", f"action=admin_delete_safety&target={uid}")})
    contents.append({"type": "button", "style": "secondary", "action": postback("重新整理名單", "action=admin_roster")})
    return flex_message("人員登記管理", {"type": "bubble", "body": {"type": "box", "layout": "vertical", "spacing": "sm", "contents": contents}})


def cancel_start_message():
    contents = {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {
                    "type": "text",
                    "text": "請在群組輸入事故地點",
                    "weight": "bold",
                    "size": "lg"
                },
                {
                    "type": "text",
                    "text": (
                        "任何群組成員皆可輸入。收到第一筆有效地點文字後，"
                        "將立即發送緊急救護通知。等待時間為 5 分鐘。"
                    ),
                    "wrap": True,
                    "size": "sm"
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "height": "sm",
                    "action": postback(
                        "取消啟動",
                        "action=cancel_start"
                    )
                }
            ]
        }
    }

    return flex_message("請輸入事故地點或取消啟動", contents)


def cancel_correction_message():
    contents = {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {
                    "type": "text",
                    "text": "請輸入新的事故地點",
                    "weight": "bold",
                    "size": "lg"
                },
                {
                    "type": "text",
                    "text": (
                        "任何群組成員皆可提供新地點；"
                        "5 分鐘內未收到內容將自動取消。"
                    ),
                    "wrap": True,
                    "size": "sm"
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "height": "sm",
                    "action": postback(
                        "取消修正",
                        "action=cancel_correction"
                    )
                }
            ]
        }
    }

    return flex_message("請輸入新事故地點或取消修正", contents)


def response_message_contents(incident_id):
    """同一張 Flex 卡片分成護理人員及工安人員兩個獨立回報區。"""
    def response_buttons(role, available_label, unavailable_label):
        return [
            {
                "type": "button", "style": "primary", "color": "#16834A",
                "height": "md",
                "action": postback(
                    available_label,
                    f"action=ack_{role}_available&incident_id={incident_id}"
                )
            },
            {
                "type": "button", "style": "primary", "color": "#B3261E",
                "height": "md",
                "action": postback(
                    unavailable_label,
                    f"action=ack_{role}_unavailable&incident_id={incident_id}"
                )
            },
        ]

    contents = [
        {"type": "text", "text": "救護人員回報", "weight": "bold", "size": "lg"},
        {"type": "text", "text": "請依登記身分選擇專屬回報區；每人僅記錄第一次回報。", "wrap": True, "size": "sm"},
        {"type": "separator", "margin": "md"},
        {"type": "text", "text": "護理人員回報（已登記護理人員專用）", "weight": "bold", "size": "md", "color": "#16834A"},
    ]
    contents.extend(response_buttons("nurse", "收到，可以前往", "收到，但無法前往"))
    contents.extend([
        {"type": "separator", "margin": "md"},
        {"type": "text", "text": "工安人員回報（已登記工安人員專用）", "weight": "bold", "size": "md", "color": "#2563EB"},
    ])
    contents.extend(response_buttons("safety", "收到，可以前往支援", "收到，但無法前往支援"))
    return {
        "type": "bubble",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": contents}
    }


def response_message(incident_id):
    return flex_message(
        "請回報是否可以前往支援",
        response_message_contents(incident_id)
    )


# ============================================================
# LINE 群組真正的 @All 通知
# ============================================================
def reply_emergency_notice(reply_token, notice, incident_id, group_id):
    """
    優先以 Reply 發送真正 @All、群組內已登記護理人員 Mention 及回報卡片。
    先確認護理人員確實屬於本次事件群組，避免跨群組 Mention 導致整筆訊息 HTTP 400。
    Reply 失敗後改用 Push 補送，絕不重複使用同一個 reply_token。
    """
    substitutions = {
        "everyone": {
            "type": "mention",
            "mentionee": {"type": "all"}
        }
    }

    mention_tokens = []
    for slot, record in get_group_nurses():
        user_id = record.get("user_id")
        if not user_id:
            continue

        # 全域名單不代表每位人員都是本次事件群組的成員。
        # 只有確認屬於目前群組，才加入 Mention，避免 LINE API 拒絕整筆訊息。
        try:
            line_bot_api.get_group_member_profile(group_id, user_id)
        except Exception as member_error:
            print(
                f"本群組未能確認護理人員 {record.get('display_name') or user_id} "
                f"為群組成員，略過該員 Mention：{member_error}"
            )
            continue

        token_name = f"nurse{slot}"
        mention_tokens.append("{" + token_name + "}")
        substitutions[token_name] = {
            "type": "mention",
            "mentionee": {
                "type": "user",
                "userId": user_id
            }
        }

    mention_line = " ".join(mention_tokens)
    text_v2 = "{everyone}\n"
    if mention_line:
        text_v2 += mention_line + "\n"
    text_v2 += notice

    flex_contents = response_message_contents(incident_id)
    reply_payload = {
        "replyToken": reply_token,
        "messages": [
            {
                "type": "textV2",
                "text": text_v2,
                "substitution": substitutions
            },
            {
                "type": "flex",
                "altText": "請回報是否可以前往支援",
                "contents": flex_contents
            }
        ]
    }

    reply_request = urllib.request.Request(
        "https://api.line.me/v2/bot/message/reply",
        data=json.dumps(reply_payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(reply_request, timeout=10) as response:
            response.read()

        print(
            "緊急救護通知已發送：真正的 @All，"
            f"並 Mention 本群組內 {len(mention_tokens)} 名已登記護理人員。"
        )
        return

    except Exception as error:
        # 記錄 LINE 回傳的 HTTP 狀態及錯誤本文，方便追查真正原因。
        error_body = ""
        if hasattr(error, "read"):
            try:
                error_body = error.read().decode("utf-8", errors="replace")
            except Exception:
                error_body = ""
        error_status = getattr(error, "code", "未知")
        print(
            f"LINE 緊急救護 Reply 發送失敗；HTTP 狀態：{error_status}；"
            f"錯誤內容：{error_body or str(error)}"
        )

    # Reply Token 僅能使用一次；第一次 Reply 失敗後，不可再次拿它呼叫 reply_message。
    # 改用 Push API 發送 @All 及回報卡片，不依賴原 reply_token。
    push_payload = {
        "to": group_id,
        "messages": [
            {
                "type": "textV2",
                "text": "{everyone}\n" + notice,
                "substitution": {
                    "everyone": {
                        "type": "mention",
                        "mentionee": {"type": "all"}
                    }
                }
            },
            {
                "type": "flex",
                "altText": "請回報是否可以前往支援",
                "contents": flex_contents
            }
        ]
    }

    push_request = urllib.request.Request(
        "https://api.line.me/v2/bot/message/push",
        data=json.dumps(push_payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(push_request, timeout=10) as response:
            response.read()
        print("緊急救護通知已改用 Push API 補送：@All 及回報卡片。")
        return

    except Exception as push_error:
        push_body = ""
        if hasattr(push_error, "read"):
            try:
                push_body = push_error.read().decode("utf-8", errors="replace")
            except Exception:
                push_body = ""
        push_status = getattr(push_error, "code", "未知")
        print(
            f"Push @All 補送失敗；HTTP 狀態：{push_status}；"
            f"錯誤內容：{push_body or str(push_error)}"
        )

    # 最後一道保底：即使 @All 格式也失敗，仍嘗試送出一般群組通知及回報卡片。
    try:
        line_bot_api.push_message(
            group_id,
            [
                TextSendMessage(text=notice),
                response_message(incident_id)
            ]
        )
        print("一般緊急救護通知及回報卡片已透過 Push API 補送。")
    except Exception as fallback_error:
        print(f"一般 Push 通知補送仍失敗：{fallback_error}")


# ============================================================
# 新增：指定原始啟動人員 @Mention 通知
# ============================================================
def push_initiator_mention(group_id, user_id, message, display_name):
    """
    在原救護事件群組中，真正 @ 指定的原始啟動人員。
    不使用 @All。
    """
    payload = {
        "to": group_id,
        "messages": [
            {
                "type": "textV2",
                "text": "{initiator}\n" + message,
                "substitution": {
                    "initiator": {
                        "type": "mention",
                        "mentionee": {
                            "type": "user",
                            "userId": user_id
                        }
                    }
                }
            }
        ]
    }

    request = urllib.request.Request(
        "https://api.line.me/v2/bot/message/push",
        data=json.dumps(
            payload,
            ensure_ascii=False
        ).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()

        print(
            f"救護事件1小時提醒已發送，"
            f"指定人員：{display_name}"
        )

    except Exception as error:
        print(f"指定人員 @Mention 發送失敗：{error}")

        # API 失敗時，至少補送一般群組文字通知
        try:
            line_bot_api.push_message(
                group_id,
                TextSendMessage(
                    text=(
                        f"🔔 {display_name}，救護事件尚未結束，"
                        "請確認目前處理進度。"
                    )
                )
            )
        except Exception as fallback_error:
            print(f"1小時提醒補送失敗：{fallback_error}")


# ============================================================
# 新增：救護事件定時提醒及自動結束
# ============================================================
def cancel_incident_timer(group_id):
    """取消指定群組目前的救護事件計時任務。"""
    task = incident_timer_tasks.pop(group_id, None)

    if task and not task.done():
        task.cancel()
        print(f"已取消群組 {group_id} 的救護事件計時任務。")


def schedule_incident_timer(group_id, incident):
    """依事件原始建立時間計算1小時提醒及2小時自動結束，不因重啟而重設。"""
    cancel_incident_timer(group_id)
    incident_id = incident["id"]
    initiator = incident["initiator"]
    initiator_name = incident["initiator_name"]
    created_at = incident["created_at"]
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)

    async def wait_and_process():
        try:
            elapsed = (now_tw() - created_at.astimezone(TAIWAN_TZ)).total_seconds()
            reminder_wait = max(0, INCIDENT_REMINDER_SECONDS - elapsed)
            await asyncio.sleep(reminder_wait)

            current = active_incidents.get(group_id)
            if not current or current["id"] != incident_id:
                return

            elapsed = (now_tw() - created_at.astimezone(TAIWAN_TZ)).total_seconds()
            if elapsed < INCIDENT_REMINDER_SECONDS:
                await asyncio.sleep(INCIDENT_REMINDER_SECONDS - elapsed)
                current = active_incidents.get(group_id)
                if not current or current["id"] != incident_id:
                    return

            reminder_text = (
                "⏰【救護事件尚未結束提醒】\n"
                f"事件編號：{incident_id}\n"
                f"事故地點：{current['location']}\n"
                "本救護事件已啟動1小時，目前仍未結束，"
                "請確認處理進度，並於完成後執行結束操作。"
            )
            push_initiator_mention(group_id, initiator, reminder_text, initiator_name)

            elapsed = (now_tw() - created_at.astimezone(TAIWAN_TZ)).total_seconds()
            await asyncio.sleep(max(0, INCIDENT_REMINDER_SECONDS + INCIDENT_AUTO_CLOSE_SECONDS - elapsed))

            current = active_incidents.get(group_id)
            if not current or current["id"] != incident_id:
                return

            # 先寫入資料庫，成功後才從記憶體移除。
            if close_incident_record(incident_id, "auto_closed"):
                active_incidents.pop(group_id, None)
                pending_closures.pop(group_id, None)
                auto_close_text = (
                    "⏰【救護事件自動結束公告】\n"
                    f"{incident_status_text(current, include_unavailable=True)}\n"
                    "本救護事件啟動已滿2小時，"
                    "因尚未執行人工結束操作，系統已自動結束本次事件。\n"
                    "如仍有救護需求，請重新啟動緊急救護流程。"
                )
                try:
                    line_bot_api.push_message(group_id, TextSendMessage(text=auto_close_text))
                    print(f"救護事件已自動結束：{incident_id}")
                except Exception as error:
                    print(f"自動結束群組公告發送失敗：{error}")

        except asyncio.CancelledError:
            print(f"救護事件計時任務已停止：{incident_id}")
            return
        except Exception as error:
            print(f"救護事件計時任務發生錯誤：{incident_id}，{error}")
        finally:
            current_task = asyncio.current_task()
            if incident_timer_tasks.get(group_id) is current_task:
                incident_timer_tasks.pop(group_id, None)

    incident_timer_tasks[group_id] = asyncio.create_task(wait_and_process())
    print(f"已建立救護事件計時任務：{incident_id}，依原始啟動時間計算1小時提醒、2小時自動結束。")


# ============================================================
# 救護事件結束確認
# ============================================================
def close_confirmation_message(incident_id, status_text):
    contents = {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {
                    "type": "text",
                    "text": "確認結束救護事件？",
                    "weight": "bold",
                    "size": "lg"
                },
                {
                    "type": "text",
                    "text": status_text,
                    "wrap": True,
                    "size": "sm"
                },
                {
                    "type": "button",
                    "style": "primary",
                    "color": "#D93025",
                    "height": "md",
                    "action": postback(
                        "確認結束",
                        f"action=confirm_close&incident_id={incident_id}"
                    )
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "height": "md",
                    "action": postback(
                        "取消",
                        f"action=cancel_close&incident_id={incident_id}"
                    )
                }
            ]
        }
    }

    return flex_message("請確認是否結束救護事件", contents)


def incident_status_text(incident, include_unavailable=False):
    """護理人員與工安人員分開統計；進行中僅顯示可出動資訊，結束時顯示完整統計。"""
    groups = {
        "nurse": {"available": [], "unavailable": []},
        "safety": {"available": [], "unavailable": []},
    }
    for record in incident["responses"].values():
        role = record.get("role")
        if role not in groups:
            continue
        name = record.get("display_name") or "群組成員"
        entry = f"{name}｜{time_text(record['time'])}"
        groups[role][record["status"]].append(entry)

    lines = [
        "📋【緊急救護即時回報狀況】",
        f"事件編號：{incident['id']}",
        f"事故地點：{incident['location']}",
        "",
        "【護理人員回報】",
        f"已回報：{sum(len(v) for v in groups['nurse'].values())} 人",
        f"可以前往：{len(groups['nurse']['available'])} 人",
        "可以前往名單：" + ("、".join(groups['nurse']['available']) or "目前無"),
    ]
    if include_unavailable:
        lines.extend([
            f"無法前往：{len(groups['nurse']['unavailable'])} 人",
            "無法前往名單：" + ("、".join(groups['nurse']['unavailable']) or "目前無"),
        ])

    lines.extend([
        "",
        "【工安人員回報】",
        f"已回報：{sum(len(v) for v in groups['safety'].values())} 人",
        f"可以前往：{len(groups['safety']['available'])} 人",
        "可以前往名單：" + ("、".join(groups['safety']['available']) or "目前無"),
    ])
    if include_unavailable:
        lines.extend([
            f"無法前往：{len(groups['safety']['unavailable'])} 人",
            "無法前往名單：" + ("、".join(groups['safety']['unavailable']) or "目前無"),
        ])

    return "\n".join(lines)


def reply_text(reply_token, text):
    line_bot_api.reply_message(
        reply_token,
        TextSendMessage(text=text)
    )


# ============================================================
# PostgreSQL 啟動初始化及狀態還原
# ============================================================
@app.on_event("startup")
async def startup_restore_state():
    # 若 DATABASE_URL、資料庫連線或資料表初始化失敗，讓 Render 明確顯示啟動失敗。
    init_database()
    load_persistent_state()
    print(
        "PostgreSQL 已連線；已還原護理人員 "
        f"{len(nurse_users)} 名、工安人員 {len(safety_users)} 名、"
        f"進行中救護事件 {len(active_incidents)} 件。"
    )
    for group_id, incident in list(active_incidents.items()):
        created_at = incident["created_at"]
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        elapsed = (now_tw() - created_at.astimezone(TAIWAN_TZ)).total_seconds()
        total_seconds = INCIDENT_REMINDER_SECONDS + INCIDENT_AUTO_CLOSE_SECONDS
        if elapsed >= total_seconds:
            # 服務停機期間已超過2小時：直接結束，不補發過期的1小時提醒。
            if close_incident_record(incident["id"], "auto_closed"):
                active_incidents.pop(group_id, None)
                pending_closures.pop(group_id, None)
                message = (
                    "⏰【救護事件自動結束公告】\n"
                    f"{incident_status_text(incident, include_unavailable=True)}\n"
                    "本救護事件啟動已滿2小時，因服務重新啟動時已逾自動結束時間，"
                    "系統已自動結束本次事件。\n"
                    "如仍有救護需求，請重新啟動緊急救護流程。"
                )
                try:
                    line_bot_api.push_message(group_id, TextSendMessage(text=message))
                except Exception as error:
                    print(f"重啟後自動結束公告發送失敗：{error}")
        else:
            schedule_incident_timer(group_id, incident)


# ============================================================
# 網頁及 LINE Webhook
# ============================================================
@app.get("/")
def home():
    return {"status": "Line Bot is running!"}


@app.post("/callback")
async def callback(request: Request):
    global nurse_registration_open, safety_registration_open
    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()
    body_text = body.decode("utf-8")

    try:
        events = parser.parse(body_text, signature)

    except InvalidSignatureError:
        raise HTTPException(
            status_code=400,
            detail="Invalid signature."
        )

    for event in events:
        group_id = get_group_id(event)

        # ====================================================
        # 加入群組時，自動發送緊急救護面板
        # ====================================================
        if isinstance(event, JoinEvent) and group_id:
            line_bot_api.reply_message(
                event.reply_token,
                panel_message()
            )
            continue

        if not group_id:
            if (
                isinstance(event, MessageEvent)
                and isinstance(event.message, TextMessage)
            ):
                reply_text(
                    event.reply_token,
                    "本功能僅供指定 LINE 群組使用。"
                )
            continue

        # ====================================================
        # Postback 按鈕處理
        # ====================================================
        if isinstance(event, PostbackEvent):
            data = parse_qs(
                getattr(event.postback, "data", "")
            )

            action = data.get("action", [""])[0]
            user_id = get_user_id(event)
            incident_id = data.get("incident_id", [""])[0]

            # ------------------------------------------------
            # 最高管理者人員管理面板按鈕
            # ------------------------------------------------
            if action.startswith("admin_"):
                if not is_admin(user_id):
                    reply_text(event.reply_token, "⛔ 此操作僅限最高管理者執行。")
                elif action == "admin_nurse_open":
                    if get_next_nurse_slot() is None:
                        reply_text(event.reply_token, "三名護理人員名額已滿。")
                    else:
                        nurse_registration_open = True
                        set_app_setting("nurse_registration_open", True)
                        line_bot_api.reply_message(event.reply_token, registration_prompt_message("nurse"))
                elif action == "admin_nurse_close":
                    nurse_registration_open = False
                    set_app_setting("nurse_registration_open", False)
                    line_bot_api.reply_message(event.reply_token, admin_roster_message())
                elif action == "admin_safety_open":
                    safety_registration_open = True
                    set_app_setting("safety_registration_open", True)
                    line_bot_api.reply_message(event.reply_token, registration_prompt_message("safety"))
                elif action == "admin_safety_close":
                    safety_registration_open = False
                    set_app_setting("safety_registration_open", False)
                    line_bot_api.reply_message(event.reply_token, admin_roster_message())
                elif action == "admin_delete_nurse":
                    try:
                        slot = int(data.get("slot", ["0"])[0])
                    except ValueError:
                        slot = 0
                    if slot in (1, 2, 3):
                        delete_nurse_record(slot)
                    removed = nurse_users.pop(slot, None) if slot in (1, 2, 3) else None
                    line_bot_api.reply_message(event.reply_token, admin_roster_message())
                elif action == "admin_delete_safety":
                    target = data.get("target", [""])[0]
                    delete_safety_record(target)
                    safety_users.pop(target, None)
                    line_bot_api.reply_message(event.reply_token, admin_roster_message())
                elif action == "admin_roster":
                    line_bot_api.reply_message(event.reply_token, admin_roster_message())
                continue

            # ------------------------------------------------
            # 人員自行登記按鈕（開放時立即生效，不需核准）
            # ------------------------------------------------
            if action in ("register_nurse", "register_safety"):
                if not user_id:
                    reply_text(event.reply_token, "無法辨識你的 LINE 帳號，請確認以個人 LINE 帳號操作。")
                elif action == "register_nurse":
                    if not nurse_registration_open:
                        reply_text(event.reply_token, "目前未開放護理人員登記。")
                    elif is_nurse(user_id):
                        slot = next(slot for slot, rec in nurse_users.items() if rec.get("user_id") == user_id)
                        reply_text(event.reply_token, f"你已登記為護理人員{slot}，不需重複登記。")
                    else:
                        slot = get_next_nurse_slot()
                        if slot is None:
                            nurse_registration_open = False
                            set_app_setting("nurse_registration_open", False)
                            reply_text(event.reply_token, "三名護理人員名額已滿，登記已自動關閉。")
                        else:
                            display_name = get_display_name(group_id, user_id)
                            new_record = {"user_id": user_id, "display_name": display_name}
                            save_nurse_record(slot, new_record)
                            nurse_users[slot] = new_record
                            if get_next_nurse_slot() is None:
                                nurse_registration_open = False
                                set_app_setting("nurse_registration_open", False)
                            reply_text(event.reply_token, f"✅ 護理人員{slot}登記成功！\n登記人員：{display_name}\n" + nurse_registration_status_text())
                else:
                    if not safety_registration_open:
                        reply_text(event.reply_token, "目前未開放工安人員登記。")
                    elif is_safety_staff(user_id):
                        reply_text(event.reply_token, "你已登記為工安人員，不需重複登記。")
                    else:
                        display_name = get_display_name(group_id, user_id)
                        new_record = {"user_id": user_id, "display_name": display_name}
                        save_safety_record(user_id, new_record)
                        safety_users[user_id] = new_record
                        reply_text(event.reply_token, f"✅ 工安人員登記成功！\n登記人員：{display_name}\n" + safety_registration_status_text())
                continue

            # ------------------------------------------------
            # 啟動緊急救護
            # ------------------------------------------------
            if action == "start_emergency":
                if group_id in active_incidents:
                    reply_text(
                        event.reply_token,
                        "⚠️ 目前已有進行中的救護事件，請先結束現有事件。"
                    )

                elif group_id in pending_locations:
                    reply_text(
                        event.reply_token,
                        "目前已有緊急救護啟動流程正在等待事故地點。"
                    )

                else:
                    pending = {
                        "initiator": user_id,
                        "initiator_name": get_display_name(
                            group_id,
                            user_id
                        ),
                        "created_at": now_tw(),
                    }

                    pending_locations[group_id] = pending

                    schedule_timeout(
                        "location",
                        group_id,
                        pending["created_at"]
                    )

                    line_bot_api.reply_message(
                        event.reply_token,
                        cancel_start_message()
                    )

            # ------------------------------------------------
            # 取消啟動
            # ------------------------------------------------
            elif action == "cancel_start":
                pending = pending_locations.get(group_id)

                if pending and user_id == pending["initiator"]:
                    pending_locations.pop(group_id, None)

                    task = timeout_tasks.pop(
                        ("location", group_id),
                        None
                    )

                    if task and not task.done():
                        task.cancel()

                    reply_text(
                        event.reply_token,
                        "已取消緊急救護啟動流程，尚未發送正式通知。"
                    )

                else:
                    reply_text(
                        event.reply_token,
                        "只有原始啟動人員可以取消此啟動流程。"
                    )

            # ------------------------------------------------
            # 取消事故地點修正
            # ------------------------------------------------
            elif action == "cancel_correction":
                pending = pending_corrections.pop(
                    group_id,
                    None
                )

                if pending:
                    task = timeout_tasks.pop(
                        ("correction", group_id),
                        None
                    )

                    if task and not task.done():
                        task.cancel()

                    reply_text(
                        event.reply_token,
                        "已取消本次事故地點修正，維持原事故地點。"
                    )

                else:
                    reply_text(
                        event.reply_token,
                        "目前沒有等待中的事故地點修正流程。"
                    )

            # ------------------------------------------------
            # 護理人員／工安人員分流回報
            # ------------------------------------------------
            elif action in (
                "ack_nurse_available", "ack_nurse_unavailable",
                "ack_safety_available", "ack_safety_unavailable"
            ):
                incident = active_incidents.get(group_id)
                requested_role = "nurse" if action.startswith("ack_nurse_") else "safety"
                status = "available" if action.endswith("_available") else "unavailable"
                actual_role = get_response_role(user_id)

                if not incident or incident["id"] != incident_id:
                    reply_text(event.reply_token, "此救護事件已結束或通知已失效。")
                elif not user_id:
                    reply_text(event.reply_token, "無法辨識回報帳號，請確認以個人 LINE 帳號操作。")
                elif actual_role is None:
                    reply_text(event.reply_token, "你尚未登記為護理人員或工安人員，無法使用專屬回報功能。")
                elif actual_role != requested_role:
                    if actual_role == "nurse":
                        reply_text(event.reply_token, "你已登記為護理人員，請使用「護理人員回報」區，不需使用工安人員回報區。")
                    else:
                        reply_text(event.reply_token, "此按鈕僅限已登記護理人員使用，請使用「工安人員回報」區。")
                elif user_id in incident["responses"]:
                    old = incident["responses"][user_id]
                    old_status = "可以前往" if old["status"] == "available" else "無法前往"
                    reply_text(
                        event.reply_token,
                        f"已收到你的回報，不重複更新紀錄。\n首次回報身分：{'護理人員' if old.get('role') == 'nurse' else '工安人員'}\n首次回報狀態：{old_status}\n首次回報時間：{time_text(old['time'])}"
                    )
                else:
                    display_name = get_display_name(group_id, user_id)
                    timestamp = now_tw()
                    response_record = {
                        "role": actual_role,
                        "status": status,
                        "time": timestamp,
                        "display_name": display_name,
                    }
                    if not save_incident_response(incident_id, user_id, response_record):
                        reply_text(event.reply_token, "已收到你的回報，不重複更新紀錄。")
                        continue
                    incident["responses"][user_id] = response_record
                    role_label = "護理人員" if actual_role == "nurse" else "工安人員"
                    status_label = (
                        "收到，可以前往" if status == "available"
                        else "收到，但無法前往"
                    )
                    line_bot_api.reply_message(
                        event.reply_token,
                        [
                            TextSendMessage(text=(
                                f"✅ 回報已登記\n回報身分：{role_label}\n"
                                f"回報人員：{display_name}\n回報狀態：{status_label}\n"
                                f"首次回報時間：{time_text(timestamp)}"
                            )),
                            TextSendMessage(text=incident_status_text(incident)),
                        ]
                    )

            # ------------------------------------------------
            # 確認人工結束事件
            # ------------------------------------------------
            elif action == "confirm_close":
                incident = active_incidents.get(group_id)

                if not incident or incident["id"] != incident_id:
                    reply_text(
                        event.reply_token,
                        "此救護事件已結束或確認畫面已失效。"
                    )

                elif user_id != incident["initiator"]:
                    reply_text(
                        event.reply_token,
                        "只有原始啟動人員可以結束此救護事件。"
                    )

                else:
                    # 先永久更新資料庫；成功後才停止計時並清除記憶體狀態。
                    if not close_incident_record(incident_id, "closed"):
                        reply_text(event.reply_token, "資料庫未能確認事件結束，事件仍維持進行中，請稍後再試。")
                        continue
                    cancel_incident_timer(group_id)
                    active_incidents.pop(group_id, None)
                    pending_closures.pop(group_id, None)

                    reply_text(
                        event.reply_token,
                        f"✅ 救護事件已結束。\n"
                        f"{incident_status_text(incident, include_unavailable=True)}"
                    )

            # ------------------------------------------------
            # 取消人工結束確認
            # ------------------------------------------------
            elif action == "cancel_close":
                incident = active_incidents.get(group_id)

                if (
                    incident
                    and incident["id"] == incident_id
                    and user_id == incident["initiator"]
                ):
                    pending_closures.pop(group_id, None)

                    reply_text(
                        event.reply_token,
                        "已取消結束作業，救護事件及回報資料繼續保留。"
                    )

                else:
                    reply_text(
                        event.reply_token,
                        "無法取消此結束確認，請確認事件狀態及操作權限。"
                    )

            continue

        # ====================================================
        # 一般文字訊息處理
        # ====================================================
        if not (
            isinstance(event, MessageEvent)
            and isinstance(event.message, TextMessage)
        ):
            continue

        text = event.message.text.strip()
        user_id = get_user_id(event)

        # ----------------------------------------------------
        # 人員登記管理（全域名單；僅最高管理者可開關及管理）
        # ----------------------------------------------------
        if text == "啟動護理人員登記":
            if not is_admin(user_id):
                reply_text(event.reply_token, "⛔ 此操作僅限最高管理者執行。")
            elif get_next_nurse_slot() is None:
                reply_text(event.reply_token, "三名護理人員名額均已登記完成。\n" + nurse_registration_status_text())
            else:
                nurse_registration_open = True
                set_app_setting("nurse_registration_open", True)
                line_bot_api.reply_message(event.reply_token, registration_prompt_message("nurse"))
            continue

        if text in ("取消護理人員登記", "關閉護理人員登記"):
            if not is_admin(user_id):
                reply_text(event.reply_token, "⛔ 此操作僅限最高管理者執行。")
            else:
                nurse_registration_open = False
                set_app_setting("nurse_registration_open", False)
                reply_text(event.reply_token, "已關閉護理人員登記；既有名單不受影響。\n" + nurse_registration_status_text())
            continue

        if text == "啟動工安人員登記":
            if not is_admin(user_id):
                reply_text(event.reply_token, "⛔ 此操作僅限最高管理者執行。")
            else:
                safety_registration_open = True
                set_app_setting("safety_registration_open", True)
                line_bot_api.reply_message(event.reply_token, registration_prompt_message("safety"))
            continue

        if text in ("關閉工安人員登記", "取消工安人員登記"):
            if not is_admin(user_id):
                reply_text(event.reply_token, "⛔ 此操作僅限最高管理者執行。")
            else:
                safety_registration_open = False
                set_app_setting("safety_registration_open", False)
                reply_text(event.reply_token, "已關閉工安人員登記；既有名單不受影響。\n" + safety_registration_status_text())
            continue

        if text in ("查詢護理人員登記", "查詢護理人員"):
            if not is_admin(user_id):
                reply_text(event.reply_token, "⛔ 此操作僅限最高管理者執行。")
            else:
                reply_text(event.reply_token, nurse_registration_status_text())
            continue

        if text in ("查詢工安人員", "查詢工安人員登記"):
            if not is_admin(user_id):
                reply_text(event.reply_token, "⛔ 此操作僅限最高管理者執行。")
            else:
                reply_text(event.reply_token, safety_registration_status_text())
            continue

        if text in ("查詢人員名單", "人員管理"):
            if not is_admin(user_id):
                reply_text(event.reply_token, "⛔ 此操作僅限最高管理者執行。")
            else:
                line_bot_api.reply_message(event.reply_token, admin_roster_message())
            continue

        if text in ("刪除護理人員1", "刪除護理人員2", "刪除護理人員3"):
            if not is_admin(user_id):
                reply_text(event.reply_token, "⛔ 此操作僅限最高管理者執行。")
            else:
                slot = int(text[-1])
                delete_nurse_record(slot)
                removed = nurse_users.pop(slot, None)
                reply_text(event.reply_token, (f"✅ 已刪除護理人員{slot}：{removed.get('display_name') or '群組成員'}。\n" if removed else f"護理人員{slot}目前沒有登記資料。\n") + nurse_registration_status_text())
            continue

        if text == "重設護理人員名單":
            if not is_admin(user_id):
                reply_text(event.reply_token, "⛔ 此操作僅限最高管理者執行。")
            else:
                clear_nurse_records()
                nurse_users.clear()
                nurse_registration_open = False
                set_app_setting("nurse_registration_open", False)
                reply_text(event.reply_token, "✅ 已清空全部護理人員名單，登記模式已關閉。\n" + nurse_registration_status_text())
            continue

        if text == "重設工安人員名單":
            if not is_admin(user_id):
                reply_text(event.reply_token, "⛔ 此操作僅限最高管理者執行。")
            else:
                clear_safety_records()
                safety_users.clear()
                safety_registration_open = False
                set_app_setting("safety_registration_open", False)
                reply_text(event.reply_token, "✅ 已清空全部工安人員名單，登記模式已關閉。\n" + safety_registration_status_text())
            continue

        # 舊文字登記指令保留相容；新流程以按鈕為主。
        if text == "我要登記護理人員":
            if not nurse_registration_open:
                reply_text(event.reply_token, "目前未開放護理人員登記，請聯絡最高管理者。")
                continue
            if not user_id:
                reply_text(event.reply_token, "無法辨識你的 LINE 帳號。")
                continue
            existing = next((slot for slot, rec in nurse_users.items() if rec.get("user_id") == user_id), None)
            if existing:
                reply_text(event.reply_token, f"你已登記為護理人員{existing}。")
                continue
            slot = get_next_nurse_slot()
            if slot is None:
                nurse_registration_open = False
                set_app_setting("nurse_registration_open", False)
                reply_text(event.reply_token, "三名護理人員名額已滿。")
                continue
            display_name = get_display_name(group_id, user_id)
            new_record = {"user_id": user_id, "display_name": display_name}
            save_nurse_record(slot, new_record)
            nurse_users[slot] = new_record
            if get_next_nurse_slot() is None:
                nurse_registration_open = False
                set_app_setting("nurse_registration_open", False)
            reply_text(event.reply_token, f"✅ 護理人員{slot}登記成功：{display_name}。\n" + nurse_registration_status_text())
            continue

        if text == "我要登記工安人員":
            if not safety_registration_open:
                reply_text(event.reply_token, "目前未開放工安人員登記，請聯絡最高管理者。")
                continue
            if not user_id:
                reply_text(event.reply_token, "無法辨識你的 LINE 帳號。")
                continue
            if user_id in safety_users:
                reply_text(event.reply_token, "你已登記為工安人員，不需重複登記。")
                continue
            display_name = get_display_name(group_id, user_id)
            new_record = {"user_id": user_id, "display_name": display_name}
            save_safety_record(user_id, new_record)
            safety_users[user_id] = new_record
            reply_text(event.reply_token, f"✅ 工安人員登記成功：{display_name}。\n" + safety_registration_status_text())
            continue

        if text.startswith("刪除工安人員 "):
            if not is_admin(user_id):
                reply_text(event.reply_token, "⛔ 此操作僅限最高管理者執行。")
            else:
                target_name = text.replace("刪除工安人員 ", "", 1).strip()
                matches = [uid for uid, rec in safety_users.items() if rec.get("display_name") == target_name or uid == target_name]
                if len(matches) == 1:
                    delete_safety_record(matches[0])
                    removed = safety_users.pop(matches[0])
                    reply_text(event.reply_token, f"✅ 已刪除工安人員：{removed.get('display_name') or '工安人員'}。\n" + safety_registration_status_text())
                elif len(matches) > 1:
                    reply_text(event.reply_token, "同名人員不只一位，請使用人員管理面板個別刪除。")
                else:
                    reply_text(event.reply_token, "找不到指定工安人員，請先查詢名單。")
            continue

        # ----------------------------------------------------
        # 119：顯示緊急救護面板
        # ----------------------------------------------------
        if text == "119":
            line_bot_api.reply_message(
                event.reply_token,
                panel_message()
            )
            continue

        # ----------------------------------------------------
        # 回報狀況
        # ----------------------------------------------------
        if text == "回報狀況":
            incident = active_incidents.get(group_id)

            if incident:
                reply_text(
                    event.reply_token,
                    incident_status_text(incident)
                )
            else:
                reply_text(
                    event.reply_token,
                    "目前沒有進行中的救護事件。"
                )

            continue

        # ----------------------------------------------------
        # 結束救護事件
        # ----------------------------------------------------
        if text == "結束救護事件":
            incident = active_incidents.get(group_id)

            if not incident:
                reply_text(
                    event.reply_token,
                    "目前沒有進行中的救護事件。"
                )

            elif user_id != incident["initiator"]:
                reply_text(
                    event.reply_token,
                    "只有原始啟動人員可以提出結束救護事件。"
                )

            else:
                status = incident_status_text(incident)

                pending_closures[group_id] = {
                    "incident_id": incident["id"],
                    "initiator": user_id
                }

                line_bot_api.reply_message(
                    event.reply_token,
                    close_confirmation_message(
                        incident["id"],
                        status
                    )
                )

            continue

        # ----------------------------------------------------
        # 修改事故地點
        # ----------------------------------------------------
        if text == "修改事故地點":
            incident = active_incidents.get(group_id)

            if not incident:
                reply_text(
                    event.reply_token,
                    "目前沒有進行中的救護事件，無法修改事故地點。"
                )

            elif group_id in pending_corrections:
                reply_text(
                    event.reply_token,
                    "目前已有事故地點修正流程，請先完成或取消。"
                )

            else:
                pending = {
                    "created_at": now_tw(),
                    "incident_id": incident["id"]
                }

                pending_corrections[group_id] = pending

                schedule_timeout(
                    "correction",
                    group_id,
                    pending["created_at"]
                )

                line_bot_api.reply_message(
                    event.reply_token,
                    cancel_correction_message()
                )

            continue

        # ----------------------------------------------------
        # 先檢查事故地點修正等待狀態
        # ----------------------------------------------------
        if group_id in pending_corrections:
            if expire_if_needed("correction", group_id):
                reply_text(
                    event.reply_token,
                    "事故地點修正已逾時取消，維持原事故地點。"
                )
                continue

            pending = pending_corrections.pop(group_id)

            task = timeout_tasks.pop(
                ("correction", group_id),
                None
            )

            if task and not task.done():
                task.cancel()

            incident = active_incidents.get(group_id)

            if incident and incident["id"] == pending["incident_id"]:
                old_location = incident["location"]
                if not update_incident_location(incident["id"], text):
                    reply_text(event.reply_token, "事故地點更新失敗，原地點維持不變，請稍後再試。")
                    continue
                incident["location"] = text

                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(
                        text=(
                            f"📍【事故地點更正】\n"
                            f"原事故地點：{old_location}\n"
                            f"最新事故地點：{text}\n"
                            "請所有救護人員以最新地點為準。"
                        )
                    )
                )

            else:
                reply_text(
                    event.reply_token,
                    "原救護事件已不存在，本次地點修正未套用。"
                )

            continue

        # ----------------------------------------------------
        # 再檢查初始事故地點等待狀態
        # ----------------------------------------------------
        if group_id in pending_locations:
            if expire_if_needed("location", group_id):
                reply_text(
                    event.reply_token,
                    "緊急救護啟動流程已逾時取消，請重新點選緊急救護。"
                )
                continue

            pending = pending_locations.pop(group_id)

            task = timeout_tasks.pop(
                ("location", group_id),
                None
            )

            if task and not task.done():
                task.cancel()

            incident_id = next_incident_id()

            incident = {
                "id": incident_id,
                "location": text,
                "initiator": pending["initiator"],
                "initiator_name": pending["initiator_name"],
                "created_at": now_tw(),
                "responses": {},
            }

            # 事件資料先成功寫入 PostgreSQL，才正式啟動通知及計時。
            try:
                create_incident_record(incident, group_id)
            except Exception as error:
                print(f"救護事件寫入資料庫失敗：{error}")
                reply_text(event.reply_token, "⚠️ 救護事件資料保存失敗，未發送正式通知；請稍後重新啟動。")
                continue

            active_incidents[group_id] = incident

            # 建立1小時提醒及2小時自動結束任務
            schedule_incident_timer(
                group_id,
                incident
            )

            notice = (
                "🚨【緊急救護通知】🚨\n"
                f"事件編號：{incident_id}\n"
                f"事故地點：{text}\n"
                "請相關救護人員前往處理。\n"
                "已登記護理人員及工安人員，請依身分使用專屬按鈕回報。"
            )

            # 保留原有真正的 LINE 群組 @All 通知
            reply_emergency_notice(
                event.reply_token,
                notice,
                incident_id,
                group_id
            )

            continue

    return "OK"
