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

# 救護事件自動結束設定：啟動滿 1 小時自動結束
INCIDENT_AUTO_CLOSE_SECONDS = 60 * 60

# ============================================================
# 暫存資料（正式事件、地點歷程及所有回應紀錄均永久保存於 PostgreSQL）
# ============================================================
pending_locations = {}
pending_corrections = {}
active_incidents = {}
pending_closures = {}
admin_pending_closures = {}
timeout_tasks = {}

# 每個群組各自的救護事件定時任務
incident_timer_tasks = {}


# ============================================================
# PostgreSQL 資料庫連線與初始化
# ============================================================
def get_db_connection():
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("Render 尚未設定 DATABASE_URL，服務停止啟動以避免資料僅存在記憶體。")
    return psycopg.connect(database_url, row_factory=dict_row, connect_timeout=10)


def init_database():
    """建立所需資料表；保留既有歷史資料，資料庫無法使用時直接讓服務啟動失敗。"""
    statements = [
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
                CHECK (status IN ('active', 'closed', 'auto_closed', 'admin_closed'))
        )""",
        """CREATE UNIQUE INDEX IF NOT EXISTS one_active_incident_per_group
            ON incidents(group_id) WHERE status = 'active'""",
        """CREATE TABLE IF NOT EXISTS incident_responses (
            response_seq BIGSERIAL PRIMARY KEY,
            incident_id TEXT NOT NULL REFERENCES incidents(incident_id),
            user_id TEXT NOT NULL,
            display_name TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('available', 'unavailable')),
            action_type TEXT NOT NULL DEFAULT 'initial'
                CHECK (action_type IN ('initial', 'update')),
            responded_at TIMESTAMPTZ NOT NULL
        )""",
        """CREATE INDEX IF NOT EXISTS idx_incident_responses_incident_seq
            ON incident_responses(incident_id, response_seq)""",
        """CREATE TABLE IF NOT EXISTS incident_location_logs (
            log_seq BIGSERIAL PRIMARY KEY,
            incident_id TEXT NOT NULL REFERENCES incidents(incident_id),
            old_location TEXT NOT NULL,
            new_location TEXT NOT NULL,
            updated_by TEXT NOT NULL,
            updated_by_name TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        )""",
        """CREATE INDEX IF NOT EXISTS idx_incident_location_logs_incident_seq
            ON incident_location_logs(incident_id, log_seq)""",
    ]
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
            # 相容舊版資料庫結構：若舊表存在主鍵或舊欄位，安全升級以保留歷史紀錄
            cur.execute("""
                ALTER TABLE incidents
                DROP CONSTRAINT IF EXISTS incidents_status_check
            """)
            cur.execute("""
                ALTER TABLE incidents
                ADD CONSTRAINT incidents_status_check
                CHECK (status IN ('active', 'closed', 'auto_closed', 'admin_closed'))
            """)
            cur.execute("""
                ALTER TABLE incident_responses
                DROP CONSTRAINT IF EXISTS incident_responses_pkey
            """)
            cur.execute("""
                ALTER TABLE incident_responses
                ADD COLUMN IF NOT EXISTS response_seq BIGSERIAL PRIMARY KEY
            """)
            cur.execute("""
                ALTER TABLE incident_responses
                ADD COLUMN IF NOT EXISTS action_type TEXT NOT NULL DEFAULT 'initial'
            """)
            cur.execute("""
                ALTER TABLE incident_responses
                ALTER COLUMN role DROP NOT NULL
            """) if column_exists(cur, "incident_responses", "role") else None


def column_exists(cur, table_name, column_name):
    row = cur.execute(
        """
        SELECT 1 FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
        """,
        (table_name, column_name),
    ).fetchone()
    return row is not None


def create_incident_record(incident, group_id):
    with get_db_connection() as conn:
        conn.execute(
            "INSERT INTO incidents (incident_id, group_id, location, initiator, "
            "initiator_name, created_at, status) VALUES (%s, %s, %s, %s, %s, %s, 'active')",
            (
                incident["id"],
                group_id,
                incident["location"],
                incident["initiator"],
                incident["initiator_name"],
                incident["created_at"],
            ),
        )


def save_incident_response(incident_id, record):
    """每次點擊均寫入一筆新紀錄，完整保留首次回應及後續修改歷程。"""
    with get_db_connection() as conn:
        row = conn.execute(
            "INSERT INTO incident_responses "
            "(incident_id, user_id, display_name, status, action_type, responded_at) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "RETURNING response_seq",
            (
                incident_id,
                record["user_id"],
                record.get("display_name") or "群組成員",
                record["status"],
                record["action_type"],
                record["time"],
            ),
        ).fetchone()
        return row["response_seq"] if row else None


def update_incident_location(incident_id, old_location, new_location, user_id, user_name, updated_at):
    """更新事件最新地點，並同步寫入地點修改歷史紀錄表。"""
    with get_db_connection() as conn:
        cur = conn.execute(
            "UPDATE incidents SET location = %s WHERE incident_id = %s AND status = 'active'",
            (new_location, incident_id),
        )
        if cur.rowcount != 1:
            return False
        conn.execute(
            "INSERT INTO incident_location_logs "
            "(incident_id, old_location, new_location, updated_by, updated_by_name, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (incident_id, old_location, new_location, user_id, user_name, updated_at),
        )
        return True


def close_incident_record(incident_id, status, ended_at=None):
    ended_at = ended_at or now_tw()
    with get_db_connection() as conn:
        cur = conn.execute(
            "UPDATE incidents SET status = %s, ended_at = %s "
            "WHERE incident_id = %s AND status = 'active'",
            (status, ended_at, incident_id),
        )
        return cur.rowcount == 1


def fetch_incident_full_data(incident_id):
    """從 PostgreSQL 讀取單一事件（含進行中或已結束）完整主檔、地點修改歷程與所有回應紀錄。"""
    with get_db_connection() as conn:
        inc_row = conn.execute(
            "SELECT incident_id, group_id, location, initiator, initiator_name, "
            "created_at, ended_at, status FROM incidents WHERE incident_id = %s",
            (incident_id,),
        ).fetchone()
        if not inc_row:
            return None

        resp_rows = conn.execute(
            "SELECT response_seq, user_id, display_name, status, action_type, responded_at "
            "FROM incident_responses WHERE incident_id = %s ORDER BY response_seq ASC",
            (incident_id,),
        ).fetchall()

        loc_rows = conn.execute(
            "SELECT log_seq, old_location, new_location, updated_by, updated_by_name, updated_at "
            "FROM incident_location_logs WHERE incident_id = %s ORDER BY log_seq ASC",
            (incident_id,),
        ).fetchall()

    responses = [
        {
            "seq": r["response_seq"],
            "user_id": r["user_id"],
            "display_name": r["display_name"],
            "status": r["status"],
            "action_type": r["action_type"],
            "time": r["responded_at"],
        }
        for r in resp_rows
    ]
    location_logs = [
        {
            "seq": l["log_seq"],
            "old_location": l["old_location"],
            "new_location": l["new_location"],
            "updated_by": l["updated_by"],
            "updated_by_name": l["updated_by_name"],
            "updated_at": l["updated_at"],
        }
        for l in loc_rows
    ]

    # 推算原始啟動地點（若有修改紀錄，第一筆的 old_location 即為原始地點）
    initial_location = location_logs[0]["old_location"] if location_logs else inc_row["location"]

    return {
        "id": inc_row["incident_id"],
        "group_id": inc_row["group_id"],
        "initial_location": initial_location,
        "location": inc_row["location"],
        "initiator": inc_row["initiator"],
        "initiator_name": inc_row["initiator_name"],
        "created_at": inc_row["created_at"],
        "ended_at": inc_row["ended_at"],
        "status": inc_row["status"],
        "responses": responses,
        "location_logs": location_logs,
    }


def load_persistent_state():
    """啟動時從 PostgreSQL 還原所有進行中事件、地點歷程與回應紀錄。"""
    with get_db_connection() as conn:
        incidents = conn.execute(
            "SELECT incident_id, group_id, location, initiator, initiator_name, created_at "
            "FROM incidents WHERE status = 'active' ORDER BY created_at"
        ).fetchall()
        responses = conn.execute(
            "SELECT r.response_seq, r.incident_id, r.user_id, r.display_name, "
            "r.status, r.action_type, r.responded_at "
            "FROM incident_responses r JOIN incidents i ON i.incident_id = r.incident_id "
            "WHERE i.status = 'active' ORDER BY r.response_seq ASC"
        ).fetchall()
        loc_logs = conn.execute(
            "SELECT l.log_seq, l.incident_id, l.old_location, l.new_location, "
            "l.updated_by, l.updated_by_name, l.updated_at "
            "FROM incident_location_logs l JOIN incidents i ON i.incident_id = l.incident_id "
            "WHERE i.status = 'active' ORDER BY l.log_seq ASC"
        ).fetchall()

    active_incidents.clear()
    by_id = {}
    for row in incidents:
        inc = {
            "id": row["incident_id"],
            "group_id": row["group_id"],
            "initial_location": row["location"],
            "location": row["location"],
            "initiator": row["initiator"],
            "initiator_name": row["initiator_name"],
            "created_at": row["created_at"],
            "responses": [],
            "location_logs": [],
        }
        active_incidents[row["group_id"]] = inc
        by_id[row["incident_id"]] = inc

    for row in loc_logs:
        inc = by_id.get(row["incident_id"])
        if inc:
            if not inc["location_logs"]:
                inc["initial_location"] = row["old_location"]
            inc["location_logs"].append({
                "seq": row["log_seq"],
                "old_location": row["old_location"],
                "new_location": row["new_location"],
                "updated_by": row["updated_by"],
                "updated_by_name": row["updated_by_name"],
                "updated_at": row["updated_at"],
            })

    for row in responses:
        inc = by_id.get(row["incident_id"])
        if inc:
            inc["responses"].append({
                "seq": row["response_seq"],
                "user_id": row["user_id"],
                "display_name": row["display_name"],
                "status": row["status"],
                "action_type": row["action_type"],
                "time": row["responded_at"],
            })


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
    if value is None:
        return "尚未結束"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(TAIWAN_TZ).strftime("%Y-%m-%d %H:%M:%S")


def duration_text(start_at, end_at):
    if not start_at or not end_at:
        return "計算中"
    if start_at.tzinfo is None:
        start_at = start_at.replace(tzinfo=timezone.utc)
    if end_at.tzinfo is None:
        end_at = end_at.replace(tzinfo=timezone.utc)
    total_sec = max(0, int((end_at - start_at).total_seconds()))
    hours, rem = divmod(total_sec, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours > 0:
        return f"{hours} 小時 {minutes} 分 {seconds} 秒"
    return f"{minutes} 分 {seconds} 秒"


def get_source_type(event):
    return getattr(getattr(event, "source", None), "type", None)


def get_group_id(event):
    if get_source_type(event) == "group":
        return getattr(event.source, "group_id", None)
    return None


def get_user_id(event):
    return getattr(getattr(event, "source", None), "user_id", None)


def get_display_name(group_id, user_id):
    if not user_id:
        return "群組成員"
    if group_id:
        try:
            return line_bot_api.get_group_member_profile(group_id, user_id).display_name
        except Exception:
            pass
    try:
        return line_bot_api.get_profile(user_id).display_name
    except Exception:
        return "LINE使用者"


def is_admin(user_id):
    """確認是否為唯一最高管理者。"""
    return bool(user_id) and user_id == ADMIN_USER_ID


# ============================================================
# 一般等待逾時處理（5 分鐘未輸入地點自動取消）
# ============================================================
def schedule_timeout(kind, group_id, created_at):
    key = (kind, group_id)
    old_task = timeout_tasks.get(key)

    if old_task and not old_task.done():
        old_task.cancel()

    async def wait_and_expire():
        try:
            await asyncio.sleep(WAIT_SECONDS)

            current = pending_locations if kind == "location" else pending_corrections
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
                            ),
                        )
                    else:
                        line_bot_api.push_message(
                            group_id,
                            TextSendMessage(
                                text=(
                                    "⌛ 已超過 5 分鐘未收到新的事故地點，"
                                    "本次修正已自動取消，維持原事故地點。"
                                )
                            ),
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
    current = pending_locations if kind == "location" else pending_corrections
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
    return FlexSendMessage(alt_text=alt_text, contents=contents)


def postback(label, data, display_text=None):
    return {
        "type": "postback",
        "label": label,
        "data": data,
        "displayText": display_text or label,
    }


def ask_location_message():
    contents = {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {
                    "type": "text",
                    "text": "🚨 請在群組輸入事故地點",
                    "weight": "bold",
                    "size": "lg",
                    "color": "#D93025",
                },
                {
                    "type": "text",
                    "text": (
                        "任何群組成員皆可直接輸入事故地點。"
                        "收到第一筆有效地點文字後，將立即啟動救護事件並通知全體成員（@All）。"
                        "等待時間為 5 分鐘。"
                    ),
                    "wrap": True,
                    "size": "sm",
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "height": "sm",
                    "action": postback("取消啟動", "action=cancel_start"),
                },
            ],
        },
    }
    return flex_message("請在群組輸入事故地點", contents)


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
                    "size": "lg",
                },
                {
                    "type": "text",
                    "text": (
                        "任何群組成員皆可提供新地點；"
                        "系統將完整保留修改紀錄。5 分鐘內未收到內容將自動取消。"
                    ),
                    "wrap": True,
                    "size": "sm",
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "height": "sm",
                    "action": postback("取消修正", "action=cancel_correction"),
                },
            ],
        },
    }
    return flex_message("請輸入新事故地點或取消修正", contents)


def response_message_contents(incident_id):
    """全體群組成員共用回報卡片；支援首次回報及後續變更狀態。"""
    contents = [
        {"type": "text", "text": "救護支援回報", "weight": "bold", "size": "lg"},
        {
            "type": "text",
            "text": "所有群組成員皆可點擊回報；若狀況異動可再次點擊更新，系統將完整保留每次紀錄。",
            "wrap": True,
            "size": "sm",
            "color": "#555555",
        },
        {
            "type": "button",
            "style": "primary",
            "color": "#16834A",
            "height": "md",
            "action": postback(
                "收到，可以前往",
                f"action=ack_available&incident_id={incident_id}",
            ),
        },
        {
            "type": "button",
            "style": "primary",
            "color": "#B3261E",
            "height": "md",
            "action": postback(
                "收到，但無法前往",
                f"action=ack_unavailable&incident_id={incident_id}",
            ),
        },
    ]
    return {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": contents,
        },
    }


def response_message(incident_id):
    return flex_message("請回報是否可以前往支援", response_message_contents(incident_id))


# ============================================================
# LINE 群組真正的 @All 通知（沿用已測試成功之 textV2 機制）
# ============================================================
def reply_emergency_notice(reply_token, notice, incident_id, group_id):
    """
    優先以 Reply 發送真正 @All 及回報卡片。
    Reply 失敗後改用 Push 補送，絕不重複使用同一個 reply_token。
    """
    substitutions = {
        "everyone": {
            "type": "mention",
            "mentionee": {"type": "all"},
        }
    }

    text_v2 = "{everyone}\n" + notice
    flex_contents = response_message_contents(incident_id)

    reply_payload = {
        "replyToken": reply_token,
        "messages": [
            {
                "type": "textV2",
                "text": text_v2,
                "substitution": substitutions,
            },
            {
                "type": "flex",
                "altText": "請回報是否可以前往支援",
                "contents": flex_contents,
            },
        ],
    }

    reply_request = urllib.request.Request(
        "https://api.line.me/v2/bot/message/reply",
        data=json.dumps(reply_payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(reply_request, timeout=10) as response:
            response.read()
        print(f"緊急救護通知已發送（Reply @All）：事件編號 {incident_id}")
        return

    except Exception as error:
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

    # Reply 失敗後改用 Push API 發送 @All 及回報卡片
    push_payload = {
        "to": group_id,
        "messages": [
            {
                "type": "textV2",
                "text": "{everyone}\n" + notice,
                "substitution": {
                    "everyone": {
                        "type": "mention",
                        "mentionee": {"type": "all"},
                    }
                },
            },
            {
                "type": "flex",
                "altText": "請回報是否可以前往支援",
                "contents": flex_contents,
            },
        ],
    }

    push_request = urllib.request.Request(
        "https://api.line.me/v2/bot/message/push",
        data=json.dumps(push_payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(push_request, timeout=10) as response:
            response.read()
        print(f"緊急救護通知已改用 Push API 補送（@All）：事件編號 {incident_id}")
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

    # 最後一道保底：送出一般群組通知及回報卡片
    try:
        line_bot_api.push_message(
            group_id,
            [
                TextSendMessage(text=notice),
                response_message(incident_id),
            ],
        )
        print("一般緊急救護通知及回報卡片已透過 Push API 補送。")
    except Exception as fallback_error:
        print(f"一般 Push 通知補送仍失敗：{fallback_error}")


# ============================================================
# 救護事件狀態統計與完整結案報告
# ============================================================
def summarize_latest_responses(responses):
    """
    依時間順序彙整每位成員的最新狀態，同時保留完整歷程筆數。
    responses 必須已依 seq / 時間由舊到新排序。
    """
    latest_by_user = {}
    for rec in responses:
        latest_by_user[rec["user_id"]] = rec

    available = []
    unavailable = []
    for rec in latest_by_user.values():
        name = rec.get("display_name") or "群組成員"
        entry = f"{name}（{time_text(rec['time'])}）"
        if rec["status"] == "available":
            available.append(entry)
        else:
            unavailable.append(entry)

    return latest_by_user, available, unavailable


def incident_status_text(incident, include_unavailable=False):
    """進行中即時回報狀況摘要。"""
    latest_by_user, available, unavailable = summarize_latest_responses(incident["responses"])
    total_records = len(incident["responses"])

    lines = [
        "📋【緊急救護即時回報狀況】",
        f"事件編號：{incident['id']}",
        f"啟動時間：{time_text(incident['created_at'])}",
        f"啟動人員：{incident['initiator_name']}",
        f"目前事故地點：{incident['location']}",
        f"已回報人數：{len(latest_by_user)} 人（累計回報/修改 {total_records} 筆）",
        f"可以前往：{len(available)} 人",
        "可以前往名單：" + ("、".join(available) or "目前無"),
    ]
    if include_unavailable:
        lines.extend([
            f"無法前往：{len(unavailable)} 人",
            "無法前往名單：" + ("、".join(unavailable) or "目前無"),
        ])

    return "\n".join(lines)


def build_final_incident_report(incident, close_reason_label, ended_at=None):
    """產出包含地點修改歷程、最新統計與完整人員回應/修改時間軸的事件報告。"""
    ended_at = ended_at or incident.get("ended_at") or now_tw()
    created_at = incident["created_at"]
    latest_by_user, available, unavailable = summarize_latest_responses(incident["responses"])
    location_logs = incident.get("location_logs", [])
    initial_location = incident.get("initial_location") or (
        location_logs[0]["old_location"] if location_logs else incident["location"]
    )

    lines = [
        "📑【緊急救護事件完整結案報告】",
        f"事件編號：{incident['id']}",
        f"結案方式：{close_reason_label}",
        f"啟動人員：{incident['initiator_name']}",
        f"啟動時間：{time_text(created_at)}",
        f"結束時間：{time_text(ended_at)}",
        f"事件總歷時：{duration_text(created_at, ended_at)}",
        f"初始事故地點：{initial_location}",
        f"最終事故地點：{incident['location']}",
        "",
        f"【地點修改紀錄（共 {len(location_logs)} 次）】",
    ]

    if location_logs:
        for idx, log in enumerate(location_logs, 1):
            lines.append(
                f"{idx}. {time_text(log['updated_at'])}｜{log['updated_by_name']}："
                f"「{log['old_location']}」→「{log['new_location']}」"
            )
    else:
        lines.append("無地點修改紀錄")

    lines.extend([
        "",
        "【最終人員支援統計（依每人最新狀態）】",
        f"參與回報總人數：{len(latest_by_user)} 人（總紀錄 {len(incident['responses'])} 筆）",
        f"可以前往（{len(available)} 人）：" + ("、".join(available) or "無"),
        f"無法前往（{len(unavailable)} 人）：" + ("、".join(unavailable) or "無"),
        "",
        f"【完整回應與修改時間軸（共 {len(incident['responses'])} 筆）】",
    ])

    if incident["responses"]:
        for idx, rec in enumerate(incident["responses"], 1):
            action_label = "首次回報" if rec.get("action_type") == "initial" else "修改狀態"
            status_label = "可以前往" if rec["status"] == "available" else "無法前往"
            lines.append(
                f"{idx}. {time_text(rec['time'])}｜{rec.get('display_name') or '群組成員'}"
                f"【{action_label}】→ {status_label}"
            )
    else:
        lines.append("本次事件無人員點擊回報")

    return "\n".join(lines)


# ============================================================
# 救護事件定時自動結束（啟動滿 1 小時自動結束）
# ============================================================
def cancel_incident_timer(group_id):
    """取消指定群組目前的救護事件計時任務。"""
    task = incident_timer_tasks.pop(group_id, None)
    if task and not task.done():
        task.cancel()
        print(f"已取消群組 {group_id} 的救護事件計時任務。")


def schedule_incident_timer(group_id, incident):
    """依事件原始建立時間計算滿 1 小時自動結束，不因重啟而重設。"""
    cancel_incident_timer(group_id)
    incident_id = incident["id"]
    created_at = incident["created_at"]
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)

    async def wait_and_auto_close():
        try:
            elapsed = (now_tw() - created_at.astimezone(TAIWAN_TZ)).total_seconds()
            wait_seconds = max(0, INCIDENT_AUTO_CLOSE_SECONDS - elapsed)
            await asyncio.sleep(wait_seconds)

            current = active_incidents.get(group_id)
            if not current or current["id"] != incident_id:
                return

            # 再次確認已滿 1 小時
            elapsed = (now_tw() - created_at.astimezone(TAIWAN_TZ)).total_seconds()
            if elapsed < INCIDENT_AUTO_CLOSE_SECONDS:
                await asyncio.sleep(INCIDENT_AUTO_CLOSE_SECONDS - elapsed)
                current = active_incidents.get(group_id)
                if not current or current["id"] != incident_id:
                    return

            ended_at = now_tw()
            if close_incident_record(incident_id, "auto_closed", ended_at=ended_at):
                active_incidents.pop(group_id, None)
                pending_closures.pop(group_id, None)
                report_text = build_final_incident_report(
                    current,
                    close_reason_label="啟動滿 1 小時系統自動結束",
                    ended_at=ended_at,
                )
                auto_close_msg = (
                    "⏰【救護事件已滿 1 小時自動結束】\n"
                    "本救護事件啟動已滿 1 小時，系統已自動結束本次事件並封存紀錄。\n"
                    "如仍有救護需求，請重新輸入 119 啟動。\n\n"
                    f"{report_text}"
                )
                try:
                    line_bot_api.push_message(group_id, TextSendMessage(text=auto_close_msg))
                    print(f"救護事件滿 1 小時已自動結束：{incident_id}")
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

    incident_timer_tasks[group_id] = asyncio.create_task(wait_and_auto_close())
    print(f"已建立救護事件計時任務：{incident_id}，將於啟動滿 1 小時後自動結束。")


# ============================================================
# 救護事件結束確認卡片
# ============================================================
def close_confirmation_message(incident_id, status_text, action_prefix=""):
    confirm_action = f"{action_prefix}confirm_close"
    cancel_action = f"{action_prefix}cancel_close"
    contents = {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {
                    "type": "text",
                    "text": f"確認結束救護事件（{incident_id}）？",
                    "weight": "bold",
                    "size": "lg",
                },
                {
                    "type": "text",
                    "text": status_text,
                    "wrap": True,
                    "size": "sm",
                },
                {
                    "type": "button",
                    "style": "primary",
                    "color": "#D93025",
                    "height": "md",
                    "action": postback(
                        "確認結束並產出報告",
                        f"action={confirm_action}&incident_id={incident_id}",
                    ),
                },
                {
                    "type": "button",
                    "style": "secondary",
                    "height": "md",
                    "action": postback(
                        "取消",
                        f"action={cancel_action}&incident_id={incident_id}",
                    ),
                },
            ],
        },
    }
    return flex_message("請確認是否結束救護事件", contents)


def reply_text(reply_token, text):
    line_bot_api.reply_message(reply_token, TextSendMessage(text=text))


# ============================================================
# PostgreSQL 啟動初始化及狀態還原
# ============================================================
@app.on_event("startup")
async def startup_restore_state():
    init_database()
    load_persistent_state()
    print(f"PostgreSQL 已連線；已還原進行中救護事件 {len(active_incidents)} 件。")

    for group_id, incident in list(active_incidents.items()):
        created_at = incident["created_at"]
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        elapsed = (now_tw() - created_at.astimezone(TAIWAN_TZ)).total_seconds()

        if elapsed >= INCIDENT_AUTO_CLOSE_SECONDS:
            ended_at = now_tw()
            if close_incident_record(incident["id"], "auto_closed", ended_at=ended_at):
                active_incidents.pop(group_id, None)
                pending_closures.pop(group_id, None)
                report_text = build_final_incident_report(
                    incident,
                    close_reason_label="啟動滿 1 小時系統自動結束（服務重啟結算）",
                    ended_at=ended_at,
                )
                message = (
                    "⏰【救護事件已滿 1 小時自動結束】\n"
                    "本救護事件啟動已滿 1 小時，服務重啟檢查時已逾自動結束時間，"
                    "系統已自動結束本次事件並封存紀錄。\n\n"
                    f"{report_text}"
                )
                try:
                    line_bot_api.push_message(group_id, TextSendMessage(text=message))
                except Exception as error:
                    print(f"重啟後自動結束公告發送失敗：{error}")
        else:
            schedule_incident_timer(group_id, incident)


# ============================================================
# 最高管理者私訊專屬指令與按鈕處理
# ============================================================
def admin_help_text():
    return (
        "🛡️【最高管理者私訊指令說明】\n"
        "1. 查詢進行中事件\n"
        "   → 檢視目前所有進行中的救護事件與編號。\n"
        "2. 查詢事件 事件編號（例如：查詢事件 2026112301）\n"
        "   → 調閱指定事件（含已結案歷史資料）的完整事件報告。\n"
        "3. 結束事件 事件編號（例如：結束事件 2026112301）\n"
        "   → 人工強制結束指定進行中事件，並於原群組及私訊產出完整結案報告。"
    )


def handle_admin_private_postback(event, user_id, data):
    if not is_admin(user_id):
        reply_text(event.reply_token, "⛔ 此操作僅限最高管理者透過私人訊息執行。")
        return

    action = data.get("action", [""])[0]
    incident_id = data.get("incident_id", [""])[0]

    if action == "admin_confirm_close":
        target_group_id = None
        target_incident = None
        for gid, inc in active_incidents.items():
            if inc["id"] == incident_id:
                target_group_id = gid
                target_incident = inc
                break

        if not target_incident or not target_group_id:
            admin_pending_closures.pop(incident_id, None)
            reply_text(event.reply_token, f"⚠️ 事件 {incident_id} 已結束或不存在。")
            return

        ended_at = now_tw()
        if not close_incident_record(incident_id, "admin_closed", ended_at=ended_at):
            reply_text(event.reply_token, "資料庫未能確認事件結束，請稍後再試。")
            return

        cancel_incident_timer(target_group_id)
        active_incidents.pop(target_group_id, None)
        pending_closures.pop(target_group_id, None)
        admin_pending_closures.pop(incident_id, None)

        report_text = build_final_incident_report(
            target_incident,
            close_reason_label="最高管理者私訊結束",
            ended_at=ended_at,
        )

        # 同步在原群組發布完整結案報告
        try:
            line_bot_api.push_message(
                target_group_id,
                TextSendMessage(
                    text=f"✅ 最高管理者已結束本次救護事件。\n\n{report_text}"
                ),
            )
        except Exception as error:
            print(f"管理者結束事件後推播群組報告失敗：{error}")

        reply_text(
            event.reply_token,
            f"✅ 已成功結束事件 {incident_id}，並已於原群組發布報告。\n\n{report_text}",
        )

    elif action == "admin_cancel_close":
        admin_pending_closures.pop(incident_id, None)
        reply_text(event.reply_token, f"已取消結束事件 {incident_id}，事件維持進行中。")


def handle_admin_private_text(event, user_id, text):
    if not is_admin(user_id):
        reply_text(event.reply_token, "本功能僅供指定 LINE 群組使用。")
        return

    if text in ("管理說明", "指令", "幫助", "help", "Help"):
        reply_text(event.reply_token, admin_help_text())
        return

    if text in ("查詢進行中事件", "查詢事件", "進行中事件"):
        if not active_incidents:
            reply_text(event.reply_token, "目前沒有任何進行中的救護事件。\n\n" + admin_help_text())
            return

        lines = [f"📋【目前進行中救護事件（共 {len(active_incidents)} 件）】"]
        for idx, (gid, inc) in enumerate(active_incidents.items(), 1):
            latest_by_user, available, _ = summarize_latest_responses(inc["responses"])
            lines.extend([
                f"\n{idx}. 事件編號：{inc['id']}",
                f"   群組 ID：{gid}",
                f"   啟動時間：{time_text(inc['created_at'])}",
                f"   啟動人員：{inc['initiator_name']}",
                f"   目前地點：{inc['location']}",
                f"   回報狀況：已回報 {len(latest_by_user)} 人（可以前往 {len(available)} 人）",
            ])
        lines.append("\n輸入「結束事件 事件編號」可結束指定事件，或「查詢事件 事件編號」檢視完整紀錄。")
        reply_text(event.reply_token, "\n".join(lines))
        return

    if text.startswith("查詢事件 "):
        target_id = text.replace("查詢事件 ", "", 1).strip()
        if not target_id:
            reply_text(event.reply_token, "請輸入欲查詢的事件編號，例如：查詢事件 2026112301")
            return

        # 優先讀取記憶體進行中最新狀態，若已結束則從 PostgreSQL 讀取完整歷史資料
        record = next((inc for inc in active_incidents.values() if inc["id"] == target_id), None)
        if record:
            report_text = build_final_incident_report(
                record,
                close_reason_label="進行中（即時預覽）",
                ended_at=now_tw(),
            )
            reply_text(event.reply_token, report_text)
            return

        db_record = fetch_incident_full_data(target_id)
        if not db_record:
            reply_text(event.reply_token, f"找不到事件編號：{target_id}")
            return

        status_map = {
            "active": "進行中",
            "closed": "原發起人手動結束",
            "auto_closed": "啟動滿 1 小時系統自動結束",
            "admin_closed": "最高管理者私訊結束",
        }
        report_text = build_final_incident_report(
            db_record,
            close_reason_label=status_map.get(db_record["status"], db_record["status"]),
            ended_at=db_record["ended_at"],
        )
        reply_text(event.reply_token, report_text)
        return

    if text.startswith("結束事件 "):
        target_id = text.replace("結束事件 ", "", 1).strip()
        if not target_id:
            reply_text(event.reply_token, "請輸入欲結束的事件編號，例如：結束事件 2026112301")
            return

        target_incident = next((inc for inc in active_incidents.values() if inc["id"] == target_id), None)
        if not target_incident:
            reply_text(event.reply_token, f"⚠️ 找不到進行中的事件 {target_id}（可能已結束或編號錯誤）。")
            return

        admin_pending_closures[target_id] = {
            "incident_id": target_id,
            "requested_at": now_tw(),
        }
        status_summary = incident_status_text(target_incident, include_unavailable=True)
        line_bot_api.reply_message(
            event.reply_token,
            close_confirmation_message(target_id, status_summary, action_prefix="admin_"),
        )
        return

    reply_text(event.reply_token, admin_help_text())


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
    body_text = body.decode("utf-8")

    try:
        events = parser.parse(body_text, signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature.")

    for event in events:
        group_id = get_group_id(event)
        user_id = get_user_id(event)

        # ====================================================
        # 加入群組時，發送使用提示
        # ====================================================
        if isinstance(event, JoinEvent) and group_id:
            reply_text(
                event.reply_token,
                "🚨 廠區緊急救護通知機器人已就緒。\n"
                "發生緊急狀況時，請直接在群組輸入「119」，並依提示輸入事故地點即可立即啟動通知。",
            )
            continue

        # ====================================================
        # 非群組來源（私人訊息）：僅最高管理者可使用查詢與結束指令
        # ====================================================
        if not group_id:
            if isinstance(event, PostbackEvent):
                data = parse_qs(getattr(event.postback, "data", ""))
                handle_admin_private_postback(event, user_id, data)
            elif isinstance(event, MessageEvent) and isinstance(event.message, TextMessage):
                handle_admin_private_text(event, user_id, event.message.text.strip())
            continue

        # ====================================================
        # 群組 Postback 按鈕處理
        # ====================================================
        if isinstance(event, PostbackEvent):
            data = parse_qs(getattr(event.postback, "data", ""))
            action = data.get("action", [""])[0]
            incident_id = data.get("incident_id", [""])[0]

            # ------------------------------------------------
            # 取消啟動
            # ------------------------------------------------
            if action == "cancel_start":
                pending = pending_locations.get(group_id)

                if pending and user_id == pending["initiator"]:
                    pending_locations.pop(group_id, None)
                    task = timeout_tasks.pop(("location", group_id), None)
                    if task and not task.done():
                        task.cancel()

                    reply_text(
                        event.reply_token,
                        "已取消緊急救護啟動流程，尚未發送正式通知。",
                    )
                else:
                    reply_text(
                        event.reply_token,
                        "只有原始啟動人員可以取消此啟動流程。",
                    )

            # ------------------------------------------------
            # 取消事故地點修正
            # ------------------------------------------------
            elif action == "cancel_correction":
                pending = pending_corrections.pop(group_id, None)

                if pending:
                    task = timeout_tasks.pop(("correction", group_id), None)
                    if task and not task.done():
                        task.cancel()

                    reply_text(
                        event.reply_token,
                        "已取消本次事故地點修正，維持原事故地點。",
                    )
                else:
                    reply_text(
                        event.reply_token,
                        "目前沒有等待中的事故地點修正流程。",
                    )

            # ------------------------------------------------
            # 全體群組成員回報（可重複點擊更新，完整保留每次紀錄）
            # ------------------------------------------------
            elif action in ("ack_available", "ack_unavailable"):
                incident = active_incidents.get(group_id)
                status = "available" if action == "ack_available" else "unavailable"

                if not incident or incident["id"] != incident_id:
                    reply_text(event.reply_token, "此救護事件已結束或通知已失效。")
                elif not user_id:
                    reply_text(event.reply_token, "無法辨識回報帳號，請確認以個人 LINE 帳號操作。")
                else:
                    display_name = get_display_name(group_id, user_id)
                    timestamp = now_tw()

                    # 檢查該人員先前是否已有回報紀錄
                    previous_records = [
                        r for r in incident["responses"] if r["user_id"] == user_id
                    ]
                    is_first = len(previous_records) == 0
                    action_type = "initial" if is_first else "update"

                    # 若狀態與上一筆完全相同，提示不重複寫入相同狀態，避免誤觸洗版
                    if not is_first and previous_records[-1]["status"] == status:
                        same_label = "可以前往" if status == "available" else "無法前往"
                        reply_text(
                            event.reply_token,
                            f"ℹ️ {display_name}，你目前的回報狀態已經是「{same_label}」，"
                            f"無需重複點擊（最後更新：{time_text(previous_records[-1]['time'])}）。",
                        )
                        continue

                    record = {
                        "user_id": user_id,
                        "display_name": display_name,
                        "status": status,
                        "action_type": action_type,
                        "time": timestamp,
                    }

                    seq = save_incident_response(incident_id, record)
                    if not seq:
                        reply_text(event.reply_token, "回報寫入失敗，請稍後再試。")
                        continue

                    record["seq"] = seq
                    incident["responses"].append(record)

                    status_label = "收到，可以前往" if status == "available" else "收到，但無法前往"
                    header_label = "✅ 首次回報已登記" if is_first else "🔄 回報狀態已更新（已保留原紀錄）"

                    line_bot_api.reply_message(
                        event.reply_token,
                        [
                            TextSendMessage(
                                text=(
                                    f"{header_label}\n"
                                    f"回報人員：{display_name}\n"
                                    f"最新狀態：{status_label}\n"
                                    f"紀錄時間：{time_text(timestamp)}"
                                )
                            ),
                            TextSendMessage(text=incident_status_text(incident)),
                        ],
                    )

            # ------------------------------------------------
            # 確認人工結束事件（原發起人於群組確認）
            # ------------------------------------------------
            elif action == "confirm_close":
                incident = active_incidents.get(group_id)

                if not incident or incident["id"] != incident_id:
                    reply_text(event.reply_token, "此救護事件已結束或確認畫面已失效。")
                elif user_id != incident["initiator"]:
                    reply_text(event.reply_token, "只有原始啟動人員可以結束此救護事件。")
                else:
                    ended_at = now_tw()
                    if not close_incident_record(incident_id, "closed", ended_at=ended_at):
                        reply_text(event.reply_token, "資料庫未能確認事件結束，事件仍維持進行中，請稍後再試。")
                        continue

                    cancel_incident_timer(group_id)
                    active_incidents.pop(group_id, None)
                    pending_closures.pop(group_id, None)

                    report_text = build_final_incident_report(
                        incident,
                        close_reason_label="原發起人手動結束",
                        ended_at=ended_at,
                    )
                    reply_text(
                        event.reply_token,
                        f"✅ 救護事件已結束。\n\n{report_text}",
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
                        "已取消結束作業，救護事件及回報資料繼續保留。",
                    )
                else:
                    reply_text(
                        event.reply_token,
                        "無法取消此結束確認，請確認事件狀態及操作權限。",
                    )

            continue

        # ====================================================
        # 群組一般文字訊息處理
        # ====================================================
        if not (isinstance(event, MessageEvent) and isinstance(event.message, TextMessage)):
            continue

        text = event.message.text.strip()

        # ----------------------------------------------------
        # 119：直接詢問事故地點並啟動 5 分鐘等待
        # ----------------------------------------------------
        if text == "119":
            if group_id in active_incidents:
                reply_text(
                    event.reply_token,
                    "⚠️ 目前已有進行中的救護事件，請先結束現有事件（或輸入「回報狀況」檢視進度）。",
                )
            elif group_id in pending_locations:
                reply_text(
                    event.reply_token,
                    "⏳ 目前已有緊急救護啟動流程正在等待事故地點，請直接輸入事故地點文字。",
                )
            else:
                pending = {
                    "initiator": user_id,
                    "initiator_name": get_display_name(group_id, user_id),
                    "created_at": now_tw(),
                }
                pending_locations[group_id] = pending
                schedule_timeout("location", group_id, pending["created_at"])

                line_bot_api.reply_message(
                    event.reply_token,
                    ask_location_message(),
                )
            continue

        # ----------------------------------------------------
        # 回報狀況
        # ----------------------------------------------------
        if text == "回報狀況":
            incident = active_incidents.get(group_id)
            if incident:
                reply_text(event.reply_token, incident_status_text(incident, include_unavailable=True))
            else:
                reply_text(event.reply_token, "目前沒有進行中的救護事件。")
            continue

        # ----------------------------------------------------
        # 結束救護事件（原啟動人員）
        # ----------------------------------------------------
        if text == "結束救護事件":
            incident = active_incidents.get(group_id)

            if not incident:
                reply_text(event.reply_token, "目前沒有進行中的救護事件。")
            elif user_id != incident["initiator"]:
                reply_text(
                    event.reply_token,
                    "只有原始啟動人員（或最高管理者透過私訊）可以提出結束救護事件。",
                )
            else:
                status = incident_status_text(incident, include_unavailable=True)
                pending_closures[group_id] = {
                    "incident_id": incident["id"],
                    "initiator": user_id,
                }
                line_bot_api.reply_message(
                    event.reply_token,
                    close_confirmation_message(incident["id"], status),
                )
            continue

        # ----------------------------------------------------
        # 修改事故地點
        # ----------------------------------------------------
        if text == "修改事故地點":
            incident = active_incidents.get(group_id)

            if not incident:
                reply_text(event.reply_token, "目前沒有進行中的救護事件，無法修改事故地點。")
            elif group_id in pending_corrections:
                reply_text(event.reply_token, "目前已有事故地點修正流程，請先完成或取消。")
            else:
                pending = {
                    "created_at": now_tw(),
                    "incident_id": incident["id"],
                }
                pending_corrections[group_id] = pending
                schedule_timeout("correction", group_id, pending["created_at"])

                line_bot_api.reply_message(
                    event.reply_token,
                    cancel_correction_message(),
                )
            continue

        # ----------------------------------------------------
        # 先檢查事故地點修正等待狀態
        # ----------------------------------------------------
        if group_id in pending_corrections:
            if expire_if_needed("correction", group_id):
                reply_text(event.reply_token, "事故地點修正已逾時取消，維持原事故地點。")
                continue

            pending = pending_corrections.pop(group_id)
            task = timeout_tasks.pop(("correction", group_id), None)
            if task and not task.done():
                task.cancel()

            incident = active_incidents.get(group_id)

            if incident and incident["id"] == pending["incident_id"]:
                old_location = incident["location"]
                updated_at = now_tw()
                updater_name = get_display_name(group_id, user_id)

                if not update_incident_location(
                    incident["id"],
                    old_location,
                    text,
                    user_id or "unknown",
                    updater_name,
                    updated_at,
                ):
                    reply_text(event.reply_token, "事故地點更新失敗，原地點維持不變，請稍後再試。")
                    continue

                incident["location"] = text
                incident["location_logs"].append({
                    "old_location": old_location,
                    "new_location": text,
                    "updated_by": user_id or "unknown",
                    "updated_by_name": updater_name,
                    "updated_at": updated_at,
                })

                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(
                        text=(
                            f"📍【事故地點更正】\n"
                            f"事件編號：{incident['id']}\n"
                            f"原事故地點：{old_location}\n"
                            f"最新事故地點：{text}\n"
                            f"更正人員：{updater_name}（{time_text(updated_at)}）\n"
                            "請所有救護支援人員以最新地點為準。"
                        )
                    ),
                )
            else:
                reply_text(event.reply_token, "原救護事件已不存在，本次地點修正未套用。")
            continue

        # ----------------------------------------------------
        # 再檢查初始事故地點等待狀態
        # ----------------------------------------------------
        if group_id in pending_locations:
            if expire_if_needed("location", group_id):
                reply_text(event.reply_token, "緊急救護啟動流程已逾時取消，請重新輸入 119 啟動。")
                continue

            pending = pending_locations.pop(group_id)
            task = timeout_tasks.pop(("location", group_id), None)
            if task and not task.done():
                task.cancel()

            incident_id = next_incident_id()
            created_at = now_tw()

            incident = {
                "id": incident_id,
                "group_id": group_id,
                "initial_location": text,
                "location": text,
                "initiator": pending["initiator"],
                "initiator_name": pending["initiator_name"],
                "created_at": created_at,
                "responses": [],
                "location_logs": [],
            }

            try:
                create_incident_record(incident, group_id)
            except Exception as error:
                print(f"救護事件寫入資料庫失敗：{error}")
                reply_text(event.reply_token, "⚠️ 救護事件資料保存失敗，未發送正式通知；請稍後重新輸入 119 啟動。")
                continue

            active_incidents[group_id] = incident

            # 建立啟動滿 1 小時自動結束計時任務
            schedule_incident_timer(group_id, incident)

            notice = (
                "🚨【緊急救護通知】🚨\n"
                f"事件編號：{incident_id}\n"
                f"事故地點：{text}\n"
                f"啟動人員：{pending['initiator_name']}\n"
                f"啟動時間：{time_text(created_at)}\n"
                "請相關人員前往協助處理，並點擊下方按鈕回報是否可以前往。"
            )

            # 沿用原有已測試成功的 LINE 群組 @All 通知機制
            reply_emergency_notice(
                event.reply_token,
                notice,
                incident_id,
                group_id,
            )
            continue

    return "OK"
