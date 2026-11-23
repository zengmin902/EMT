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

# 救護事件於啟動滿 1 小時自動結束
INCIDENT_AUTO_CLOSE_SECONDS = 60 * 60

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
    ]
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)
            cur.execute("ALTER TABLE incident_responses DROP CONSTRAINT IF EXISTS incident_responses_role_check")
            cur.execute("ALTER TABLE incident_responses ADD CONSTRAINT incident_responses_role_check CHECK (role IN ('nurse', 'safety', 'member'))")
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
    """更新目前狀態，並永久保存每次回應或修改歷程。"""
    with get_db_connection() as conn:
        old = conn.execute(
            "SELECT status FROM incident_responses WHERE incident_id=%s AND user_id=%s",
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
            (incident_id, user_id, record["status"], record["time"],
             record.get("display_name") or "群組成員")
        )
        conn.execute(
            "INSERT INTO incident_response_history "
            "(incident_id, user_id, display_name, status, changed_at, previous_status) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (incident_id, user_id, record.get("display_name") or "群組成員",
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


def get_incident_by_id(incident_id):
    with get_db_connection() as conn:
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


def schedule_timeout(kind, group_id, created_at):
    key = (kind, group_id)
    old_task = timeout_tasks.get(key)
    if old_task and not old_task.done():
        old_task.cancel()

    async def wait_and_expire():
        try:
            await asyncio.sleep(WAIT_SECONDS)
            pending = pending_locations if kind == "location" else pending_corrections
            item = pending.get(group_id)
            if item and item["created_at"] == created_at:
                pending.pop(group_id, None)
                timeout_tasks.pop(key, None)
                notice = ("⌛ 已超過 5 分鐘未收到事故地點，緊急救護啟動流程已自動取消。"
                          if kind == "location" else
                          "⌛ 已超過 5 分鐘未收到新的事故地點，本次修正已自動取消，維持原事故地點。")
                try:
                    line_bot_api.push_message(group_id, TextSendMessage(text=notice))
                except Exception as error:
                    print(f"等待流程逾時通知失敗：{error}")
        except asyncio.CancelledError:
            return

    timeout_tasks[key] = asyncio.create_task(wait_and_expire())


def expire_if_needed(kind, group_id):
    pending = pending_locations if kind == "location" else pending_corrections
    item = pending.get(group_id)
    if not item:
        return False
    if (now_tw() - item["created_at"]).total_seconds() >= WAIT_SECONDS:
        pending.pop(group_id, None)
        task = timeout_tasks.pop((kind, group_id), None)
        if task and not task.done():
            task.cancel()
        return True
    return False


def response_message_contents(incident_id):
    return {
        "type": "bubble",
        "body": {"type": "box", "layout": "vertical", "spacing": "md", "contents": [
            {"type": "text", "text": "救護人員回報", "weight": "bold", "size": "lg"},
            {"type": "text", "text": "請所有群組成員依目前狀況選擇，之後可再次修改。", "wrap": True, "size": "sm"},
            {"type": "button", "style": "primary", "color": "#16834A", "height": "md",
             "action": postback("可前往", f"action=ack_available&incident_id={incident_id}")},
            {"type": "button", "style": "secondary", "color": "#B3261E", "height": "md",
             "action": postback("無法前往", f"action=ack_unavailable&incident_id={incident_id}")}
        ]}
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
    """保留已測試的真正 @All 通知及 Flex 卡片；Reply 失敗改用 Push。"""
    flex_contents = response_message_contents(incident_id)
    def send(url, payload):
        request = urllib.request.Request(
            url, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"},
            method="POST"
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()
    reply_payload = {
        "replyToken": reply_token,
        "messages": [
            {"type": "textV2", "text": "{everyone}\n" + notice,
             "substitution": {"everyone": {"type": "mention", "mentionee": {"type": "all"}}}},
            {"type": "flex", "altText": "請回報是否可以前往支援", "contents": flex_contents}
        ]
    }
    try:
        send("https://api.line.me/v2/bot/message/reply", reply_payload)
        print("緊急救護通知已透過 Reply API 發送真正 @All 及回報卡片。")
        return
    except Exception as error:
        body = error.read().decode("utf-8", errors="replace") if hasattr(error, "read") else str(error)
        print(f"LINE Reply 發送失敗：{getattr(error, 'code', '未知')}；{body}")
    push_payload = {
        "to": group_id,
        "messages": [
            {"type": "textV2", "text": "{everyone}\n" + notice,
             "substitution": {"everyone": {"type": "mention", "mentionee": {"type": "all"}}}},
            {"type": "flex", "altText": "請回報是否可以前往支援", "contents": flex_contents}
        ]
    }
    try:
        send("https://api.line.me/v2/bot/message/push", push_payload)
        print("緊急救護通知已改用 Push API 補送。")
    except Exception as error:
        body = error.read().decode("utf-8", errors="replace") if hasattr(error, "read") else str(error)
        print(f"Push @All 失敗：{getattr(error, 'code', '未知')}；{body}")
        try:
            line_bot_api.push_message(group_id, [TextSendMessage(text=notice), response_message(incident_id)])
        except Exception as fallback_error:
            print(f"一般 Push 補送失敗：{fallback_error}")


# ============================================================
# 救護事件定時自動結束（啟動滿 1 小時）
# ============================================================
def cancel_incident_timer(group_id):
    task = incident_timer_tasks.pop(group_id, None)
    if task and not task.done():
        task.cancel()


def finish_incident(group_id, incident_id, end_type):
    incident = active_incidents.get(group_id)
    if not incident or incident["id"] != incident_id:
        return False
    status = "auto_closed" if end_type == "系統自動結束" else "closed"
    if not close_incident_record(incident_id, status, end_type):
        return False
    cancel_incident_timer(group_id)
    active_incidents.pop(group_id, None)
    pending_closures.pop(group_id, None)
    try:
        line_bot_api.push_message(group_id, TextSendMessage(text=build_final_report(incident, end_type)))
    except Exception as error:
        print(f"事件結束報告發送失敗：{incident_id}；{error}")
    return True


def schedule_incident_timer(group_id, incident):
    cancel_incident_timer(group_id)
    incident_id = incident["id"]
    created_at = incident["created_at"]
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)

    async def wait_and_close():
        try:
            elapsed = (now_tw() - created_at.astimezone(TAIWAN_TZ)).total_seconds()
            await asyncio.sleep(max(0, INCIDENT_AUTO_CLOSE_SECONDS - elapsed))
            if finish_incident(group_id, incident_id, "系統自動結束"):
                print(f"救護事件滿 1 小時自動結束：{incident_id}")
        except asyncio.CancelledError:
            return
        except Exception as error:
            print(f"自動結束計時錯誤：{incident_id}；{error}")
        finally:
            if incident_timer_tasks.get(group_id) is asyncio.current_task():
                incident_timer_tasks.pop(group_id, None)

    incident_timer_tasks[group_id] = asyncio.create_task(wait_and_close())


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
        "結束救護事件 YYYYMMDD01：立即結束指定事件\n"
        "系統狀態：查詢機器人及資料庫狀態"
    )


def query_active_incidents_text():
    with get_db_connection() as conn:
        rows = conn.execute(
            "SELECT incident_id, group_id, location, initiator_name, created_at "
            "FROM incidents WHERE status='active' ORDER BY created_at"
        ).fetchall()
    if not rows:
        return "目前沒有進行中的救護事件。"
    lines = ["【目前進行中的救護事件】"]
    for row in rows:
        incident = get_incident_by_id(row["incident_id"])
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


# ============================================================
# PostgreSQL 啟動初始化及狀態還原
# ============================================================
@app.on_event("startup")
async def startup_restore_state():
    # 若 DATABASE_URL、資料庫連線或資料表初始化失敗，讓 Render 明確顯示啟動失敗。
    init_database()
    load_persistent_state()
    print(f"PostgreSQL 已連線；已還原進行中救護事件 {len(active_incidents)} 件。")
    for group_id, incident in list(active_incidents.items()):
        created_at = incident["created_at"]
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        elapsed = (now_tw() - created_at.astimezone(TAIWAN_TZ)).total_seconds()
        if elapsed >= INCIDENT_AUTO_CLOSE_SECONDS:
            finish_incident(group_id, incident["id"], "系統自動結束")
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
    signature = request.headers.get("X-Line-Signature", "")
    body = await request.body()
    try:
        events = parser.parse(body.decode("utf-8"), signature)
    except InvalidSignatureError:
        raise HTTPException(status_code=400, detail="Invalid signature.")

    for event in events:
        group_id = get_group_id(event)
        source_type = getattr(getattr(event, "source", None), "type", None)
        user_id = get_user_id(event)

        if isinstance(event, JoinEvent) and group_id:
            line_bot_api.reply_message(event.reply_token, panel_message())
            continue

        # ----------------------------------------------------
        # 最高管理者私訊管理：所有管理指令僅接受管理者 DM
        # ----------------------------------------------------
        if isinstance(event, MessageEvent) and isinstance(event.message, TextMessage):
            text = event.message.text.strip()
            if not group_id:
                if source_type != "user":
                    reply_text(event.reply_token, "管理功能僅能在與救護機器人的一對一私訊中執行。")
                    continue
                if is_admin(user_id):
                    if text == "管理選單":
                        reply_text(event.reply_token, build_admin_menu())
                    elif text == "查詢目前事件":
                        reply_text(event.reply_token, query_active_incidents_text())
                    elif text.startswith("查詢救護事件 "):
                        incident_id = text.replace("查詢救護事件 ", "", 1).strip()
                        incident = get_incident_by_id(incident_id)
                        reply_text(event.reply_token, build_historical_report(incident) if incident
                                   else "查無此救護事件，請確認事件編號。")
                    elif text.startswith("結束救護事件 "):
                        incident_id = text.replace("結束救護事件 ", "", 1).strip()
                        incident = get_incident_by_id(incident_id)
                        if not incident:
                            reply_text(event.reply_token, "查無此救護事件，請確認事件編號。")
                        elif incident["status"] != "active":
                            reply_text(event.reply_token, "此救護事件已結束，無須重複操作。")
                        elif finish_incident(incident["group_id"], incident_id, "最高管理者人工結束"):
                            reply_text(event.reply_token, f"已立即結束救護事件 {incident_id}，完整結束報告已發送至原救護群組。")
                        else:
                            reply_text(event.reply_token, "事件結束失敗，資料庫狀態未變更，請稍後再試。")
                    elif text == "系統狀態":
                        try:
                            with get_db_connection() as conn:
                                conn.execute("SELECT 1")
                            db_status = "正常"
                        except Exception as error:
                            db_status = f"異常：{error}"
                        reply_text(event.reply_token,
                                   f"救護機器人：運作中\nNeon PostgreSQL：{db_status}\n目前記憶體內進行中事件：{len(active_incidents)} 件")
                    else:
                        reply_text(event.reply_token, "最高管理者私訊管理模式。輸入「管理選單」查看可用指令。")
                else:
                    reply_text(event.reply_token, "本機器人僅接受指定群組救護流程；管理功能僅限最高管理者私訊操作。")
                continue

            # 管理者在群組輸入管理指令，一律不執行。
            if is_admin(user_id) and (
                text in ("管理選單", "查詢目前事件", "系統狀態")
                or text.startswith("查詢救護事件 ")
                or text.startswith("結束救護事件 ")
                or text in ("人員管理", "查詢人員名單", "啟動護理人員登記", "關閉護理人員登記",
                            "啟動工安人員登記", "關閉工安人員登記", "重設護理人員名單", "重設工安人員名單")
            ):
                reply_text(event.reply_token, "⛔ 最高管理者管理指令僅能在與救護機器人的一對一私訊中執行。")
                continue

        if not group_id:
            continue

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
                    continue
                if not user_id:
                    reply_text(event.reply_token, "無法辨識回報帳號，請以個人 LINE 帳號操作。")
                    continue

                status = "available" if action == "ack_available" else "unavailable"
                display_name = get_display_name(group_id, user_id)
                timestamp = now_tw()
                previous = incident["responses"].get(user_id)
                old_status = previous.get("status") if previous else None
                record = {"status": status, "time": timestamp, "display_name": display_name}
                try:
                    save_incident_response(incident_id, user_id, record)
                except Exception as error:
                    print(f"回應資料保存失敗：{incident_id}；{error}")
                    reply_text(event.reply_token, "回應保存失敗，請稍後重新操作。")
                    continue
                incident["responses"][user_id] = record

                status_label = "可前往" if status == "available" else "無法前往"
                reply_text(event.reply_token, f"✅ 已登記：{status_label}\n回報人員：{display_name}\n回報時間：{time_text(timestamp)}")

                # 可前往時通知群組；無法前往不額外通知。
                # 從無法前往改為可前往，也會再次通知最新狀態。
                if status == "available" and old_status != "available":
                    try:
                        line_bot_api.push_message(
                            group_id,
                            TextSendMessage(text=f"【救護回應】\n{display_name}：可前往")
                        )
                    except Exception as error:
                        print(f"可前往群組通知失敗：{error}")
                continue

            if action == "start_emergency":
                # 舊版面板仍可用；按下後直接進入等候地點，不再提供取消確認卡片。
                if group_id in active_incidents:
                    reply_text(event.reply_token, "⚠️ 目前已有進行中的救護事件，請先由最高管理者結束現有事件。")
                elif group_id in pending_locations:
                    reply_text(event.reply_token, "目前已有啟動流程正在等待事故地點。")
                else:
                    pending = {"initiator": user_id,
                               "initiator_name": get_display_name(group_id, user_id),
                               "created_at": now_tw()}
                    pending_locations[group_id] = pending
                    schedule_timeout("location", group_id, pending["created_at"])
                    reply_text(event.reply_token, "🚨 請直接輸入事故地點；收到地點後將立即建立救護事件並通知全群。")
                continue

            # 停用舊版護理/工安登記及結束確認按鈕。
            if action.startswith(("admin_", "register_", "ack_nurse_", "ack_safety_", "confirm_close", "cancel_close")):
                reply_text(event.reply_token, "此為舊版功能按鈕，已停用。請使用最新救護事件流程。")
                continue
            if action in ("cancel_start", "cancel_correction"):
                if action == "cancel_start":
                    pending_locations.pop(group_id, None)
                    task = timeout_tasks.pop(("location", group_id), None)
                else:
                    pending_corrections.pop(group_id, None)
                    task = timeout_tasks.pop(("correction", group_id), None)
                if task and not task.done():
                    task.cancel()
                reply_text(event.reply_token, "已取消目前等待流程。")
                continue

        if not (isinstance(event, MessageEvent) and isinstance(event.message, TextMessage)):
            continue

        text = event.message.text.strip()

        # 119：直接啟動，要求輸入事故地點
        if text == "119":
            if group_id in active_incidents:
                reply_text(event.reply_token, "⚠️ 目前已有進行中的救護事件，請先由最高管理者結束現有事件。")
            elif group_id in pending_locations:
                reply_text(event.reply_token, "目前已有啟動流程正在等待事故地點，請直接輸入事故地點。")
            else:
                pending = {"initiator": user_id,
                           "initiator_name": get_display_name(group_id, user_id),
                           "created_at": now_tw()}
                pending_locations[group_id] = pending
                schedule_timeout("location", group_id, pending["created_at"])
                reply_text(event.reply_token, "🚨【緊急救護啟動】\n請直接輸入事故地點；收到地點後將立即建立救護事件並通知全群。")
            continue

        # 回報目前狀況
        if text == "回報狀況":
            incident = active_incidents.get(group_id)
            reply_text(event.reply_token, incident_status_text(incident) if incident else "目前沒有進行中的救護事件。")
            continue

        # 事故地點修正流程
        if text == "修改事故地點":
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
            continue

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
                if not update_incident_location(incident["id"], text, get_display_name(group_id, user_id)):
                    reply_text(event.reply_token, "事故地點更新失敗，原地點維持不變，請稍後再試。")
                else:
                    old_location = incident["location"]
                    incident["location"] = text
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text=f"📍【事故地點更正】\n原事故地點：{old_location}\n最新事故地點：{text}\n請所有救護人員以最新地點為準。")
                    )
            else:
                reply_text(event.reply_token, "原救護事件已不存在，本次地點修正未套用。")
            continue

        # 等待初始事故地點：第一筆文字直接建立事件
        if group_id in pending_locations:
            if expire_if_needed("location", group_id):
                reply_text(event.reply_token, "緊急救護啟動流程已逾時取消，請重新輸入 119。")
                continue
            pending = pending_locations.pop(group_id)
            task = timeout_tasks.pop(("location", group_id), None)
            if task and not task.done():
                task.cancel()

            incident_id = next_incident_id()
            incident = {
                "id": incident_id, "location": text,
                "initiator": pending["initiator"],
                "initiator_name": pending["initiator_name"],
                "created_at": now_tw(), "responses": {}
            }
            try:
                create_incident_record(incident, group_id)
            except Exception as error:
                print(f"救護事件寫入資料庫失敗：{error}")
                reply_text(event.reply_token, "⚠️ 救護事件資料保存失敗，未發送正式通知；請稍後重新啟動。")
                continue

            active_incidents[group_id] = incident
            schedule_incident_timer(group_id, incident)
            notice = (
                "🚨【緊急救護通知】🚨\n"
                f"事件編號：{incident_id}\n"
                f"事故地點：{text}\n"
                "請群組全體成員確認是否可以前往支援，並使用下方按鈕回報。"
            )
            reply_emergency_notice(event.reply_token, notice, incident_id, group_id)
            continue

    return "OK"
