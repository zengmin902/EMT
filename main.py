import os
import uuid
import asyncio
from datetime import datetime, timezone, timedelta
from urllib.parse import parse_qs

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
    FlexContainer,
    PostbackAction,
)

app = FastAPI()

# ============================================================
# LINE 機器人設定
# ============================================================
CHANNEL_ACCESS_TOKEN = os.environ.get("CHANNEL_ACCESS_TOKEN", "")
CHANNEL_SECRET = os.environ.get("CHANNEL_SECRET", "")

line_bot_api = LineBotApi(CHANNEL_ACCESS_TOKEN)
parser = WebhookParser(CHANNEL_SECRET)

# ============================================================
# 指定救護人員名單
# 請取得救護人員的 LINE User ID 後填入。
# 尚未填入前採「拒絕計入」原則，避免非指定人員被列為有效回報。
# 範例：RESCUE_USER_IDS = {"Uxxxxxxxxxxxxxxxx", "Uyyyyyyyyyyyyyyyy"}
# ============================================================
RESCUE_USER_IDS = set()

TAIWAN_TZ = timezone(timedelta(hours=8))
WAIT_SECONDS = 5 * 60

# 僅存放記憶體；Render 重啟、重新部署或服務休眠重啟後，資料可能消失。
pending_locations = {}       # group_id -> pending start data
pending_corrections = {}     # group_id -> pending correction data
active_incidents = {}        # group_id -> active incident data
pending_closures = {}        # group_id -> close confirmation data
timeout_tasks = {}           # (kind, group_id) -> asyncio.Task


def now_tw():
    return datetime.now(TAIWAN_TZ)


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
        return line_bot_api.get_group_member_profile(group_id, user_id).display_name
    except Exception:
        return "群組成員"


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
                            TextSendMessage(text="⌛ 已超過 5 分鐘未收到事故地點，緊急救護啟動流程已自動取消。")
                        )
                    else:
                        line_bot_api.push_message(
                            group_id,
                            TextSendMessage(text="⌛ 已超過 5 分鐘未收到新的事故地點，本次修正已自動取消，維持原事故地點。")
                        )
                except Exception:
                    pass
        except asyncio.CancelledError:
            return

    try:
        timeout_tasks[key] = asyncio.create_task(wait_and_expire())
    except RuntimeError:
        # 若當下沒有可用 event loop，後續仍會在收到訊息時依時間差檢查逾時。
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


def flex_message(alt_text, contents):
    return FlexSendMessage(
        alt_text=alt_text,
        contents=contents
    )


def button(label, action, color="#1769AA"):
    return {
        "type": "button",
        "style": "primary",
        "color": color,
        "height": "sm",
        "action": action,
    }


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
                {"type": "text", "text": "廠區緊急救護面板", "weight": "bold", "size": "lg"},
                {"type": "text", "text": "發生緊急狀況時，請點選下方按鈕啟動救護通知。", "wrap": True, "size": "sm", "color": "#666666"},
                {"type": "button", "style": "primary", "color": "#D93025", "height": "md",
                 "action": postback("🚨 緊急救護", "action=start_emergency")}
            ]
        }
    }
    return flex_message("廠區緊急救護面板", contents)


def cancel_start_message():
    contents = {
        "type": "bubble",
        "body": {
            "type": "box", "layout": "vertical", "spacing": "md",
            "contents": [
                {"type": "text", "text": "請在群組輸入事故地點", "weight": "bold", "size": "lg"},
                {"type": "text", "text": "任何群組成員皆可輸入。收到第一筆有效地點文字後，將立即發送緊急救護通知。等待時間為 5 分鐘。", "wrap": True, "size": "sm"},
                {"type": "button", "style": "secondary", "height": "sm",
                 "action": postback("取消啟動", "action=cancel_start")}
            ]
        }
    }
    return flex_message("請輸入事故地點或取消啟動", contents)


def cancel_correction_message():
    contents = {
        "type": "bubble",
        "body": {
            "type": "box", "layout": "vertical", "spacing": "md",
            "contents": [
                {"type": "text", "text": "請輸入新的事故地點", "weight": "bold", "size": "lg"},
                {"type": "text", "text": "任何群組成員皆可提供新地點；5 分鐘內未收到內容將自動取消。", "wrap": True, "size": "sm"},
                {"type": "button", "style": "secondary", "height": "sm",
                 "action": postback("取消修正", "action=cancel_correction")}
            ]
        }
    }
    return flex_message("請輸入新事故地點或取消修正", contents)


def response_message(incident_id):
    contents = {
        "type": "bubble",
        "body": {
            "type": "box", "layout": "vertical", "spacing": "md",
            "contents": [
                {"type": "text", "text": "救護人員回報", "weight": "bold", "size": "lg"},
                {"type": "text", "text": "請指定救護人員依目前狀況選擇：", "wrap": True, "size": "sm"},
                {"type": "button", "style": "primary", "color": "#16834A", "height": "md",
                 "action": postback("收到，可以前往", f"action=ack_available&incident_id={incident_id}")},
                {"type": "button", "style": "primary", "color": "#B3261E", "height": "md",
                 "action": postback("收到，但無法前往", f"action=ack_unavailable&incident_id={incident_id}")}
            ]
        }
    }
    return flex_message("請回報是否可以前往支援", contents)


def close_confirmation_message(incident_id, status_text):
    contents = {
        "type": "bubble",
        "body": {
            "type": "box", "layout": "vertical", "spacing": "md",
            "contents": [
                {"type": "text", "text": "確認結束救護事件？", "weight": "bold", "size": "lg"},
                {"type": "text", "text": status_text, "wrap": True, "size": "sm"},
                {"type": "button", "style": "primary", "color": "#D93025", "height": "md",
                 "action": postback("確認結束", f"action=confirm_close&incident_id={incident_id}")},
                {"type": "button", "style": "secondary", "height": "md",
                 "action": postback("取消", f"action=cancel_close&incident_id={incident_id}")}
            ]
        }
    }
    return flex_message("請確認是否結束救護事件", contents)


def incident_status_text(group_id, incident):
    roster_total = len(RESCUE_USER_IDS)
    available = []
    unavailable = []
    not_responded = []

    for user_id in RESCUE_USER_IDS:
        record = incident["responses"].get(user_id)
        name = get_display_name(group_id, user_id)
        if not record:
            not_responded.append(name)
        elif record["status"] == "available":
            available.append(name)
        else:
            unavailable.append(name)

    responded_count = len(available) + len(unavailable)
    lines = [
        "📋【救護人員回報狀況】",
        f"事件編號：{incident['id']}",
        f"事故地點：{incident['location']}",
        f"指定救護人員：{roster_total} 人",
        f"已回報：{responded_count} 人",
        f"可以前往：{len(available)} 人",
        f"無法前往：{len(unavailable)} 人",
        f"尚未回報：{len(not_responded)} 人",
        "",
        "【可以前往】",
        "、".join(available) if available else "目前無",
        "",
        "【無法前往】",
        "、".join(unavailable) if unavailable else "目前無",
        "",
        "【尚未回報】",
        "、".join(not_responded) if not_responded else "目前無",
    ]
    if roster_total == 0:
        lines.extend(["", "⚠️ 尚未設定指定救護人員 LINE User ID，目前無法進行有效人員回報統計。"])
    return "\n".join(lines)


def reply_text(reply_token, text):
    line_bot_api.reply_message(reply_token, TextSendMessage(text=text))


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

        # 加入群組時，自動發送緊急救護面板
        if isinstance(event, JoinEvent) and group_id:
            line_bot_api.reply_message(event.reply_token, panel_message())
            continue

        if not group_id:
            # 本機器人僅處理 LINE 群組操作
            if isinstance(event, MessageEvent) and isinstance(event.message, TextMessage):
                reply_text(event.reply_token, "本功能僅供指定 LINE 群組使用。")
            continue

        if isinstance(event, PostbackEvent):
            data = parse_qs(getattr(event.postback, "data", ""))
            action = data.get("action", [""])[0]
            user_id = get_user_id(event)
            incident_id = data.get("incident_id", [""])[0]

            if action == "start_emergency":
                if group_id in active_incidents:
                    reply_text(event.reply_token, "⚠️ 目前已有進行中的救護事件，請先結束現有事件。")
                elif group_id in pending_locations:
                    reply_text(event.reply_token, "目前已有緊急救護啟動流程正在等待事故地點。")
                else:
                    pending = {
                        "initiator": user_id,
                        "initiator_name": get_display_name(group_id, user_id),
                        "created_at": now_tw(),
                    }
                    pending_locations[group_id] = pending
                    schedule_timeout("location", group_id, pending["created_at"])
                    line_bot_api.reply_message(event.reply_token, cancel_start_message())

            elif action == "cancel_start":
                pending = pending_locations.get(group_id)
                if pending and user_id == pending["initiator"]:
                    pending_locations.pop(group_id, None)
                    task = timeout_tasks.pop(("location", group_id), None)
                    if task and not task.done():
                        task.cancel()
                    reply_text(event.reply_token, "已取消緊急救護啟動流程，尚未發送正式通知。")
                else:
                    reply_text(event.reply_token, "只有原始啟動人員可以取消此啟動流程。")

            elif action == "cancel_correction":
                pending = pending_corrections.pop(group_id, None)
                if pending:
                    task = timeout_tasks.pop(("correction", group_id), None)
                    if task and not task.done():
                        task.cancel()
                    reply_text(event.reply_token, "已取消本次事故地點修正，維持原事故地點。")
                else:
                    reply_text(event.reply_token, "目前沒有等待中的事故地點修正流程。")

            elif action in ("ack_available", "ack_unavailable"):
                incident = active_incidents.get(group_id)
                if not incident or incident["id"] != incident_id:
                    reply_text(event.reply_token, "此救護事件已結束或通知已失效。")
                elif user_id not in RESCUE_USER_IDS:
                    reply_text(event.reply_token, "此帳號不在指定救護人員名單內，本次回報不列入有效統計。")
                elif user_id in incident["responses"]:
                    old = incident["responses"][user_id]
                    old_status = "可以前往" if old["status"] == "available" else "無法前往"
                    reply_text(event.reply_token, f"已收到你的回報，不重複更新紀錄。\n首次回報狀態：{old_status}\n首次回報時間：{time_text(old['time'])}")
                else:
                    status = "available" if action == "ack_available" else "unavailable"
                    display_name = get_display_name(group_id, user_id)
                    timestamp = now_tw()
                    incident["responses"][user_id] = {
                        "status": status,
                        "time": timestamp,
                        "display_name": display_name,
                    }
                    status_label = "收到，可以前往" if status == "available" else "收到，但無法前往"
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text=f"✅ 救護回報\n回報人員：{display_name}\n回報狀態：{status_label}\n首次回報時間：{time_text(timestamp)}")
                    )

            elif action == "confirm_close":
                incident = active_incidents.get(group_id)
                if not incident or incident["id"] != incident_id:
                    reply_text(event.reply_token, "此救護事件已結束或確認畫面已失效。")
                elif user_id != incident["initiator"]:
                    reply_text(event.reply_token, "只有原始啟動人員可以結束此救護事件。")
                else:
                    active_incidents.pop(group_id, None)
                    pending_closures.pop(group_id, None)
                    reply_text(event.reply_token, f"✅ 救護事件已結束。\n事件編號：{incident_id}\n事故地點：{incident['location']}\n事件資料已從本次執行中的暫存狀態清除。")

            elif action == "cancel_close":
                incident = active_incidents.get(group_id)
                if incident and incident["id"] == incident_id and user_id == incident["initiator"]:
                    pending_closures.pop(group_id, None)
                    reply_text(event.reply_token, "已取消結束作業，救護事件及回報資料繼續保留。")
                else:
                    reply_text(event.reply_token, "無法取消此結束確認，請確認事件狀態及操作權限。")
            continue

        if not (isinstance(event, MessageEvent) and isinstance(event.message, TextMessage)):
            continue

        text = event.message.text.strip()
        user_id = get_user_id(event)

        if text == "緊急面板":
            line_bot_api.reply_message(event.reply_token, panel_message())
            continue

        if text == "回報狀況":
            incident = active_incidents.get(group_id)
            if incident:
                reply_text(event.reply_token, incident_status_text(group_id, incident))
            else:
                reply_text(event.reply_token, "目前沒有進行中的救護事件。")
            continue

        if text == "結束救護事件":
            incident = active_incidents.get(group_id)
            if not incident:
                reply_text(event.reply_token, "目前沒有進行中的救護事件。")
            elif user_id != incident["initiator"]:
                reply_text(event.reply_token, "只有原始啟動人員可以提出結束救護事件。")
            else:
                status = incident_status_text(group_id, incident)
                pending_closures[group_id] = {"incident_id": incident["id"], "initiator": user_id}
                line_bot_api.reply_message(
                    event.reply_token,
                    close_confirmation_message(incident["id"], status)
                )
            continue

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
                line_bot_api.reply_message(event.reply_token, cancel_correction_message())
            continue

        # 先檢查事故地點修正等待狀態
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
                incident["location"] = text
                line_bot_api.reply_message(
                    event.reply_token,
                    TextSendMessage(text=f"📍【事故地點更正】\n原事故地點：{old_location}\n最新事故地點：{text}\n請所有救護人員以最新地點為準。")
                )
            else:
                reply_text(event.reply_token, "原救護事件已不存在，本次地點修正未套用。")
            continue

        # 再檢查初始事故地點等待狀態
        if group_id in pending_locations:
            if expire_if_needed("location", group_id):
                reply_text(event.reply_token, "緊急救護啟動流程已逾時取消，請重新點選緊急救護。")
                continue
            pending = pending_locations.pop(group_id)
            task = timeout_tasks.pop(("location", group_id), None)
            if task and not task.done():
                task.cancel()

            incident_id = uuid.uuid4().hex[:8].upper()
            incident = {
                "id": incident_id,
                "location": text,
                "initiator": pending["initiator"],
                "initiator_name": pending["initiator_name"],
                "created_at": now_tw(),
                "responses": {},
            }
            active_incidents[group_id] = incident

            notice = (
                "🚨【緊急救護通知】🚨\n"
                f"事件編號：{incident_id}\n"
                f"事故地點：{text}\n"
                "請相關救護人員前往處理。\n"
                "收到通知的救護成員，請依自身狀況選擇回報。"
            )
            line_bot_api.reply_message(
                event.reply_token,
                [
                    TextSendMessage(text=notice),
                    response_message(incident_id),
                ]
            )
            continue

    return "OK"
