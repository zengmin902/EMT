
import os
import asyncio
import json
import urllib.request
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
# 僅存放記憶體；Render 重啟、重新部署或服務休眠重啟後，資料可能消失。
pending_locations = {}
pending_corrections = {}
active_incidents = {}
pending_closures = {}
timeout_tasks = {}

# 每個群組各自的救護事件定時任務
incident_timer_tasks = {}

# ============================================================
# 護理人員登記資料（測試版：僅存在記憶體）
# ============================================================
# nurse_users[group_id] = {
#     1: {"user_id": "...", "display_name": "..."},
#     2: {"user_id": "...", "display_name": "..."},
#     3: {"user_id": "...", "display_name": "..."},
# }
nurse_users = {}

# 正在開放登記的群組 ID
nurse_registration_groups = set()

# 每日事件流水號（測試版僅存於記憶體；重新啟動後會重新計數）
# 格式：YYYYMMDD01；已產生的編號不回收。
event_counters = {}


def now_tw():
    return datetime.now(TAIWAN_TZ)


def next_incident_id():
    """產生 YYYYMMDD01 格式的每日事件流水編號。"""
    date_key = now_tw().strftime("%Y%m%d")
    next_number = event_counters.get(date_key, 0) + 1
    event_counters[date_key] = next_number
    return f"{date_key}{next_number:02d}"


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


def get_group_nurses(group_id):
    """依護理人員 1、2、3 順序取得該群組已登記名單。"""
    records = nurse_users.get(group_id, {})
    return [
        (slot, records[slot])
        for slot in (1, 2, 3)
        if slot in records
    ]


def get_next_nurse_slot(group_id):
    """回傳第一個空缺名額；沒有空缺時回傳 None。"""
    records = nurse_users.get(group_id, {})
    for slot in (1, 2, 3):
        if slot not in records:
            return slot
    return None


def nurse_registration_status_text(group_id):
    """整理目前三個護理人員名額的登記狀況。"""
    records = nurse_users.get(group_id, {})
    lines = ["📋【護理人員登記狀況】"]

    for slot in (1, 2, 3):
        record = records.get(slot)
        if record:
            lines.append(
                f"護理人員{slot}：{record.get('display_name') or '群組成員'}"
            )
        else:
            lines.append(f"護理人員{slot}：尚未登記")

    if group_id in nurse_registration_groups:
        next_slot = get_next_nurse_slot(group_id)
        if next_slot:
            lines.append(f"登記模式：開啟（下一位登記為護理人員{next_slot}）")
        else:
            lines.append("登記模式：名額已滿")
    else:
        lines.append("登記模式：關閉")

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
    return {
        "type": "bubble",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {
                    "type": "text",
                    "text": "救護人員回報",
                    "weight": "bold",
                    "size": "lg"
                },
                {
                    "type": "text",
                    "text": "請收到通知的群組成員依目前狀況選擇：",
                    "wrap": True,
                    "size": "sm"
                },
                {
                    "type": "button",
                    "style": "primary",
                    "color": "#16834A",
                    "height": "md",
                    "action": postback(
                        "收到，可以前往",
                        f"action=ack_available&incident_id={incident_id}"
                    )
                },
                {
                    "type": "button",
                    "style": "primary",
                    "color": "#B3261E",
                    "height": "md",
                    "action": postback(
                        "收到，但無法前往",
                        f"action=ack_unavailable&incident_id={incident_id}"
                    )
                }
            ]
        }
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
    使用 LINE Text Message V2 同時真正 @All 及 @已登記護理人員。
    只標記該群組已完成登記的護理人員；空缺名額不會影響通知。
    若 API 呼叫失敗，改用一般通知及回報卡片。
    """
    substitutions = {
        "everyone": {
            "type": "mention",
            "mentionee": {
                "type": "all"
            }
        }
    }

    mention_tokens = []

    for slot, record in get_group_nurses(group_id):
        user_id = record.get("user_id")
        if not user_id:
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
    if mention_line:
        text_v2 = "{everyone}\n" + mention_line + "\n" + notice
    else:
        text_v2 = "{everyone}\n" + notice

    payload = {
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
                "contents": response_message_contents(incident_id)
            }
        ]
    }

    request = urllib.request.Request(
        "https://api.line.me/v2/bot/message/reply",
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

        nurse_count = len(mention_tokens)
        print(
            "緊急救護通知已發送真正的 @All，"
            f"並標記 {nurse_count} 名已登記護理人員。"
        )

    except Exception as error:
        print(f"LINE @All／護理人員 @Mention 發送失敗，改用一般通知：{error}")

        try:
            line_bot_api.reply_message(
                reply_token,
                [
                    TextSendMessage(text=notice),
                    response_message(incident_id)
                ]
            )

            print("一般緊急救護通知及回報卡片已送出。")

        except Exception as fallback_error:
            print(f"一般通知補送也失敗：{fallback_error}")


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
    """
    事件啟動後：
    1. 滿1小時，在群組 @ 原始啟動人員。
    2. 滿2小時，若事件仍未結束，自動結束並公告。
    """
    cancel_incident_timer(group_id)

    incident_id = incident["id"]
    initiator = incident["initiator"]
    initiator_name = incident["initiator_name"]
    location = incident["location"]

    async def wait_and_process():
        try:
            # 第一次等待：1小時
            await asyncio.sleep(INCIDENT_REMINDER_SECONDS)

            current = active_incidents.get(group_id)

            # 確認仍為同一件進行中的事件
            if not current or current["id"] != incident_id:
                return

            reminder_text = (
                "⏰【救護事件尚未結束提醒】\n"
                f"事件編號：{incident_id}\n"
                f"事故地點：{location}\n"
                "本救護事件已啟動1小時，目前仍未結束，"
                "請確認處理進度，並於完成後執行結束操作。"
            )

            # 在群組中真正 @ 原始啟動人員
            push_initiator_mention(
                group_id,
                initiator,
                reminder_text,
                initiator_name
            )

            # 第二次等待：再等1小時，滿2小時
            await asyncio.sleep(INCIDENT_AUTO_CLOSE_SECONDS)

            current = active_incidents.get(group_id)

            # 再次確認事件仍存在且編號相同
            if not current or current["id"] != incident_id:
                return

            # 自動結束事件
            active_incidents.pop(group_id, None)
            pending_closures.pop(group_id, None)

            auto_close_text = (
                "⏰【救護事件自動結束公告】\n"
                f"事件編號：{incident_id}\n"
                f"事故地點：{location}\n"
                "本救護事件啟動已滿2小時，"
                "因尚未執行人工結束操作，系統已自動結束本次事件。\n"
                "如仍有救護需求，請重新啟動緊急救護流程。"
            )

            try:
                # 群組公告，不使用 @All，也不標記特定人員
                line_bot_api.push_message(
                    group_id,
                    TextSendMessage(text=auto_close_text)
                )

                print(
                    f"救護事件已自動結束：{incident_id}"
                )

            except Exception as error:
                print(f"自動結束群組公告發送失敗：{error}")

        except asyncio.CancelledError:
            print(
                f"救護事件計時任務已停止：{incident_id}"
            )
            return

        except Exception as error:
            print(
                f"救護事件計時任務發生錯誤："
                f"{incident_id}，{error}"
            )

        finally:
            current_task = asyncio.current_task()

            # 避免舊任務刪除新事件的計時任務
            if incident_timer_tasks.get(group_id) is current_task:
                incident_timer_tasks.pop(group_id, None)

    incident_timer_tasks[group_id] = asyncio.create_task(
        wait_and_process()
    )

    print(
        f"已建立救護事件計時任務：{incident_id}，"
        "1小時提醒、2小時自動結束。"
    )


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


def incident_status_text(incident):
    """統計所有已回報群組成員。"""
    available = []
    unavailable = []

    for record in incident["responses"].values():
        name = record.get("display_name") or "群組成員"
        entry = f"{name}｜{time_text(record['time'])}"

        if record["status"] == "available":
            available.append(entry)
        else:
            unavailable.append(entry)

    responded_count = len(available) + len(unavailable)

    lines = [
        "📋【緊急救護即時支援狀況】",
        f"事件編號：{incident['id']}",
        f"事故地點：{incident['location']}",
        f"已回報總人數：{responded_count} 人",
        f"可以前往：{len(available)} 人",
        f"無法前往：{len(unavailable)} 人",
        "",
        "🟢【可以前往】",
        *(available if available else ["目前尚無人回報可以前往"]),
        "",
        "🔴【無法前往】",
        *(unavailable if unavailable else ["目前尚無人回報無法前往"]),
        "",
    ]

    return "\n".join(lines)


def reply_text(reply_token, text):
    line_bot_api.reply_message(
        reply_token,
        TextSendMessage(text=text)
    )


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
            # 救護人員回報
            # ------------------------------------------------
            elif action in ("ack_available", "ack_unavailable"):
                incident = active_incidents.get(group_id)

                if not incident or incident["id"] != incident_id:
                    reply_text(
                        event.reply_token,
                        "此救護事件已結束或通知已失效。"
                    )

                elif not user_id:
                    reply_text(
                        event.reply_token,
                        "無法辨識回報帳號，請確認以個人 LINE 帳號操作。"
                    )

                elif user_id in incident["responses"]:
                    old = incident["responses"][user_id]

                    old_status = (
                        "可以前往"
                        if old["status"] == "available"
                        else "無法前往"
                    )

                    reply_text(
                        event.reply_token,
                        f"已收到你的回報，不重複更新紀錄。\n"
                        f"首次回報狀態：{old_status}\n"
                        f"首次回報時間：{time_text(old['time'])}"
                    )

                else:
                    status = (
                        "available"
                        if action == "ack_available"
                        else "unavailable"
                    )

                    display_name = get_display_name(
                        group_id,
                        user_id
                    )

                    timestamp = now_tw()

                    incident["responses"][user_id] = {
                        "status": status,
                        "time": timestamp,
                        "display_name": display_name,
                    }

                    status_label = (
                        "收到，可以前往"
                        if status == "available"
                        else "收到，但無法前往"
                    )

                    line_bot_api.reply_message(
                        event.reply_token,
                        [
                            TextSendMessage(
                                text=(
                                    f"✅ 救護回報已登記\n"
                                    f"回報人員：{display_name}\n"
                                    f"回報狀態：{status_label}\n"
                                    f"首次回報時間：{time_text(timestamp)}"
                                )
                            ),
                            TextSendMessage(
                                text=incident_status_text(incident)
                            ),
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
                    # 先停止1小時提醒及2小時自動結束任務
                    cancel_incident_timer(group_id)

                    active_incidents.pop(group_id, None)
                    pending_closures.pop(group_id, None)

                    reply_text(
                        event.reply_token,
                        f"✅ 救護事件已結束。\n"
                        f"事件編號：{incident_id}\n"
                        f"事故地點：{incident['location']}"
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
        # 護理人員登記管理（僅最高管理者可執行管理命令）
        # ----------------------------------------------------
        if text == "啟動護理人員登記":
            if not is_admin(user_id):
                reply_text(
                    event.reply_token,
                    "⛔ 此操作僅限最高管理者執行。"
                )
            else:
                next_slot = get_next_nurse_slot(group_id)
                if next_slot is None:
                    nurse_registration_groups.discard(group_id)
                    reply_text(
                        event.reply_token,
                        "三名護理人員名額均已登記完成。\n"
                        + nurse_registration_status_text(group_id)
                    )
                else:
                    nurse_registration_groups.add(group_id)
                    reply_text(
                        event.reply_token,
                        "✅ 已開啟護理人員登記。\n"
                        "請欲登記的人員依序在群組輸入：我要登記護理人員\n"
                        f"目前下一個名額：護理人員{next_slot}\n"
                        "每位人員只能登記一次；登記滿三名後會自動關閉。"
                    )

            continue

        if text == "取消護理人員登記":
            if not is_admin(user_id):
                reply_text(
                    event.reply_token,
                    "⛔ 此操作僅限最高管理者執行。"
                )
            elif group_id in nurse_registration_groups:
                nurse_registration_groups.discard(group_id)
                reply_text(
                    event.reply_token,
                    "已關閉護理人員登記模式；已登記名單不受影響。\n"
                    + nurse_registration_status_text(group_id)
                )
            else:
                reply_text(
                    event.reply_token,
                    "目前沒有開啟中的護理人員登記模式。"
                )

            continue

        if text == "查詢護理人員登記":
            if not is_admin(user_id):
                reply_text(
                    event.reply_token,
                    "⛔ 此操作僅限最高管理者執行。"
                )
            else:
                reply_text(
                    event.reply_token,
                    nurse_registration_status_text(group_id)
                )

            continue

        if text in (
            "刪除護理人員1",
            "刪除護理人員2",
            "刪除護理人員3"
        ):
            if not is_admin(user_id):
                reply_text(
                    event.reply_token,
                    "⛔ 此操作僅限最高管理者執行。"
                )
            else:
                slot = int(text[-1])
                records = nurse_users.get(group_id, {})
                removed = records.pop(slot, None)

                if not records:
                    nurse_users.pop(group_id, None)

                if removed:
                    reply_text(
                        event.reply_token,
                        f"✅ 已刪除護理人員{slot}："
                        f"{removed.get('display_name') or '群組成員'}。\n"
                        "其他已登記人員不受影響。\n"
                        + nurse_registration_status_text(group_id)
                    )
                else:
                    reply_text(
                        event.reply_token,
                        f"護理人員{slot}目前沒有登記資料。\n"
                        + nurse_registration_status_text(group_id)
                    )

            continue

        if text == "重設護理人員名單":
            if not is_admin(user_id):
                reply_text(
                    event.reply_token,
                    "⛔ 此操作僅限最高管理者執行。"
                )
            else:
                nurse_users.pop(group_id, None)
                nurse_registration_groups.discard(group_id)
                reply_text(
                    event.reply_token,
                    "✅ 已清空本群組全部護理人員名單，登記模式已關閉。\n"
                    + nurse_registration_status_text(group_id)
                )

            continue

        if text == "我要登記護理人員":
            if group_id not in nurse_registration_groups:
                reply_text(
                    event.reply_token,
                    "目前未開放護理人員登記，請聯絡最高管理者啟動登記。"
                )
                continue

            if not user_id:
                reply_text(
                    event.reply_token,
                    "無法辨識你的 LINE 帳號，請確認以個人 LINE 帳號操作。"
                )
                continue

            records = nurse_users.setdefault(group_id, {})
            already_registered_slot = next(
                (
                    slot for slot, record in records.items()
                    if record.get("user_id") == user_id
                ),
                None
            )

            if already_registered_slot is not None:
                reply_text(
                    event.reply_token,
                    f"你已登記為護理人員{already_registered_slot}，不需重複登記。"
                )
                continue

            next_slot = get_next_nurse_slot(group_id)

            if next_slot is None:
                nurse_registration_groups.discard(group_id)
                reply_text(
                    event.reply_token,
                    "三名護理人員名額已滿，登記模式已自動關閉。"
                )
                continue

            display_name = get_display_name(group_id, user_id)
            records[next_slot] = {
                "user_id": user_id,
                "display_name": display_name
            }

            if get_next_nurse_slot(group_id) is None:
                nurse_registration_groups.discard(group_id)
                completion_text = "三名護理人員名額已滿，登記模式已自動關閉。"
            else:
                following_slot = get_next_nurse_slot(group_id)
                completion_text = (
                    f"下一位登記者將登記為護理人員{following_slot}。"
                )

            reply_text(
                event.reply_token,
                f"✅ 登記成功！\n"
                f"登記身分：護理人員{next_slot}\n"
                f"登記人員：{display_name}\n"
                f"{completion_text}\n"
                + nurse_registration_status_text(group_id)
            )
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
                "收到通知的群組成員，請依自身狀況選擇回報。"
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
