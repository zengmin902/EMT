import os
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, HTTPException, Request
from linebot import LineBotApi, WebhookParser
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent,
    MessageAction,
    QuickReply,
    QuickReplyButton,
    TextMessage,
    TextSendMessage,
)

app = FastAPI()

# 讀取環境變數
channel_access_token = os.environ.get("CHANNEL_ACCESS_TOKEN", "")
channel_secret = os.environ.get("CHANNEL_SECRET", "")

line_bot_api = LineBotApi(channel_access_token)
parser = WebhookParser(channel_secret)


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
            detail="Invalid signature. Please check your channel secret.",
        )

    for event in events:
        if isinstance(event, MessageEvent) and isinstance(event.message, TextMessage):

            text = event.message.text.strip()
            reply_token = event.reply_token

            # 1. 緊急面板
            if text == "緊急面板":
                quick_reply = QuickReply(
                    items=[
                        QuickReplyButton(
                            action=MessageAction(
                                label="🚨 緊急救護",
                                text="🚨緊急救護"
                            )
                        )
                    ]
                )

                message = TextSendMessage(
                    text="請點擊下方按鈕發起緊急救護通知：",
                    quick_reply=quick_reply
                )

                line_bot_api.reply_message(reply_token, message)

            # 2. 發送緊急救護通知
            elif text == "🚨緊急救護":
                message = TextSendMessage(
                    text=(
                        "🚨【緊急救護通知】🚨\n\n"
                        "請相關救護人員依緊急應變程序前往處理。\n\n"
                        "📢 收到通知的群組成員，"
                        "請在群組輸入「收到」，回報已收到通知。"
                    )
                )

                line_bot_api.reply_message(reply_token, message)

            # 3. 成員輸入「收到」
            elif text == "收到":

                source = event.source
                display_name = "群組成員"

                # 取得群組成員名稱
                if (
                    getattr(source, "type", None) == "group"
                    and getattr(source, "group_id", None)
                    and getattr(source, "user_id", None)
                ):
                    try:
                        profile = line_bot_api.get_group_member_profile(
                            source.group_id,
                            source.user_id
                        )
                        display_name = profile.display_name

                    except Exception:
                        display_name = "群組成員"

                # 使用台灣時間
                taiwan_time = datetime.now(
                    timezone(timedelta(hours=8))
                ).strftime("%Y-%m-%d %H:%M:%S")

                message = TextSendMessage(
                    text=(
                        "✅ 已收到回報！\n\n"
                        f"回報人員：{display_name}\n"
                        f"回報時間：{taiwan_time}"
                    )
                )

                line_bot_api.reply_message(reply_token, message)

    return "OK"
