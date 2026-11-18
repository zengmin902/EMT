import os
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
        detail="Invalid signature. Please check your channel secret/access token.",
    )

  for event in events:
    if isinstance(event, MessageEvent) and isinstance(event.message, TextMessage):
      text = event.message.text
      reply_token = event.reply_token

      # 1. 觸發緊急面板（帶有 Quick Reply 按鈕）
      if text == "緊急面板":
        quick_reply = QuickReply(
            items=[
                QuickReplyButton(
                    action=MessageAction(label="🚨 緊急救護", text="🚨緊急救護")
                )
            ]
        )
        message = TextSendMessage(
            text="請點擊下方按鈕發起緊急救護通知：", quick_reply=quick_reply
        )
        line_bot_api.reply_message(reply_token, message)

      # 2. 收到緊急救護通知
      elif text == "🚨緊急救護":
        message = TextSendMessage(text="🚨【緊急救護通知】🚨")
        line_bot_api.reply_message(reply_token, message)

      # 3. 收到已收到通知回覆
      elif text == "✅我已收到緊急通知":
        message = TextSendMessage(text="收到！已記錄您的回報。")
        line_bot_api.reply_message(reply_token, message)

  return "OK"
