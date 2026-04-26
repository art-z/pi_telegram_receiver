import asyncio
import html
import json
import os
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse


app = FastAPI(title="JSON Telegram Receiver")


APP_VERSION = "no-auth-stt-text-batch-v3"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# Сколько секунд ждать перед отправкой неполного батча
STT_FLUSH_INTERVAL_SECONDS = int(os.getenv("STT_FLUSH_INTERVAL_SECONDS", "20"))

# Сколько фраз накопить перед мгновенной отправкой
STT_MAX_PHRASES = int(os.getenv("STT_MAX_PHRASES", "20"))

stt_buffer: list[dict[str, Any]] = []
stt_lock = asyncio.Lock()
stt_flush_task: asyncio.Task | None = None


def check_config() -> None:
    missing = []

    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")

    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")

    if missing:
        raise HTTPException(
            status_code=500,
            detail={
                "error": "missing_config",
                "missing": missing,
            },
        )


def get_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_datetime(value: Any) -> datetime | None:
    if not value:
        return None

    try:
        text = str(value).strip()

        if text.endswith("Z"):
            text = text[:-1] + "+00:00"

        return datetime.fromisoformat(text)
    except Exception:
        return None


def format_json_for_telegram(data: Any) -> str:
    pretty_json = json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
        default=str,
    )

    if len(pretty_json) > 3400:
        pretty_json = pretty_json[:3400] + "\n...\n[trimmed]"

    safe_json = html.escape(pretty_json)

    return f"Новый JSON\n\n<pre>{safe_json}</pre>"


def format_stt_batch_for_telegram(items: list[dict[str, Any]]) -> str:
    rows: list[tuple[datetime | None, str]] = []

    for item in items:
        text = str(item.get("text", "")).strip()

        if not text:
            continue

        dt = (
            parse_iso_datetime(item.get("recognized_at"))
            or parse_iso_datetime(item.get("received_at"))
        )

        rows.append((dt, text))

    if not rows:
        return "STT batch is empty"

    last_dt = rows[-1][0]

    if last_dt:
        date_title = last_dt.strftime("%Y-%m-%d")
    else:
        date_title = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    lines = [date_title, ""]

    for dt, phrase in rows:
        if dt:
            time_text = dt.strftime("%H:%M:%S")
        else:
            time_text = "--:--:--"

        lines.append(f"{time_text} - {phrase}")

    message = "\n".join(lines)

    # Telegram sendMessage limit is 4096 chars.
    # Основное ограничение у нас по количеству фраз, но оставляем защиту.
    if len(message) > 3900:
        message = message[:3900] + "\n...\n[trimmed]"

    return message


async def send_to_telegram(text: str, parse_mode: str | None = "HTML") -> dict:
    check_config()

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    payload: dict[str, Any] = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }

    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.post(url, json=payload)
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "telegram_request_failed",
                "message": str(e),
            },
        )

    try:
        result = response.json()
    except Exception:
        result = {
            "raw": response.text,
        }

    if response.status_code >= 400:
        raise HTTPException(
            status_code=response.status_code,
            detail={
                "error": "telegram_api_error",
                "telegram_status_code": response.status_code,
                "telegram_response": result,
                "chat_id": TELEGRAM_CHAT_ID,
            },
        )

    return result


async def flush_stt_buffer() -> dict:
    async with stt_lock:
        if not stt_buffer:
            return {
                "ok": True,
                "flushed": False,
                "count": 0,
            }

        items = stt_buffer.copy()
        stt_buffer.clear()

    text = format_stt_batch_for_telegram(items)
    telegram_result = await send_to_telegram(text, parse_mode=None)

    return {
        "ok": True,
        "flushed": True,
        "count": len(items),
        "telegram_ok": telegram_result.get("ok"),
    }


async def stt_periodic_flush_loop() -> None:
    while True:
        await asyncio.sleep(STT_FLUSH_INTERVAL_SECONDS)

        try:
            await flush_stt_buffer()
        except Exception as e:
            print(f"[STT_FLUSH_ERROR] {repr(e)}", flush=True)


@app.on_event("startup")
async def startup_event():
    global stt_flush_task

    stt_flush_task = asyncio.create_task(stt_periodic_flush_loop())


@app.on_event("shutdown")
async def shutdown_event():
    global stt_flush_task

    if stt_flush_task:
        stt_flush_task.cancel()

    try:
        await flush_stt_buffer()
    except Exception as e:
        print(f"[STT_SHUTDOWN_FLUSH_ERROR] {repr(e)}", flush=True)


async def handle_stt_payload(data: dict[str, Any]) -> dict:
    text = str(data.get("text", "")).strip()

    if not text:
        return {
            "ok": True,
            "batched": False,
            "reason": "empty_text",
        }

    item = {
        "id": data.get("id"),
        "text": text,
        "recognized_at": data.get("recognized_at"),
        "received_at": get_now_iso(),
    }

    should_flush_now = False

    async with stt_lock:
        stt_buffer.append(item)
        buffered_count = len(stt_buffer)

        if buffered_count >= STT_MAX_PHRASES:
            should_flush_now = True

    if should_flush_now:
        flush_result = await flush_stt_buffer()

        return {
            "ok": True,
            "batched": True,
            "flushed_now": True,
            "buffered_count_before_flush": buffered_count,
            "flush_result": flush_result,
        }

    return {
        "ok": True,
        "batched": True,
        "flushed_now": False,
        "buffered_count": buffered_count,
        "max_phrases": STT_MAX_PHRASES,
        "flush_interval_seconds": STT_FLUSH_INTERVAL_SECONDS,
    }


@app.get("/")
async def root():
    return {
        "status": "ok",
        "service": "json-telegram-receiver",
        "version": APP_VERSION,
    }


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "version": APP_VERSION,
        "stt_buffer_size": len(stt_buffer),
        "stt_flush_interval_seconds": STT_FLUSH_INTERVAL_SECONDS,
        "stt_max_phrases": STT_MAX_PHRASES,
    }


@app.post("/receive")
async def receive_json(request: Request):
    check_config()

    content_type = request.headers.get("content-type", "")

    if "application/json" not in content_type.lower():
        raise HTTPException(
            status_code=415,
            detail={
                "error": "unsupported_media_type",
                "message": "Content-Type must be application/json",
                "content_type": content_type,
            },
        )

    try:
        data = await request.json()
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_json",
                "message": str(e),
            },
        )

    if isinstance(data, dict) and data.get("type") == "stt":
        result = await handle_stt_payload(data)
        return JSONResponse(result)

    text = format_json_for_telegram(data)
    telegram_result = await send_to_telegram(text, parse_mode="HTML")

    return JSONResponse(
        {
            "ok": True,
            "version": APP_VERSION,
            "telegram_ok": telegram_result.get("ok"),
            "telegram_result": telegram_result,
        }
    )


@app.post("/flush")
async def flush_now():
    result = await flush_stt_buffer()
    return JSONResponse(result)