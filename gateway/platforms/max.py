"""
MAX (max.ru) platform adapter for Hermes Agent.
Uses MAX Bot API (platform-api.max.ru) with Long Polling.
"""
import asyncio
import json
import logging
import os
import re
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

try:
    import aiohttp
    MAX_AVAILABLE = True
except ImportError:
    MAX_AVAILABLE = False

import sys
from pathlib import Path as _Path
sys.path.insert(0, str(_Path(__file__).resolve().parents[2]))

from hermes_constants import get_hermes_home

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_image_from_bytes,
    cache_audio_from_bytes,
    cache_document_from_bytes,
)

import sqlite3
import time as _time_module

_MAX_DEDUP_DB_PATH = get_hermes_home() / "max_dedup.db"


class _MessageDedup:
    def __init__(self, db_path=None, ttl=300):
        path = _MAX_DEDUP_DB_PATH if db_path is None else db_path
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("CREATE TABLE IF NOT EXISTS seen (mid TEXT PRIMARY KEY, ts REAL)")
        self.db.commit()
        self.ttl = ttl

    def is_duplicate(self, mid: str) -> bool:
        now = _time_module.time()
        self.db.execute("DELETE FROM seen WHERE ts < ?", (now - self.ttl,))
        cur = self.db.execute("INSERT OR IGNORE INTO seen (mid, ts) VALUES (?, ?)", (mid, now))
        self.db.commit()
        return cur.rowcount == 0

_dedup = _MessageDedup()

MAX_API_BASE = "https://platform-api.max.ru"
MAX_MESSAGE_LENGTH = 4096


def check_max_requirements() -> bool:
    return MAX_AVAILABLE


class MaxAdapter(BasePlatformAdapter):

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.MAX)
        self.token = config.token or os.getenv("MAX_BOT_TOKEN", "")
        self._session: Optional[Any] = None
        self._polling_task: Optional[asyncio.Task] = None
        self._running = False
        self._marker: Optional[int] = None
        self._bot_id: Optional[int] = None

    @property
    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": self.token,
            "Content-Type": "application/json",
        }

    async def _api(self, method: str, path: str, **kwargs) -> Optional[Dict]:
        url = f"{MAX_API_BASE}{path}"
        try:
            async with self._session.request(method, url, headers=self._headers, **kwargs) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    text = await resp.text()
                    logger.warning("MAX API %s %s -> %s: %s", method, path, resp.status, text[:200])
                    return None
        except Exception as e:
            logger.error("MAX API error %s %s: %s", method, path, e)
            return None

    async def connect(self) -> bool:
        if not self.token:
            logger.error("MAX: no token configured (MAX_BOT_TOKEN)")
            return False
        if not MAX_AVAILABLE:
            logger.error("MAX: aiohttp not available")
            return False

        self._session = aiohttp.ClientSession()
        me = await self._api("GET", "/me")
        if not me:
            logger.error("MAX: failed to get bot info — invalid token?")
            await self._session.close()
            return False

        self._bot_id = me.get("user_id")
        logger.info("MAX: connected as %s (id=%s)", me.get("name", "unknown"), self._bot_id)
        print(f"MAX CONNECT bot_id={self._bot_id} name={me.get('name', 'unknown')!r}", flush=True)

        self._running = True
        if self._polling_task and not self._polling_task.done():
            self._polling_task.cancel()
        self._polling_task = asyncio.create_task(self._poll_loop())
        return True

    async def disconnect(self):
        self._running = False
        if self._polling_task:
            self._polling_task.cancel()
            try:
                await self._polling_task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()
            self._session = None

    async def _poll_loop(self):
        backoff = 1
        while self._running:
            try:
                params = {"timeout": 30}
                if self._marker is not None:
                    params["marker"] = self._marker

                data = await self._api("GET", "/updates", params=params)
                if data is None:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)
                    continue

                backoff = 1
                self._marker = data.get("marker", self._marker)

                for update in data.get("updates", []):
                    await self._handle_update(update)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("MAX polling error: %s", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _handle_update(self, update: Dict):
        update_type = update.get("update_type")
        if update_type == "message_created":
            mid = (update.get("message") or {}).get("body", {}).get("mid") or str(update)
            if _dedup.is_duplicate(mid):
                return
            # Мгновенный acknowledgement перед обработкой
            _ack_phrases = [
                "Сейчас посмотрим... 🔍",
                "Хороший вопрос, дай разберусь ⚡",
                "Принял, работаю над этим 🧠",
                "Секунду, копаю данные... 🔬",
                "О, интересно! Сейчас разберу 💡",
                "Уже на связи, анализирую... 🎯",
            ]
            import random as _rnd
            _text = (update.get("message") or {}).get("body", {}).get("text", "") or ""
            if len(_text) > 30:
                _chat = str(((update.get("message") or {}).get("recipient") or {}).get("chat_id", ""))
                if _chat:
                    try:
                        await self.send(_chat, _rnd.choice(_ack_phrases))
                    except Exception:
                        pass
            await self._handle_message(update)
        elif update_type == "bot_started":
            chat_id = str(update.get("chat_id", ""))
            user = update.get("user", {})
            source = self.build_source(
                chat_id=chat_id,
                user_id=str(user.get("user_id", "")),
                user_name=user.get("name", ""),
                chat_type="dm",
            )
            event = MessageEvent(
                source=source,
                text="/start",
                message_type=MessageType.TEXT,
                raw_message=update,
            )
            await self.handle_message(event)

    async def _handle_message(self, update: Dict):
        message = update.get("message") if isinstance(update, dict) and isinstance(update.get("message"), dict) else update
        message = message or {}

        if not message:
            return

        sender = message.get("sender", {}) or {}
        sender_id = sender.get("user_id")

        if sender_id and sender_id == self._bot_id:
            return

        recipient = message.get("recipient", {}) or {}
        chat_id = str(recipient.get("chat_id") or recipient.get("user_id") or "")

        if not chat_id:
            return

        user_id = str(sender_id or "")
        user_name = sender.get("name", "") or sender.get("username", "")
        chat_type_raw = recipient.get("chat_type", "dialog")
        chat_type = "group" if chat_type_raw == "chat" else "dm"

        body = message.get("body", {}) or {}
        text = body.get("text", "") or ""
        attachments = body.get("attachments", []) or []

        media_urls = []
        media_types = []
        msg_type = MessageType.TEXT

        for att in attachments:
            if not isinstance(att, dict):
                continue

            att_type = att.get("type", "")
            payload = att.get("payload", {}) or {}
            url = payload.get("url")

            if att_type == "image":
                msg_type = MessageType.PHOTO
                cached = None
                if url and self._session:
                    try:
                        async with self._session.get(url, headers=self._headers) as resp:
                            if resp.status == 200:
                                data = await resp.read()
                                cached = cache_image_from_bytes(data, ".jpg")
                    except Exception as e:
                        logger.warning("MAX: failed to download image: %s", e)
                resolved = cached or url
                if resolved:
                    media_urls.append(resolved)
                    media_types.append("image/jpeg")
                break

            elif att_type == "audio":
                msg_type = MessageType.VOICE
                logger.info("MAX: detected VOICE attachment, url=%s", url)
                cached = None
                if url and self._session:
                    try:
                        async with self._session.get(url, headers=self._headers) as resp:
                            if resp.status == 200:
                                data = await resp.read()
                                cached = cache_audio_from_bytes(data, ".ogg")
                    except Exception as e:
                        logger.warning("MAX: failed to download audio: %s", e)
                resolved = cached or url
                if resolved:
                    media_urls.append(resolved)
                    media_types.append("audio/ogg")
                break

            elif att_type in ("file", "document"):
                msg_type = MessageType.DOCUMENT
                filename = payload.get("name", "file")
                cached = None
                if url and self._session:
                    try:
                        async with self._session.get(url, headers=self._headers) as resp:
                            if resp.status == 200:
                                data = await resp.read()
                                safe_name = filename if filename else "document.bin"
                                cached = cache_document_from_bytes(data, safe_name)
                    except Exception as e:
                        logger.warning("MAX: failed to download document: %s", e)
                resolved = cached or url
                if resolved:
                    media_urls.append(resolved)
                    media_types.append("application/octet-stream")
                break

        if msg_type == MessageType.PHOTO and not text:
            text = "[image]"
        elif msg_type != MessageType.TEXT and not text:
            text = "[attachment]"

        source = self.build_source(
            chat_id=chat_id,
            user_id=user_id,
            user_name=user_name,
            chat_type=chat_type,
        )

        event = MessageEvent(
            source=source,
            text=text,
            message_type=msg_type,
            raw_message=update,
            media_urls=media_urls,
            media_types=media_types,
        )
        await self.handle_message(event)

    async def send(self, chat_id: str, text: str = "", **kwargs) -> SendResult:
        if not text:
            content = kwargs.get("content")
            if isinstance(content, str):
                text = content
            elif content is not None:
                text = str(content)

        original_text = text or ""
        text = original_text

        text = re.sub(r'(?im)^\s*[🔎🔍📚🐍💻⚙️🛠️].*(?:\n|$)', '', text)
        text = re.sub(r'(?im)^.*\b(?:skill_view|execute_code|search_files|terminal|tool_call|tool_use)\b.*(?:\n|$)', '', text)
        text = re.sub(r'(?im)^\s*(?:ок[,! ]*)?(?:сейчас посмотрим.*|применяю .*|использую .*|задача выполнена.*|проанализировано.*|выполняю .*|проверяю .*|думаю .*|рассуждаю .*|наш[её]л.*|просто информировал.*)(?:\n|$)', '', text)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()

        if not text:
            text = original_text.strip()

        chunks = [text[i:i + MAX_MESSAGE_LENGTH] for i in range(0, len(text), MAX_MESSAGE_LENGTH)] or [""]
        last_result = SendResult(success=False)

        for chunk in chunks:
            payload = {"text": chunk, "notify": True}

            _has_html = bool(re.search(r'<(?:b|strong|i|em|u|ins|s|del|code|pre|a)(?:\s+[^>]*)?>'  , chunk, re.IGNORECASE))
            _has_markdown = bool(re.search(r'(\*\*[^\n]+\*\*|__[^\n]+__|`[^\n]+`|\[[^\]]+\]\([^\)]+\))', chunk))

            if _has_html:
                payload["format"] = "html"
            elif _has_markdown:
                payload["format"] = "markdown"

            try:
                async with self._session.post(
                    f"{MAX_API_BASE}/messages",
                    headers=self._headers,
                    params={"chat_id": chat_id},
                    json=payload,
                ) as resp:
                    raw = await resp.read()
                    err_preview = (
                        raw.decode("utf-8", errors="replace")[:1000] if raw else ""
                    )
                    if resp.status == 200:
                        try:
                            data = json.loads(raw.decode("utf-8") or "{}")
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            data = {}
                        msg = data.get("message") if isinstance(data, dict) else None
                        success = bool(
                            data.get("ok")
                            or data.get("message_id") is not None
                            or (isinstance(msg, dict) and msg)
                        )
                        if success and isinstance(msg, dict):
                            last_result = SendResult(
                                success=True,
                                message_id=str(msg.get("mid", "")),
                            )
                        elif success:
                            last_result = SendResult(
                                success=True,
                                message_id=str(data.get("message_id", "")),
                            )
                        else:
                            last_result = SendResult(
                                success=False,
                                error=str(data) if data else err_preview,
                            )
                    else:
                        last_result = SendResult(success=False, error=err_preview)
            except Exception as e:
                last_result = SendResult(success=False, error=str(e))

        return last_result

    _THINKING_MESSAGES = [
        "🧠 Думаю... это требует глубокого анализа",
        "⏳ Копаюсь в данных... скоро будет результат",
        "🔍 Ищу лучший ответ... терпение — суперсила!",
        "💡 Мёд, найденный в гробницах фараонов, всё ещё съедобен спустя 3000 лет",
        "🚀 Свет от Солнца идёт до Земли 8 минут 20 секунд. А я пока думаю...",
        "🎯 Почти готово... кстати, в океане больше вирусов, чем звёзд во Вселенной",
        "🌍 Работаю... а на Земле больше деревьев (3 трлн), чем звёзд в Млечном Пути (400 млрд)",
        "⚡ Ещё чуть-чуть... у осьминога три сердца и голубая кровь",
        "🧩 Собираю ответ... Венеция стоит на 118 островах и 400+ мостах",
        "🎲 Финальный штрих... бананы на 60% генетически совпадают с человеком",
    ]
    _THINKING_USAGE_PATH = get_hermes_home() / "thinking_usage.json"
    _THINKING_MAX_PER_MONTH = 3

    @classmethod
    def _get_available_messages(cls):
        import json
        import time as _t
        now = _t.time()
        month_ago = now - 30 * 86400
        try:
            with open(cls._THINKING_USAGE_PATH) as f:
                usage = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            usage = {}
        # Чистим старые записи
        for k in list(usage):
            usage[k] = [ts for ts in usage[k] if ts > month_ago]
            if not usage[k]:
                del usage[k]
        available = []
        for i, msg in enumerate(cls._THINKING_MESSAGES):
            key = str(i)
            if len(usage.get(key, [])) < cls._THINKING_MAX_PER_MONTH:
                available.append(i)
        return available, usage

    @classmethod
    def _record_message_usage(cls, idx):
        import json
        import time as _t
        _, usage = cls._get_available_messages()
        key = str(idx)
        usage.setdefault(key, []).append(_t.time())
        try:
            with open(cls._THINKING_USAGE_PATH, "w") as f:
                json.dump(usage, f)
        except Exception:
            pass

    async def send_typing(self, chat_id: str, metadata=None):
        try:
            await self._api("POST", f"/chats/{chat_id}/actions", json={"action": "typing_on"})
        except Exception:
            pass

    async def _keep_typing(self, chat_id: str, interval: float = 2.0, metadata=None):
        import random
        _msg_interval = 45
        _elapsed = 0
        try:
            while True:
                await self.send_typing(chat_id, metadata=metadata)
                await asyncio.sleep(interval)
                _elapsed += interval
                if _elapsed >= _msg_interval and int(_elapsed / _msg_interval) > int((_elapsed - interval) / _msg_interval):
                    available, _ = self._get_available_messages()
                    if not available:
                        continue
                    idx = random.choice(available)
                    self._record_message_usage(idx)
                    try:
                        await self.send(chat_id, self._THINKING_MESSAGES[idx])
                    except Exception:
                        pass
        except asyncio.CancelledError:
            pass

    async def send_image(self, chat_id: str, image_url: str, caption: str = "") -> SendResult:
        payload = {
            "recipient": {"chat_id": int(chat_id) if chat_id.lstrip("-").isdigit() else chat_id},
            "type": "default",
            "body": {
                "type": "text",
                "text": caption or "",
                "attachments": [{"type": "image", "payload": {"url": image_url}}],
            },
        }
        data = await self._api("POST", "/messages", json=payload)
        if data and data.get("message"):
            return SendResult(success=True, message_id=str(data["message"].get("mid", "")))
        return SendResult(success=False, error=str(data))

    async def send_document(self, chat_id: str, path: str, caption: str = "") -> SendResult:
        return await self._upload_file(chat_id, path, caption, file_type="file")

    async def send_voice(self, chat_id: str, audio_path: str = "", metadata=None, **kwargs) -> SendResult:
        # Convert to OGG/OPUS for MAX API compatibility
        _send_path = audio_path
        _tmp_ogg = None
        if audio_path and not audio_path.endswith(('.ogg', '.opus')):
            import subprocess, tempfile
            _tmp_ogg = audio_path.rsplit('.', 1)[0] + '.ogg'
            try:
                subprocess.run(
                    ['ffmpeg', '-y', '-i', audio_path, '-c:a', 'libopus', '-b:a', '48k', _tmp_ogg],
                    capture_output=True, timeout=15
                )
                if os.path.exists(_tmp_ogg) and os.path.getsize(_tmp_ogg) > 0:
                    _send_path = _tmp_ogg
            except Exception:
                pass
        result = await self._upload_file(chat_id, _send_path, "", file_type="audio")
        if _tmp_ogg and os.path.exists(_tmp_ogg):
            try:
                os.remove(_tmp_ogg)
            except Exception:
                pass
        return result

    async def _upload_file(self, chat_id: str, path: str, caption: str, file_type: str) -> SendResult:
        try:
            upload_info = await self._api("POST", f"/uploads?type={file_type}")
            if not upload_info or not upload_info.get("url"):
                return SendResult(success=False, error="Failed to get upload URL")

            upload_url = upload_info["url"]
            # For audio/video, token comes from /uploads response, not from upload server
            _pre_token = upload_info.get("token")
            with open(path, "rb") as f:
                file_data = f.read()

            filename = os.path.basename(path)
            form = aiohttp.FormData()
            form.add_field("data", file_data, filename=filename)

            async with self._session.post(upload_url, data=form) as resp:
                if resp.status != 200:
                    return SendResult(success=False, error=f"Upload failed: {resp.status}")
                resp_text = await resp.text()
                try:
                    import json as _json
                    upload_result = _json.loads(resp_text)
                except Exception:
                    # Try to extract token from XML <retval>...</retval>
                    import re as _re
                    _m = _re.search(r"<retval>(.+?)</retval>", resp_text)
                    if _m:
                        upload_result = {"token": _m.group(1)}
                    else:
                        upload_result = {"token": resp_text.strip().strip('"')}

            # For audio/video: use token from /uploads; for image/file: use token from upload response
            if _pre_token:
                token = _pre_token
            else:
                token = upload_result.get("token")
            if not token:
                return SendResult(success=False, error=f"No token in upload response: {resp_text[:200]}")

            _cid = int(chat_id) if chat_id.lstrip("-").isdigit() else chat_id
            payload = {
                "text": caption or "",
                "attachments": [{"type": file_type, "payload": {"token": token}}],
            }
            import aiohttp as _aiohttp2
            _url = f"{MAX_API_BASE}/messages?chat_id={_cid}"
            _headers = {"Authorization": self.token, "Content-Type": "application/json"}
            import asyncio as _asyncio
            for _attempt in range(5):
                if _attempt > 0:
                    await _asyncio.sleep(2 * _attempt)
                async with self._session.post(_url, json=payload, headers=_headers) as _raw:
                    _raw_text = await _raw.text()
                    try:
                        data = _json.loads(_raw_text)
                    except Exception:
                        data = None
                if data and data.get("code") == "attachment.not.ready":
                    continue
                if data and data.get("code"):
                    return SendResult(success=False, error=str(data))
                if data and data.get("message"):
                    return SendResult(success=True)
                return SendResult(success=False, error=str(data))
            return SendResult(success=False, error="attachment not ready after 5 retries")

        except Exception as e:
            logger.error("MAX upload error: %s", e)
            return SendResult(success=False, error=str(e))

    async def get_chat_info(self, chat_id: str) -> Dict:
        data = await self._api("GET", f"/chats/{chat_id}")
        if data:
            return {
                "name": data.get("title") or data.get("name", chat_id),
                "type": data.get("type", "dm"),
                "chat_id": chat_id,
            }
        return {"name": chat_id, "type": "unknown", "chat_id": chat_id}
