"""
MAX (max.ru) platform adapter for Hermes Agent.
Uses MAX Bot API (platform-api.max.ru) with Long Polling.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)
_DEBUG_LOG_PATH = Path("/root/.hermes/.cursor/debug-e13273.log")


def _debug_log(run_id: str, hypothesis_id: str, location: str, message: str, data: Dict[str, Any]) -> None:
    try:
        payload = {
            "sessionId": "e13273",
            "runId": run_id,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(_time_module.time() * 1000),
        }
        _DEBUG_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _DEBUG_LOG_PATH.open("a", encoding="utf-8") as _f:
            _f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass

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
from gateway.formatters.post_formatter import format_post
from extensions.hermes_spare24.integrations.max.client import MaxClient
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

import sqlite3
import time as _time_module

try:
    from tools.url_safety import is_safe_url
except ImportError:  # pragma: no cover

    def is_safe_url(url: str) -> bool:  # type: ignore[misc]
        return bool(url and url.startswith(("http://", "https://")))


_MAX_DEDUP_DB_PATH = get_hermes_home() / "max_dedup.db"

MAX_API_BASE = "https://platform-api.max.ru"
# Per MAX Bot API (NewMessageBody): text up to 4000 characters per message.
MAX_MESSAGE_LENGTH = 4000

_MAX_IMAGE_BYTES = 10 * 1024 * 1024
_ALLOWED_IMAGE_CT = frozenset({"image/jpeg", "image/png", "image/webp"})
_MAX_RELAY_TMP = Path("/tmp/max-bot")
_MAX_API_RETRIES = 3
_MAX_NOT_READY_ATTEMPTS = 8


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


def _extract_image_urls_from_body(body: Dict[str, Any]) -> List[str]:
    """Collect HTTPS image URLs from ``body.attachments`` (type ``image``, ``payload.url``)."""
    urls: List[str] = []
    for att in body.get("attachments") or []:
        if not isinstance(att, dict):
            continue
        if str(att.get("type") or "").lower() != "image":
            continue
        payload = att.get("payload") if isinstance(att.get("payload"), dict) else {}
        url = str((payload or {}).get("url") or "").strip()
        if url.startswith("http://") or url.startswith("https://"):
            urls.append(url)
    return urls


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
        allowed = (os.getenv("MAX_ALLOWED_USERS", "") or "").strip()
        self._allowed_user_ids: Set[str] = {
            v.strip() for v in allowed.split(",") if v.strip()
        }
        self._relay_sem = asyncio.Semaphore(3)

    @property
    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": self.token,
            "Content-Type": "application/json",
        }

    def _relay_channel_id(self) -> str:
        return (
            (os.getenv("MAX_RELAY_CHANNEL_ID") or os.getenv("MAX_CHANNEL_ID") or "").strip()
        )

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

    async def _max_json_with_5xx_retry(
        self,
        method: str,
        url: str,
        *,
        json_payload: Optional[Dict[str, Any]] = None,
        data: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Tuple[int, Dict[str, Any]]:
        """Call MAX platform-api; retry up to 3 times on 5xx with exponential backoff."""
        hdrs = dict(headers or self._headers)
        last_status = 0
        last_body: Dict[str, Any] = {}
        delay = 0.5
        for attempt in range(_MAX_API_RETRIES):
            try:
                async with self._session.request(
                    method,
                    url,
                    headers=hdrs,
                    json=json_payload,
                    data=data,
                    timeout=aiohttp.ClientTimeout(total=120),
                ) as resp:
                    raw = await resp.read()
                    text = raw.decode("utf-8", errors="replace")
                    try:
                        parsed = json.loads(text) if text.strip() else {}
                    except json.JSONDecodeError:
                        parsed = {"_raw": text[:500]}
                    last_status = resp.status
                    last_body = parsed if isinstance(parsed, dict) else {"_raw": str(parsed)}
                    if 200 <= resp.status < 300:
                        return resp.status, last_body
                    if 400 <= resp.status < 500:
                        return resp.status, last_body
                    if 500 <= resp.status < 600 and attempt < _MAX_API_RETRIES - 1:
                        await asyncio.sleep(delay + random.random() * 0.2)
                        delay *= 2
                        continue
                    return resp.status, last_body
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                logger.warning("MAX request error attempt %s %s: %s", attempt + 1, url, exc)
                if attempt < _MAX_API_RETRIES - 1:
                    await asyncio.sleep(delay + random.random() * 0.2)
                    delay *= 2
                    continue
                return 0, {"error": str(exc)}
        return last_status, last_body

    async def _ensure_relay_tmp(self) -> None:
        await asyncio.to_thread(_MAX_RELAY_TMP.mkdir, parents=True, exist_ok=True)

    async def _download_image_to_temp(self, url: str) -> Tuple[str, str]:
        """Stream image to a temp file; enforce size and Content-Type. Returns (path, content_type)."""
        if not is_safe_url(url):
            raise ValueError("blocked or invalid image URL")

        dl_timeout = aiohttp.ClientTimeout(total=30)

        # HEAD: Content-Length and Content-Type when possible
        try:
            async with self._session.head(url, timeout=dl_timeout, allow_redirects=True) as hresp:
                if hresp.status == 200:
                    cl = hresp.headers.get("Content-Length")
                    if cl and cl.isdigit() and int(cl) > _MAX_IMAGE_BYTES:
                        raise ValueError("image too large")
                    ct = (hresp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
                    if ct and ct not in _ALLOWED_IMAGE_CT:
                        raise ValueError(f"unsupported image type: {ct}")
        except Exception as exc:
            logger.debug("MAX image HEAD skipped or failed: %s", exc)

        await self._ensure_relay_tmp()
        path = _MAX_RELAY_TMP / f"max_{uuid.uuid4().hex}.img"
        try:
            written = 0
            async with self._session.get(url, timeout=dl_timeout, allow_redirects=True) as resp:
                if resp.status >= 400:
                    raise ValueError(f"download HTTP {resp.status}")
                cl = resp.headers.get("Content-Length")
                if cl and cl.isdigit() and int(cl) > _MAX_IMAGE_BYTES:
                    raise ValueError("image too large")
                ct = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
                if ct not in _ALLOWED_IMAGE_CT:
                    raise ValueError(f"unsupported image type: {ct or 'unknown'}")
                with path.open("wb") as out:
                    async for chunk in resp.content.iter_chunked(64 * 1024):
                        written += len(chunk)
                        if written > _MAX_IMAGE_BYTES:
                            raise ValueError("image too large")
                        out.write(chunk)
            return str(path), ct
        except Exception:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    async def _upload_local_image_to_max(self, file_path: str, filename: str, content_type: str) -> str:
        """Reserve slot, POST multipart to upload URL, return attachment token (never reuse inbound URL)."""
        slot_url = f"{MAX_API_BASE}/uploads?type=image"
        status, slot = await self._max_json_with_5xx_retry("POST", slot_url, json_payload={})
        if status != 200:
            raise RuntimeError(f"uploads slot failed: HTTP {status} {slot}")
        upload_url = str(slot.get("url") or slot.get("upload_url") or "").strip()
        if not upload_url:
            raise RuntimeError(f"uploads missing url: {slot}")

        data_bytes = await asyncio.to_thread(Path(file_path).read_bytes)
        body, ctype_hdr = MaxClient._multipart_form_data("data", filename, content_type, data_bytes)
        headers = {
            "Authorization": self.token,
            "Content-Type": ctype_hdr,
            "Content-Length": str(len(body)),
        }
        st, up = await self._max_json_with_5xx_retry("POST", upload_url, data=body, headers=headers)
        if st != 200:
            raise RuntimeError(f"upload POST failed: HTTP {st} {up}")
        tok = MaxClient._attachment_token_from_upload_response(up)
        if not tok:
            raise RuntimeError(f"upload response missing token: {up}")
        return tok

    async def _post_channel_message_with_attachments(
        self,
        channel_id: str,
        text: str,
        attachment_tokens: List[str],
    ) -> bool:
        """POST /messages with text + attachments; retry attachment.not.ready."""
        from gateway.formatters.spare24_formatter import (
            apply_spare24_formatting_for_max_outbound,
        )

        text = apply_spare24_formatting_for_max_outbound(channel_id, text or "")

        url = f"{MAX_API_BASE}/messages?chat_id={channel_id}"
        attachments = [{"type": "image", "payload": {"token": t}} for t in attachment_tokens]
        # Body text is HTML from ``format_post``; keep HTML format for correct rendering.
        payload: Dict[str, Any] = {
            "text": (text or "")[:4000],
            "format": "html",
            "notify": True,
            "attachments": attachments,
        }
        delay = 2.0
        for attempt in range(_MAX_NOT_READY_ATTEMPTS):
            status, body = await self._max_json_with_5xx_retry("POST", url, json_payload=payload)
            if status == 200:
                blob = json.dumps(body, ensure_ascii=False).lower()
                if "not.ready" in blob or "attachment.not.ready" in blob:
                    await asyncio.sleep(random.uniform(2.0, 5.0))
                    continue
                return True
            if 400 <= status < 500:
                logger.error("MAX channel post 4xx: %s %s", status, body)
                return False
            if status >= 500 and attempt < _MAX_NOT_READY_ATTEMPTS - 1:
                await asyncio.sleep(delay + random.random())
                delay = min(delay * 2, 16.0)
                continue
            return False
        return False

    async def _relay_images_to_channel(
        self,
        *,
        origin_chat_id: str,
        channel_id: str,
        text: str,
        image_urls: List[str],
    ) -> bool:
        """Download → re-upload → single channel message. On failure notify origin chat (no text-only channel post)."""
        await self._ensure_relay_tmp()

        async def one(url: str) -> str:
            path: Optional[str] = None
            try:
                async with self._relay_sem:
                    path, ct = await self._download_image_to_temp(url)
                    fname = Path(url).name.split("?")[0] or "image.bin"
                    if "." not in fname:
                        fname = "image.bin"
                    return await self._upload_local_image_to_max(path, fname, ct)
            finally:
                if path:
                    try:
                        Path(path).unlink(missing_ok=True)
                    except OSError:
                        pass

        try:
            results = await asyncio.gather(
                *[one(u) for u in image_urls],
                return_exceptions=True,
            )
            tokens: List[str] = []
            for r in results:
                if isinstance(r, Exception):
                    raise r
                tokens.append(str(r))
        except Exception as exc:
            logger.warning("MAX relay image pipeline failed: %s", exc, exc_info=True)
            await self.send(
                origin_chat_id,
                f"Не удалось обработать изображение: {exc}",
            )
            return False

        logger.info(
            "MAX relay: format_post (LLM HTML) before channel POST /messages, text_len=%s images=%s",
            len(text or ""),
            len(tokens),
        )
        formatted_text = await asyncio.to_thread(format_post, text)
        ok = await self._post_channel_message_with_attachments(channel_id, formatted_text, tokens)
        if not ok:
            await self.send(
                origin_chat_id,
                "Не удалось опубликовать пост в канале (ошибка API).",
            )
        return ok

    async def connect(self) -> bool:
        # region agent log
        _debug_log(
            run_id="pre-fix",
            hypothesis_id="H9",
            location="extensions/.../max.py:connect",
            message="max connect invoked",
            data={
                "token_present": bool(self.token),
                "allowed_user_ids": sorted(list(self._allowed_user_ids))[:20],
            },
        )
        # endregion
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
        logger.info(
            "MAX CONNECT bot_id=%s name=%r",
            self._bot_id,
            me.get("name", "unknown"),
        )
        logger.info(
            "MAX bot username=%r — user DMs must go to this bot for /updates to receive message_created",
            me.get("username", ""),
        )

        # Webhook and long poll are mutually exclusive on MAX (dev.max.ru).
        sub_payload = await self._api("GET", "/subscriptions")
        if sub_payload is not None:
            subs = sub_payload.get("subscriptions") or []
            if subs:
                logger.warning(
                    "MAX: %s webhook subscription(s) active — long poll will NOT receive events; "
                    "remove webhook in MAX developer UI or implement webhook ingress",
                    len(subs),
                )
            else:
                logger.info("MAX: /subscriptions empty — long polling is the active delivery mode")
        else:
            logger.warning("MAX: GET /subscriptions failed — could not confirm webhook vs long poll mode")

        chats_payload = await self._api("GET", "/chats")
        if chats_payload is not None:
            chat_list = chats_payload.get("chats") or []
            if isinstance(chat_list, list):
                logger.info("MAX: GET /chats count=%s (channels/chats visible to this bot token)", len(chat_list))

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
        # region agent log
        _debug_log(
            run_id="pre-fix",
            hypothesis_id="H6",
            location="extensions/.../max.py:_handle_update",
            message="max update received",
            data={"update_type": update_type, "has_message": isinstance(update.get("message"), dict)},
        )
        # endregion
        if update_type == "message_created":
            mid = (update.get("message") or {}).get("body", {}).get("mid") or str(update)
            if _dedup.is_duplicate(mid):
                mid_s = mid if isinstance(mid, str) else str(mid)
                logger.info("MAX skip duplicate update mid=%s", mid_s[:120])
                return
            await self._handle_message(update)
        elif update_type == "bot_started":
            chat_id = str(update.get("chat_id", ""))
            user = update.get("user", {}) or {}
            logger.info(
                "MAX bot_started chat_id=%s user_id=%s",
                chat_id or "(empty)",
                user.get("user_id", ""),
            )
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
        author_id = message.get("author_id")

        if self._bot_id is not None:
            try:
                bid = int(self._bot_id)
                if sender_id is not None and int(sender_id) == bid:
                    logger.info("MAX skip message from self (bot) sender_id=%s", sender_id)
                    return
                if author_id is not None and int(author_id) == bid:
                    logger.info("MAX skip message from self (bot) author_id=%s", author_id)
                    return
            except (TypeError, ValueError):
                if sender_id == self._bot_id or author_id == self._bot_id:
                    logger.info("MAX skip message from self (bot) sender_id=%s author_id=%s", sender_id, author_id)
                    return
        if self._allowed_user_ids and str(sender_id or "") not in self._allowed_user_ids:
            # region agent log
            _debug_log(
                run_id="pre-fix",
                hypothesis_id="H7",
                location="extensions/.../max.py:_handle_message",
                message="max allowlist reject",
                data={
                    "sender_id": str(sender_id or ""),
                    "allowed_user_ids": sorted(list(self._allowed_user_ids))[:20],
                },
            )
            # endregion
            logger.info(
                "MAX skip sender not in MAX_ALLOWED_USERS sender_id=%s allowed=%s",
                sender_id,
                sorted(self._allowed_user_ids),
            )
            return
        # region agent log
        _debug_log(
            run_id="pre-fix",
            hypothesis_id="H8",
            location="extensions/.../max.py:_handle_message",
            message="max allowlist passed or disabled",
            data={
                "sender_id": str(sender_id or ""),
                "allowlist_enabled": bool(self._allowed_user_ids),
                "allowed_user_ids": sorted(list(self._allowed_user_ids))[:20],
            },
        )
        # endregion

        recipient = message.get("recipient", {}) or {}
        chat_id = str(recipient.get("chat_id") or recipient.get("user_id") or "")

        if not chat_id:
            logger.warning("MAX skip no chat_id on recipient payload_keys=%s", list(recipient.keys()))
            return

        user_id = str(sender_id or "")
        user_name = sender.get("name", "") or sender.get("username", "")
        chat_type_raw = recipient.get("chat_type", "dialog")
        chat_type = "group" if chat_type_raw == "chat" else "dm"

        body = message.get("body", {}) or {}
        text = body.get("text", "") or ""
        attachments = body.get("attachments") or []
        # region agent log
        _debug_log(
            run_id="pre-fix",
            hypothesis_id="H10",
            location="extensions/.../max.py:_handle_message",
            message="max message payload parsed",
            data={
                "chat_id": chat_id,
                "user_id": user_id,
                "text_len": len(text or ""),
                "attachments_count": len(attachments or []),
            },
        )
        # endregion
        if not text and not attachments:
            logger.info("MAX skip empty text and no attachments chat_id=%s", chat_id)
            # region agent log
            _debug_log(
                run_id="pre-fix",
                hypothesis_id="H11",
                location="extensions/.../max.py:_handle_message",
                message="max message skipped: empty text and attachments",
                data={"chat_id": chat_id, "user_id": user_id},
            )
            # endregion
            return

        image_urls = _extract_image_urls_from_body(body)
        relay_ch = self._relay_channel_id()

        if image_urls:
            if not relay_ch:
                await self.send(
                    chat_id,
                    "Пересылка изображений в канал не настроена. Задайте MAX_CHANNEL_ID или MAX_RELAY_CHANNEL_ID.",
                )
                return
            ok = await self._relay_images_to_channel(
                origin_chat_id=chat_id,
                channel_id=relay_ch,
                text=text,
                image_urls=image_urls,
            )
            if ok:
                return
            return

        if not text:
            logger.info("MAX skip attachments-only without relay text chat_id=%s", chat_id)
            return

        source = self.build_source(
            chat_id=chat_id,
            user_id=user_id,
            user_name=user_name,
            chat_type=chat_type,
        )

        event = MessageEvent(
            source=source,
            text=text,
            message_type=MessageType.TEXT,
            raw_message=update,
        )
        logger.info(
            "MAX dispatch chat_id=%s user_id=%s chat_type=%s text_len=%d",
            chat_id,
            user_id,
            chat_type,
            len(text or ""),
        )
        # region agent log
        _debug_log(
            run_id="pre-fix",
            hypothesis_id="H12",
            location="extensions/.../max.py:_handle_message",
            message="max dispatch to gateway handler",
            data={"chat_id": chat_id, "user_id": user_id, "chat_type": chat_type, "text_len": len(text or "")},
        )
        # endregion
        await self.handle_message(event)

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        _ = reply_to, metadata
        text = (content or "").strip()
        from gateway.formatters.spare24_formatter import (
            apply_spare24_formatting_for_max_outbound,
        )

        text = apply_spare24_formatting_for_max_outbound(chat_id, text)

        chunks = [text[i : i + MAX_MESSAGE_LENGTH] for i in range(0, len(text), MAX_MESSAGE_LENGTH)] or [""]
        last_result = SendResult(success=False)

        tag_re = re.compile(r"<[a-zA-Z][\w-]*(\s|>|/)")

        async def _post_chunk(p: Dict[str, Any]) -> Tuple[int, str, Dict[str, Any]]:
            async with self._session.post(
                f"{MAX_API_BASE}/messages",
                headers=self._headers,
                params={"chat_id": chat_id},
                json=p,
            ) as resp:
                raw = await resp.read()
                err_preview = raw.decode("utf-8", errors="replace")[:1000] if raw else ""
                try:
                    data = json.loads(raw.decode("utf-8") or "{}")
                except (json.JSONDecodeError, UnicodeDecodeError):
                    data = {}
                return resp.status, err_preview, data if isinstance(data, dict) else {}

        for chunk in chunks:
            if tag_re.search(chunk or ""):
                payloads: List[Dict[str, Any]] = [
                    {"text": chunk, "notify": True, "format": "html"},
                ]
            else:
                payloads = [
                    {"text": chunk, "notify": True, "format": "markdown"},
                    {"text": chunk, "notify": True},
                ]

            chunk_result: Optional[SendResult] = None
            for pi, payload in enumerate(payloads):
                try:
                    status, err_preview, data = await _post_chunk(payload)
                except Exception as e:
                    last_result = SendResult(success=False, error=str(e))
                    chunk_result = last_result
                    break

                if status == 200:
                    msg = data.get("message") if isinstance(data, dict) else None
                    success = bool(
                        data.get("ok")
                        or data.get("message_id") is not None
                        or (isinstance(msg, dict) and msg)
                    )
                    if success and isinstance(msg, dict):
                        chunk_result = SendResult(
                            success=True,
                            message_id=str(msg.get("mid", "")),
                        )
                    elif success:
                        chunk_result = SendResult(
                            success=True,
                            message_id=str(data.get("message_id", "")),
                        )
                    else:
                        chunk_result = SendResult(
                            success=False,
                            error=str(data) if data else err_preview,
                        )
                    break

                if status == 400 and pi == 0 and payload.get("format") == "markdown":
                    logger.warning(
                        "MAX send: markdown rejected (HTTP 400), retrying plain text: %s",
                        err_preview[:200],
                    )
                    continue

                chunk_result = SendResult(success=False, error=err_preview)
                break

            if chunk_result is not None:
                last_result = chunk_result
            if not last_result.success:
                break

        return last_result

    async def get_chat_info(self, chat_id: str) -> Dict:
        data = await self._api("GET", f"/chats/{chat_id}")
        if data:
            return {
                "name": data.get("title") or data.get("name", chat_id),
                "type": data.get("type", "dm"),
                "chat_id": chat_id,
            }
        return {"name": chat_id, "type": "unknown", "chat_id": chat_id}
