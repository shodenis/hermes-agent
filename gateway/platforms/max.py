"""
MAX (max.ru) platform adapter for Hermes Agent.
Uses MAX Bot API (platform-api.max.ru) with Long Polling.
"""
import asyncio
import json
import logging
import os
from typing import Any, Dict, Optional, Set

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
        # In-memory marker: on adapter restart it is reset and /updates may replay.
        self._marker: Optional[int] = None
        self._bot_id: Optional[int] = None
        allowed = (os.getenv("MAX_ALLOWED_USERS", "") or "").strip()
        self._allowed_user_ids: Set[str] = {
            v.strip() for v in allowed.split(",") if v.strip()
        }

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
        logger.info(
            "MAX CONNECT bot_id=%s name=%r",
            self._bot_id,
            me.get("name", "unknown"),
        )

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
        if self._allowed_user_ids and str(sender_id or "") not in self._allowed_user_ids:
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
        if not text:
            # Text-only MVP: ignore non-text updates.
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

        chunks = [text[i:i + MAX_MESSAGE_LENGTH] for i in range(0, len(text), MAX_MESSAGE_LENGTH)] or [""]
        last_result = SendResult(success=False)

        for chunk in chunks:
            payload = {"text": chunk, "notify": True}

            try:
                async with self._session.post(
                    f"{MAX_API_BASE}/messages",
                    headers=self._headers,
                    params={"chat_id": chat_id},
                    json=payload,
                ) as resp:
                    raw = await resp.read()
                    err_preview = raw.decode("utf-8", errors="replace")[:1000] if raw else ""
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

    async def get_chat_info(self, chat_id: str) -> Dict:
        data = await self._api("GET", f"/chats/{chat_id}")
        if data:
            return {
                "name": data.get("title") or data.get("name", chat_id),
                "type": data.get("type", "dm"),
                "chat_id": chat_id,
            }
        return {"name": chat_id, "type": "unknown", "chat_id": chat_id}
