from __future__ import annotations

import os
from typing import Optional

from .client import MaxClient


class MaxService:
    """High-level Max messenger notification service.

    Follows the same factory pattern as BitrixService.from_env().
    Wraps MaxClient to provide a single send_notify() method with
    retry/backoff inherited from MaxClient.
    """

    def __init__(self, token: str, chat_id: str) -> None:
        self.client = MaxClient(token)
        self.chat_id = chat_id

    @classmethod
    def from_env(cls) -> Optional["MaxService"]:
        """Construct from environment variables. Returns None if not configured."""
        token = (os.getenv("MAX_BOT_TOKEN") or "").strip()
        chat_id = (
            os.getenv("MAX_NOTIFY_CHAT_ID") or os.getenv("MAX_NOTIFY_CHAT") or ""
        ).strip()
        if not token or not chat_id:
            return None
        return cls(token=token, chat_id=chat_id)

    def send_notify(self, text: str) -> bool:
        """Send a notification message. Returns True on success, False on failure."""
        url = f"https://platform-api.max.ru/messages?chat_id={self.chat_id}"
        return self.client.post(url, {"text": text, "notify": False})
