from __future__ import annotations

import json
import random
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional

from .errors import MaxAPIError, FATAL_HTTP_STATUSES, RETRYABLE_HTTP_STATUSES

# Backoff schedule in seconds for attempts 1, 2, 3
_BACKOFF_SCHEDULE = [0.5, 1.5, 3.5]
_JITTER_FACTOR = 0.2  # ±20%


class MaxClient:
    """HTTP client for Max messenger API with retry/backoff logic.

    Mirrors the Bitrix24Client pattern:
    - max_attempts=3 with exponential backoff + jitter
    - Fatal HTTP codes (400/401/403/404) raise immediately, no retry
    - Retryable: network errors, timeout, HTTP 429, 5xx
    - No rate limiter (notifications are infrequent)
    """

    def __init__(
        self,
        token: str,
        *,
        timeout: int = 10,
        max_attempts: int = 3,
    ) -> None:
        self.token = token
        self.timeout = timeout
        self.max_attempts = max_attempts

    def post(
        self,
        url: str,
        payload: Dict[str, Any],
    ) -> bool:
        """POST JSON payload to url. Returns True on 2xx success.

        Raises MaxAPIError on fatal errors or exhausted retries.
        """
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Authorization": self.token,
        }

        last_exc: Optional[Exception] = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                req = urllib.request.Request(
                    url=url,
                    method="POST",
                    headers=headers,
                    data=body,
                )
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    resp.read()
                return True

            except urllib.error.HTTPError as exc:
                status = exc.code
                if status in FATAL_HTTP_STATUSES:
                    raise MaxAPIError(
                        f"Max API fatal HTTP error: {status}",
                        status=status,
                        fatal=True,
                        retryable=False,
                    ) from exc
                if status in RETRYABLE_HTTP_STATUSES and attempt < self.max_attempts:
                    last_exc = exc
                    self._backoff(attempt)
                    continue
                raise MaxAPIError(
                    f"Max API HTTP error: {status}",
                    status=status,
                    retryable=False,
                ) from exc

            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last_exc = exc
                if attempt < self.max_attempts:
                    self._backoff(attempt)
                    continue
                raise MaxAPIError(
                    f"Max API network error: {exc}",
                    status=0,
                    code="NETWORK_ERROR",
                ) from exc

        raise MaxAPIError(
            f"Max API retries exhausted after {self.max_attempts} attempts",
            code="RETRIES_EXHAUSTED",
        )

    @staticmethod
    def _backoff(attempt: int) -> None:
        """Exponential backoff with ±20% jitter. Schedule: 0.5s, 1.5s, 3.5s."""
        idx = min(attempt - 1, len(_BACKOFF_SCHEDULE) - 1)
        base = _BACKOFF_SCHEDULE[idx]
        jitter = base * _JITTER_FACTOR * (random.random() * 2 - 1)
        time.sleep(max(0.0, base + jitter))
