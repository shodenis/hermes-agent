from __future__ import annotations

import json
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .errors import BitrixAPIError

MAX_BACKOFF_MS = 30000


@dataclass(frozen=True)
class WebhookConfig:
    webhook_url: str


class _NoopRateLimiter:
    def acquire(self) -> None:
        return


class _TokenBucketRateLimiter:
    def __init__(self, *, rate_per_sec: float, burst: float) -> None:
        self.rate = max(rate_per_sec, 0.1)
        self.capacity = max(burst, 1.0)
        self.tokens = self.capacity
        self.updated_at = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self.updated_at)
                self.updated_at = now
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                needed = (1.0 - self.tokens) / self.rate
            time.sleep(min(max(needed, 0.01), 0.5))


class Bitrix24Client:
    def __init__(
        self,
        webhook_url: str,
        *,
        timeout: int = 10,
        max_attempts: int = 5,
        rate_per_sec: Optional[float] = None,
        burst: Optional[float] = None,
    ) -> None:
        self.config = WebhookConfig(webhook_url=webhook_url.rstrip("/"))
        self.timeout = timeout
        self.max_attempts = max_attempts
        if rate_per_sec is None or burst is None:
            self.rate_limiter = _NoopRateLimiter()
        else:
            self.rate_limiter = _TokenBucketRateLimiter(rate_per_sec=rate_per_sec, burst=burst)

    def call(self, method: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.config.webhook_url}/{method}.json"
        payload = self._encode_params(params or {})

        for attempt in range(1, self.max_attempts + 1):
            self.rate_limiter.acquire()
            try:
                return self._post(url, payload)
            except BitrixAPIError as exc:
                if exc.fatal or not exc.retryable or attempt == self.max_attempts:
                    raise
                self._backoff(attempt)
            except urllib.error.HTTPError as exc:
                status, body = self._read_http_error(exc)
                api_exc = self._to_api_error(status=status, body=body)
                if api_exc.fatal or not api_exc.retryable or attempt == self.max_attempts:
                    raise api_exc
                self._backoff(attempt)
            except urllib.error.URLError as exc:
                if attempt == self.max_attempts:
                    raise BitrixAPIError(
                        f"Network error: {exc}",
                        status=0,
                        code="NETWORK_ERROR",
                    ) from exc
                self._backoff(attempt)

        raise BitrixAPIError("Retries exhausted", code="RETRIES_EXHAUSTED")

    @staticmethod
    def _encode_params(params: Dict[str, Any]) -> Dict[str, Any]:
        encoded: Dict[str, Any] = {}
        for key, value in params.items():
            if isinstance(value, list):
                for idx, item in enumerate(value):
                    encoded[f"{key}[{idx}]"] = item
            else:
                encoded[key] = value
        return encoded

    def _post(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        body = urllib.parse.urlencode(payload).encode("utf-8")
        req = urllib.request.Request(
            url=url,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
            data=body,
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        parsed = self._safe_json_parse(raw)
        self._raise_for_api_error(parsed, status=200)
        return parsed

    @staticmethod
    def _read_http_error(exc: urllib.error.HTTPError) -> tuple[int, Dict[str, Any]]:
        try:
            raw = exc.read().decode("utf-8", errors="replace")
            body = Bitrix24Client._safe_json_parse(raw)
        except Exception:
            body = {}
        return exc.code, body

    @staticmethod
    def _safe_json_parse(raw: str) -> Dict[str, Any]:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as e:
            raise BitrixAPIError("Invalid JSON response", code="INVALID_JSON") from e
        if isinstance(payload, dict):
            return payload
        return {"result": payload}

    def _raise_for_api_error(self, body: Dict[str, Any], *, status: int) -> None:
        exc = self._to_api_error(status=status, body=body)
        if exc.code:
            raise exc

    @staticmethod
    def _to_api_error(*, status: int, body: Dict[str, Any]) -> BitrixAPIError:
        if "error" in body and isinstance(body["error"], str):
            code = body.get("error", "")
            msg = body.get("error_description", code) or code
            return BitrixAPIError(msg, status=status, code=code, payload=body)
        if isinstance(body.get("error"), dict):
            err = body["error"]
            code = str(err.get("code", "") or "")
            msg = str(err.get("message", code) or code)
            return BitrixAPIError(msg, status=status, code=code, payload=body)
        return BitrixAPIError("", status=status, code="", payload=body)

    @staticmethod
    def _backoff(attempt: int) -> None:
        base_ms = min(500 * (2 ** (attempt - 1)), MAX_BACKOFF_MS)
        jitter_ms = random.randint(0, 250)
        time.sleep((base_ms + jitter_ms) / 1000.0)
