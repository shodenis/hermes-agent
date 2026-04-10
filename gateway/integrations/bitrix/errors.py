from __future__ import annotations

from typing import Any, Dict, Optional, Set

FATAL_ERROR_CODES: Set[str] = frozenset(
    {
        "WRONG_AUTH_TYPE",
        "insufficient_scope",
        "INVALID_CREDENTIALS",
        "NO_AUTH_FOUND",
        "METHOD_NOT_FOUND",
        "ERROR_METHOD_NOT_FOUND",
        "INVALID_REQUEST",
        "ACCESS_DENIED",
        "PAYMENT_REQUIRED",
    }
)

RETRYABLE_CODES: Set[str] = frozenset(
    {
        "QUERY_LIMIT_EXCEEDED",
        "TOO_MANY_REQUESTS",
        "TEMPORARY_UNAVAILABLE",
        "NETWORK_ERROR",
        "TIMEOUT",
        "INTERNAL_ERROR",
    }
)


class BitrixAPIError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status: int = 0,
        code: str = "",
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.code = code or ""
        self.payload = payload or {}

    @property
    def retryable(self) -> bool:
        if self.code in RETRYABLE_CODES:
            return True
        return self.status in {408, 425, 429, 500, 502, 503, 504}

    @property
    def fatal(self) -> bool:
        return self.code in FATAL_ERROR_CODES
