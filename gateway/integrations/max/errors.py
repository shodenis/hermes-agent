from __future__ import annotations

from typing import Set

FATAL_HTTP_STATUSES: Set[int] = frozenset({400, 401, 403, 404})
RETRYABLE_HTTP_STATUSES: Set[int] = frozenset({429, 500, 502, 503, 504})


class MaxAPIError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status: int = 0,
        code: str = "",
        fatal: bool = False,
        retryable: bool = True,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.code = code or ""
        self._fatal = fatal
        self._retryable = retryable

    @property
    def fatal(self) -> bool:
        if self._fatal:
            return True
        return self.status in FATAL_HTTP_STATUSES

    @property
    def retryable(self) -> bool:
        if not self._retryable:
            return False
        return self.status in RETRYABLE_HTTP_STATUSES or self.status == 0
