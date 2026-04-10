import sqlite3
import threading
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class EmailApprovalStore:
    """Persistent store for human-in-the-loop email approvals."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._check_or_recreate()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _check_or_recreate(self) -> None:
        if not self.db_path.exists():
            return
        try:
            conn = sqlite3.connect(str(self.db_path))
            try:
                result = conn.execute("PRAGMA integrity_check;").fetchone()
            finally:
                conn.close()
            if not result or result[0] != "ok":
                raise ValueError(f"integrity_check failed: {result[0] if result else 'empty result'}")
        except Exception as e:
            corrupt_path = self.db_path.with_suffix(
                f".corrupted.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            try:
                self.db_path.rename(corrupt_path)
                logger.error("Corrupted DB moved to %s, will recreate: %s", corrupt_path, e)
            except Exception:
                logger.exception("Failed rotating corrupted DB %s", self.db_path)
                raise

    def _init_db(self) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS email_approvals (
                    approval_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    email_to TEXT NOT NULL,
                    email_subject TEXT NOT NULL,
                    email_in_reply_to TEXT,
                    reply_from_mailbox TEXT,
                    draft_text TEXT NOT NULL,
                    inbound_text_snapshot TEXT,
                    session_id TEXT,
                    session_key TEXT,
                    max_chat_id TEXT NOT NULL,
                    correction_round INTEGER NOT NULL DEFAULT 0,
                    last_max_message_id TEXT
                )
                """
            )
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA wal_autocheckpoint=100")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_email_approvals_status_expires "
                "ON email_approvals(status, expires_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_email_approvals_chat_status_created "
                "ON email_approvals(max_chat_id, status, created_at)"
            )

    def create_pending(
        self,
        *,
        approval_id: str,
        email_to: str,
        email_subject: str,
        email_in_reply_to: str,
        reply_from_mailbox: str,
        draft_text: str,
        inbound_text_snapshot: str,
        session_id: str,
        session_key: str,
        max_chat_id: str,
        timeout_minutes: int = 30,
    ) -> None:
        now = _utc_now()
        expires = now + timedelta(minutes=timeout_minutes)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO email_approvals(
                    approval_id, status, created_at, expires_at, updated_at,
                    email_to, email_subject, email_in_reply_to, reply_from_mailbox,
                    draft_text, inbound_text_snapshot, session_id, session_key,
                    max_chat_id, correction_round, last_max_message_id
                ) VALUES (?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, '')
                """,
                (
                    approval_id,
                    now.isoformat(),
                    expires.isoformat(),
                    now.isoformat(),
                    email_to,
                    email_subject,
                    email_in_reply_to or "",
                    reply_from_mailbox or "",
                    draft_text or "",
                    inbound_text_snapshot or "",
                    session_id or "",
                    session_key or "",
                    max_chat_id,
                ),
            )

    def get_pending_by_id(self, approval_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM email_approvals WHERE approval_id=? AND status='pending'",
                (approval_id,),
            ).fetchone()
            return dict(row) if row else None

    def get_latest_pending_for_chat(self, max_chat_id: str) -> Optional[Dict[str, Any]]:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM email_approvals
                WHERE max_chat_id=? AND status='pending'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (max_chat_id,),
            ).fetchone()
            return dict(row) if row else None

    def mark_status(self, approval_id: str, status: str) -> bool:
        now = _utc_now().isoformat()
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                "UPDATE email_approvals SET status=?, updated_at=? WHERE approval_id=? AND status='pending'",
                (status, now, approval_id),
            )
            return cur.rowcount > 0

    def update_draft_with_correction(self, approval_id: str, draft_text: str) -> bool:
        now = _utc_now().isoformat()
        with self._lock, self._connect() as conn:
            cur = conn.execute(
                """
                UPDATE email_approvals
                SET draft_text=?, correction_round=correction_round+1, updated_at=?
                WHERE approval_id=? AND status='pending'
                """,
                (draft_text, now, approval_id),
            )
            return cur.rowcount > 0

    def expire_due(self) -> list[Dict[str, Any]]:
        now = _utc_now().isoformat()
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM email_approvals WHERE status='pending' AND expires_at <= ?",
                (now,),
            ).fetchall()
            conn.execute(
                "UPDATE email_approvals SET status='expired', updated_at=? "
                "WHERE status='pending' AND expires_at <= ?",
                (now, now),
            )
            return [dict(r) for r in rows]

    def close(self) -> None:
        """Compatibility hook for graceful shutdown."""
        return
