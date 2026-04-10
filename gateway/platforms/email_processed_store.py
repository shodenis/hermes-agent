"""Persistent SQLite registry for inbound email processing (dedup across restarts)."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# Outcomes written by the email adapter:
# - processing: claim before handle_message; agent_completed distinguishes retry vs finalize-only
# - dispatched: finalize succeeded (registry + IMAP \\Seen path)
# - skipped_automated / skipped_self / pre-existing: terminal without agent


class EmailProcessedStore:
    """SQLite-backed registry: which messages were handled and one-time UID migration."""

    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()
        self._migrate_add_agent_completed()
        self._migrate_add_classification_columns()

    def _init_schema(self) -> None:
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS email_processed (
                dedup_key TEXT PRIMARY KEY,
                mailbox TEXT NOT NULL,
                message_id TEXT,
                imap_uid TEXT,
                sender TEXT,
                subject TEXT,
                processed_at REAL NOT NULL,
                outcome TEXT NOT NULL,
                agent_completed INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS email_registry_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_email_processed_mailbox ON email_processed(mailbox, processed_at)"
        )
        self._conn.commit()

    def _migrate_add_agent_completed(self) -> None:
        cols = {
            r[1]
            for r in self._conn.execute("PRAGMA table_info(email_processed)").fetchall()
        }
        if "agent_completed" not in cols:
            self._conn.execute(
                "ALTER TABLE email_processed ADD COLUMN agent_completed INTEGER NOT NULL DEFAULT 0"
            )
            self._conn.commit()

    def _migrate_add_classification_columns(self) -> None:
        cols = {
            r[1]
            for r in self._conn.execute("PRAGMA table_info(email_processed)").fetchall()
        }
        if "request_type" not in cols:
            self._conn.execute(
                "ALTER TABLE email_processed ADD COLUMN request_type TEXT"
            )
        if "classification_confidence" not in cols:
            self._conn.execute(
                "ALTER TABLE email_processed ADD COLUMN classification_confidence REAL"
            )
        if "classification_source" not in cols:
            self._conn.execute(
                "ALTER TABLE email_processed ADD COLUMN classification_source TEXT"
            )
        if "classification_reason" not in cols:
            self._conn.execute(
                "ALTER TABLE email_processed ADD COLUMN classification_reason TEXT"
            )
        self._conn.commit()

    def is_migration_complete(self) -> bool:
        row = self._conn.execute(
            "SELECT value FROM email_registry_meta WHERE key = ?",
            ("uid_seed_v1",),
        ).fetchone()
        return row is not None and row[0] == "1"

    def set_migration_complete(self) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO email_registry_meta (key, value) VALUES (?, ?)",
            ("uid_seed_v1", "1"),
        )
        self._conn.commit()

    def has(self, dedup_key: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM email_processed WHERE dedup_key = ? LIMIT 1",
            (dedup_key,),
        ).fetchone()
        return row is not None

    def get_row(self, dedup_key: str) -> Optional[Dict[str, Any]]:
        row = self._conn.execute(
            "SELECT outcome, agent_completed FROM email_processed WHERE dedup_key = ?",
            (dedup_key,),
        ).fetchone()
        if not row:
            return None
        return {"outcome": row[0], "agent_completed": bool(row[1])}

    def claim_processing(
        self,
        *,
        dedup_key: str,
        mailbox: str,
        message_id: Optional[str],
        imap_uid: str,
        sender: Optional[str],
        subject: Optional[str],
    ) -> bool:
        """INSERT outcome=processing, agent_completed=0. Returns True if this call inserted the row."""
        now = time.time()
        cur = self._conn.execute(
            """
            INSERT OR IGNORE INTO email_processed (
                dedup_key, mailbox, message_id, imap_uid, sender, subject, processed_at, outcome, agent_completed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'processing', 0)
            """,
            (
                dedup_key,
                mailbox,
                message_id,
                imap_uid,
                sender,
                subject,
                now,
            ),
        )
        self._conn.commit()
        return cur.rowcount > 0

    def set_agent_completed(self, dedup_key: str) -> None:
        """After handle_message succeeds; enables finalize-only recovery if finalize crashes."""
        self._conn.execute(
            "UPDATE email_processed SET agent_completed = 1 WHERE dedup_key = ? AND outcome = 'processing'",
            (dedup_key,),
        )
        self._conn.commit()

    def delete_processing_claim(self, dedup_key: str) -> None:
        """On handle_message failure: remove claim so the next poll can retry."""
        self._conn.execute(
            "DELETE FROM email_processed WHERE dedup_key = ? AND outcome = 'processing'",
            (dedup_key,),
        )
        self._conn.commit()

    def upgrade_to_dispatched(
        self,
        *,
        dedup_key: str,
        mailbox: str,
        message_id: Optional[str],
        imap_uid: str,
        sender: Optional[str],
        subject: Optional[str],
    ) -> bool:
        """UPDATE processing+agent_completed -> dispatched. Returns whether a row was updated."""
        now = time.time()
        cur = self._conn.execute(
            """
            UPDATE email_processed SET
                mailbox = ?,
                message_id = ?,
                imap_uid = ?,
                sender = ?,
                subject = ?,
                processed_at = ?,
                outcome = 'dispatched'
            WHERE dedup_key = ? AND outcome = 'processing' AND agent_completed = 1
            """,
            (
                mailbox,
                message_id,
                imap_uid,
                sender,
                subject,
                now,
                dedup_key,
            ),
        )
        self._conn.commit()
        return cur.rowcount > 0

    def mark(
        self,
        *,
        dedup_key: str,
        mailbox: str,
        message_id: Optional[str],
        imap_uid: str,
        sender: Optional[str],
        subject: Optional[str],
        outcome: str,
    ) -> None:
        now = time.time()
        self._conn.execute(
            """
            INSERT OR IGNORE INTO email_processed (
                dedup_key, mailbox, message_id, imap_uid, sender, subject, processed_at, outcome, agent_completed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (
                dedup_key,
                mailbox,
                message_id,
                imap_uid,
                sender,
                subject,
                now,
                outcome,
            ),
        )
        self._conn.commit()

    def seed_pre_existing_uids(self, addr_key: str, uids: List[bytes]) -> int:
        """Insert migrate:{mailbox}:{uid} rows with outcome pre-existing. Idempotent."""
        if not uids:
            return 0
        now = time.time()
        rows: List[Any] = []
        for uid in uids:
            uid_s = uid.decode() if isinstance(uid, bytes) else str(uid)
            dk = f"migrate:{addr_key}:{uid_s}"
            rows.append(
                (dk, addr_key, None, uid_s, None, None, now, "pre-existing", 0)
            )
        self._conn.executemany(
            """
            INSERT OR IGNORE INTO email_processed (
                dedup_key, mailbox, message_id, imap_uid, sender, subject, processed_at, outcome, agent_completed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self._conn.commit()
        return len(rows)

    def set_classification(
        self,
        *,
        dedup_key: str,
        request_type: str,
        confidence: float,
        source: str,
        reason: str,
    ) -> bool:
        """Set classification exactly once for a dedup row."""
        cur = self._conn.execute(
            """
            UPDATE email_processed
            SET request_type = ?, classification_confidence = ?, classification_source = ?, classification_reason = ?
            WHERE dedup_key = ? AND request_type IS NULL
            """,
            (
                request_type,
                float(confidence),
                source,
                reason,
                dedup_key,
            ),
        )
        self._conn.commit()
        return cur.rowcount > 0
