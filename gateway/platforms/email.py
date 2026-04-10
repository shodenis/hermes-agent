"""
Email platform adapter for the Hermes gateway.

Allows users to interact with Hermes by sending emails.
Uses IMAP to receive and SMTP to send messages.

Environment variables:
    EMAIL_IMAP_HOST     — IMAP server host (e.g., imap.gmail.com)
    EMAIL_IMAP_PORT     — IMAP server port (default: 993)
    EMAIL_SMTP_HOST     — SMTP server host (e.g., smtp.gmail.com)
    EMAIL_SMTP_PORT     — SMTP server port (default: 587)
    EMAIL_ADDRESS       — Email address for the agent
    EMAIL_PASSWORD      — Email password or app-specific password
    YANDEX_EMAIL_2      — Optional second inbox (same IMAP/SMTP host as primary)
    YANDEX_PASSWORD_2   — Password for the second inbox
    EMAIL_POLL_INTERVAL — Seconds between mailbox checks (default: 15)
    EMAIL_ALLOWED_USERS — Comma-separated list of allowed sender addresses

Inbound deduplication uses $HERMES_HOME/email_processed.db (SQLite). One-time
migration records existing IMAP UIDs as pre-existing; fetch uses BODY.PEEK[]
and sets \\Seen after successful finalize.

Two-phase registry: INSERT ``processing`` before ``handle_message``; on agent
failure DELETE the claim; after ``handle_message`` set ``agent_completed`` then
UPDATE to ``dispatched`` and STORE \\Seen. If finalize crashes after the agent
ran, ``processing`` + ``agent_completed`` allows finalize-only retry (no second
agent run). Crash before ``agent_completed`` retries the agent on the same claim.
"""

import asyncio
import email as email_lib
import hashlib
import imaplib
import json
import logging
import os
import re
import smtplib
import ssl
import time
import uuid
import urllib.request
from email.header import decode_header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_document_from_bytes,
    cache_image_from_bytes,
)
from gateway.config import Platform, PlatformConfig
from gateway.platforms.email_processed_store import EmailProcessedStore
from gateway.email_approval_store import EmailApprovalStore
from gateway.notifications.max_notify import send_max_text_sync
from gateway.integrations.bitrix import BitrixService, BitrixServiceAdapter, CRMUseCases
from gateway.utils.classifier import (
    SKIP_LLM_REASONS,
    classify_email_heuristic,
    normalize_llm_result,
)

logger = logging.getLogger(__name__)

try:
    from hermes_constants import get_hermes_home
except Exception:  # pragma: no cover
    def get_hermes_home():
        from pathlib import Path
        return Path(os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes")))


def _migrate_uid_key(addr_key: str, uid: bytes) -> str:
    u = uid.decode() if isinstance(uid, bytes) else str(uid)
    return f"migrate:{addr_key}:{u}"


def _normalize_message_id_dedup(raw: str) -> str:
    if not raw or not str(raw).strip():
        return ""
    s = str(raw).strip().strip("<>")
    return f"mid:{s.lower()}"


def _fallback_dedup_key(addr_key: str, uid: bytes, sender: str, subject: str) -> str:
    u = uid.decode() if isinstance(uid, bytes) else str(uid)
    h = hashlib.sha256(
        f"{addr_key}|{u}|{sender}|{subject}".encode("utf-8", errors="replace")
    ).hexdigest()
    return f"fallback:{h}"


def _compute_dedup_key(addr_key: str, uid: bytes, message_id: str, sender: str, subject: str) -> str:
    mid = _normalize_message_id_dedup(message_id)
    if mid and mid != "mid:":
        return mid
    return _fallback_dedup_key(addr_key, uid, sender, subject)


def _extract_peek_body_bytes(msg_data: Any) -> Optional[bytes]:
    """Extract RFC822 bytes from imaplib fetch response (BODY.PEEK[] or RFC822)."""
    if not msg_data or not msg_data[0]:
        return None
    part = msg_data[0]
    if isinstance(part, tuple) and len(part) >= 2:
        return part[1]
    if isinstance(part, bytes):
        return part
    return None


def _imap_uid_store_seen(imap: imaplib.IMAP4_SSL, uid: bytes) -> None:
    typ, _ = imap.uid("STORE", uid, "+FLAGS", r"(\Seen)")
    if typ != "OK":
        logger.warning("[Email] STORE \\Seen failed for uid %s: %s", uid, typ)


def _sanitize_outbound_email_body(text: str) -> str:
    """Remove tool/agent leakage from plain-text bodies before SMTP.

    Models sometimes emit <tool_call>, execute_code, or curl snippets in the
    final assistant string; those must never reach the client's inbox.
    """
    if not text or not isinstance(text, str):
        return (text or "").strip() if isinstance(text, str) else ""
    t = re.sub(r"<tool_call>\s*[\s\S]*?</tool_call>", "", text, flags=re.IGNORECASE).strip()
    markers = (
        "<tool_call>",
        "execute_code",
        "curl ",
        "platform-api.max.ru",
        "<parameter=code>",
        "</function>",
        "<function=",
    )
    lower = t.lower()
    for m in markers:
        pos = lower.find(m.lower())
        if pos != -1:
            t = t[:pos].strip()
            lower = t.lower()
    t = t.strip()
    if not t:
        return (
            "Добрый день. Не удалось сформировать корректный ответ. "
            "Пожалуйста, кратко повторите запрос и укажите все поля заявки."
        )
    return t


def _append_signature_if_missing(text: str) -> str:
    """Append configured outbound signature unless already present."""
    body = (text or "").strip()
    if "с уважением" in body.lower():
        return body
    raw_sig = os.getenv("EMAIL_SIGNATURE_TEXT", "").strip()
    if not raw_sig:
        return body
    sig = raw_sig.replace("\\n", "\n").strip()
    if not sig:
        return body
    if not body:
        return sig
    return f"{body}\n\n{sig}"


_FOLLOW_UP_SUBJECT_RE = re.compile(r"^\s*re\s*:", re.IGNORECASE)
_FOLLOW_UP_BODY_RE = re.compile(
    r"(ожидаю|напоминаю|апдейт|update|статус|есть новости|follow[\s-]?up|обратн\w+\s+связ\w+)",
    re.IGNORECASE,
)
_INTERNAL_ONLY_RE = re.compile(
    r"(^|\n)\s*(💾\s*Memory updated|Memory updated|Session reviewed|Skill updated|PENDING enqueue:|PENDING dequeue:)",
    re.IGNORECASE,
)


def _looks_like_follow_up(subject: str, body: str, in_reply_to: str) -> bool:
    if (in_reply_to or "").strip():
        return True
    if _FOLLOW_UP_SUBJECT_RE.search(subject or ""):
        return True
    return bool(_FOLLOW_UP_BODY_RE.search(body or ""))


def _is_internal_only_output(text: str) -> bool:
    stripped = (text or "").strip()
    if not stripped:
        return True
    return bool(_INTERNAL_ONLY_RE.search(stripped))


# Automated sender patterns — emails from these are silently ignored
_NOREPLY_PATTERNS = (
    "noreply", "no-reply", "no_reply", "donotreply", "do-not-reply",
    "mailer-daemon", "postmaster", "bounce", "notifications@",
    "automated@", "auto-confirm", "auto-reply", "automailer",
)

# RFC headers that indicate bulk/automated mail
_AUTOMATED_HEADERS = {
    "Auto-Submitted": lambda v: v.lower() != "no",
    "Precedence": lambda v: v.lower() in ("bulk", "list", "junk"),
    "X-Auto-Response-Suppress": lambda v: bool(v),
    "List-Unsubscribe": lambda v: bool(v),
}

# Gmail-safe max length per email body
MAX_MESSAGE_LENGTH = 50_000

# Supported image extensions for inline detection
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}

def _is_automated_sender(address: str, headers: dict) -> bool:
    """Return True if this email is from an automated/noreply source."""
    addr = address.lower()
    if any(pattern in addr for pattern in _NOREPLY_PATTERNS):
        return True
    for header, check in _AUTOMATED_HEADERS.items():
        value = headers.get(header, "")
        if value and check(value):
            return True
    return False
    
def check_email_requirements() -> bool:
    """Check if email platform dependencies are available."""
    addr = os.getenv("EMAIL_ADDRESS")
    pwd = os.getenv("EMAIL_PASSWORD")
    imap = os.getenv("EMAIL_IMAP_HOST")
    smtp = os.getenv("EMAIL_SMTP_HOST")
    if not all([addr, pwd, imap, smtp]):
        return False
    return True


def _decode_header_value(raw: str) -> str:
    """Decode an RFC 2047 encoded email header into a plain string."""
    parts = decode_header(raw)
    decoded = []
    for part, charset in parts:
        if isinstance(part, bytes):
            decoded.append(part.decode(charset or "utf-8", errors="replace"))
        else:
            decoded.append(part)
    return " ".join(decoded)


def _extract_text_body(msg: email_lib.message.Message) -> str:
    """Extract the plain-text body from a potentially multipart email."""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", ""))
            # Skip attachments
            if "attachment" in disposition:
                continue
            if content_type == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")
        # Fallback: try text/html and strip tags
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", ""))
            if "attachment" in disposition:
                continue
            if content_type == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    html = payload.decode(charset, errors="replace")
                    return _strip_html(html)
        return ""
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="replace")
            if msg.get_content_type() == "text/html":
                return _strip_html(text)
            return text
        return ""


def _strip_html(html: str) -> str:
    """Naive HTML tag stripper for fallback text extraction."""
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<p[^>]*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_email_address(raw: str) -> str:
    """Extract bare email address from 'Name <addr>' format."""
    match = re.search(r"<([^>]+)>", raw)
    if match:
        return match.group(1).strip().lower()
    return raw.strip().lower()


def _extract_attachments(
    msg: email_lib.message.Message,
    skip_attachments: bool = False,
) -> List[Dict[str, Any]]:
    """Extract attachment metadata and cache files locally.

    When *skip_attachments* is True, all attachment/inline parts are ignored
    (useful for malware protection or bandwidth savings).
    """
    attachments = []
    if not msg.is_multipart():
        return attachments

    for part in msg.walk():
        disposition = str(part.get("Content-Disposition", ""))
        if skip_attachments and ("attachment" in disposition or "inline" in disposition):
            continue
        if "attachment" not in disposition and "inline" not in disposition:
            continue
        # Skip text/plain and text/html body parts
        content_type = part.get_content_type()
        if content_type in ("text/plain", "text/html") and "attachment" not in disposition:
            continue

        filename = part.get_filename()
        if filename:
            filename = _decode_header_value(filename)
        else:
            ext = part.get_content_subtype() or "bin"
            filename = f"attachment.{ext}"

        payload = part.get_payload(decode=True)
        if not payload:
            continue

        ext = Path(filename).suffix.lower()
        if ext in _IMAGE_EXTS:
            cached_path = cache_image_from_bytes(payload, ext)
            attachments.append({
                "path": cached_path,
                "filename": filename,
                "type": "image",
                "media_type": content_type,
            })
        else:
            cached_path = cache_document_from_bytes(payload, filename)
            attachments.append({
                "path": cached_path,
                "filename": filename,
                "type": "document",
                "media_type": content_type,
            })

    return attachments


class EmailAdapter(BasePlatformAdapter):
    """Email gateway adapter using IMAP (receive) and SMTP (send)."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.EMAIL)

        self._address = os.getenv("EMAIL_ADDRESS", "")
        self._password = os.getenv("EMAIL_PASSWORD", "")
        self._imap_host = os.getenv("EMAIL_IMAP_HOST", "")
        self._imap_port = int(os.getenv("EMAIL_IMAP_PORT", "993"))
        self._smtp_host = os.getenv("EMAIL_SMTP_HOST", "")
        self._smtp_port = int(os.getenv("EMAIL_SMTP_PORT", "587"))
        self._poll_interval = int(os.getenv("EMAIL_POLL_INTERVAL", "15"))

        # Skip attachments — configured via config.yaml:
        #   platforms:
        #     email:
        #       skip_attachments: true
        extra = config.extra or {}
        self._skip_attachments = extra.get("skip_attachments", False)

        self._accounts = self._build_accounts()
        # All IMAP/SMTP identities we own — never run the agent on mail *from* these
        # (avoids cross-mailbox ping-pong when two inboxes are polled by one gateway).
        self._our_addresses: frozenset[str] = frozenset(
            a["address"].strip().lower()
            for a in self._accounts
            if (a.get("address") or "").strip()
        )
        self._smtp_by_address: Dict[str, str] = {
            a["address"].lower(): a["password"] for a in self._accounts
        }
        self._poll_task: Optional[asyncio.Task] = None
        self._registry = EmailProcessedStore(get_hermes_home() / "email_processed.db")
        self._approval_store = EmailApprovalStore(get_hermes_home() / "email_approval.db")
        self._crm: Optional[CRMUseCases] = None
        bitrix_service = BitrixService.from_env()
        if bitrix_service is not None:
            bitrix_user_id = (os.getenv("BITRIX_USER_ID") or "").strip()
            if not bitrix_user_id:
                bitrix_user_id = "1"
                logger.warning("[Email] BITRIX_USER_ID is not set; defaulting to %s", bitrix_user_id)
            adapter = BitrixServiceAdapter(bitrix_service)
            self._crm = CRMUseCases(actions=adapter, responsible_id=bitrix_user_id)

        # Map chat_id (sender email) -> last subject + message-id + reply mailbox
        self._thread_context: Dict[str, Dict[str, Any]] = {}
        # Turn-level idempotency guard: prevent second send in same gateway turn.
        self._sent_turn_keys: Dict[str, float] = {}
        self._sent_turn_ttl_sec = 1800.0

        logger.info("[Email] Adapter initialized for %s", self._address)
        if len(self._accounts) > 1:
            logger.info("[Email] Also polling %s", self._accounts[1]["address"])

    @staticmethod
    def _approval_required() -> bool:
        return os.getenv("EMAIL_APPROVAL_REQUIRED", "false").strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _max_notify_chat_id() -> str:
        return (os.getenv("MAX_NOTIFY_CHAT_ID") or os.getenv("MAX_NOTIFY_CHAT") or "").strip()

    def _build_approval_prompt(self, *, to_addr: str, subject: str, draft_text: str) -> str:
        return (
            "📬 ЧЕРНОВИК ОТВЕТА\n"
            f"Кому: {to_addr}\n"
            f"Тема: {subject or '(no subject)'}\n"
            "---\n"
            f"{draft_text}\n"
            "---\n"
            'Отправить? Напиши "да", "нет" или исправление.'
        )

    async def _enqueue_for_approval(
        self,
        *,
        to_addr: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        max_token = (os.getenv("MAX_BOT_TOKEN") or "").strip()
        max_chat_id = self._max_notify_chat_id()
        if not max_token or not max_chat_id:
            return SendResult(success=False, error="EMAIL_APPROVAL_REQUIRED but MAX_BOT_TOKEN/MAX_NOTIFY_CHAT(_ID) missing")

        ctx = self._thread_context.get(to_addr, {})
        subject = ctx.get("subject", "Hermes Agent")
        if not subject.startswith("Re:"):
            subject = f"Re: {subject}"
        draft_text = _append_signature_if_missing(content)
        approval_id = uuid.uuid4().hex[:8].upper()
        prompt = self._build_approval_prompt(to_addr=to_addr, subject=subject, draft_text=draft_text)
        ok = await asyncio.to_thread(send_max_text_sync, prompt, chat_id=max_chat_id, token=max_token)
        if not ok:
            return SendResult(success=False, error="Failed to send approval prompt to MAX")

        self._approval_store.create_pending(
            approval_id=approval_id,
            email_to=to_addr,
            email_subject=subject,
            email_in_reply_to=ctx.get("message_id", ""),
            reply_from_mailbox=ctx.get("reply_from", ""),
            draft_text=draft_text,
            inbound_text_snapshot=ctx.get("inbound_text", ""),
            session_id=str((metadata or {}).get("session_id") or ""),
            session_key=str((metadata or {}).get("session_key") or to_addr),
            max_chat_id=max_chat_id,
            timeout_minutes=30,
        )
        logger.info("[Email][Approval] Queued draft %s for %s (MAX chat %s)", approval_id, to_addr, max_chat_id)
        return SendResult(success=True, message_id=f"pending-approval:{approval_id}")

    @staticmethod
    def _build_accounts() -> List[Dict[str, str]]:
        """Primary inbox plus optional YANDEX_EMAIL_2 when both are configured."""
        primary = os.getenv("EMAIL_ADDRESS", "").strip()
        primary_pw = os.getenv("EMAIL_PASSWORD", "")
        accounts: List[Dict[str, str]] = [{"address": primary, "password": primary_pw}]
        sec = os.getenv("YANDEX_EMAIL_2", "").strip()
        sec_pw = os.getenv("YANDEX_PASSWORD_2", "").strip()
        if sec and sec_pw and sec.lower() != primary.lower():
            accounts.append({"address": sec, "password": sec_pw})
        return accounts

    def _resolve_outbound_identity(self, to_addr: str) -> tuple[str, str]:
        """Return (From address, SMTP password) for replies — mailbox that received the thread."""
        ctx = self._thread_context.get(to_addr, {})
        key = (ctx.get("reply_from") or self._address).lower()
        password = self._smtp_by_address.get(key, self._password)
        from_addr = next(
            (a["address"] for a in self._accounts if a["address"].lower() == key),
            self._address,
        )
        return from_addr, password

    def _bitrix_precheck(self, sender_email: str) -> Dict[str, Any]:
        """Best-effort CRM precheck by sender email."""
        out: Dict[str, Any] = {
            "crm_hit": False,
            "crm_contact_id": "",
            "crm_company_id": "",
            "crm_open_lead_id": "",
            "crm_open_lead_exists": False,
            "crm_open_deal_id": "",
            "crm_open_deal_exists": False,
        }
        if not self._crm or not sender_email:
            return out
        try:
            pre = self._crm.precheck_by_email(sender_email)
            out["crm_contact_id"] = pre.get("contact_id", "")
            out["crm_company_id"] = pre.get("company_id", "")
            out["crm_open_lead_id"] = pre.get("open_lead_id", "")
            out["crm_open_deal_id"] = pre.get("open_deal_id", "")
            out["crm_hit"] = bool(out["crm_contact_id"] or out["crm_company_id"])
            out["crm_open_lead_exists"] = bool(out["crm_open_lead_id"])
            out["crm_open_deal_exists"] = bool(out["crm_open_deal_id"])
        except Exception as exc:
            logger.debug("[Email] Bitrix precheck failed for %s: %s", sender_email, exc)
        return out

    async def _classify_with_llm(
        self,
        *,
        subject: str,
        trimmed_body: str,
        heuristic_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Return validated LLM classification or heuristic fallback."""
        if heuristic_result.get("reason") in SKIP_LLM_REASONS:
            return heuristic_result
        if not heuristic_result.get("needs_llm"):
            return heuristic_result

        api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
        if not api_key:
            out = dict(heuristic_result)
            out["reason"] = "llm_failed"
            return out

        model = (os.getenv("EMAIL_CLASSIFIER_MODEL") or "mimo-v2-flash").strip()
        prompt = (
            "Classify inbound B2B industrial email into exactly one request_type.\n"
            "Allowed request_type: import, logistics, accounting, status, other.\n"
            "Return STRICT JSON only: {\"request_type\": \"...\", \"confidence\": 0..1}\n"
            "No markdown, no comments.\n\n"
            f"Subject: {subject or ''}\n"
            f"Body:\n{trimmed_body or ''}\n"
        )

        def _call() -> Dict[str, Any]:
            payload = {
                "model": model,
                "messages": [
                    {"role": "user", "content": prompt},
                ],
            }
            base_url = os.getenv("OPENAI_BASE_URL", "https://api.xiaomimimo.com/v1")
            logger.debug("[Classifier] LLM call → %s model=%s", base_url, model)
            req = urllib.request.Request(
                f"{base_url}/chat/completions",
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=2.0) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="replace"))
            content = (
                (((data or {}).get("choices") or [{}])[0].get("message") or {}).get("content") or "{}"
            )
            return json.loads(content)

        try:
            raw = await asyncio.wait_for(asyncio.to_thread(_call), timeout=2.0)
            validated = normalize_llm_result(raw)
            if validated:
                return validated
        except Exception as e:
            logger.debug("[Classifier] LLM failed: %s", e)

        out = dict(heuristic_result)
        out["reason"] = "llm_failed"
        return out

    async def connect(self) -> bool:
        """Connect to the IMAP server and start polling for new messages."""
        try:
            migration_pending = not self._registry.is_migration_complete()
            for acc in self._accounts:
                addr_key = acc["address"].lower()
                imap = imaplib.IMAP4_SSL(self._imap_host, self._imap_port, timeout=30)
                imap.login(acc["address"], acc["password"])
                imap.select("INBOX")
                if migration_pending:
                    status, data = imap.uid("search", None, "ALL")
                    n_seed = 0
                    if status == "OK" and data and data[0]:
                        n_seed = self._registry.seed_pre_existing_uids(
                            addr_key, data[0].split()
                        )
                    logger.info(
                        "[Email] IMAP OK for %s. One-time migration: %d pre-existing UIDs recorded.",
                        acc["address"],
                        n_seed,
                    )
                else:
                    logger.info("[Email] IMAP connection test passed for %s.", acc["address"])
                imap.logout()
            if migration_pending:
                self._registry.set_migration_complete()
                logger.info("[Email] One-time UID migration completed; registry is authoritative.")
        except Exception as e:
            logger.error("[Email] IMAP connection failed: %s", e)
            return False

        try:
            for acc in self._accounts:
                smtp = smtplib.SMTP(self._smtp_host, self._smtp_port, timeout=30)
                smtp.starttls(context=ssl.create_default_context())
                smtp.login(acc["address"], acc["password"])
                smtp.quit()
                logger.info("[Email] SMTP connection test passed for %s.", acc["address"])
        except Exception as e:
            logger.error("[Email] SMTP connection failed: %s", e)
            return False

        self._running = True
        self._poll_task = asyncio.create_task(self._poll_loop())
        if len(self._accounts) > 1:
            print(f"[Email] Connected as {self._address} + {self._accounts[1]['address']}")
        else:
            print(f"[Email] Connected as {self._address}")
        return True

    async def disconnect(self) -> None:
        """Stop polling and disconnect."""
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        logger.info("[Email] Disconnected.")

    async def _poll_loop(self) -> None:
        """Poll IMAP for new messages at regular intervals."""
        while self._running:
            try:
                await self._check_inbox()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("[Email] Poll error: %s", e)
            await asyncio.sleep(self._poll_interval)

    async def _check_inbox(self) -> None:
        """Check INBOX for unseen messages and dispatch them."""
        loop = asyncio.get_running_loop()
        messages = await loop.run_in_executor(None, self._fetch_new_messages)
        for msg_data in messages:
            mode = msg_data.get("dispatch_mode", "new")
            dedup_key = msg_data["dedup_key"]
            try:
                if mode == "finalize_only":
                    await loop.run_in_executor(None, self._finalize_dispatched, msg_data)
                elif mode == "retry_agent":
                    try:
                        should_finalize = await self._dispatch_message(msg_data)
                    except Exception:
                        await loop.run_in_executor(
                            None, self._registry.delete_processing_claim, dedup_key
                        )
                        raise
                    if not should_finalize:
                        await loop.run_in_executor(
                            None, self._registry.delete_processing_claim, dedup_key
                        )
                        continue
                    await loop.run_in_executor(
                        None, self._registry.set_agent_completed, dedup_key
                    )
                    await loop.run_in_executor(None, self._finalize_dispatched, msg_data)
                else:
                    claimed = await loop.run_in_executor(
                        None, self._claim_processing_row, msg_data
                    )
                    if not claimed:
                        continue
                    try:
                        should_finalize = await self._dispatch_message(msg_data)
                    except Exception:
                        await loop.run_in_executor(
                            None, self._registry.delete_processing_claim, dedup_key
                        )
                        raise
                    if not should_finalize:
                        await loop.run_in_executor(
                            None, self._registry.delete_processing_claim, dedup_key
                        )
                        continue
                    await loop.run_in_executor(
                        None, self._registry.set_agent_completed, dedup_key
                    )
                    await loop.run_in_executor(None, self._finalize_dispatched, msg_data)
            except Exception:
                logger.exception(
                    "[Email] Dispatch or finalize failed (dedup_key=%s, mode=%s)",
                    dedup_key,
                    mode,
                )

    def _claim_processing_row(self, msg_data: Dict[str, Any]) -> bool:
        """INSERT processing claim; False if another worker already claimed (race)."""
        return self._registry.claim_processing(
            dedup_key=msg_data["dedup_key"],
            mailbox=msg_data["receive_address"],
            message_id=msg_data.get("message_id_raw") or None,
            imap_uid=msg_data["imap_uid_str"],
            sender=msg_data.get("sender_addr"),
            subject=msg_data.get("subject"),
        )

    def _finalize_dispatched(self, msg_data: Dict[str, Any]) -> None:
        """UPDATE to dispatched and set \\Seen on the server (requires agent_completed)."""
        ok = self._registry.upgrade_to_dispatched(
            dedup_key=msg_data["dedup_key"],
            mailbox=msg_data["receive_address"],
            message_id=msg_data.get("message_id_raw") or None,
            imap_uid=msg_data["imap_uid_str"],
            sender=msg_data.get("sender_addr"),
            subject=msg_data.get("subject"),
        )
        if not ok:
            logger.warning(
                "[Email] upgrade_to_dispatched skipped (dedup_key=%s) — not processing+agent_completed?",
                msg_data.get("dedup_key"),
            )
            return
        self._imap_store_seen_account(msg_data)

    def _imap_store_seen_account(self, msg_data: Dict[str, Any]) -> None:
        """Second IMAP connection to set \\Seen after async dispatch."""
        acc = msg_data["account"]
        uid = msg_data["uid"]
        try:
            imap = imaplib.IMAP4_SSL(self._imap_host, self._imap_port, timeout=30)
            imap.login(acc["address"], acc["password"])
            imap.select("INBOX")
            _imap_uid_store_seen(imap, uid)
            imap.logout()
        except Exception as e:
            logger.warning("[Email] Could not set \\Seen after dispatch (uid=%s): %s", uid, e)

    def _mark_email_unread(self, uid: Any, mailbox: str) -> None:
        """Remove \\Seen for a specific message in mailbox INBOX."""
        mailbox_key = (mailbox or "").strip().lower()
        acc = next((a for a in self._accounts if a["address"].lower() == mailbox_key), None)
        if not acc:
            logger.warning("[Email] Cannot mark unread: mailbox account not found (%s)", mailbox)
            return
        uid_str = uid.decode() if isinstance(uid, bytes) else str(uid)
        try:
            imap = imaplib.IMAP4_SSL(self._imap_host, self._imap_port, timeout=30)
            imap.login(acc["address"], acc["password"])
            imap.select("INBOX")
            imap.uid("store", uid_str, "-FLAGS", "(\\Seen)")
            imap.logout()
            logger.info("[Email] Marked message unread: mailbox=%s uid=%s", mailbox_key, uid_str)
        except Exception as exc:
            logger.warning("[Email] Failed to mark message unread: mailbox=%s uid=%s err=%s", mailbox_key, uid_str, exc)

    def _fetch_new_messages(self) -> List[Dict[str, Any]]:
        """Fetch UNSEEN via BODY.PEEK[]; registry decides processing. Runs in executor thread."""
        results: List[Dict[str, Any]] = []
        for acc in self._accounts:
            addr_key = acc["address"].lower()
            poll_cycle: set = set()
            try:
                imap = imaplib.IMAP4_SSL(self._imap_host, self._imap_port, timeout=30)
                try:
                    imap.login(acc["address"], acc["password"])
                    imap.select("INBOX")

                    status, data = imap.uid("search", None, "UNSEEN")
                    if status != "OK" or not data or not data[0]:
                        continue

                    for uid in data[0].split():
                        cycle_key = (addr_key, uid)
                        if cycle_key in poll_cycle:
                            continue
                        poll_cycle.add(cycle_key)

                        if self._registry.has(_migrate_uid_key(addr_key, uid)):
                            continue

                        status, raw_fetch = imap.uid("fetch", uid, "(BODY.PEEK[])")
                        if status != "OK":
                            continue
                        raw_email = _extract_peek_body_bytes(raw_fetch)
                        if not raw_email:
                            logger.warning("[Email] Empty PEEK body for uid %s", uid)
                            continue

                        msg = email_lib.message_from_bytes(raw_email)

                        sender_raw = msg.get("From", "")
                        sender_addr = _extract_email_address(sender_raw)
                        sender_name = _decode_header_value(sender_raw)
                        if "<" in sender_name:
                            sender_name = sender_name.split("<")[0].strip().strip('"')

                        subject = _decode_header_value(msg.get("Subject", "(no subject)"))
                        message_id_raw = msg.get("Message-ID", "") or ""
                        in_reply_to = msg.get("In-Reply-To", "")
                        msg_headers = dict(msg.items())

                        dedup_key = _compute_dedup_key(
                            addr_key, uid, message_id_raw, sender_addr, subject
                        )

                        row = self._registry.get_row(dedup_key)
                        if row is not None:
                            if row["outcome"] == "processing":
                                dispatch_mode = (
                                    "finalize_only"
                                    if row["agent_completed"]
                                    else "retry_agent"
                                )
                            else:
                                _imap_uid_store_seen(imap, uid)
                                continue
                        else:
                            dispatch_mode = "new"

                        if dispatch_mode == "new" and _is_automated_sender(sender_addr, msg_headers):
                            logger.debug("[Email] Skipping automated sender: %s", sender_addr)
                            self._registry.mark(
                                dedup_key=dedup_key,
                                mailbox=addr_key,
                                message_id=message_id_raw or None,
                                imap_uid=uid.decode() if isinstance(uid, bytes) else str(uid),
                                sender=sender_addr,
                                subject=subject,
                                outcome="skipped_automated",
                            )
                            _imap_uid_store_seen(imap, uid)
                            continue

                        recv = addr_key
                        sender_norm = (sender_addr or "").strip().lower()
                        if dispatch_mode == "new" and sender_norm in self._our_addresses:
                            logger.info(
                                "[Email] Skipping mail from our own address (%s → inbox %s): %s",
                                sender_addr,
                                recv,
                                subject[:120] if subject else "",
                            )
                            self._registry.mark(
                                dedup_key=dedup_key,
                                mailbox=addr_key,
                                message_id=message_id_raw or None,
                                imap_uid=uid.decode() if isinstance(uid, bytes) else str(uid),
                                sender=sender_addr,
                                subject=subject,
                                outcome="skipped_self",
                            )
                            _imap_uid_store_seen(imap, uid)
                            continue

                        body = _extract_text_body(msg)
                        attachments = _extract_attachments(
                            msg, skip_attachments=self._skip_attachments
                        )

                        results.append({
                            "uid": uid,
                            "account": acc,
                            "receive_address": addr_key,
                            "dedup_key": dedup_key,
                            "imap_uid_str": uid.decode() if isinstance(uid, bytes) else str(uid),
                            "message_id_raw": message_id_raw,
                            "sender_addr": sender_addr,
                            "sender_name": sender_name,
                            "subject": subject,
                            "message_id": message_id_raw,
                            "in_reply_to": in_reply_to,
                            "body": body,
                            "attachments": attachments,
                            "date": msg.get("Date", ""),
                            "dispatch_mode": dispatch_mode,
                        })
                finally:
                    try:
                        imap.logout()
                    except Exception:
                        pass
            except Exception as e:
                logger.error("[Email] IMAP fetch error for %s: %s", acc["address"], e)
        return results

    async def _dispatch_message(self, msg_data: Dict[str, Any]) -> bool:
        """Convert a fetched email into a MessageEvent and dispatch it."""
        sender_addr = msg_data["sender_addr"]
        recv = msg_data.get("receive_address") or self._address.lower()

        # Defense in depth (fetch path already filters self / automated)
        if (sender_addr or "").strip().lower() in self._our_addresses:
            return False
        if _is_automated_sender(sender_addr, {}):
            logger.debug("[Email] Dropping automated sender at dispatch: %s", sender_addr)
            return False

        subject = msg_data["subject"]
        body = msg_data["body"].strip()
        attachments = msg_data["attachments"]
        in_reply_to = msg_data.get("in_reply_to") or ""

        is_follow_up = _looks_like_follow_up(subject, body, in_reply_to)
        crm_ctx = self._bitrix_precheck(sender_addr)

        heuristic = classify_email_heuristic(
            subject=subject,
            body=body,
            follow_up=is_follow_up,
            crm_hit=bool(crm_ctx.get("crm_hit")),
            crm_open_deal_exists=bool(crm_ctx.get("crm_open_deal_exists")),
        )
        classification = await self._classify_with_llm(
            subject=subject,
            trimmed_body=heuristic.get("trimmed_text", ""),
            heuristic_result=heuristic,
        )
        self._registry.set_classification(
            dedup_key=msg_data["dedup_key"],
            request_type=str(classification.get("request_type", "other")),
            confidence=float(classification.get("confidence", 0.0)),
            source=str(classification.get("source", "heuristic")),
            reason=str(classification.get("reason", "heuristic_strong")),
        )

        if (not crm_ctx.get("crm_open_lead_exists")) and crm_ctx.get("crm_open_deal_exists"):
            open_deal_id = str(crm_ctx.get("crm_open_deal_id") or "")
            self._mark_email_unread(msg_data.get("uid"), recv)
            max_token = (os.getenv("MAX_BOT_TOKEN") or "").strip()
            max_chat_id = (os.getenv("MAX_NOTIFY_CHAT_ID") or os.getenv("MAX_NOTIFY_CHAT") or "").strip()
            if max_token and max_chat_id:
                max_text = (
                    f"📨 Письмо от {sender_addr} — есть открытая сделка #{open_deal_id}. "
                    "Письмо помечено непрочитанным."
                )
                await asyncio.to_thread(
                    send_max_text_sync,
                    max_text,
                    chat_id=max_chat_id,
                    token=max_token,
                )
            logger.info(
                "[Email] Routing stop for %s: open deal exists #%s, message left unread",
                sender_addr,
                open_deal_id or "unknown",
            )
            return False

        # Build message text: include subject as context
        text = body
        if subject and not subject.startswith("Re:"):
            text = f"[Subject: {subject}]\n\n{body}"
        text = (
            f"[EMAIL_CONTEXT follow_up={'true' if is_follow_up else 'false'} "
            f"crm_hit={'true' if crm_ctx.get('crm_hit') else 'false'} "
            f"crm_contact_id={crm_ctx.get('crm_contact_id') or 'none'} "
            f"crm_company_id={crm_ctx.get('crm_company_id') or 'none'} "
            f"crm_open_lead_id={crm_ctx.get('crm_open_lead_id') or 'none'} "
            f"crm_open_lead_exists={'true' if crm_ctx.get('crm_open_lead_exists') else 'false'} "
            f"crm_open_deal_id={crm_ctx.get('crm_open_deal_id') or 'none'} "
            f"crm_open_deal_exists={'true' if crm_ctx.get('crm_open_deal_exists') else 'false'} "
            f"classification_type={classification.get('request_type') or 'other'} "
            f"classification_confidence={classification.get('confidence', 0.0)} "
            f"classification_source={classification.get('source') or 'heuristic'} "
            f"classification_reason={classification.get('reason') or 'heuristic_strong'}]\n{text}"
        )

        # Determine message type and media
        media_urls = []
        media_types = []
        msg_type = MessageType.TEXT

        for att in attachments:
            media_urls.append(att["path"])
            media_types.append(att["media_type"])
            if att["type"] == "image":
                msg_type = MessageType.PHOTO

        # Store thread context for reply threading (reply_from = receiving mailbox)
        self._thread_context[sender_addr] = {
            "subject": subject,
            "message_id": msg_data["message_id"],
            "reply_from": recv,
            "inbound_text": body,
            "crm_contact_id": crm_ctx.get("crm_contact_id") or "",
            "crm_company_id": crm_ctx.get("crm_company_id") or "",
            "crm_open_lead_id": crm_ctx.get("crm_open_lead_id") or "",
            "crm_open_lead_exists": bool(crm_ctx.get("crm_open_lead_exists")),
            "crm_open_deal_id": crm_ctx.get("crm_open_deal_id") or "",
            "crm_open_deal_exists": bool(crm_ctx.get("crm_open_deal_exists")),
            "classification_request_type": classification.get("request_type") or "other",
            "classification_confidence": float(classification.get("confidence", 0.0)),
            "classification_source": classification.get("source") or "heuristic",
            "classification_reason": classification.get("reason") or "heuristic_strong",
        }

        source = self.build_source(
            chat_id=sender_addr,
            chat_name=msg_data["sender_name"] or sender_addr,
            chat_type="dm",
            user_id=sender_addr,
            user_name=msg_data["sender_name"] or sender_addr,
        )

        event = MessageEvent(
            text=text or "(empty email)",
            message_type=msg_type,
            source=source,
            message_id=msg_data["message_id"],
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=msg_data["in_reply_to"] or None,
            email_subject=subject,
            email_receive_address=recv,
        )

        logger.info("[Email] New message from %s: %s", sender_addr, subject)
        await self.handle_message(event)
        return True

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send an email reply to the given address."""
        try:
            logger.warning(
                "[Email] send() called, approval_required=%s",
                "true" if self._approval_required() else "false",
            )
            if _is_internal_only_output(content):
                logger.warning("[Email] Suppressed internal-only output to %s", chat_id)
                return SendResult(success=True, message_id="internal-suppressed")
            content = _sanitize_outbound_email_body(content)
            if _is_internal_only_output(content):
                logger.warning("[Email] Suppressed internal-only output after sanitize to %s", chat_id)
                return SendResult(success=True, message_id="internal-suppressed")
            turn_key = str((metadata or {}).get("delivery_turn_key") or "").strip()
            logger.debug(
                "[Email] send() metadata delivery_turn_key=%s chat_id=%s",
                turn_key if turn_key else "None",
                chat_id,
            )
            if turn_key and self._is_turn_already_sent(turn_key):
                logger.warning("[Email] Suppressed duplicate send for turn_key=%s to %s", turn_key, chat_id)
                return SendResult(success=True, message_id="duplicate-suppressed")
            if self._approval_required():
                queued = await self._enqueue_for_approval(to_addr=chat_id, content=content, metadata=metadata)
                if queued.success and turn_key:
                    self._mark_turn_sent(turn_key)
                return queued
            loop = asyncio.get_running_loop()
            message_id = await loop.run_in_executor(
                None, self._send_email, chat_id, content, reply_to
            )
            if turn_key:
                self._mark_turn_sent(turn_key)
            return SendResult(success=True, message_id=message_id)
        except Exception as e:
            logger.error("[Email] Send failed to %s: %s", chat_id, e)
            return SendResult(success=False, error=str(e))

    def _is_turn_already_sent(self, turn_key: str) -> bool:
        now = time.monotonic()
        self._cleanup_sent_turn_keys(now)
        return turn_key in self._sent_turn_keys

    def _mark_turn_sent(self, turn_key: str) -> None:
        now = time.monotonic()
        self._sent_turn_keys[turn_key] = now
        self._cleanup_sent_turn_keys(now)

    def _cleanup_sent_turn_keys(self, now: Optional[float] = None) -> None:
        now = time.monotonic() if now is None else now
        cutoff = now - self._sent_turn_ttl_sec
        stale = [k for k, ts in self._sent_turn_keys.items() if ts < cutoff]
        for k in stale:
            self._sent_turn_keys.pop(k, None)

    def _smtp_send_with_telemetry(
        self,
        *,
        from_addr: str,
        smtp_password: str,
        msg: MIMEMultipart,
    ) -> None:
        smtp = smtplib.SMTP(self._smtp_host, self._smtp_port, timeout=30)
        try:
            code, resp = smtp.ehlo()
            logger.info("[Email][SMTP] EHLO pre-TLS for %s: code=%s resp=%r", from_addr, code, resp)
            code, resp = smtp.starttls(context=ssl.create_default_context())
            logger.info("[Email][SMTP] STARTTLS for %s: code=%s resp=%r", from_addr, code, resp)
            code, resp = smtp.ehlo()
            logger.info("[Email][SMTP] EHLO post-TLS for %s: code=%s resp=%r", from_addr, code, resp)
            code, resp = smtp.login(from_addr, smtp_password)
            logger.info("[Email][SMTP] LOGIN %s: code=%s resp=%r", from_addr, code, resp)
            refused = smtp.send_message(msg)
            if refused:
                logger.warning("[Email][SMTP] Recipients refused from %s: %r", from_addr, refused)
                raise smtplib.SMTPRecipientsRefused(refused)
        except smtplib.SMTPException as e:
            logger.error(
                "[Email][SMTP] Send failed from %s: %s smtp_code=%s smtp_error=%r",
                from_addr,
                type(e).__name__,
                getattr(e, "smtp_code", None),
                getattr(e, "smtp_error", None),
            )
            raise
        finally:
            try:
                smtp.quit()
            except Exception:
                smtp.close()

    def _append_to_sent_if_enabled(
        self,
        *,
        from_addr: str,
        smtp_password: str,
        msg: MIMEMultipart,
    ) -> None:
        if os.getenv("EMAIL_APPEND_TO_SENT", "false").strip().lower() not in {"1", "true", "yes", "on"}:
            return
        imap = imaplib.IMAP4_SSL(self._imap_host, self._imap_port, timeout=30)
        try:
            imap.login(from_addr, smtp_password)
            status, boxes = imap.list()
            box_dump = "\n".join(
                b.decode(errors="replace") if isinstance(b, bytes) else str(b)
                for b in (boxes or [])
            )
            target = "Sent"
            if "Отправ" in box_dump:
                target = "Отправленные"
            elif "Sent" in box_dump:
                target = "Sent"
            typ, data = imap.append(
                target,
                "\\Seen",
                imaplib.Time2Internaldate(time.time()),
                msg.as_bytes(),
            )
            if typ != "OK":
                logger.warning("[Email][IMAP] APPEND to %s failed for %s: %s %r", target, from_addr, typ, data)
            else:
                logger.info("[Email][IMAP] APPEND to %s succeeded for %s", target, from_addr)
        except Exception as e:
            logger.warning("[Email][IMAP] APPEND failed for %s: %s", from_addr, e)
        finally:
            try:
                imap.logout()
            except Exception:
                pass

    def _send_email(
        self,
        to_addr: str,
        body: str,
        reply_to_msg_id: Optional[str] = None,
    ) -> str:
        """Send an email via SMTP. Runs in executor thread."""
        from_addr, smtp_password = self._resolve_outbound_identity(to_addr)
        msg = MIMEMultipart()
        msg["From"] = from_addr
        msg["To"] = to_addr

        ctx = self._thread_context.get(to_addr, {})
        subject = ctx.get("subject", "Hermes Agent")
        if not subject.startswith("Re:"):
            subject = f"Re: {subject}"
        msg["Subject"] = subject

        original_msg_id = reply_to_msg_id or ctx.get("message_id")
        if original_msg_id:
            msg["In-Reply-To"] = original_msg_id
            msg["References"] = original_msg_id

        msg_id = f"<hermes-{uuid.uuid4().hex[:12]}@{from_addr.split('@')[1]}>"
        msg["Message-ID"] = msg_id
        msg["Date"] = email_lib.utils.format_datetime(datetime.now().astimezone())

        body = _append_signature_if_missing(body)
        msg.attach(MIMEText(body, "plain", "utf-8"))

        self._smtp_send_with_telemetry(from_addr=from_addr, smtp_password=smtp_password, msg=msg)
        self._append_to_sent_if_enabled(from_addr=from_addr, smtp_password=smtp_password, msg=msg)

        logger.info(
            "[Email] Sent reply to %s from %s (subject: %s, message_id: %s)",
            to_addr,
            from_addr,
            subject,
            msg_id,
        )
        return msg_id

    async def send_approved_draft(
        self,
        *,
        to_addr: str,
        subject: str,
        in_reply_to: str,
        reply_from_mailbox: str,
        draft_text: str,
    ) -> SendResult:
        """Send previously approved draft through the existing SMTP path."""
        prev_ctx = self._thread_context.get(to_addr, {}).copy()
        self._thread_context[to_addr] = {
            **prev_ctx,
            "subject": subject or prev_ctx.get("subject", "Hermes Agent"),
            "message_id": in_reply_to or prev_ctx.get("message_id", ""),
            "reply_from": reply_from_mailbox or prev_ctx.get("reply_from", ""),
        }
        try:
            loop = asyncio.get_running_loop()
            message_id = await loop.run_in_executor(
                None, self._send_email, to_addr, draft_text, in_reply_to or None
            )
            return SendResult(success=True, message_id=message_id)
        except Exception as e:
            logger.error("[Email] Approved draft send failed to %s: %s", to_addr, e)
            return SendResult(success=False, error=str(e))

    async def send_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Email has no typing indicator — no-op."""

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send an image URL as part of an email body."""
        text = caption or ""
        text += f"\n\nImage: {image_url}"
        return await self.send(chat_id, text.strip(), reply_to, metadata=metadata)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a file as an email attachment."""
        try:
            turn_key = str((metadata or {}).get("delivery_turn_key") or "").strip()
            if turn_key and self._is_turn_already_sent(turn_key):
                logger.warning("[Email] Suppressed duplicate attachment send for turn_key=%s to %s", turn_key, chat_id)
                return SendResult(success=True, message_id="duplicate-suppressed")
            if self._approval_required():
                attachment_name = file_name or Path(file_path).name
                draft_text = (
                    (caption or "").strip() + f"\n\n[Вложение: {attachment_name}]"
                ).strip()
                queued = await self._enqueue_for_approval(
                    to_addr=chat_id,
                    content=draft_text,
                    metadata=metadata,
                )
                if queued.success and turn_key:
                    self._mark_turn_sent(turn_key)
                return queued
            loop = asyncio.get_running_loop()
            message_id = await loop.run_in_executor(
                None,
                self._send_email_with_attachment,
                chat_id,
                caption or "",
                file_path,
                file_name,
            )
            if turn_key:
                self._mark_turn_sent(turn_key)
            return SendResult(success=True, message_id=message_id)
        except Exception as e:
            logger.error("[Email] Send document failed: %s", e)
            return SendResult(success=False, error=str(e))

    def _send_email_with_attachment(
        self,
        to_addr: str,
        body: str,
        file_path: str,
        file_name: Optional[str] = None,
    ) -> str:
        """Send an email with a file attachment via SMTP."""
        from_addr, smtp_password = self._resolve_outbound_identity(to_addr)
        msg = MIMEMultipart()
        msg["From"] = from_addr
        msg["To"] = to_addr

        ctx = self._thread_context.get(to_addr, {})
        subject = ctx.get("subject", "Hermes Agent")
        if not subject.startswith("Re:"):
            subject = f"Re: {subject}"
        msg["Subject"] = subject

        original_msg_id = ctx.get("message_id")
        if original_msg_id:
            msg["In-Reply-To"] = original_msg_id
            msg["References"] = original_msg_id

        msg_id = f"<hermes-{uuid.uuid4().hex[:12]}@{from_addr.split('@')[1]}>"
        msg["Message-ID"] = msg_id
        msg["Date"] = email_lib.utils.format_datetime(datetime.now().astimezone())

        body = _append_signature_if_missing(body)
        if body:
            msg.attach(MIMEText(body, "plain", "utf-8"))

        p = Path(file_path)
        fname = file_name or p.name
        with open(p, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={fname}")
            msg.attach(part)

        self._smtp_send_with_telemetry(from_addr=from_addr, smtp_password=smtp_password, msg=msg)
        self._append_to_sent_if_enabled(from_addr=from_addr, smtp_password=smtp_password, msg=msg)

        return msg_id

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return basic info about the email chat."""
        ctx = self._thread_context.get(chat_id, {})
        return {
            "name": chat_id,
            "type": "dm",
            "chat_id": chat_id,
            "subject": ctx.get("subject", ""),
        }
