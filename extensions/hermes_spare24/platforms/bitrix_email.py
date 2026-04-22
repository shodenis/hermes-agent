"""Bitrix CRM activity-based email adapter.

This adapter replaces IMAP polling by reading incoming email activities from
Bitrix24 CRM and sending replies via ``crm.activity.add``.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import importlib.util
import json
import logging
import os
import re
import sqlite3
import time
import urllib.request
import uuid
from datetime import datetime, timezone
from email.utils import parseaddr
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from gateway.bitrix_activity_ingress.downstream_idempotency import (
    BitrixDownstreamEffect,
    DownstreamIdempotencyStore,
)
from gateway.bitrix_activity_ingress.layer_b_orchestration import (
    poll_skip_if_already_committed_layer_b,
)
from gateway.bitrix_activity_ingress.snapshot_builder import build_activity_snapshot
from gateway.config import Platform, PlatformConfig
from gateway.email_approval_store import EmailApprovalStore
from gateway.email_quality_store import EmailQualityStore
from extensions.hermes_spare24.email.context import format_email_context_line
from extensions.hermes_spare24.email.inbound_classification import classify_inbound_email
from extensions.hermes_spare24.model_lockdown import (
    EXPECTED_BASE_URL,
    EXPECTED_MODEL,
    EXPECTED_PROVIDER,
    assert_effective_model_or_raise,
    assert_lockdown_config_or_raise,
    enforce_process_env_lockdown,
)
from plugins.memory.gbrain import autosave_email_context
from extensions.hermes_spare24.crm.bitrix.service import BitrixService
from extensions.hermes_spare24.crm.bitrix.adapters.bitrix_service_adapter import BitrixServiceAdapter
from extensions.hermes_spare24.crm.bitrix.use_cases import CRMUseCases
from gateway.platforms.email_processed_store import EmailProcessedStore
from extensions.hermes_spare24.crm.bitrix.client import Bitrix24Client
from extensions.hermes_spare24.crm.bitrix.errors import BitrixAPIError
from extensions.hermes_spare24.notifications.max_notify import send_max_text_sync
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_document_from_bytes,
    cache_image_from_bytes,
)
from hermes_cli.config import get_hermes_home
from hermes_state import SessionDB
from extensions.hermes_spare24.email.client_reply_validation import validate_client_email_segment
from extensions.hermes_spare24.email.interaction_log_store import (
    EmailInteractionLogStore,
)
from extensions.hermes_spare24.email.send_dedup_store import EmailSendDedupStore
from extensions.hermes_spare24.email.thread_state_machine import (
    ThreadState,
    ThreadStateStore,
    derive_thread_key,
    guard_template_for_state,
)
from extensions.hermes_spare24.email.transport_helpers import (
    approval_required,
    append_signature_if_missing,
    build_safe_fallback_reply,
    is_internal_only_output,
    looks_like_follow_up,
    remove_hermes_markers,
    sanitize_outbound_email_body,
    validate_client_facing_reply,
)

# profiles/bitrix/classifier.py — same INTERNAL_EMAILS + internal_email_extra as webhook classifier.
_classifier_mod_path = get_hermes_home() / "classifier.py"
if _classifier_mod_path.is_file():
    _spec = importlib.util.spec_from_file_location(
        "profiles.bitrix.classifier",
        _classifier_mod_path,
    )
    if _spec and _spec.loader:
        _bitrix_classifier_mod = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_bitrix_classifier_mod)
        _internal_email_set = _bitrix_classifier_mod._internal_email_set
    else:
        def _internal_email_set():  # type: ignore[misc]
            return set()

else:

    def _internal_email_set():  # type: ignore[misc]
        return set()

logger = logging.getLogger(__name__)

_POLL_INTERVAL_SECONDS = 60
_OVERLAP_SECONDS = 300.0
_STALE_PROCESSING_SECONDS = 600.0
_STALE_PROCESSING_HARD_CLEANUP_SECONDS = 86400.0
_PROCESSED_RETENTION_SECONDS = 2592000.0
_INN_REQUEST_TTL_SECONDS = 604800.0
_MAX_ATTACHMENT_SIZE = 10 * 1024 * 1024
_EMAIL_ACTIVITY_TYPE_ID = "4"
# Read receipts, DSN/MDN, OOF — substring match on lowercased subject; keep specific phrases to limit false positives.
_SYSTEM_EMAIL_SUBJECT_MARKERS = (
    "out of office",
    "auto-reply",
    "автоответ",
    "delivery failed",
    "mailer-daemon",
    "undeliverable",
    "недоставлено",
    "вне офиса",
    "прочитано",
    "read receipt",
    "delivery receipt",
    "delivery notification",
    "уведомление о доставке",
    "mail delivery",
    "undelivered mail",
    "delivery status notification",
    "returned mail",
    "failure notice",
    "message delayed",
)
# Local-part@ prefix checks on normalized lower address (includes noreply@* e.g. noreply@mail.ru).
_SYSTEM_EMAIL_SENDER_PREFIXES = (
    "no-reply@",
    "noreply@",
    "mailer-daemon@",
    "postmaster@",
)
_IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp")


def check_bitrix_email_requirements() -> bool:
    """Check whether the Bitrix email adapter can run."""
    return bool((os.getenv("BITRIX_WEBHOOK_URL") or "").strip())


class BitrixEmailAdapter(BasePlatformAdapter):
    """Email adapter backed by Bitrix CRM activities."""

    def __init__(
        self,
        config: PlatformConfig,
        *,
        downstream_idempotency_store: Optional[DownstreamIdempotencyStore] = None,
        thread_state_store: Optional[ThreadStateStore] = None,
        interaction_log_store: Optional[EmailInteractionLogStore] = None,
        send_dedup_store: Optional[EmailSendDedupStore] = None,
    ):
        super().__init__(config, Platform.BITRIX_EMAIL)
        enforce_process_env_lockdown()
        assert_lockdown_config_or_raise()
        webhook_url = (os.getenv("BITRIX_WEBHOOK_URL") or "").strip().rstrip("/")
        self._webhook_url = webhook_url
        self._poll_interval = _POLL_INTERVAL_SECONDS
        self._poll_task: Optional[asyncio.Task] = None
        self._poll_lock = asyncio.Lock()
        self._db_path = get_hermes_home() / "bitrix_email.db"
        self._conn: Optional[sqlite3.Connection] = None
        # Use the existing client implementation with built-in retry/backoff/rate-limit.
        self._client = Bitrix24Client(
            webhook_url,
            timeout=30,
            max_attempts=5,
            rate_per_sec=2.0,
            burst=2.0,
        )
        self._approval_store = EmailApprovalStore(get_hermes_home() / "email_approval.db")
        self._quality_store = EmailQualityStore(get_hermes_home() / "email_quality.db")
        self._downstream_idem = (
            downstream_idempotency_store or DownstreamIdempotencyStore.from_default_path()
        )
        self._thread_state_store = thread_state_store or ThreadStateStore.from_default_path()
        self._interaction_log_store = interaction_log_store or EmailInteractionLogStore()
        self._send_dedup_store = send_dedup_store or EmailSendDedupStore()
        self._sent_turn_keys: Dict[str, float] = {}
        self._sent_turn_ttl_sec = 1800.0
        self._crm: Optional[CRMUseCases] = None
        bitrix_svc = BitrixService.from_env()
        if bitrix_svc is not None:
            bitrix_user_id = (os.getenv("BITRIX_USER_ID") or "").strip() or "1"
            self._crm = CRMUseCases(
                actions=BitrixServiceAdapter(bitrix_svc),
                responsible_id=bitrix_user_id,
            )

    @staticmethod
    def _thread_state_machine_enabled() -> bool:
        v = (os.getenv("BITRIX_EMAIL_THREAD_STATE") or "1").strip().lower()
        return v not in ("0", "false", "no", "off")

    def _apply_thread_state_after_successful_outbound(
        self,
        metadata: Optional[Dict[str, Any]],
        activity_id: str,
    ) -> None:
        if not BitrixEmailAdapter._thread_state_machine_enabled():
            return
        tk = str((metadata or {}).get("thread_key") or "").strip()
        tt = str((metadata or {}).get("_thread_state_final_template_type") or "").strip().upper()
        if not tk or tt not in ("T1", "T2", "T4"):
            return
        try:
            if tt == "T1":
                self._thread_state_store.apply_transition(
                    tk,
                    ThreadState.WAITING_FOR_CLIENT,
                    allowed_from={ThreadState.NEW, ThreadState.WAITING_FOR_CLIENT},
                    activity_id=str(activity_id),
                )
            elif tt == "T4":
                self._thread_state_store.apply_transition(
                    tk,
                    ThreadState.COMPLETED,
                    allowed_from={ThreadState.READY_TO_RESPOND, ThreadState.COMPLETED},
                    activity_id=str(activity_id),
                )
        except Exception:
            logger.exception(
                "[BitrixEmail] thread_state advance failed activity=%s template=%s",
                activity_id,
                tt,
            )

    @staticmethod
    def _inbound_snapshot_hash_from_metadata(metadata: Optional[Dict[str, Any]]) -> str:
        """Layer B snapshot hash propagated on inbound processing; empty if unknown (no ledger row)."""
        return str((metadata or {}).get("inbound_layer_b_snapshot_hash") or "").strip()

    @staticmethod
    def _should_skip_system_incoming(subject: str, sender_email: str) -> bool:
        """True for read receipts, DSNs, OOF, and other automated system mail."""
        subj = (subject or "").lower()
        if subj and any(marker in subj for marker in _SYSTEM_EMAIL_SUBJECT_MARKERS):
            return True
        addr = (sender_email or "").strip().lower()
        if not addr:
            return False
        if any(addr.startswith(prefix) for prefix in _SYSTEM_EMAIL_SENDER_PREFIXES):
            return True
        return False

    def _bitrix_precheck(self, sender_email: str) -> Dict[str, Any]:
        """Best-effort CRM precheck by sender email (same shape as IMAP EmailAdapter)."""
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
            logger.debug("[BitrixEmail] Bitrix precheck failed for %s: %s", sender_email, exc)
        return out

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

    @staticmethod
    def _client_email_validation_strict() -> bool:
        return os.getenv("EMAIL_CLIENT_VALIDATION_STRICT", "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }

    @staticmethod
    def _parse_template_marker_first_line(content: str) -> Tuple[str, str, bool]:
        """Strip optional first-line template marker; return (template_type, body, had_marker)."""
        lines = (content or "").splitlines()
        first_non_empty = next((l.strip() for l in lines if l.strip()), "")
        if not first_non_empty:
            return "T2", (content or "").strip(), False
        idx = next(i for i, line in enumerate(lines) if line.strip())
        m = re.match(r"<<<EMAIL_TEMPLATE:(T1|T2|T4)>>>\s*$", first_non_empty, re.IGNORECASE)
        if not m:
            return "T2", (content or "").strip(), False
        t = m.group(1).upper()
        rest = lines[:idx] + lines[idx + 1 :]
        return t, "\n".join(rest).strip(), True

    async def _prepare_client_plain_for_outbound(
        self,
        *,
        content: str,
        metadata: Optional[Dict[str, Any]],
        customer_name: str,
        subject: str,
        activity_id: str,
        sender_email: str,
    ) -> Tuple[str, Optional[SendResult], str, bool]:
        """Sanitize, template-validate, optional strict block; return (plain, early_error, reason, fallback_used)."""
        meta_tt_raw = str((metadata or {}).get("email_template_type") or "").strip().upper()
        marker_tt, body, had_marker = BitrixEmailAdapter._parse_template_marker_first_line(content)
        if meta_tt_raw in ("T1", "T2", "T4"):
            template_type = meta_tt_raw
            source = "metadata"
        else:
            template_type = marker_tt
            source = "marker" if had_marker else "default"
        marker_tt_resolved = marker_tt if had_marker else ""
        logger.info(
            "event=email_template_resolved "
            "source=%s template_type=%s metadata_tt=%s marker_tt=%s activity_id=%s",
            source,
            template_type,
            meta_tt_raw,
            marker_tt_resolved,
            activity_id,
        )
        logger.info(
            "event=email_metric name=email_template_source tags=source=%s activity_id=%s",
            source,
            activity_id,
        )
        if meta_tt_raw and marker_tt_resolved and meta_tt_raw != marker_tt_resolved:
            logger.info(
                "event=template_drift metadata_tt=%s marker_tt=%s resolved=%s activity_id=%s",
                meta_tt_raw,
                marker_tt_resolved,
                template_type,
                activity_id,
            )

        if BitrixEmailAdapter._thread_state_machine_enabled():
            thread_key_guard = str((metadata or {}).get("thread_key") or "").strip()
            if thread_key_guard:
                st = self._thread_state_store.load_state(thread_key_guard)
                intended_tt = template_type
                final_tt, decision_thr = guard_template_for_state(st, intended_tt)
                logger.info(
                    "event=thread_state_decision intended_template=%s final_template=%s "
                    "current_state=%s decision=%s activity_id=%s",
                    intended_tt,
                    final_tt,
                    st.value,
                    decision_thr,
                    activity_id,
                )
                template_type = final_tt

        if isinstance(metadata, dict):
            metadata["_thread_state_final_template_type"] = template_type

        # Client body before configured signature: sanitize only, then validate; append signature last.
        client_body = sanitize_outbound_email_body(body)
        client_body = remove_hermes_markers(client_body)
        tmpl_res, _seg = validate_client_email_segment(client_body, template_type)
        base_ok, base_reason = validate_client_facing_reply(client_body)
        strict = self._client_email_validation_strict()

        if tmpl_res.ok:
            logger.info(
                "event=email_metric name=email_validation_ok activity_id=%s",
                activity_id,
            )
        else:
            logger.info(
                "event=email_metric name=email_validation_fail activity_id=%s",
                activity_id,
            )

        def _should_log_failure_context() -> bool:
            raw = (os.getenv("EMAIL_VALIDATION_CONTEXT_SAMPLE_RATE") or "1.0").strip()
            try:
                rate = float(raw)
            except Exception:
                rate = 1.0
            if rate <= 0:
                return False
            if rate >= 1:
                return True
            key = f"{activity_id}|{template_type}|{source}"
            bucket = int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:8], 16) / float(
                0xFFFFFFFF
            )
            return bucket < rate

        def _log_validation_summary(*, outcome: str, reason: str = "") -> None:
            logger.info(
                "event=email_metric_summary activity_id=%s source=%s template_type=%s strict=%s outcome=%s reason=%s",
                activity_id,
                source,
                template_type,
                "true" if strict else "false",
                outcome,
                reason,
            )

        _outcome_emitted: Optional[str] = None

        # Outcome metric contract:
        # - Emit exactly ONE canonical outcome metric per execution of
        #   _prepare_client_plain_for_outbound.
        # - Allowed outcome values: ok | fallback | blocked.
        # - `primary=1` marks the canonical dashboard metric:
        #   event=email_metric name=email_validation_outcome tags=outcome=<...> primary=1
        # - email_validation_ok / email_validation_fail / email_validation_blocked
        #   are auxiliary diagnostics and must not be treated as the canonical outcome.
        def _emit_outcome_once(tag: str) -> None:
            """Single dashboard outcome per execution: ok | fallback | blocked."""
            nonlocal _outcome_emitted
            if _outcome_emitted is not None:
                if _outcome_emitted != tag:
                    logger.debug(
                        "event=email_metric_inconsistency activity_id=%s first=%s second=%s",
                        activity_id,
                        _outcome_emitted,
                        tag,
                    )
                return
            _outcome_emitted = tag
            logger.info(
                "event=email_metric name=email_validation_outcome tags=outcome=%s primary=1 activity_id=%s",
                tag,
                activity_id,
            )

        async def _strict_validation_block(
            *,
            validation_reason: str,
        ) -> Tuple[str, Optional[SendResult], str, bool]:
            logger.info(
                "event=email_metric name=email_validation_blocked activity_id=%s",
                activity_id,
            )
            _emit_outcome_once("blocked")
            _log_validation_summary(outcome="blocked", reason=validation_reason)
            max_chat_id = (
                os.getenv("MAX_NOTIFY_CHAT_ID") or os.getenv("MAX_NOTIFY_CHAT") or ""
            ).strip()
            token = (os.getenv("MAX_BOT_TOKEN") or "").strip()
            if not (token and max_chat_id):
                logger.warning(
                    "event=validation_blocked_no_notify reason=missing_max_config activity_id=%s",
                    activity_id,
                )
            try:
                self._quality_store.record_validation_blocked(
                    activity_id=str(activity_id),
                    session_key=str(activity_id),
                    max_chat_id=max_chat_id,
                    email_to=str(sender_email or ""),
                    email_subject=subject or "",
                    raw_draft_text=client_body[:8000],
                    template_type=template_type,
                    validation_reason=str(validation_reason or ""),
                )
            except Exception:
                logger.exception(
                    "[BitrixEmail] record_validation_blocked failed activity=%s",
                    activity_id,
                )
            if token and max_chat_id:
                note = (
                    f"❌ Email blocked by validation\n"
                    f"activity_id={activity_id}\n"
                    f"reason={validation_reason}\n"
                    f"template={template_type}"
                )
                try:
                    await asyncio.to_thread(
                        send_max_text_sync,
                        note,
                        chat_id=max_chat_id,
                        token=token,
                    )
                except Exception:
                    logger.exception(
                        "[BitrixEmail] MAX validation notify failed activity=%s",
                        activity_id,
                    )
            vr = str(validation_reason or "validation_failed")
            return (
                "",
                SendResult(success=False, error=f"validation_blocked:{vr}"),
                vr,
                False,
            )

        fallback_used = False
        outbound_body = client_body

        if not tmpl_res.ok:
            logger.info(
                "event=email_validation ok=false reason=%s template_type=%s activity_id=%s template_source=%s",
                tmpl_res.reason,
                template_type,
                activity_id,
                source,
            )
            if _should_log_failure_context():
                logger.info(
                    "event=email_validation_context activity_id=%s kind=template reason=%s strict=%s source=%s template_type=%s metadata_tt=%s marker_tt=%s body_len=%s",
                    activity_id,
                    tmpl_res.reason,
                    "true" if strict else "false",
                    source,
                    template_type,
                    meta_tt_raw,
                    marker_tt_resolved,
                    len(client_body),
                )
            if strict:
                return await _strict_validation_block(
                    validation_reason=str(tmpl_res.reason or "template_invalid"),
                )
            outbound_body = build_safe_fallback_reply(
                customer_name=customer_name,
                subject=subject,
            )
            fallback_used = True
            base_reason = f"template_fallback:{tmpl_res.reason}"
        elif not base_ok:
            if _should_log_failure_context():
                logger.info(
                    "event=email_validation_context activity_id=%s kind=base reason=%s strict=%s source=%s template_type=%s metadata_tt=%s marker_tt=%s body_len=%s",
                    activity_id,
                    base_reason,
                    "true" if strict else "false",
                    source,
                    template_type,
                    meta_tt_raw,
                    marker_tt_resolved,
                    len(client_body),
                )
            if strict:
                return await _strict_validation_block(
                    validation_reason=str(base_reason or "base_validation_failed"),
                )
            outbound_body = build_safe_fallback_reply(
                customer_name=customer_name,
                subject=subject,
            )
            fallback_used = True

        final_text = append_signature_if_missing(outbound_body)
        outcome = "fallback" if fallback_used else "ok"
        _emit_outcome_once(outcome)
        _log_validation_summary(outcome=outcome, reason=base_reason if fallback_used else "ok")
        return final_text, None, base_reason if fallback_used else "ok", fallback_used

    def _build_approval_prompt(self, *, to_display: str, subject: str, draft_text: str) -> str:
        return (
            "📬 ЧЕРНОВИК ОТВЕТА\n"
            f"Кому: {to_display}\n"
            f"Тема: {subject or '(no subject)'}\n"
            "---\n"
            f"{draft_text}\n"
            "---\n"
            'Отправить? Напиши "да", "нет" или исправление.'
        )

    async def _enqueue_for_approval(
        self,
        *,
        chat_id: str,
        sender_email: str,
        content: str,
        metadata: Optional[Dict[str, Any]],
        subject: str,
    ) -> SendResult:
        max_token = (os.getenv("MAX_BOT_TOKEN") or "").strip()
        max_chat_id = (os.getenv("MAX_NOTIFY_CHAT_ID") or os.getenv("MAX_NOTIFY_CHAT") or "").strip()
        if not max_token or not max_chat_id:
            return SendResult(
                success=False,
                error="EMAIL_APPROVAL_REQUIRED but MAX_BOT_TOKEN/MAX_NOTIFY_CHAT(_ID) missing",
            )

        parsed = self._parse_chat_id(chat_id)
        activity_id = parsed[3] if parsed is not None else str((metadata or {}).get("activity_id") or "").strip()
        session_key = str(activity_id or "")
        if not session_key:
            session_key = str((metadata or {}).get("session_key") or chat_id)
        existing = self._approval_store.get_pending_by_session_key(
            session_key,
            max_chat_id=max_chat_id,
        )
        if existing:
            existing_id = str(existing.get("approval_id") or "")
            logger.info(
                "[BitrixEmail][DraftValidation] activity_id=%s validation_reason=%s fallback_used=%s approval_id=%s",
                session_key,
                "reused_pending",
                "false",
                existing_id or "",
            )
            logger.info(
                "[BitrixEmail][Approval] Reusing pending draft %s for session_key=%s",
                existing_id or "?",
                session_key,
            )
            self._persist_bitrix_email_clean_reply_artifact(
                session_id=str((metadata or {}).get("session_id") or ""),
                activity_id=str(session_key),
                clean_text=None,
                fallback_used=False,
                raw_model_text=content,
                persist_error="reused_pending",
            )
            return SendResult(success=True, message_id=f"pending-approval:{existing_id or 'existing'}")

        approval_id = uuid.uuid4().hex[:8].upper()
        draft_text, early_err, reason, fallback_used = await self._prepare_client_plain_for_outbound(
            content=content,
            metadata=metadata,
            customer_name=str((metadata or {}).get("customer_name") or "").strip(),
            subject=subject,
            activity_id=session_key,
            sender_email=sender_email,
        )
        if early_err is not None:
            self._persist_bitrix_email_clean_reply_artifact(
                session_id=str((metadata or {}).get("session_id") or ""),
                activity_id=str(session_key),
                clean_text=None,
                fallback_used=False,
                raw_model_text=content,
                persist_error="outbound_prepare_failed",
            )
            return early_err
        self._persist_bitrix_email_clean_reply_artifact(
            session_id=str((metadata or {}).get("session_id") or ""),
            activity_id=str(session_key),
            clean_text=str(draft_text or ""),
            fallback_used=fallback_used,
            raw_model_text=content,
        )
        try:
            self._quality_store.interaction_log_upsert_draft(
                activity_id=str(session_key),
                draft_text=str(draft_text or ""),
            )
        except Exception:
            logger.exception(
                "[BitrixEmail] interaction_log_upsert_draft failed activity=%s",
                session_key,
            )
        logger.info(
            "[BitrixEmail][DraftValidation] activity_id=%s validation_reason=%s fallback_used=%s approval_id=%s",
            session_key,
            reason,
            "true" if fallback_used else "false",
            approval_id,
        )
        prompt = self._build_approval_prompt(
            to_display=sender_email,
            subject=subject,
            draft_text=draft_text,
        )
        idem_hash = self._inbound_snapshot_hash_from_metadata(metadata)
        if not self._downstream_idem.try_claim_first_execution(
            session_key,
            idem_hash,
            BitrixDownstreamEffect.MAX_APPROVAL_PROMPT,
        ):
            return SendResult(success=True, message_id="idempotency-max-approval-suppressed")
        ok = await asyncio.to_thread(send_max_text_sync, prompt, chat_id=max_chat_id, token=max_token)
        if not ok:
            return SendResult(success=False, error="Failed to send approval prompt to MAX")

        inbound_snapshot = ""
        if isinstance(metadata, dict):
            inbound_snapshot = str(
                metadata.get("inbound_snapshot") or metadata.get("inbound_text") or ""
            )

        self._approval_store.create_pending(
            approval_id=approval_id,
            email_to=sender_email,
            email_subject=subject or "Hermes Agent",
            email_in_reply_to="",
            reply_from_mailbox="",
            draft_text=draft_text,
            inbound_text_snapshot=inbound_snapshot,
            session_id=str((metadata or {}).get("session_id") or ""),
            session_key=str(activity_id or session_key),
            max_chat_id=max_chat_id,
            timeout_minutes=30,
            outbound_platform="bitrix_email",
            outbound_chat_id=chat_id,
        )
        try:
            self._quality_store.interaction_log_set_approval(
                activity_id=str(session_key),
                approval_id=str(approval_id),
            )
        except Exception:
            logger.exception(
                "[BitrixEmail] interaction_log_set_approval failed activity=%s approval=%s",
                session_key,
                approval_id,
            )
        logger.info(
            "[BitrixEmail][Approval] Queued draft %s for %s (outbound chat_id present)",
            approval_id,
            sender_email,
        )
        return SendResult(success=True, message_id=f"pending-approval:{approval_id}")

    @staticmethod
    def _bitrix_clean_reply_source_label(clean_text: str, fallback_used: bool) -> str:
        if fallback_used:
            return "fallback"
        if (clean_text or "").strip().upper() == "<NO_REPLY>":
            return "no_reply"
        return "prepared_outbound"

    def _persist_bitrix_email_clean_reply_artifact(
        self,
        *,
        session_id: str,
        activity_id: str,
        clean_text: Optional[str],
        fallback_used: bool,
        raw_model_text: str,
        persist_error: Optional[str] = None,
    ) -> None:
        """Persist exact outbound-prepared plain text; never mutates transcript."""
        sid = (session_id or "").strip()
        aid = (activity_id or "").strip()
        raw_flag = "true" if (raw_model_text or "").strip() else "false"

        if persist_error:
            logger.info(
                "event=bitrix_email_clean_reply_persist activity_id=%s session_id=%s "
                "clean_reply_persisted=false clean_reply_source=missing "
                "raw_content_persisted=%s persist_error=%s",
                aid or "(empty)",
                sid or "(empty)",
                raw_flag,
                persist_error,
            )
            return

        if not sid or not aid:
            logger.info(
                "event=bitrix_email_clean_reply_persist activity_id=%s session_id=%s "
                "clean_reply_persisted=false clean_reply_source=missing "
                "raw_content_persisted=%s persist_error=no_session_or_activity",
                aid or "(empty)",
                sid or "(empty)",
                raw_flag,
            )
            return

        ct = clean_text if clean_text is not None else ""
        src = BitrixEmailAdapter._bitrix_clean_reply_source_label(ct, fallback_used)

        try:
            db = SessionDB()
            try:
                db.upsert_bitrix_email_clean_reply(
                    session_id=sid,
                    activity_id=aid,
                    clean_text=ct,
                    clean_reply_source=src,
                )
            finally:
                db.close()
            logger.info(
                "event=bitrix_email_clean_reply_persist activity_id=%s session_id=%s "
                "clean_reply_persisted=true clean_reply_source=%s raw_content_persisted=%s",
                aid,
                sid,
                src,
                raw_flag,
            )
        except Exception as exc:
            logger.info(
                "event=bitrix_email_clean_reply_persist activity_id=%s session_id=%s "
                "clean_reply_persisted=false clean_reply_source=%s raw_content_persisted=%s "
                "persist_error=%s",
                aid,
                sid,
                src,
                raw_flag,
                exc,
            )

    @staticmethod
    def _strip_internal_outbound_markers(text: str) -> str:
        """Remove Hermes-internal suffixes that must not appear in client email bodies."""
        s = (text or "").strip()
        s = remove_hermes_markers(s)
        s = re.sub(r"\n?\s*\[msg:[^\]]+\]\s*$", "", s, flags=re.IGNORECASE).strip()
        return s

    @staticmethod
    def _unescape_env_newlines(s: str) -> str:
        """Turn literal ``\\n`` / ``\\r`` sequences from .env into real newlines."""
        if not s:
            return s
        return (
            s.replace("\\r\\n", "\n")
            .replace("\\n", "\n")
            .replace("\\r", "\n")
        )

    @staticmethod
    def _sender_attribution_html() -> str:
        """Visible «От:» line in HTML (Bitrix/webmail often hide technical From)."""
        explicit = BitrixEmailAdapter._unescape_env_newlines(
            (os.getenv("BITRIX_EMAIL_SENDER_DISPLAY") or "").strip(),
        )
        if explicit:
            safe = html.escape(explicit).replace("\n", "<br/>\n")
            return f"<p><strong>От:</strong> {safe}</p>\n"
        sig = BitrixEmailAdapter._unescape_env_newlines(os.getenv("EMAIL_SIGNATURE_TEXT") or "")
        m = re.search(r"(?im)E-mail:\s*(\S+)", sig)
        email = (m.group(1).strip() if m else "") or (os.getenv("YANDEX_EMAIL_2") or "").strip()
        if not email:
            return ""
        name = (os.getenv("BITRIX_EMAIL_SENDER_NAME") or "Купол").strip()
        return f"<p><strong>От:</strong> {html.escape(name)} · {html.escape(email)}</p>\n"

    @staticmethod
    def _signature_block_html() -> str:
        raw = BitrixEmailAdapter._unescape_env_newlines(
            (os.getenv("BITRIX_EMAIL_REPLY_SIGNATURE") or os.getenv("EMAIL_SIGNATURE_TEXT") or "").strip(),
        )
        if not raw:
            return ""
        body = html.escape(raw).replace("\n", "<br/>\n")
        return (
            f'<p style="margin-top:1.2em;padding-top:0.8em;border-top:1px solid #ddd;'
            f'color:#444;font-size:0.95em;line-height:1.45;">{body}</p>\n'
        )

    @classmethod
    def _plain_to_html_blocks(cls, plain: str) -> str:
        """Turn model plain-text into simple HTML (paragraphs + numbered lists)."""
        text = cls._strip_internal_outbound_markers(plain)
        if not text:
            return ""
        blocks = re.split(r"\n\s*\n+", text)
        parts: List[str] = []
        for block in blocks:
            block = block.strip()
            if not block:
                continue
            lines = [ln.rstrip() for ln in block.split("\n")]
            non_empty = [ln for ln in lines if ln.strip()]
            if non_empty and all(re.match(r"^\d+\.\s*", ln.strip()) for ln in non_empty):
                items: List[str] = []
                for ln in non_empty:
                    m = re.match(r"^(\d+)\.\s*(.*)$", ln.strip())
                    if m:
                        items.append(f"<li>{html.escape(m.group(2).strip())}</li>")
                parts.append("<ol style=\"margin:0.5em 0 0.5em 1.2em;padding:0;\">" + "".join(items) + "</ol>")
            else:
                inner = "<br/>\n".join(html.escape(ln) for ln in lines)
                parts.append(f"<p style=\"margin:0 0 0.9em 0;line-height:1.5;\">{inner}</p>")
        return "\n".join(parts)

    def _build_outbound_html_body(self, content: str) -> str:
        header = self._sender_attribution_html()
        main = self._plain_to_html_blocks(content)
        footer = self._signature_block_html()
        return f'<div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;color:#222;">{header}{main}{footer}</div>'

    async def connect(self) -> bool:
        if not self._webhook_url:
            logger.error("[BitrixEmail] BITRIX_WEBHOOK_URL is not configured")
            return False
        try:
            self._init_db()
            self._recover_stale_processing()
            self._cleanup_old_rows()
            self._poll_task = asyncio.create_task(self._poll_loop())
            self._mark_connected()
            logger.info("[BitrixEmail] Connected, poll interval=%ss", self._poll_interval)
            return True
        except Exception as exc:
            logger.exception("[BitrixEmail] connect failed: %s", exc)
            self._mark_disconnected()
            return False

    async def disconnect(self) -> None:
        self._mark_disconnected()
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        logger.info("[BitrixEmail] Disconnected")

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        start_ts = time.time()
        attempt_id = uuid.uuid4().hex
        send_status = "failure"
        log_written = False
        activity_id_for_log: Optional[str] = None
        template_type_for_log = str(
            (metadata or {}).get("_thread_state_final_template_type")
            or (metadata or {}).get("email_template_type")
            or ""
        ).strip().upper() or None

        def _log_once(*, outbound_text: str, approval_pending: bool, result: SendResult) -> None:
            nonlocal log_written
            if log_written:
                return
            log_written = True
            payload: Dict[str, Any] = {
                "activity_id": str(activity_id_for_log or "") or None,
                "thread_key": str((metadata or {}).get("thread_key") or "") or None,
                "template_type": template_type_for_log,
                "inbound_text": str((metadata or {}).get("inbound_text") or "") or None,
                "outbound_text": str(outbound_text or ""),
                "tools_used": (metadata or {}).get("tools_used", []),
                "attempt_id": attempt_id,
                "send_status": "success" if result.success else "failure",
                "duration_ms": int((time.time() - start_ts) * 1000),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            async def _write_task() -> None:
                try:
                    await self._interaction_log_store.log_email_interaction(payload)
                except Exception as exc:
                    logger.warning(
                        "event=interaction_log_write_failed activity_id=%s approval_pending=%s error=%s",
                        payload.get("activity_id"),
                        "true" if approval_pending else "false",
                        exc,
                    )
            asyncio.create_task(_write_task())
            logger.info(
                "event=interaction_log_written activity_id=%s approval_pending=%s",
                payload.get("activity_id"),
                "true" if approval_pending else "false",
            )

        async def _return_with_log(
            result: SendResult,
            *,
            outbound_text: str,
            approval_pending: bool,
        ) -> SendResult:
            _log_once(
                outbound_text=outbound_text,
                approval_pending=approval_pending,
                result=result,
            )
            return result
        del reply_to
        enforce_process_env_lockdown()
        assert_effective_model_or_raise(
            path="send",
            model=EXPECTED_MODEL,
            provider=EXPECTED_PROVIDER,
            base_url=EXPECTED_BASE_URL,
        )
        bypass_approval = isinstance(metadata, dict) and bool(metadata.get("_hermes_approval_send"))
        logger.warning(
            "[BitrixEmail] send() called, approval_required=%s bypass=%s",
            "true" if approval_required() else "false",
            bypass_approval,
        )
        if not bypass_approval and is_internal_only_output(content):
            logger.warning("[BitrixEmail] Suppressed internal-only output to %s", chat_id)
            return await _return_with_log(
                SendResult(success=True, message_id="internal-suppressed"),
                outbound_text=content,
                approval_pending=False,
            )
        if not bypass_approval:
            content = sanitize_outbound_email_body(content)
        if not bypass_approval and is_internal_only_output(content):
            logger.warning("[BitrixEmail] Suppressed internal-only output after sanitize to %s", chat_id)
            return await _return_with_log(
                SendResult(success=True, message_id="internal-suppressed"),
                outbound_text=content,
                approval_pending=False,
            )

        turn_key = str((metadata or {}).get("delivery_turn_key") or "").strip()
        if turn_key and self._is_turn_already_sent(turn_key):
            logger.warning(
                "[BitrixEmail] Suppressed duplicate send for turn_key=%s to %s",
                turn_key,
                chat_id,
            )
            return await _return_with_log(
                SendResult(success=True, message_id="duplicate-suppressed"),
                outbound_text=content,
                approval_pending=False,
            )

        parsed = self._parse_chat_id(chat_id)
        if parsed is None:
            logger.error("[BitrixEmail] invalid chat_id format: %s", chat_id)
            return await _return_with_log(
                SendResult(success=False, error="Invalid bitrix_email chat_id"),
                outbound_text=content,
                approval_pending=False,
            )
        owner_type, owner_id, sender_email, activity_id = parsed
        activity_id_for_log = str(activity_id)
        if not owner_id.isdigit():
            logger.error("[BitrixEmail] owner_id is not numeric in chat_id=%s", chat_id)
            return await _return_with_log(
                SendResult(success=False, error="owner_id must be numeric"),
                outbound_text=content,
                approval_pending=False,
            )

        raw_subject = ""
        activity_subject = ""
        if isinstance(metadata, dict):
            activity_subject = str(
                metadata.get("_hermes_bitrix_activity_subject") or ""
            ).strip()
            model_subject = str(metadata.get("subject") or "").strip()
            raw_subject = model_subject or activity_subject
        subject = self._normalize_reply_subject(raw_subject)

        if not bypass_approval and approval_required():
            queued = await self._enqueue_for_approval(
                chat_id=chat_id,
                sender_email=sender_email,
                content=content,
                metadata=metadata,
                subject=subject,
            )
            if queued.success and turn_key:
                self._mark_turn_sent(turn_key)
            return await _return_with_log(
                queued,
                outbound_text=content,
                approval_pending=True,
            )

        latest_approval: Optional[Dict[str, Any]] = None
        if bypass_approval and approval_required():
            max_chat_id = (os.getenv("MAX_NOTIFY_CHAT_ID") or os.getenv("MAX_NOTIFY_CHAT") or "").strip()
            forced_approval_id = str((metadata or {}).get("_hermes_email_approval_id") or "").strip()
            if forced_approval_id:
                forced_pending = self._approval_store.get_pending_by_id(forced_approval_id)
                if not forced_pending:
                    logger.warning(
                        "[BitrixEmail] blocked bypass send: forced pending approval missing activity=%s approval_id=%s",
                        activity_id,
                        forced_approval_id,
                    )
                    return await _return_with_log(
                        SendResult(success=False, error="Forced pending approval not found"),
                        outbound_text=content,
                        approval_pending=False,
                    )
                if str(forced_pending.get("session_key") or "").strip() != str(activity_id):
                    logger.warning(
                        "[BitrixEmail] blocked bypass send: forced approval/session mismatch activity=%s approval_id=%s session_key=%s",
                        activity_id,
                        forced_approval_id,
                        str(forced_pending.get("session_key") or ""),
                    )
                    return await _return_with_log(
                        SendResult(success=False, error="Forced pending approval session mismatch"),
                        outbound_text=content,
                        approval_pending=False,
                    )
                latest_approval = forced_pending
                approved_text = str(latest_approval.get("draft_text") or "").strip()
                if not approved_text:
                    return await _return_with_log(
                        SendResult(success=False, error="Forced pending draft is empty"),
                        outbound_text=content,
                        approval_pending=False,
                    )
                content = approved_text
            else:
                latest = self._approval_store.get_latest_by_session_key(
                    activity_id,
                    max_chat_id=max_chat_id,
                )
                if not latest:
                    logger.warning(
                        "[BitrixEmail] blocked bypass send without approval context activity=%s",
                        activity_id,
                    )
                    return await _return_with_log(
                        SendResult(success=False, error="Approval context not found for activity"),
                        outbound_text=content,
                        approval_pending=False,
                    )
                status = str(latest.get("status") or "").strip().lower()
                if status != "approved":
                    logger.warning(
                        "[BitrixEmail] blocked bypass send activity=%s status=%s",
                        activity_id,
                        status or "?",
                    )
                    return await _return_with_log(
                        SendResult(success=False, error=f"Approval status is {status or 'unknown'}"),
                        outbound_text=content,
                        approval_pending=False,
                    )
                latest_approval = latest
                approved_text = str(latest_approval.get("draft_text") or "").strip()
                if not approved_text:
                    logger.error(
                        "[BitrixEmail] approved draft missing activity=%s",
                        activity_id,
                    )
                    return await _return_with_log(
                        SendResult(success=False, error="Approved draft is empty"),
                        outbound_text=content,
                        approval_pending=False,
                    )
                # Single source of truth for approval-required bypass send.
                content = approved_text

        # Enforce safe outbound content for every real send path (including bypass).
        final_text, early_send, validation_reason, fallback_used = await self._prepare_client_plain_for_outbound(
            content=content,
            metadata=metadata,
            customer_name=str((metadata or {}).get("customer_name") or "").strip(),
            subject=subject,
            activity_id=str(activity_id),
            sender_email=sender_email,
        )
        if early_send is not None:
            self._persist_bitrix_email_clean_reply_artifact(
                session_id=str((metadata or {}).get("session_id") or ""),
                activity_id=str(activity_id),
                clean_text=None,
                fallback_used=False,
                raw_model_text=content,
                persist_error=f"early_send:{early_send.error or 'unknown'}",
            )
            return await _return_with_log(
                early_send,
                outbound_text=content,
                approval_pending=False,
            )
        if fallback_used:
            logger.warning(
                "[BitrixEmail] outbound content fallback applied activity=%s reason=%s",
                activity_id,
                validation_reason,
            )

        template_type = str(
            (metadata or {}).get("_thread_state_final_template_type")
            or (metadata or {}).get("email_template_type")
            or ""
        ).strip().upper() or None
        template_type_for_log = template_type
        send_text_for_client = str(final_text or "")
        self._persist_bitrix_email_clean_reply_artifact(
            session_id=str((metadata or {}).get("session_id") or ""),
            activity_id=str(activity_id),
            clean_text=send_text_for_client,
            fallback_used=fallback_used,
            raw_model_text=content,
        )

        body_html = self._build_outbound_html_body(final_text)
        outbound_hash = hashlib.sha256(send_text_for_client.encode("utf-8")).hexdigest()
        logger.info(
            "event=bitrix_outbound_hash activity_id=%s hash=%s",
            activity_id,
            outbound_hash,
        )

        params = {
            "fields[TYPE_ID]": _EMAIL_ACTIVITY_TYPE_ID,
            "fields[DIRECTION]": "2",
            # For CRM_EMAIL, COMPLETED=Y is required to send the message instead of
            # leaving it as an unsent planned activity in timeline.
            "fields[COMPLETED]": "Y",
            "fields[OWNER_TYPE_ID]": "1" if owner_type == "lead" else "2",
            "fields[OWNER_ID]": owner_id,
            "fields[SUBJECT]": subject,
            "fields[DESCRIPTION]": body_html,
            # 1=plain, 2=bbCode, 3=HTML (crm.enum.contenttype)
            "fields[DESCRIPTION_TYPE]": "3",
            "fields[PROVIDER_ID]": "CRM_EMAIL",
            "fields[COMMUNICATIONS][0][TYPE]": "EMAIL",
            "fields[COMMUNICATIONS][0][VALUE]": sender_email,
        }
        responsible_id = (os.getenv("BITRIX_USER_ID") or "").strip()
        if responsible_id and responsible_id.isdigit():
            params["fields[RESPONSIBLE_ID]"] = responsible_id
        else:
            logger.warning(
                "[BitrixEmail] BITRIX_USER_ID not set or invalid, omitting RESPONSIBLE_ID",
            )

        idem_hash = self._inbound_snapshot_hash_from_metadata(metadata)
        if not self._downstream_idem.try_claim_first_execution(
            str(activity_id),
            idem_hash,
            BitrixDownstreamEffect.CRM_ACTIVITY_ADD_OUTBOUND,
        ):
            if turn_key:
                self._mark_turn_sent(turn_key)
            return await _return_with_log(
                SendResult(success=True, message_id="idempotency-outbound-suppressed"),
                outbound_text=send_text_for_client,
                approval_pending=False,
            )

        result: Optional[SendResult] = None
        try:
            activity_id_str = str(activity_id or "").strip()
            attempted = self._quality_store.interaction_log_mark_attempted(
                activity_id=activity_id_str,
                provider_payload_hash=outbound_hash,
                outbound_final_text=send_text_for_client,
            )
            if not attempted:
                logger.warning(
                    "[BitrixEmail] interaction_log attempt transition denied activity=%s",
                    activity_id_str,
                )
                result = SendResult(success=False, error="interaction_log_invalid_transition")
                return await _return_with_log(
                    result,
                    outbound_text=send_text_for_client,
                    approval_pending=False,
                )
            dedup_inserted = self._send_dedup_store.try_claim_send(
                activity_id_str,
                outbound_hash,
            )
            if dedup_inserted:
                logger.info(
                    "event=send_dedup_inserted activity_id=%s outbound_hash=%s",
                    activity_id_str,
                    outbound_hash,
                )
            else:
                logger.info(
                    "event=send_dedup_conflict activity_id=%s outbound_hash=%s",
                    activity_id_str,
                    outbound_hash,
                )
                logger.info(
                    "event=send_skipped_dedup activity_id=%s outbound_hash=%s",
                    activity_id_str,
                    outbound_hash,
                )
                result = SendResult(success=True, message_id="send-dedup-suppressed")
                return await _return_with_log(
                    result,
                    outbound_text=send_text_for_client,
                    approval_pending=False,
                )
            payload = await asyncio.to_thread(self._client.call, "crm.activity.add", params)
            created_id = str((payload or {}).get("result") or "")
            logger.info(
                "[BitrixEmail] outbound activity created id=%s owner=%s:%s completed=Y",
                created_id or "?",
                owner_type,
                owner_id,
            )
            if created_id:
                self._quality_store.interaction_log_mark_acknowledged(
                    activity_id=activity_id_str,
                    provider_message_id=created_id,
                )
            else:
                self._quality_store.interaction_log_mark_unknown(
                    activity_id=activity_id_str,
                )
            # MVP heuristic: treat explicit INN request as a marker.
            if "инн" in (final_text or "").lower():
                self.mark_inn_requested(owner_type, owner_id, sender_email)
            try:
                final_text_for_quality = remove_hermes_markers(final_text)
                self._quality_store.mark_sent(
                    activity_id=activity_id,
                    final_sent_text=final_text_for_quality,
                    send_result="sent",
                )
            except Exception:
                logger.exception(
                    "[BitrixEmail] quality mark_sent failed activity=%s",
                    activity_id,
                )
            self._apply_thread_state_after_successful_outbound(metadata, str(activity_id))
            if turn_key:
                self._mark_turn_sent(turn_key)
            send_status = "success"
            result = SendResult(success=True, message_id=created_id, raw_response=payload)
        except Exception as exc:
            try:
                self._quality_store.interaction_log_mark_failed(
                    activity_id=str(activity_id or "").strip(),
                    send_error=str(exc),
                )
                self._quality_store.mark_sent(
                    activity_id=activity_id,
                    final_sent_text=remove_hermes_markers(final_text),
                    send_result="failed",
                    send_error=str(exc),
                )
            except Exception:
                logger.exception(
                    "[BitrixEmail] quality mark_failed failed activity=%s",
                    activity_id,
                )
            logger.exception("[BitrixEmail] send failed: %s", exc)
            result = SendResult(success=False, error=str(exc))
        assert result is not None
        return await _return_with_log(
            result,
            outbound_text=send_text_for_client,
            approval_pending=False,
        )

    async def send_pending_approval(self, pending: Dict[str, Any]) -> SendResult:
        activity_id = str((pending or {}).get("session_key") or "").strip()
        approval_id = str((pending or {}).get("approval_id") or "").strip()
        draft_text = str((pending or {}).get("draft_text") or "").strip()
        if not (activity_id and approval_id and draft_text):
            return SendResult(success=False, error="pending approval payload is incomplete")
        try:
            payload = await asyncio.to_thread(
                self._client.call,
                "crm.activity.get",
                {"id": activity_id},
            )
            activity = (payload or {}).get("result") or {}
            if not isinstance(activity, dict):
                return SendResult(success=False, error="crm.activity.get returned invalid payload")
            sender_email = self._extract_sender_email(activity)
            owner_type, owner_id = self._extract_owner(activity)
            if not sender_email or not owner_type or not owner_id:
                return SendResult(success=False, error="cannot resolve chat identity from activity")
            chat_id = f"{owner_type}|{owner_id}|{sender_email}|{activity_id}"
            return await self.send(
                chat_id,
                draft_text,
                metadata={
                    "_hermes_approval_send": True,
                    "_hermes_email_approval_id": approval_id,
                    "session_id": str((pending or {}).get("session_id") or ""),
                    "activity_id": activity_id,
                },
            )
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    async def send_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        del chat_id, metadata
        return

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        parsed = self._parse_chat_id(chat_id)
        if parsed is None:
            return {"name": chat_id, "type": "dm", "chat_id": chat_id}
        owner_type, _owner_id, sender_email, _activity_id = parsed
        return {"name": sender_email, "type": owner_type, "chat_id": chat_id}

    def was_inn_requested_recently(self, owner_type: str, owner_id: str, sender_email: str) -> bool:
        conn = self._require_conn()
        key = f"{owner_type}|{owner_id}|{sender_email.lower().strip()}"
        row = conn.execute(
            "SELECT requested_at FROM bitrix_email_inn_requests WHERE key = ?",
            (key,),
        ).fetchone()
        if not row:
            return False
        try:
            requested_at = float(row["requested_at"])
        except Exception:
            return False
        return (time.time() - requested_at) < _INN_REQUEST_TTL_SECONDS

    def mark_inn_requested(self, owner_type: str, owner_id: str, sender_email: str) -> None:
        conn = self._require_conn()
        key = f"{owner_type}|{owner_id}|{sender_email.lower().strip()}"
        conn.execute(
            "INSERT OR REPLACE INTO bitrix_email_inn_requests(key, requested_at) VALUES (?, ?)",
            (key, time.time()),
        )
        conn.commit()

    async def _poll_loop(self) -> None:
        while True:
            try:
                async with self._poll_lock:
                    await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("[BitrixEmail] poll cycle failed: %s", exc)
            await asyncio.sleep(self._poll_interval)

    async def _poll_once(self) -> None:
        processed_ts: List[float] = []
        for src_ts in await self._retry_failed_items():
            if src_ts:
                processed_ts.append(src_ts)

        now_ts = time.time()
        raw_last_poll_ts = self._read_last_poll_ts()
        # Guard against provider clocks / malformed activity times that can push
        # last_poll_ts into the future and stall all subsequent polling.
        last_poll_ts = min(raw_last_poll_ts, now_ts)
        if raw_last_poll_ts > now_ts:
            logger.warning(
                "[BitrixEmail] last_poll_ts is in the future (raw=%.3f now=%.3f); clamping",
                raw_last_poll_ts,
                now_ts,
            )
            self._write_last_poll_ts(last_poll_ts)
        created_from = last_poll_ts - _OVERLAP_SECONDS
        logger.info(
            "[BitrixEmail] poll start last_poll_ts=%.3f overlap_from=%.3f",
            last_poll_ts,
            created_from,
        )
        start = 0
        total_listed = 0
        pages = 0
        while True:
            params = {
                "filter[TYPE_ID]": _EMAIL_ACTIVITY_TYPE_ID,
                "filter[DIRECTION]": "1",
                "filter[PROVIDER_ID]": "CRM_EMAIL",
                "filter[>=CREATED]": self._format_bitrix_datetime(created_from),
                "order[CREATED]": "ASC",
                "order[ID]": "ASC",
                "start": str(start),
            }
            payload = await asyncio.to_thread(self._client.call, "crm.activity.list", params)
            rows = (payload or {}).get("result") or []
            if not isinstance(rows, list):
                rows = []
            pages += 1
            total_listed += len(rows)
            for row in rows:
                ts = await self._process_activity_from_list(row)
                if ts:
                    processed_ts.append(ts)

            nxt = payload.get("next")
            if nxt is None:
                break
            try:
                start = int(nxt)
            except Exception:
                break

        if processed_ts:
            max_processed_ts = max(processed_ts)
            safe_last_poll_ts = min(max_processed_ts, time.time())
            if max_processed_ts > safe_last_poll_ts:
                logger.warning(
                    "[BitrixEmail] processed timestamp is in the future (raw=%.3f now=%.3f); clamping",
                    max_processed_ts,
                    safe_last_poll_ts,
                )
            self._write_last_poll_ts(safe_last_poll_ts)
            logger.info(
                "[BitrixEmail] poll done pages=%d listed=%d processed=%d last_poll_ts_updated=%.3f",
                pages,
                total_listed,
                len(processed_ts),
                safe_last_poll_ts,
            )
        else:
            logger.info(
                "[BitrixEmail] poll done pages=%d listed=%d processed=0",
                pages,
                total_listed,
            )

    async def _retry_failed_items(self) -> List[float]:
        conn = self._require_conn()
        rows = conn.execute(
            """
            SELECT item_id, source_created_at
            FROM bitrix_email_processed
            WHERE status = 'failed' AND retry_count < 3
            ORDER BY processing_started_at ASC
            """,
        ).fetchall()
        out: List[float] = []
        if rows:
            logger.info("[BitrixEmail] retry queue size=%d", len(rows))
        for row in rows:
            item_id = str(row["item_id"])
            now_ts = time.time()
            conn.execute(
                """
                UPDATE bitrix_email_processed
                   SET status='processing', processing_started_at=?, last_error=NULL
                 WHERE item_type='activity' AND item_id=?
                """,
                (now_ts, item_id),
            )
            conn.commit()
            src_ts = await self._process_activity_by_id(item_id, source_created_at=row["source_created_at"])
            if src_ts:
                out.append(src_ts)
        if rows:
            logger.info("[BitrixEmail] retry processed=%d", len(out))
        return out

    async def _process_activity_from_list(self, row: Dict[str, Any]) -> Optional[float]:
        activity_id = str(row.get("ID") or "").strip()
        if not activity_id:
            return None

        if str(row.get("DIRECTION") or "") != "1":
            logger.info("[BitrixEmail] skip activity=%s reason=direction_not_incoming", activity_id)
            return None

        subject = str(row.get("SUBJECT") or "")
        source_created_at = self._parse_bitrix_datetime(row.get("CREATED"))
        status = self._get_item_status(activity_id)
        if status == "processed":
            logger.info("[BitrixEmail] skip activity=%s reason=already_processed", activity_id)
            return None
        if status == "failed_permanent":
            logger.info("[BitrixEmail] skip activity=%s reason=failed_permanent", activity_id)
            return None

        if self._should_skip_system_incoming(subject, ""):
            self._mark_terminal_skip_processed(activity_id, source_created_at)
            logger.info("[BitrixEmail] skip activity=%s reason=system_incoming_subject", activity_id)
            return source_created_at if source_created_at is not None else time.time()

        inserted = self._insert_processing_row(activity_id, source_created_at)
        if not inserted:
            logger.info("[BitrixEmail] skip activity=%s reason=already_processing", activity_id)
            return None
        return await self._process_activity_by_id(activity_id, source_created_at=source_created_at)

    async def _process_activity_by_id(self, activity_id: str, source_created_at: Optional[float] = None) -> Optional[float]:
        try:
            payload = await asyncio.to_thread(
                self._client.call,
                "crm.activity.get",
                {"id": str(activity_id)},
            )
            activity = (payload or {}).get("result") or {}
            if not isinstance(activity, dict):
                raise RuntimeError("crm.activity.get returned invalid payload")

            if poll_skip_if_already_committed_layer_b(str(activity_id), payload):
                if source_created_at is None:
                    source_created_at = self._parse_bitrix_datetime(activity.get("CREATED"))
                self._mark_processed(activity_id, source_created_at)
                logger.info("[BitrixEmail] skip activity=%s reason=layer_b_head_match", activity_id)
                return source_created_at if source_created_at is not None else time.time()

            sender_email = self._extract_sender_email(activity)
            if not sender_email:
                self._mark_failed(activity_id, "sender_email missing")
                logger.warning("[BitrixEmail] sender missing for activity %s", activity_id)
                return None

            subject = str(activity.get("SUBJECT") or "").strip()
            if self._is_autoresponder(subject, sender_email):
                if source_created_at is None:
                    source_created_at = self._parse_bitrix_datetime(activity.get("CREATED"))
                self._mark_system_incoming_skipped(activity_id, source_created_at)
                logger.info(
                    "[BitrixEmail] skip activity=%s reason=system_incoming sender=%s",
                    activity_id,
                    sender_email,
                )
                return source_created_at if source_created_at is not None else time.time()

            if source_created_at is None:
                source_created_at = self._parse_bitrix_datetime(activity.get("CREATED"))

            if self._is_internal_sender(sender_email):
                self._mark_terminal_skip_processed(activity_id, source_created_at)
                logger.info(
                    "[BitrixEmail] skip activity=%s reason=internal_sender sender=%s",
                    activity_id,
                    sender_email,
                )
                return source_created_at if source_created_at is not None else time.time()

            owner_type, owner_id = self._extract_owner(activity)
            if owner_type is None or owner_id is None:
                self._mark_failed(activity_id, "owner_type/owner_id missing or invalid")
                logger.warning("[BitrixEmail] owner missing for activity %s", activity_id)
                return None

            if source_created_at is None:
                source_created_at = self._parse_bitrix_datetime(activity.get("CREATED"))

            _idem_snap = build_activity_snapshot(payload, expected_activity_id=str(activity_id))
            _inbound_snapshot_hash = str(_idem_snap.snapshot_hash or "") if _idem_snap.success else ""

            chat_id = f"{owner_type}|{owner_id}|{sender_email}|{activity_id}"
            body = str(activity.get("DESCRIPTION") or "").strip()
            attachment_notes, media_paths, media_types = await self._download_attachments(activity)
            text_chunks = [f"Subject: {subject or '(без темы)'}"]
            if body:
                text_chunks.append(body)
            if attachment_notes:
                text_chunks.extend(attachment_notes)
            text = "\n\n".join(chunk for chunk in text_chunks if chunk)

            in_reply_to = BitrixEmailAdapter._extract_in_reply_to(activity)
            is_follow_up = looks_like_follow_up(subject, body, in_reply_to)
            _thread_key = derive_thread_key(
                str(owner_type or ""),
                str(owner_id or ""),
                str(sender_email or ""),
            )
            if BitrixEmailAdapter._thread_state_machine_enabled():
                try:
                    self._thread_state_store.maybe_mark_ready_from_inbound(
                        _thread_key,
                        is_follow_up=is_follow_up,
                        body_non_empty=bool((body or "").strip()),
                        activity_id=str(activity_id),
                    )
                except Exception:
                    logger.exception(
                        "[BitrixEmail] thread_state inbound transition failed activity=%s",
                        activity_id,
                    )
            crm_ctx = self._bitrix_precheck(sender_email)

            if (not crm_ctx.get("crm_open_lead_exists")) and crm_ctx.get("crm_open_deal_exists"):
                open_deal_id = str(crm_ctx.get("crm_open_deal_id") or "")
                sender_norm = (sender_email or "").strip().lower()
                # Notify once per inbound activity id (no collapsing of multiple
                # different emails from the same sender/deal within a TTL window).
                dedup_key = f"bitrix_email_activity:{activity_id}"
                notify_store = EmailProcessedStore(get_hermes_home() / "email_processed.db")
                if not notify_store.is_already_notified(dedup_key):
                    max_token = (os.getenv("MAX_BOT_TOKEN") or "").strip()
                    max_chat_id = (
                        os.getenv("MAX_NOTIFY_CHAT_ID") or os.getenv("MAX_NOTIFY_CHAT") or ""
                    ).strip()
                    if max_token and max_chat_id:
                        max_text = (
                            f"\U0001f4e8 Письмо от {sender_norm} — есть открытая сделка #{open_deal_id or '?'}. "
                            f"Активность CRM id={activity_id}. Письмо не обработано ассистентом."
                        )
                        if not self._downstream_idem.try_claim_first_execution(
                            str(activity_id),
                            _inbound_snapshot_hash,
                            BitrixDownstreamEffect.MAX_NOTIFY_OPEN_DEAL,
                        ):
                            logger.info(
                                "[BitrixEmail] open-deal MAX skip idempotency activity=%s",
                                activity_id,
                            )
                            try:
                                notify_store.mark_notified(dedup_key)
                            except Exception:
                                logger.exception(
                                    "[BitrixEmail] mark_notified after idempotency skip key=%s",
                                    dedup_key,
                                )
                            ok = True
                        else:
                            ok = await asyncio.to_thread(
                                send_max_text_sync,
                                max_text,
                                chat_id=max_chat_id,
                                token=max_token,
                            )
                        if ok:
                            try:
                                notify_store.mark_notified(dedup_key)
                                logger.info(
                                    "[BitrixEmail] open-deal MAX notify dedup marked key=%s",
                                    dedup_key,
                                )
                            except Exception:
                                logger.exception(
                                    "[BitrixEmail] mark_notified failed key=%s",
                                    dedup_key,
                                )
                        else:
                            logger.warning(
                                "[BitrixEmail] open-deal MAX notify failed key=%s",
                                dedup_key,
                            )
                    else:
                        logger.warning(
                            "[BitrixEmail] open-deal MAX skipped (missing token/chat) key=%s",
                            dedup_key,
                        )
                else:
                    logger.info(
                        "[BitrixEmail] open-deal MAX suppressed (dedup) key=%s",
                        dedup_key,
                    )
                self._mark_open_deal_routing_skip(activity_id, source_created_at)
                logger.info(
                    "[BitrixEmail] routing stop activity=%s open_deal=%s",
                    activity_id,
                    open_deal_id,
                )
                return source_created_at if source_created_at is not None else time.time()

            classification = await classify_inbound_email(
                subject=subject,
                body=body,
                follow_up=is_follow_up,
                crm_hit=bool(crm_ctx.get("crm_hit")),
                crm_open_deal_exists=bool(crm_ctx.get("crm_open_deal_exists")),
            )
            try:
                autosave_email_context(
                    session_key=str(activity_id),
                    customer_id=str(sender_email or ""),
                    source="bitrix_email",
                    payload={
                        "session_key": str(activity_id),
                        "customer_id": str(sender_email or ""),
                        "subject": subject,
                        "request_type": classification.get("request_type"),
                        "crm_open_lead_exists": bool(crm_ctx.get("crm_open_lead_exists")),
                        "crm_open_deal_exists": bool(crm_ctx.get("crm_open_deal_exists")),
                        "source": "bitrix_email",
                    },
                )
            except Exception as exc:
                logger.warning(
                    "[BitrixEmail] gbrain email context autosave failed activity=%s error=%s",
                    activity_id,
                    exc,
                )
            # Plain inbound text before CRM/classification prefix — used by DealDraftProcessor DRY RUN only.
            _plain_for_deal_draft = text
            text = format_email_context_line(is_follow_up, crm_ctx, classification) + "\n" + text

            metadata = {
                "activity_id": activity_id,
                "thread_key": _thread_key,
                "inbound_layer_b_snapshot_hash": _inbound_snapshot_hash,
                "inbound_text": text,
                "owner_type": owner_type,
                "owner_id": owner_id,
                "sender_email": sender_email,
                "subject": subject,
                "_hermes_bitrix_activity_subject": subject,
                "source_created_at": source_created_at,
                "inn_was_requested_recently": self.was_inn_requested_recently(owner_type, owner_id, sender_email),
                "crm_contact_id": crm_ctx.get("crm_contact_id") or "",
                "crm_company_id": crm_ctx.get("crm_company_id") or "",
                "crm_open_lead_id": crm_ctx.get("crm_open_lead_id") or "",
                "crm_open_lead_exists": bool(crm_ctx.get("crm_open_lead_exists")),
                "crm_open_deal_id": crm_ctx.get("crm_open_deal_id") or "",
                "crm_open_deal_exists": bool(crm_ctx.get("crm_open_deal_exists")),
                "crm_hit": bool(crm_ctx.get("crm_hit")),
                "email_classification": {
                    "request_type": classification.get("request_type"),
                    "confidence": classification.get("confidence"),
                    "source": classification.get("source"),
                    "reason": classification.get("reason"),
                },
            }
            try:
                inbound_created_at_text = ""
                if source_created_at is not None:
                    inbound_created_at_text = time.strftime(
                        "%Y-%m-%d %H:%M:%S",
                        time.localtime(source_created_at),
                    )
                self._quality_store.interaction_log_inbound(
                    activity_id=str(activity_id),
                    session_key=str(activity_id),
                    sender_email=str(sender_email or ""),
                    subject=str(subject or ""),
                    inbound_text_raw=str(text or ""),
                    inbound_created_at=inbound_created_at_text,
                )
            except Exception:
                logger.exception(
                    "[BitrixEmail] interaction_log_inbound failed activity=%s",
                    activity_id,
                )
            try:
                from extensions.hermes_spare24.deal_draft_processor import DealDraftProcessor

                await asyncio.to_thread(
                    DealDraftProcessor.handle,
                    _plain_for_deal_draft,
                    "email",
                    str(activity_id),
                    {"sender_email": sender_email, "subject": subject},
                )
            except Exception:
                logger.exception(
                    "[BitrixEmail][DealDraft] processor failed correlation_id=%s activity_id=%s "
                    "decision=dry_run status=fail",
                    activity_id,
                    activity_id,
                )
            source = self.build_source(
                chat_id=chat_id,
                chat_name=sender_email,
                chat_type="dm",
                user_id=sender_email,
                user_name=sender_email,
            )
            event = MessageEvent(
                text=text,
                message_type=MessageType.TEXT,
                source=source,
                raw_message={"activity": activity, "metadata": metadata},
                message_id=str(activity_id),
                media_urls=media_paths,
                media_types=media_types,
                email_subject=subject,
            )
            logger.info(
                "[BitrixEmail] dispatch start activity=%s owner=%s:%s sender=%s",
                activity_id,
                owner_type,
                owner_id,
                sender_email,
            )
            await self.handle_message(event)
            classification_json = json.dumps(
                {
                    "request_type": classification.get("request_type"),
                    "confidence": classification.get("confidence"),
                    "source": classification.get("source"),
                    "reason": classification.get("reason"),
                },
                ensure_ascii=False,
            )
            self._mark_processed(activity_id, source_created_at, classification_json=classification_json)
            logger.info(
                "[BitrixEmail] dispatch ok activity=%s owner=%s:%s",
                activity_id,
                owner_type,
                owner_id,
            )
            return source_created_at
        except Exception as exc:
            status = self._mark_failed(activity_id, str(exc))
            if status == "failed_permanent":
                await self._send_failed_permanent_alert(activity_id)
            logger.exception("[BitrixEmail] process failed activity=%s: %s", activity_id, exc)
            return None

    async def _download_attachments(self, activity: Dict[str, Any]) -> Tuple[List[str], List[str], List[str]]:
        notes: List[str] = []
        media_paths: List[str] = []
        media_types: List[str] = []
        files = activity.get("FILES") or []
        if not isinstance(files, list):
            return notes, media_paths, media_types

        for f in files:
            if not isinstance(f, dict):
                continue
            file_id = str(f.get("id") or f.get("ID") or "").strip()
            if not file_id:
                continue
            try:
                payload = await asyncio.to_thread(self._client.call, "disk.file.get", {"id": file_id})
                info = (payload or {}).get("result") or {}
                filename = str(info.get("NAME") or f"file_{file_id}")
                size_raw = info.get("SIZE")
                try:
                    size = int(size_raw)
                except Exception:
                    size = 0
                if size > _MAX_ATTACHMENT_SIZE:
                    notes.append(f"[Attachment skipped: exceeds 10MB: {filename}]")
                    continue
                download_url = str(info.get("DOWNLOAD_URL") or "").strip()
                if not download_url:
                    notes.append(f"[Attachment unavailable: {filename}]")
                    continue
                data = await asyncio.to_thread(self._download_bytes, download_url, 30.0)
                ext = Path(filename).suffix.lower()
                if ext in _IMAGE_EXTS:
                    cached = cache_image_from_bytes(data, ext=ext or ".jpg")
                    media_types.append("image")
                else:
                    cached = cache_document_from_bytes(data, filename)
                    media_types.append("document")
                media_paths.append(cached)
            except BitrixAPIError as exc:
                filename = str(f.get("name") or f.get("id") or "unknown")
                if exc.code == "ACCESS_DENIED":
                    notes.append(f"[Attachment unavailable: {filename}]")
                    continue
                notes.append(f"[Attachment unavailable: {filename}]")
                logger.warning("[BitrixEmail] disk.file.get failed id=%s: %s", file_id, exc)
            except Exception as exc:
                filename = str(f.get("name") or f.get("id") or "unknown")
                notes.append(f"[Attachment unavailable: {filename}]")
                logger.warning("[BitrixEmail] attachment download failed id=%s: %s", file_id, exc)
        return notes, media_paths, media_types

    @staticmethod
    def _download_bytes(url: str, timeout: float) -> bytes:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.read()

    @staticmethod
    def _extract_in_reply_to(activity: Dict[str, Any]) -> str:
        settings = activity.get("SETTINGS") or {}
        if not isinstance(settings, dict):
            return ""
        email_meta = settings.get("EMAIL_META")
        if not isinstance(email_meta, dict):
            return ""
        for key in ("In-Reply-To", "IN_REPLY_TO", "in-reply-to"):
            raw = email_meta.get(key)
            if raw is not None and str(raw).strip():
                return str(raw).strip()
        return ""

    @staticmethod
    def _extract_sender_email(activity: Dict[str, Any]) -> str:
        settings = activity.get("SETTINGS") or {}
        email_meta = settings.get("EMAIL_META") if isinstance(settings, dict) else None
        from_raw = ""
        if isinstance(email_meta, dict):
            from_raw = (
                str(email_meta.get("FROM") or "")
                or str(email_meta.get("from") or "")
                or str(email_meta.get("__email") or "")
            )
        parsed = parseaddr(from_raw)[1] if from_raw else ""
        sender = (parsed or from_raw or "").strip().lower()
        if sender:
            return sender

        comms = activity.get("COMMUNICATIONS") or []
        if isinstance(comms, list):
            for comm in comms:
                if not isinstance(comm, dict):
                    continue
                value = str(comm.get("VALUE") or "").strip().lower()
                if value and "@" in value:
                    return value
        return ""

    @staticmethod
    def _extract_owner(activity: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        owner_type_id = str(activity.get("OWNER_TYPE_ID") or "").strip()
        owner_id = str(activity.get("OWNER_ID") or "").strip()
        if not owner_type_id and not owner_id:
            return None, None
        if not owner_id.isdigit():
            return None, None
        if owner_type_id == "1":
            return "lead", owner_id
        if owner_type_id == "2":
            return "deal", owner_id
        return None, None

    @staticmethod
    def _subject_looks_like_autoreply(subject: str) -> bool:
        return BitrixEmailAdapter._should_skip_system_incoming(subject, "")

    def _is_autoresponder(self, subject: str, sender_email: str) -> bool:
        return self._should_skip_system_incoming(subject, sender_email)

    def _is_internal_sender(self, sender_email: str) -> bool:
        return (sender_email or "").strip().lower() in _internal_email_set()

    @staticmethod
    def _parse_chat_id(chat_id: str) -> Optional[Tuple[str, str, str, str]]:
        parts = (chat_id or "").split("|", 3)
        if len(parts) != 4:
            return None
        owner_type = parts[0].strip().lower()
        owner_id = parts[1].strip()
        sender_email = parts[2].strip().lower()
        activity_id = parts[3].strip()
        if owner_type not in {"lead", "deal"}:
            return None
        if not owner_id or not sender_email or not activity_id:
            return None
        return owner_type, owner_id, sender_email, activity_id

    @staticmethod
    def _normalize_reply_subject(subject: str) -> str:
        cleaned = (subject or "").strip()
        while True:
            updated = re.sub(r"^\s*re\s*:\s*", "", cleaned, flags=re.IGNORECASE)
            if updated == cleaned:
                break
            cleaned = updated
        cleaned = cleaned.strip()
        if not cleaned:
            return "Re: (без темы)"
        return f"Re: {cleaned}"

    def _init_db(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bitrix_email_processed (
                item_type TEXT,
                item_id TEXT,
                status TEXT,
                processing_started_at REAL,
                processed_at REAL,
                last_error TEXT,
                source_created_at REAL,
                retry_count INTEGER DEFAULT 0,
                PRIMARY KEY (item_type, item_id)
            )
            """,
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_processing
              ON bitrix_email_processed(processing_started_at)
              WHERE status='processing'
            """,
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bitrix_email_state (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """,
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bitrix_email_inn_requests (
                key TEXT PRIMARY KEY,
                requested_at REAL
            )
            """,
        )
        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(bitrix_email_processed)")}
        if "classification_json" not in cols:
            conn.execute("ALTER TABLE bitrix_email_processed ADD COLUMN classification_json TEXT")
        conn.commit()
        self._conn = conn

    def _recover_stale_processing(self) -> None:
        conn = self._require_conn()
        cutoff = time.time() - _STALE_PROCESSING_SECONDS
        conn.execute(
            """
            UPDATE bitrix_email_processed
               SET status='failed', last_error='stale processing recovery'
             WHERE status='processing' AND processing_started_at < ?
            """,
            (cutoff,),
        )
        conn.commit()

    def _cleanup_old_rows(self) -> None:
        conn = self._require_conn()
        now_ts = time.time()
        conn.execute(
            """
            DELETE FROM bitrix_email_processed
             WHERE status IN ('processed', 'failed_permanent')
               AND processed_at IS NOT NULL
               AND processed_at < ?
            """,
            (now_ts - _PROCESSED_RETENTION_SECONDS,),
        )
        conn.execute(
            """
            DELETE FROM bitrix_email_processed
             WHERE status='processing'
               AND processing_started_at < ?
            """,
            (now_ts - _STALE_PROCESSING_HARD_CLEANUP_SECONDS,),
        )
        conn.execute(
            "DELETE FROM bitrix_email_inn_requests WHERE requested_at < ?",
            (now_ts - _INN_REQUEST_TTL_SECONDS,),
        )
        conn.commit()

    def _read_last_poll_ts(self) -> float:
        conn = self._require_conn()
        row = conn.execute(
            "SELECT value FROM bitrix_email_state WHERE key='last_poll_ts'",
        ).fetchone()
        if not row:
            return time.time() - _OVERLAP_SECONDS
        try:
            return float(row["value"])
        except Exception:
            return time.time() - _OVERLAP_SECONDS

    def _write_last_poll_ts(self, ts_value: float) -> None:
        conn = self._require_conn()
        conn.execute(
            """
            INSERT INTO bitrix_email_state(key, value)
            VALUES('last_poll_ts', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (str(float(ts_value)),),
        )
        conn.commit()

    def _insert_processing_row(self, activity_id: str, source_created_at: Optional[float]) -> bool:
        conn = self._require_conn()
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO bitrix_email_processed(
                item_type, item_id, status, processing_started_at, processed_at, last_error, source_created_at, retry_count
            ) VALUES('activity', ?, 'processing', ?, NULL, NULL, ?, 0)
            """,
            (activity_id, time.time(), source_created_at),
        )
        conn.commit()
        return cur.rowcount > 0

    def _get_item_status(self, activity_id: str) -> str:
        conn = self._require_conn()
        row = conn.execute(
            """
            SELECT status
              FROM bitrix_email_processed
             WHERE item_type='activity' AND item_id=?
            """,
            (activity_id,),
        ).fetchone()
        return str(row["status"]) if row else ""

    def _mark_processed(
        self,
        activity_id: str,
        source_created_at: Optional[float],
        classification_json: Optional[str] = None,
    ) -> None:
        conn = self._require_conn()
        conn.execute(
            """
            UPDATE bitrix_email_processed
               SET status='processed',
                   processed_at=?,
                   last_error=NULL,
                   source_created_at=COALESCE(?, source_created_at),
                   classification_json=COALESCE(?, classification_json)
             WHERE item_type='activity' AND item_id=?
            """,
            (time.time(), source_created_at, classification_json, activity_id),
        )
        conn.commit()

    def _mark_open_deal_routing_skip(self, activity_id: str, source_created_at: Optional[float]) -> None:
        """Terminal processed state: open deal exists, agent skipped (cf. IMAP terminal_open_deal)."""
        conn = self._require_conn()
        cur = conn.execute(
            """
            UPDATE bitrix_email_processed
               SET status='processed',
                   processed_at=?,
                   last_error='skipped_open_deal',
                   source_created_at=COALESCE(?, source_created_at)
             WHERE item_type='activity' AND item_id=?
            """,
            (time.time(), source_created_at, activity_id),
        )
        n = cur.rowcount
        conn.commit()
        if n == 0:
            self._mark_terminal_skip_processed(
                activity_id, source_created_at, last_error="skipped_open_deal"
            )

    def _mark_terminal_skip_processed(
        self,
        activity_id: str,
        source_created_at: Optional[float],
        *,
        last_error: Optional[str] = None,
    ) -> None:
        """Record a list-phase system-mail skip so dedup and poll cursor advance without crm.activity.get."""
        conn = self._require_conn()
        now = time.time()
        conn.execute(
            """
            INSERT OR REPLACE INTO bitrix_email_processed(
                item_type, item_id, status, processing_started_at, processed_at, last_error, source_created_at, retry_count
            ) VALUES('activity', ?, 'processed', ?, ?, ?, ?, 0)
            """,
            (activity_id, now, now, last_error, source_created_at),
        )
        conn.commit()

    def _mark_system_incoming_skipped(self, activity_id: str, source_created_at: Optional[float]) -> None:
        """Mark processed after skip; use terminal upsert if no row exists (e.g. direct by-id call)."""
        if self._get_item_status(activity_id):
            self._mark_processed(activity_id, source_created_at)
        else:
            self._mark_terminal_skip_processed(activity_id, source_created_at)

    def _mark_failed(self, activity_id: str, error_text: str) -> str:
        conn = self._require_conn()
        row = conn.execute(
            """
            SELECT retry_count
              FROM bitrix_email_processed
             WHERE item_type='activity' AND item_id=?
            """,
            (activity_id,),
        ).fetchone()
        retry_count = int(row["retry_count"]) if row else 0
        retry_count += 1
        next_status = "failed_permanent" if retry_count >= 3 else "failed"
        processed_at = time.time() if next_status == "failed_permanent" else None
        conn.execute(
            """
            UPDATE bitrix_email_processed
               SET status=?,
                   processed_at=?,
                   last_error=?,
                   retry_count=?
             WHERE item_type='activity' AND item_id=?
            """,
            (next_status, processed_at, error_text[:1000], retry_count, activity_id),
        )
        conn.commit()
        return next_status

    async def _send_failed_permanent_alert(self, activity_id: str) -> None:
        token = (os.getenv("MAX_BOT_TOKEN") or "").strip()
        chat_id = (os.getenv("MAX_NOTIFY_CHAT_ID") or os.getenv("MAX_NOTIFY_CHAT") or "").strip()
        if not token or not chat_id:
            return
        payload = await asyncio.to_thread(
            self._client.call,
            "crm.activity.get",
            {"id": str(activity_id)},
        )
        snap = build_activity_snapshot(payload or {}, expected_activity_id=str(activity_id))
        idem_hash = str(snap.snapshot_hash or "") if snap.success else ""
        if not self._downstream_idem.try_claim_first_execution(
            str(activity_id),
            idem_hash,
            BitrixDownstreamEffect.MAX_NOTIFY_FAILED_PERMANENT,
        ):
            return
        text = f"⚠️ BitrixEmail: activity {activity_id} failed permanently"
        try:
            await asyncio.to_thread(send_max_text_sync, text, chat_id=chat_id, token=token)
        except Exception:
            logger.exception("[BitrixEmail] failed to send MAX permanent-failure alert")

    @staticmethod
    def _parse_bitrix_datetime(raw: Any) -> Optional[float]:
        if raw in (None, ""):
            return None
        text = str(raw).strip()
        if not text:
            return None
        try:
            # Bitrix emits RFC3339, e.g. 2026-04-13T12:32:52+03:00
            return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except Exception:
            return None

    @staticmethod
    def _format_bitrix_datetime(ts_value: float) -> str:
        return datetime.fromtimestamp(float(ts_value), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")

    def _require_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("bitrix_email database is not initialized")
        return self._conn

