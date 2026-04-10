"""
MAX (max.ru) outbound notifications for the gateway — imperative HTTP, not tools.

POST https://platform-api.max.ru/messages (same contract as gateway/platforms/max.py).
Env: MAX_BOT_TOKEN, MAX_NOTIFY_CHAT_ID (or legacy MAX_NOTIFY_CHAT). Optional EMAIL_MAX_NOTIFY_ENABLED / platforms.email.email_max_notify_enabled.

Every processed email gets one gateway-built MAX summary; optional <<<HERMES_EMAIL_MAX>>> JSON ``text`` is merged below the baseline.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from typing import Any, Dict, Optional, Tuple

from gateway.config import _coerce_bool

logger = logging.getLogger(__name__)

MAX_API_BASE = "https://platform-api.max.ru"
MAX_MESSAGE_CHUNK = 4096
SNIPPET_LEN = 300

HERMES_EMAIL_MAX_BEGIN = "<<<HERMES_EMAIL_MAX>>>"
HERMES_EMAIL_MAX_END = "<<<END_HERMES_EMAIL_MAX>>>"

_RE_HERMES_EMAIL_MAX = re.compile(
    re.escape(HERMES_EMAIL_MAX_BEGIN) + r"\s*(.*?)\s*" + re.escape(HERMES_EMAIL_MAX_END),
    re.DOTALL,
)

# Short ops alert when the agent failed and we have no MessageEvent context.
EMAIL_MAX_FALLBACK_TEXT = (
    "[Hermes email] Агент завершился с ошибкой или без ответа — проверьте логи gateway."
)


def _load_config_yaml() -> Dict[str, Any]:
    try:
        from hermes_constants import get_hermes_home
        import yaml

        path = get_hermes_home() / "config.yaml"
        if not path.exists():
            return {}
        with open(path, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def email_max_notify_enabled() -> bool:
    """Default ON when unset; EMAIL_MAX_NOTIFY_ENABLED overrides YAML."""
    raw = os.getenv("EMAIL_MAX_NOTIFY_ENABLED")
    if raw is not None and str(raw).strip() != "":
        return _coerce_bool(str(raw).strip(), default=True)
    cfg = _load_config_yaml()
    email_cfg = (cfg.get("platforms") or {}).get("email") or {}
    if isinstance(email_cfg, dict) and "email_max_notify_enabled" in email_cfg:
        return _coerce_bool(email_cfg.get("email_max_notify_enabled"), default=True)
    return True


def parse_and_strip_hermes_email_max(text: str) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Remove <<<HERMES_EMAIL_MAX>>>…<<<END_HERMES_EMAIL_MAX>>> from the response; return JSON meta or None."""
    if not text or HERMES_EMAIL_MAX_BEGIN not in text:
        return text, None
    m = _RE_HERMES_EMAIL_MAX.search(text)
    if not m:
        return text, None
    inner = (m.group(1) or "").strip()
    cleaned = _RE_HERMES_EMAIL_MAX.sub("", text)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    meta: Optional[Dict[str, Any]] = None
    if inner:
        try:
            parsed = json.loads(inner)
            if isinstance(parsed, dict):
                meta = parsed
            else:
                logger.warning("HERMES_EMAIL_MAX JSON must be an object, got %s", type(parsed).__name__)
        except json.JSONDecodeError:
            logger.warning("HERMES_EMAIL_MAX block: invalid JSON (first 200 chars): %s", inner[:200])
    return cleaned, meta


def _snippet(s: str, max_len: int = SNIPPET_LEN) -> str:
    t = (s or "").strip()
    if len(t) <= max_len:
        return t
    return t[:max_len] + "…"


def _strip_email_context_markers(text: str) -> str:
    t = (text or "").strip()
    t = re.sub(r"\[EMAIL_CONTEXT[^\]]*\]\s*", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\[Subject:[^\]]*\]\s*", "", t, flags=re.IGNORECASE)
    return t.strip()


def _detect_type(inbound_text: str) -> str:
    t = _strip_email_context_markers(inbound_text).lower()
    follow = "follow_up=true" in (inbound_text or "").lower()
    crm_hit = "crm_hit=true" in (inbound_text or "").lower()
    if any(k in t for k in ("рекламац", "брак", "претенз", "возврат")):
        return "РЕКЛАМАЦИЯ"
    if follow and crm_hit:
        if any(k in t for k in ("статус", "ожидаю", "напомина", "когда", "апдейт")):
            return "ЗАПРОС СТАТУСА"
        return "УТОЧНЕНИЕ"
    if (not follow) and (not crm_hit):
        return "НОВЫЙ ЗАПРОС"
    if any(k in t for k in ("уточн", "детал", "подтверд", "дополн")):
        return "УТОЧНЕНИЕ"
    return "ДРУГОЕ"


def _short_summary(inbound_text: str) -> str:
    t = _strip_email_context_markers(inbound_text)
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return "Нет краткого описания запроса."
    # One compact sentence, max 80 chars.
    t = t.split(".")[0].strip() or t
    if len(t) > 80:
        t = t[:79].rstrip() + "…"
    return t


def _extract_entity_line(extra_model_text: Optional[str]) -> str:
    text = (extra_model_text or "").strip()
    if not text:
        return "Лид: —"
    m_lead = re.search(r"Лид\s*[:#]?\s*#?(\d+)", text, re.IGNORECASE)
    if m_lead:
        return f"Лид: #{m_lead.group(1)}"
    m_deal = re.search(r"Сделк[аеи]\s*[:#]?\s*#?(\d+)", text, re.IGNORECASE)
    if m_deal:
        return f"Сделка: #{m_deal.group(1)}"
    return "Лид: —"


def build_gateway_max_email_body(
    *,
    mailbox: str,
    sender: str,
    subject: str,
    session_id: str,
    inbound_text: str,
    reply_text: str,
    extra_model_text: Optional[str] = None,
    force_fallback_note: bool = False,
) -> str:
    """Single MAX message body in compact 4-line classification format."""
    msg_type = _detect_type(inbound_text)
    summary = _short_summary(inbound_text)
    entity = _extract_entity_line(extra_model_text)
    sender_line = sender or "(unknown)"
    lines = [
        f"📧 {msg_type}",
        sender_line,
        summary,
        entity,
    ]
    if force_fallback_note:
        lines.append("⚠️ Требуется проверка в gateway.log.")
    return "\n".join(lines)


def send_max_text_sync(text: str, *, chat_id: str, token: str, timeout: float = 30.0) -> bool:
    """Send one or more text chunks to MAX; returns True if all chunks returned HTTP 200."""
    import httpx

    if not text:
        text = ""
    chunks = [text[i : i + MAX_MESSAGE_CHUNK] for i in range(0, len(text), MAX_MESSAGE_CHUNK)] or [""]
    ok = True
    headers = {
        "Authorization": token,
        "Content-Type": "application/json",
    }
    url = f"{MAX_API_BASE}/messages"
    params = {"chat_id": chat_id}
    with httpx.Client(timeout=timeout) as client:
        for chunk in chunks:
            payload: Dict[str, Any] = {"text": chunk, "notify": True}
            try:
                r = client.post(url, headers=headers, params=params, json=payload)
            except Exception as exc:
                logger.warning("MAX notify POST failed: %s", exc)
                ok = False
                continue
            if r.status_code != 200:
                logger.warning(
                    "MAX notify HTTP %s: %s",
                    r.status_code,
                    (r.text or "")[:500],
                )
                ok = False
    return ok


def _max_notify_chat_id() -> str:
    """Chat id for MAX outbound; supports MAX_NOTIFY_CHAT_ID or legacy MAX_NOTIFY_CHAT in .env."""
    return (os.getenv("MAX_NOTIFY_CHAT_ID") or os.getenv("MAX_NOTIFY_CHAT") or "").strip()


def _model_extra_from_meta(meta: Optional[Dict[str, Any]]) -> Optional[str]:
    if not meta:
        return None
    t = meta.get("text")
    if isinstance(t, str) and t.strip():
        return t.strip()
    return None


async def dispatch_email_max_gateway(
    agent_result: Dict[str, Any],
    meta: Optional[Dict[str, Any]] = None,
    *,
    force_fallback: bool = False,
    event: Any = None,
    source: Any = None,
    stripped_response: str = "",
    session_id: str = "",
    inbound_text: str = "",
) -> None:
    """Send one MAX notification per email: gateway-built body + optional HERMES_EMAIL_MAX ``text``."""
    enabled = email_max_notify_enabled()
    token = (os.getenv("MAX_BOT_TOKEN") or "").strip()
    chat_id = _max_notify_chat_id()
    logger.info(
        "MAX dispatch called: enabled=%s, token_set=%s, chat_id=%s",
        enabled,
        bool(token),
        chat_id or "(empty)",
    )
    if not enabled:
        return
    if not token or not chat_id:
        logger.warning(
            "email MAX notify skipped: need MAX_BOT_TOKEN and MAX_NOTIFY_CHAT_ID (or MAX_NOTIFY_CHAT)"
        )
        return

    model_extra = _model_extra_from_meta(meta)

    # Minimal path: error with no email context
    if force_fallback and not event:
        body = EMAIL_MAX_FALLBACK_TEXT
        ok = await asyncio.to_thread(send_max_text_sync, body, chat_id=chat_id, token=token)
        if ok:
            logger.info("MAX email notify sent ok (fallback-only, chars=%s)", len(body))
        else:
            logger.warning("MAX email notify failed (fallback-only)")
        return

    mailbox = (getattr(event, "email_receive_address", None) or "").strip() if event else ""
    subject = (getattr(event, "email_subject", None) or "").strip() if event else ""
    sender = ""
    if source:
        sender = (getattr(source, "user_id", None) or getattr(source, "chat_id", None) or "").strip()

    body = build_gateway_max_email_body(
        mailbox=mailbox,
        sender=sender,
        subject=subject,
        session_id=session_id,
        inbound_text=inbound_text,
        reply_text=stripped_response,
        extra_model_text=model_extra,
        force_fallback_note=bool(force_fallback),
    )

    ok = await asyncio.to_thread(send_max_text_sync, body, chat_id=chat_id, token=token)
    if ok:
        logger.info("MAX email notify sent ok (chars=%s)", len(body))
    else:
        logger.warning("MAX email notify failed (gateway-built body; see HTTP logs above)")
