"""
Unified Spare24 channel HTML formatting (💡 Вывод + Купол signature + hashtag cleanup).

Single source of truth for Content Curator and Hermes MAX paths.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# Default Spare24 public channel (MAX); must match content_curator enrichment config.
SPARE24_CHANNEL_CHAT_ID = "-72597598792341"

_DEFAULT_CONCLUSION = "Материал подготовлен для профессионалов отрасли."
_DEFAULT_KUPOL = (
    "<i>Купол активно работает с B2B-тематикой и промышленным оборудованием для канала Spare24.</i>"
)


def _spare24_format_exclude_abs_ids() -> Set[int]:
    """Chat ids that must never receive Spare24 HTML formatting (emergency / misconfig fix).

    Env: ``SPARE24_FORMAT_EXCLUDE_CHAT_IDS`` comma-separated (e.g. user DM the bot uses).
    """
    raw = (os.getenv("SPARE24_FORMAT_EXCLUDE_CHAT_IDS") or "").strip()
    out: Set[int] = set()
    for p in raw.split(","):
        p = p.strip()
        if not p:
            continue
        try:
            out.add(abs(int(p)))
        except ValueError:
            continue
    return out


def _spare24_channel_abs_ids() -> Set[int]:
    """Numeric channel ids (abs) that count as Spare24 **public channel** for outbound formatting.

    Includes the built-in default and optional ``SPARE24_CHANNEL_CHAT_IDS``.

    **Important:** ``MAX_CHANNEL_ID`` is intentionally **not** included. It is also used for
    image relay / notify flows and is sometimes set to a **non-channel** chat by mistake,
    which caused 💡/Купол to be applied to user DMs. Use ``SPARE24_CHANNEL_CHAT_IDS`` for
    alternate **channel** numeric ids only.
    """
    ids: Set[int] = set()
    parts = [SPARE24_CHANNEL_CHAT_ID.strip()]
    extra = (os.getenv("SPARE24_CHANNEL_CHAT_IDS") or "").strip()
    if extra:
        parts.extend(p.strip() for p in extra.split(",") if p.strip())
    for p in parts:
        try:
            ids.add(abs(int(p)))
        except ValueError:
            continue
    return ids


def is_spare24_channel_chat_id(chat_id: Any) -> bool:
    """True if chat_id refers to Spare24 public channel (handles signed/unsigned).

    Matches ``SPARE24_CHANNEL_CHAT_ID`` and optional comma-separated ``SPARE24_CHANNEL_CHAT_IDS``.

    ``SPARE24_FORMAT_EXCLUDE_CHAT_IDS`` forces False even if the id would otherwise match
    (safety valve).
    """
    if chat_id is None:
        return False
    try:
        n = abs(int(chat_id))
        if n in _spare24_format_exclude_abs_ids():
            return False
        return n in _spare24_channel_abs_ids()
    except (TypeError, ValueError):
        pass
    s = str(chat_id).strip()
    try:
        n = abs(int(s))
        if n in _spare24_format_exclude_abs_ids():
            return False
    except ValueError:
        n = None
    if s == SPARE24_CHANNEL_CHAT_ID.strip():
        return True
    for p in (os.getenv("SPARE24_CHANNEL_CHAT_IDS") or "").split(","):
        if p.strip() and s == p.strip():
            return True
    return False


def manual_review_chat_ids() -> Set[str]:
    """
    MAX chat ids used for manual post review (Content Curator ``review_chat_id`` / ``max_chat_id``).

    Override: env ``SPARE24_MANUAL_REVIEW_CHAT_IDS`` comma-separated (default ``93762437``).
    """
    raw = (os.getenv("SPARE24_MANUAL_REVIEW_CHAT_IDS") or "93762437").strip()
    return {p.strip() for p in raw.split(",") if p.strip()}


def is_manual_review_chat_id(chat_id: Any) -> bool:
    """True if chat_id is a manual review chat (handles signed/unsigned)."""
    if chat_id is None:
        return False
    ids = manual_review_chat_ids()
    s = str(chat_id).strip()
    if s in ids:
        return True
    try:
        n = abs(int(s))
        for rid in ids:
            try:
                if n == abs(int(rid)):
                    return True
            except ValueError:
                continue
    except (TypeError, ValueError):
        pass
    return False


def apply_spare24_formatting_for_max_outbound(chat_id: Any, text: str) -> str:
    """
    Hermes MAX outbound: apply Spare24 HTML rules only for the public channel.

    Manual review chats, DMs, and support threads must not get 💡/Купол appended here —
    those messages are not channel posts; drafts are already normalized in Content Curator
    (``enricher`` / ``publisher``) before publish.
    """
    want = is_spare24_channel_chat_id(chat_id)
    logger.debug(
        "SPARE24_OUTBOUND_DECISION chat_id=%r is_channel=%s exclude_ids_env=%s "
        "channel_ids_env=%s text_len=%s",
        chat_id,
        want,
        bool((os.getenv("SPARE24_FORMAT_EXCLUDE_CHAT_IDS") or "").strip()),
        bool((os.getenv("SPARE24_CHANNEL_CHAT_IDS") or "").strip()),
        len(text or ""),
    )
    if want:
        logger.warning(
            "SPARE24_CHANNEL_FORMAT_APPLIED chat_id=%r (Spare24 💡/Купол HTML rules)",
            chat_id,
        )
        return apply_post_formatting_rules(text or "")
    return text or ""


def _remove_hashtag_paragraphs(body: str) -> str:
    """Drop paragraphs whose visible text starts with ``#`` (hashtag-only / hashtag lines)."""
    if not (body or "").strip():
        return body or ""
    parts = re.split(r"(?:\n\s*\n|<br\s*/?>\s*<br\s*/?>)", body, flags=re.I)
    kept: List[str] = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        plain = re.sub(r"<[^>]+>", " ", p)
        plain = re.sub(r"\s+", " ", plain).strip()
        if plain.startswith("#"):
            continue
        kept.append(p)
    return "<br/><br/>".join(kept)


def _move_kupol_italic_to_end(body: str) -> str:
    """Paragraph containing «Купол активно работает» → wrap in <i> and place last."""
    if not (body or "").strip():
        return body or ""
    parts = re.split(r"(?:\n\s*\n|<br\s*/?>\s*<br\s*/?>)", body, flags=re.I)
    parts = [p.strip() for p in parts if p.strip()]
    kupol_idx: Optional[int] = None
    for i, p in enumerate(parts):
        if re.search(r"Купол\s+активно\s+работает", p, flags=re.I):
            kupol_idx = i
            break
    if kupol_idx is None:
        return body.strip()
    block = parts.pop(kupol_idx)
    wrapped = f"<i>{block.strip()}</i>"
    rest = "<br/><br/>".join(parts)
    if rest:
        return f"{rest}<br/><br/>{wrapped}"
    return wrapped


def _extract_conclusion_and_strip_trigger(s: str) -> Tuple[str, str]:
    """Remove the «Что это значит на практике?» paragraph; return (remaining HTML, conclusion text)."""
    default = _DEFAULT_CONCLUSION
    parts = re.split(r"(?:\n\s*\n|<br\s*/?>\s*<br\s*/?>)", s, flags=re.I)
    parts = [p.strip() for p in parts if p.strip()]
    out_parts: List[str] = []
    conclusion = default
    for p in parts:
        m = re.match(
            r"Что\s+это\s+значит\s+на\s+практике\??\s*(.*)$",
            p,
            flags=re.I | re.S,
        )
        if m:
            inner = (m.group(1) or "").strip()
            if inner:
                conclusion = inner
            continue
        out_parts.append(p)
    return "<br/><br/>".join(out_parts), conclusion


def apply_post_formatting_rules(body: str, *, apply_channel_formatting: bool = True) -> str:
    """
    Deterministic formatting: append 💡 conclusion block and Купол signature (channel posts only).

    Set ``apply_channel_formatting=False`` to leave body unchanged (e.g. non-channel enrichment).

    - Removes hashtag-only paragraphs
    - If a «Что это значит на практике?» paragraph exists, its text after the question becomes
      the conclusion; the paragraph is removed from the body
    - Appends ``<b>💡 Вывод:</b> [conclusion]`` when not already present
    - Ensures a Купол ``<i>…</i>`` line at the very end (existing plain-text Купол is wrapped and moved)
    """
    s = (body or "").strip()
    if not apply_channel_formatting:
        return s
    logger.debug("Formatting input: %s", s[:200])
    if not s:
        logger.debug("Formatting output: %s", s[:200])
        logger.debug("Has vyvod: %s", "💡" in s)
        logger.debug("Has kupol: %s", "<i>Купол" in s)
        return s

    s = _remove_hashtag_paragraphs(s)
    s, conclusion = _extract_conclusion_and_strip_trigger(s)

    if "💡 Вывод" not in s:
        vyvod = f"<b>💡 Вывод:</b> {conclusion}"
        s = f"{s}<br/><br/>{vyvod}" if s else vyvod

    if "<i>Купол" not in s and "Купол активно работает" not in s:
        s = f"{s}<br/><br/>{_DEFAULT_KUPOL}"
    elif "Купол активно работает" in s and "<i>Купол" not in s:
        s = _move_kupol_italic_to_end(s)

    result = s.strip()
    logger.debug("Formatting output: %s", result[:200])
    logger.debug("Has vyvod: %s", "💡" in result)
    logger.debug("Has kupol: %s", "<i>Купол" in result)
    return result
