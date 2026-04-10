"""Hybrid email classifier: heuristic first, LLM fallback-ready."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Set, Tuple

REQUEST_TYPES: Tuple[str, ...] = ("import", "logistics", "accounting", "status", "other")
SKIP_LLM_REASONS = {"no_evidence"}

_QUOTE_LINE_RE = re.compile(r"^\s*>+")
_ON_WROTE_RE = re.compile(r"^\s*on .+wrote:\s*$", re.IGNORECASE)
_SIGNATURE_RE = re.compile(r"^\s*(--\s*$|с уважением[,! ]*$|best regards[,! ]*$)", re.IGNORECASE)
_WS_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-zа-яё0-9][a-zа-яё0-9\-/\.]{1,}", re.IGNORECASE)


def trim_email_body(body: str, max_len: int = 1500) -> str:
    """Keep latest useful content: top + last block, no quotes/signatures."""
    text = (body or "").replace("\r\n", "\n").replace("\r", "\n")
    lines = text.split("\n")

    filtered: List[str] = []
    for line in lines:
        if _QUOTE_LINE_RE.match(line):
            continue
        if _ON_WROTE_RE.match(line):
            continue
        filtered.append(line)

    # Remove signature tail from the end block (search from the bottom).
    cut = len(filtered)
    for i in range(len(filtered) - 1, -1, -1):
        if _SIGNATURE_RE.match(filtered[i]):
            cut = i
            break
    filtered = filtered[:cut]

    # Split into non-empty blocks and keep top + last blocks.
    blocks: List[str] = []
    cur: List[str] = []
    for line in filtered:
        if line.strip():
            cur.append(line.rstrip())
            continue
        if cur:
            blocks.append("\n".join(cur).strip())
            cur = []
    if cur:
        blocks.append("\n".join(cur).strip())

    if not blocks:
        return ""
    if len(blocks) == 1:
        out = blocks[0]
    else:
        out = f"{blocks[0]}\n\n{blocks[-1]}"
    return out[:max_len].strip()


def _clean_for_length(text: str) -> str:
    return _WS_RE.sub(" ", (text or "")).strip()


def _add_event(
    *,
    bucket: str,
    raw_scores: Dict[str, float],
    event_ids: Set[str],
    event_id: str,
    weight: float,
) -> None:
    if event_id in event_ids:
        return
    event_ids.add(event_id)
    raw_scores[bucket] += float(weight)


def classify_email_heuristic(
    *,
    subject: str,
    body: str,
    follow_up: bool,
    crm_hit: bool,
    crm_open_deal_exists: bool,
) -> Dict[str, Any]:
    trimmed = trim_email_body(body)
    clean_text = _clean_for_length(trimmed)
    if len(clean_text) < 15:
        return {
            "request_type": "other",
            "confidence": 0.3,
            "source": "heuristic",
            "reason": "short_text",
            "trimmed_text": trimmed,
            "raw_scores": {k: 0.0 for k in REQUEST_TYPES},
            "evidence_h": 0,
            "needs_llm": True,
            "has_import_signal": False,
        }

    full_text = f"{subject or ''}\n{trimmed}".lower()
    raw_scores: Dict[str, float] = {k: 0.0 for k in REQUEST_TYPES}
    unique_events: Set[str] = set()
    keyword_hits: Dict[str, int] = {}
    has_import_signal = False

    # Phrase/regex signals (strong).
    phrase_signals: List[Tuple[str, str, float]] = [
        ("import", r"\bподб(о|а)р\b.*\bаналог", 4.0),
        ("import", r"\bкитай|европа|япония|производител[ья]\b", 2.0),
        ("import", r"\bзакупк[аи]\b|\bинвойс\b|\bкоммерческ\w+\s+предложен\w+", 3.0),
        ("logistics", r"\bдоставк[аи]\b|\bотгрузк[аи]\b|\bтрек\b|\bнакладн\w+", 3.0),
        ("logistics", r"\bупаковочн\w+\s+лист\b|\bконосамент\b|\bawb\b|\bcmr\b", 4.0),
        ("accounting", r"\bоплат[аи]\b|\bсчет\b|\bсч[её]т[-\s]?фактур\w+", 3.0),
        ("accounting", r"\bакт\b|\bсверк\w+\b|\bдебитор\w+\b|\bкредитор\w+\b", 3.0),
        ("status", r"\bстатус\b|\bкогда\b.*\bзаказ\b|\bсрок[и]?\b", 3.0),
        ("status", r"\bнапоминаю\b|\bожидаем\b|\bапдейт\b|\bfollow[\s-]?up\b", 4.0),
    ]
    for req_type, pattern, weight in phrase_signals:
        if re.search(pattern, full_text, flags=re.IGNORECASE):
            ev = f"phr:{req_type}:{pattern}"
            _add_event(
                bucket=req_type,
                raw_scores=raw_scores,
                event_ids=unique_events,
                event_id=ev,
                weight=weight,
            )
            if req_type == "import":
                has_import_signal = True

    # Token/keyword signals with per-keyword cap = 2.
    keyword_map: Dict[str, Tuple[str, float]] = {
        "аналог": ("import", 2.0),
        "производитель": ("import", 2.0),
        "закупка": ("import", 2.0),
        "китай": ("import", 2.0),
        "европа": ("import", 2.0),
        "япония": ("import", 2.0),
        "доставка": ("logistics", 2.0),
        "отгрузка": ("logistics", 2.0),
        "трек": ("logistics", 2.0),
        "awb": ("logistics", 2.0),
        "cmr": ("logistics", 2.0),
        "оплата": ("accounting", 2.0),
        "счет": ("accounting", 2.0),
        "акт": ("accounting", 2.0),
        "сверка": ("accounting", 2.0),
        "статус": ("status", 2.0),
        "срок": ("status", 2.0),
        "когда": ("status", 1.0),
        "напоминаю": ("status", 2.0),
    }
    for token in _TOKEN_RE.findall(full_text):
        for kw, (req_type, weight) in keyword_map.items():
            if len(kw) < 4:
                if token != kw:
                    continue
            else:
                if token != kw and not token.startswith(kw):
                    continue
            n = keyword_hits.get(kw, 0)
            if n >= 2:
                continue
            keyword_hits[kw] = n + 1
            raw_scores[req_type] += weight
            unique_events.add(f"kw:{req_type}:{kw}")
            if req_type == "import":
                has_import_signal = True

    # Structure/meta signals.
    if re.search(r"\bsku\b|артикул|pn[:\s]|p/n[:\s]", full_text, flags=re.IGNORECASE):
        _add_event(
            bucket="import",
            raw_scores=raw_scores,
            event_ids=unique_events,
            event_id="struct:sku",
            weight=2.0,
        )
        has_import_signal = True
    if re.search(r"\border\b|заказ\s*#?\s*\d+", full_text, flags=re.IGNORECASE):
        _add_event(
            bucket="status",
            raw_scores=raw_scores,
            event_ids=unique_events,
            event_id="struct:order_ref",
            weight=2.0,
        )

    # CRM/follow-up conditional STATUS boost.
    if follow_up and crm_hit:
        boost = 1.0 if has_import_signal else 2.0
        raw_scores["status"] += boost
        unique_events.add("ctx:followup_crm")

    evidence_h = len(unique_events)

    ranked = sorted(
        ((k, v) for k, v in raw_scores.items() if k != "other"),
        key=lambda x: x[1],
        reverse=True,
    )
    top1, r1 = ranked[0]
    top2, r2 = ranked[1]

    if r1 <= 0:
        return {
            "request_type": "other",
            "confidence": 0.25,
            "source": "heuristic",
            "reason": "no_evidence",
            "trimmed_text": trimmed,
            "raw_scores": raw_scores,
            "evidence_h": evidence_h,
            "needs_llm": False,
            "has_import_signal": has_import_signal,
        }

    if evidence_h < 1 or (r1 < 2.0 and not has_import_signal):
        return {
            "request_type": "other",
            "confidence": 0.25,
            "source": "heuristic",
            "reason": "no_evidence",
            "trimmed_text": trimmed,
            "raw_scores": raw_scores,
            "evidence_h": evidence_h,
            "needs_llm": False,
            "has_import_signal": has_import_signal,
        }

    margin = max(0.0, (r1 - r2) / max(r1, 1.0))
    confidence = min(1.0, 0.4 + 0.6 * margin)
    if evidence_h < 3:
        confidence *= 0.7

    if crm_open_deal_exists and top1 in {"import", "logistics"}:
        confidence *= 0.8

    pos_sum = sum(max(v, 0.0) for _, v in ranked)
    share1 = (r1 / pos_sum) if pos_sum > 0 else 0.0
    share2 = (r2 / pos_sum) if pos_sum > 0 else 0.0
    mixed_intent = share1 > 0.3 and share2 > 0.3 and margin < 0.15
    conflicting = margin < 0.15
    needs_llm = mixed_intent or conflicting
    reason = "low_confidence" if needs_llm else "heuristic_strong"

    return {
        "request_type": top1,
        "confidence": round(max(0.0, min(1.0, confidence)), 4),
        "source": "heuristic",
        "reason": reason,
        "trimmed_text": trimmed,
        "raw_scores": raw_scores,
        "evidence_h": evidence_h,
        "needs_llm": needs_llm,
        "has_import_signal": has_import_signal,
    }


def normalize_llm_result(payload: Dict[str, Any]) -> Dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    req_type = str(payload.get("request_type", "")).strip().lower()
    if req_type not in REQUEST_TYPES:
        return None
    conf_raw = payload.get("confidence", 0.0)
    if not isinstance(conf_raw, (float, int)):
        return None
    conf = max(0.0, min(1.0, float(conf_raw)))
    return {
        "request_type": req_type,
        "confidence": round(conf, 4),
        "source": "llm",
        "reason": "llm_used",
    }
