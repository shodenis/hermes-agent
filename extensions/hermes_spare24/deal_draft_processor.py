"""Deal draft extraction: email → DRY RUN in MAX; max → interactive field collection + optional CRM deal.

See RULE_DealDraftProcessor.md. MAX creates at most one deal per request_key (chat_id:ts_ms:counter); email path unchanged.
MAX CRM path uses SQLite claim-before-CRM (pending → done/failed) with TTL reclaim for stale pending (DEAL_DRAFT_PENDING_TTL_SEC).
CRM idempotency: RK stored in DEAL_DRAFT_BITRIX_RK_UF when set, else appended to deal TITLE as a line RK:<request_key>; find_deal_by_request_key scans UF, COMMENTS, TITLE; COMMENTS hold only CN/RU technical blocks (no RK / no duplicate 【CN кратко】 — short Chinese title goes to DEAL_DRAFT_BITRIX_CN_TITLE_UF only).
Optional background reconcile (DEAL_DRAFT_PENDING_RECONCILE_ENABLED, INTERVAL_SEC, BATCH, SNAPSHOT_INTERVAL_SEC, RECONCILE_MIN_INTERVAL_MS, BACKOFF_BASE_SEC, BACKOFF_MAX_SEC): read-only CRM find + finalize pending; never create_deal. Manual: reconcile_request_key(rk). stop_reconcile_worker() for shutdown. Alerts: DEAL_DRAFT_PENDING_ALERT_COUNT, DEAL_DRAFT_PENDING_ALERT_AGE_SEC, DEAL_DRAFT_PENDING_ALERT_COOLDOWN_SEC (with snapshot).
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import re
import sqlite3
import threading
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Literal, Optional, Tuple

from hermes_cli.config import get_hermes_home

from extensions.hermes_spare24.crm.bitrix.service import BitrixService
from extensions.hermes_spare24.crm.bitrix.types import RKLookupResult
from extensions.hermes_spare24.model_lockdown import (
    EXPECTED_BASE_URL,
    EXPECTED_MODEL,
    assert_effective_model_or_raise,
)
from extensions.hermes_spare24.notifications.max_notify import send_max_text_sync

logger = logging.getLogger(__name__)

_max_sessions_lock = threading.Lock()
_max_sessions: Dict[str, Dict[str, Any]] = {}
# MAX channel only: in-flight / memory mirror keyed by request_key (see _make_request_key)
_max_deal_state: Dict[str, Dict[str, Any]] = {}

_counter_lock = threading.Lock()
_request_counter = 0

_chat_locks_guard = threading.Lock()
_chat_locks: Dict[str, threading.Lock] = {}


def _next_counter() -> int:
    global _request_counter
    with _counter_lock:
        _request_counter += 1
        return _request_counter


def _get_chat_lock(chat_id: str) -> threading.Lock:
    cid = str(chat_id or "").strip()
    if not cid:
        raise ValueError("chat_id required for per-chat lock")
    with _chat_locks_guard:
        if cid not in _chat_locks:
            _chat_locks[cid] = threading.Lock()
        return _chat_locks[cid]


def _normalize_rk_lookup(raw: Any) -> RKLookupResult:
    """Map legacy int/str mocks or RKLookupResult from find_deal_by_request_key."""
    if isinstance(raw, RKLookupResult):
        return raw
    if isinstance(raw, int) and raw > 0:
        return RKLookupResult("hit", raw)
    if isinstance(raw, str) and raw.strip().isdigit():
        i = int(raw.strip())
        if i > 0:
            return RKLookupResult("hit", i)
    return RKLookupResult("miss", None)


def _parse_request_key_parts(request_key: str) -> Tuple[bool, str, str]:
    """Validate chat_id:ts_ms:counter shape; ts_ms and counter must be decimal digit strings."""
    parts = str(request_key or "").split(":", 2)
    if len(parts) < 3:
        return False, "", ""
    ts_ms, counter = parts[1], parts[2]
    if not ts_ms.isdigit() or not counter.isdigit():
        return False, "", ""
    return True, ts_ms, counter


_BLOCKING_ITEM_KEYS = ("brand", "component_type", "part_number", "condition")
_BLOCKING_CLIENT_KEYS = ("client_name", "contact_person", "phone")


def _merge_parsed_with_prior(parsed: Dict[str, Any], prior: Optional[Dict[str, Any]]) -> None:
    """Fill empty fields after re-extract using previous draft (LLM often drops fields on long RFQ)."""
    if not isinstance(parsed, dict) or not isinstance(prior, dict):
        return

    prev_items = prior.get("items")
    new_items = parsed.get("items")

    if isinstance(prev_items, list) and prev_items:
        if not isinstance(new_items, list) or len(new_items) == 0:
            parsed["items"] = copy.deepcopy(prev_items)
            new_items = parsed["items"]

        if isinstance(new_items, list):
            for i, it in enumerate(new_items):
                if not isinstance(it, dict):
                    continue
                if i >= len(prev_items) or not isinstance(prev_items[i], dict):
                    continue
                pv = prev_items[i]
                for fk in _BLOCKING_ITEM_KEYS:
                    if str(it.get(fk) or "").strip():
                        continue
                    if pv.get(fk) is not None and str(pv.get(fk) or "").strip():
                        it[fk] = pv[fk]
                try:
                    qn = int(it.get("quantity")) if it.get("quantity") is not None else 0
                except (TypeError, ValueError):
                    qn = 0
                if qn <= 0:
                    pq = pv.get("quantity")
                    try:
                        pq_n = int(pq) if pq is not None else 0
                    except (TypeError, ValueError):
                        pq_n = 0
                    if pq_n > 0:
                        it["quantity"] = pq_n
                if not str(it.get("unit") or "").strip():
                    pu = pv.get("unit")
                    if pu is not None and str(pu).strip():
                        it["unit"] = pu

    nc = parsed.get("client")
    pc = prior.get("client")
    if isinstance(nc, dict) and isinstance(pc, dict):
        for fk in _BLOCKING_CLIENT_KEYS:
            if str(nc.get(fk) or "").strip():
                continue
            pv = pc.get(fk)
            if pv is not None and str(pv).strip():
                nc[fk] = pv
        if not str(nc.get("_bitrix_contact_id") or "").strip() and str(
            pc.get("_bitrix_contact_id") or ""
        ).strip():
            nc["_bitrix_contact_id"] = pc.get("_bitrix_contact_id")


def _condition_reply_hint(text: str) -> bool:
    low = (text or "").lower()
    return any(
        x in low
        for x in (
            "новый",
            "новая",
            "новое",
            "б/у",
            "бу ",
            " б/у",
            "used",
            "new",
            "refurb",
            "восстанов",
            "ремонт",
        )
    ) or len((text or "").strip()) <= 48


def _apply_followup_reply_to_missing(
    parsed: Dict[str, Any],
    raw_followup: str,
    missing_keys: List[str],
) -> None:
    """Map short operator replies onto fields listed as missing (fixes LLM losing condition on follow-up)."""
    text = (raw_followup or "").strip()
    if not text or len(text) > 600:
        return
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) > 4:
        return
    snippet = lines[0] if lines else text

    uniq = sorted(set(str(x) for x in missing_keys if x))
    if not uniq:
        return

    items = parsed.get("items") if isinstance(parsed.get("items"), list) else None
    client = parsed.get("client") if isinstance(parsed.get("client"), dict) else None

    def apply_one(path_key: str, value: str) -> None:
        mo = re.match(r"^items\[(\d+)\]\.(.+)$", path_key)
        if mo and isinstance(items, list):
            idx = int(mo.group(1))
            field = mo.group(2)
            if idx >= len(items):
                return
            it = items[idx]
            if not isinstance(it, dict):
                return
            if str(it.get(field) or "").strip():
                return
            if field == "quantity":
                digits = re.sub(r"[^\d]", "", value)
                if digits:
                    try:
                        it["quantity"] = max(1, int(digits[:9]))
                    except ValueError:
                        pass
                return
            it[field] = value.strip()
            return
        mo2 = re.match(r"^client\.(.+)$", path_key)
        if mo2 and isinstance(client, dict):
            fk = mo2.group(1)
            if fk.startswith("_"):
                return
            if str(client.get(fk) or "").strip():
                return
            client[fk] = value.strip()

    if len(uniq) == 1:
        apply_one(uniq[0], snippet)
        return

    cond_paths = [k for k in uniq if re.match(r"^items\[\d+\]\.condition$", k)]
    if cond_paths and _condition_reply_hint(snippet):
        for ck in cond_paths:
            apply_one(ck, snippet)


def _phone_from_bitrix_contact(row: Dict[str, Any]) -> str:
    ph = row.get("PHONE")
    if isinstance(ph, list):
        for entry in ph:
            if isinstance(entry, dict):
                v = str(entry.get("VALUE") or "").strip()
                if v:
                    return v
    return ""


def _pick_best_contact_row(
    contacts: List[Dict[str, Any]], query: str
) -> Optional[Dict[str, Any]]:
    if not contacts:
        return None
    q_lower = " ".join(str(query).lower().split())
    best: Optional[Dict[str, Any]] = None
    best_score = -1
    for row in contacts:
        name = str(row.get("NAME") or "").strip()
        last = str(row.get("LAST_NAME") or "").strip()
        full = " ".join(f"{name} {last}".strip().lower().split())
        phone = _phone_from_bitrix_contact(row)
        score = 0
        if full and full == q_lower:
            score += 100
        elif full and (q_lower in full or full in q_lower):
            score += 55
        elif q_lower.split() and all(
            tok in full for tok in q_lower.split() if len(tok) > 1
        ):
            score += 45
        if phone:
            score += 30
        if score > best_score:
            best_score = score
            best = row
    return best if best is not None else contacts[0]


def _lookup_query_from_client(client: Dict[str, Any]) -> str:
    cp = str(client.get("contact_person") or "").strip()
    if cp:
        return cp
    return str(client.get("client_name") or "").strip()


def _enrich_client_from_bitrix(parsed: Dict[str, Any]) -> None:
    """Fill client.phone / contact_person from CRM; store _bitrix_contact_id for deal CONTACT_ID."""
    svc = BitrixService.from_env()
    if svc is None:
        return
    client = parsed.get("client") if isinstance(parsed.get("client"), dict) else None
    if not isinstance(client, dict):
        return
    q = _lookup_query_from_client(client)
    if len(q) < 2:
        return
    need_phone = not str(client.get("phone") or "").strip()
    need_cp = not str(client.get("contact_person") or "").strip()
    try:
        rows = svc.list_contacts_by_name_guess(q)
    except Exception as exc:
        logger.warning("[DealDraft] Bitrix contact lookup error: %s", exc)
        return
    if not isinstance(rows, list):
        return
    if not rows:
        logger.info("[DealDraft] Bitrix contact lookup miss query=%s", q[:160])
        return
    best = _pick_best_contact_row(rows, q)
    if not isinstance(best, dict):
        return
    rid = best.get("ID")
    if rid is not None and str(rid).strip():
        client["_bitrix_contact_id"] = str(rid).strip()
    phone = _phone_from_bitrix_contact(best)
    if need_phone and phone:
        client["phone"] = phone
    name = str(best.get("NAME") or "").strip()
    last = str(best.get("LAST_NAME") or "").strip()
    display = f"{name} {last}".strip()
    if need_cp and display:
        client["contact_person"] = display
    logger.info(
        "[DealDraft] Bitrix contact enrich contact_id=%s filled_phone=%s filled_contact=%s",
        best.get("ID"),
        bool(need_phone and phone),
        bool(need_cp and display),
    )


async def _enrich_client_from_bitrix_async(parsed: Dict[str, Any]) -> None:
    await asyncio.to_thread(_enrich_client_from_bitrix, parsed)


def _deal_draft_enabled() -> bool:
    v = (os.getenv("DEAL_DRAFT_EMAIL_ENABLED") or "1").strip().lower()
    return v not in ("0", "false", "no", "off")


def deal_draft_max_enabled() -> bool:
    v = (os.getenv("DEAL_DRAFT_MAX_ENABLED") or "0").strip().lower()
    return v in ("1", "true", "yes", "on")


def deal_draft_max_session_waiting(chat_id: str) -> bool:
    """True if this MAX chat is in multi-turn DealDraft field collection."""
    sid = str(chat_id or "").strip()
    if not sid:
        return False
    with _max_sessions_lock:
        sess = _max_sessions.get(sid)
    if not sess:
        return False
    return str(sess.get("phase") or "") == "waiting_operator"


def deal_draft_create_enabled() -> bool:
    return os.getenv("DEAL_DRAFT_CREATE_ENABLED", "").lower() in ("1", "true", "yes", "on")


def _claim_pending_ttl_sec() -> float:
    raw = (os.getenv("DEAL_DRAFT_PENDING_TTL_SEC") or "60").strip()
    try:
        v = float(raw)
        return max(1.0, v)
    except (TypeError, ValueError):
        return 60.0


def _reconcile_interval_sec() -> float:
    raw = (os.getenv("DEAL_DRAFT_PENDING_RECONCILE_INTERVAL_SEC") or "90").strip()
    try:
        return max(30.0, float(raw))
    except (TypeError, ValueError):
        return 90.0


def _reconcile_snapshot_interval_sec() -> float:
    raw = (os.getenv("DEAL_DRAFT_PENDING_SNAPSHOT_INTERVAL_SEC") or "300").strip()
    try:
        return max(60.0, float(raw))
    except (TypeError, ValueError):
        return 300.0


def _reconcile_batch_limit() -> int:
    raw = (os.getenv("DEAL_DRAFT_PENDING_RECONCILE_BATCH") or "50").strip()
    try:
        return max(1, min(int(raw), 500))
    except (TypeError, ValueError):
        return 50


def _reconcile_min_lookup_interval_sec() -> float:
    raw = (os.getenv("DEAL_DRAFT_RECONCILE_MIN_INTERVAL_MS") or "75").strip()
    try:
        ms = float(raw)
        return max(0.0, ms / 1000.0)
    except (TypeError, ValueError):
        return 0.075


def _reconcile_backoff_base_sec() -> float:
    raw = (os.getenv("DEAL_DRAFT_RECONCILE_BACKOFF_BASE_SEC") or "1.0").strip()
    try:
        return max(0.01, float(raw))
    except (TypeError, ValueError):
        return 1.0


def _reconcile_backoff_max_sec() -> float:
    raw = (os.getenv("DEAL_DRAFT_RECONCILE_BACKOFF_MAX_SEC") or "30.0").strip()
    try:
        return max(0.05, float(raw))
    except (TypeError, ValueError):
        return 30.0


# Snapshot alert defaults (override with env DEAL_DRAFT_PENDING_ALERT_COUNT / DEAL_DRAFT_PENDING_ALERT_AGE_SEC).
DEAL_DRAFT_PENDING_ALERT_COUNT = 100
DEAL_DRAFT_PENDING_ALERT_AGE_SEC = 600
DEAL_DRAFT_PENDING_ALERT_COOLDOWN_SEC = 300


def _pending_alert_count_threshold() -> int:
    raw = (os.getenv("DEAL_DRAFT_PENDING_ALERT_COUNT") or str(DEAL_DRAFT_PENDING_ALERT_COUNT)).strip()
    try:
        return max(0, int(raw))
    except (TypeError, ValueError):
        return DEAL_DRAFT_PENDING_ALERT_COUNT


def _pending_alert_age_sec() -> float:
    raw = (os.getenv("DEAL_DRAFT_PENDING_ALERT_AGE_SEC") or str(DEAL_DRAFT_PENDING_ALERT_AGE_SEC)).strip()
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return float(DEAL_DRAFT_PENDING_ALERT_AGE_SEC)


def _pending_alert_cooldown_sec() -> float:
    raw = (os.getenv("DEAL_DRAFT_PENDING_ALERT_COOLDOWN_SEC") or str(DEAL_DRAFT_PENDING_ALERT_COOLDOWN_SEC)).strip()
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return float(DEAL_DRAFT_PENDING_ALERT_COOLDOWN_SEC)


def _coerce_row_unix_ts(val: Any, fallback: float) -> float:
    if val is None:
        return fallback
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if s.replace(".", "", 1).replace("-", "", 1).isdigit():
        try:
            return float(s)
        except ValueError:
            pass
    try:
        from datetime import datetime

        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(s[:19], fmt).timestamp()
            except ValueError:
                continue
    except Exception:
        pass
    return fallback


def _make_request_key(chat_id: str, ts_ms: int, counter: int) -> str:
    """Idempotency key: one RFQ; counter disambiguates same-ms bursts (per-process monotonic)."""
    cid = str(chat_id or "").strip()
    return f"{cid}:{int(ts_ms)}:{int(counter)}"


def _deal_title_product_segment(data: Dict[str, Any]) -> str:
    """Product line for deal title: brands + position count (no client)."""
    items = data.get("items") if isinstance(data.get("items"), list) else []
    n = len(items)
    brands: List[str] = []
    for it in items:
        if isinstance(it, dict):
            b = str(it.get("brand") or "").strip()
            if b:
                brands.append(b)
    uniq = sorted(set(brands))
    if not uniq:
        return f"Заявка — {n} поз."
    if len(uniq) == 1:
        return f"{uniq[0]} — {n} поз."
    return f"{uniq[0]} + др. — {n} поз."


def _parse_client_line_after_keyword(text: str) -> str:
    """Return raw client substring from user text after whole-word 'Клиент' (first line only).

    Empty string if keyword missing or no non-empty remainder. No CRM substitution.
    Leading punctuation after the keyword is stripped; inner spacing is preserved.
    """
    if not (text or "").strip():
        return ""
    m = re.search(r"(?i)(?<!\w)Клиент(?!\w)", text)
    if not m:
        return ""
    rest = text[m.end() :]
    rest = re.sub(r"^[\s:.,;\-–—]+", "", rest)
    if not rest:
        return ""
    line = rest.split("\n", 1)[0].strip()
    return line


def _build_deal_title(parsed: Dict[str, Any], raw_user_text: str) -> str:
    """Bitrix deal TITLE: date + product segment + client segment from raw input only."""
    date_s = datetime.now().strftime("%d.%m.%y")
    product = _deal_title_product_segment(parsed)
    raw_seg = _parse_client_line_after_keyword(raw_user_text or "")
    chosen = raw_seg if raw_seg else "Без клиента"
    crm_name = ""
    cl = parsed.get("client")
    if isinstance(cl, dict):
        crm_name = str(cl.get("client_name") or "").strip()
    logger.info(
        "event=deal_title_client_source raw_input_client=%r crm_contact_name=%r "
        "chosen_client_name=%r decision=raw_input_used",
        raw_seg,
        crm_name,
        chosen,
    )
    return f"Сделка {date_s} - {product} - {chosen}"


def _parsed_copy_for_llm(parsed: Dict[str, Any]) -> Dict[str, Any]:
    """Drop internal keys (e.g. _bitrix_contact_id) before sending draft JSON to LLM."""
    out: Dict[str, Any] = {}
    for k, v in parsed.items():
        if str(k).startswith("_"):
            continue
        if k == "client" and isinstance(v, dict):
            out[k] = {
                ck: cv
                for ck, cv in v.items()
                if not str(ck).startswith("_")
            }
        else:
            out[k] = v
    return out


def _format_deal_comments_user_prompt(parsed: Dict[str, Any], source_text: str) -> str:
    """Structured JSON + optional full RFQ narrative so COMMENTS can include all specs from chat/email."""
    payload = json.dumps(_parsed_copy_for_llm(parsed), ensure_ascii=False)
    if len(payload) > 16000:
        payload = payload[:16000] + "\n...[json_truncated]"
    src = (source_text or "").strip()
    if len(src) > 28000:
        src = src[:28000] + "\n...[source_truncated]"
    blocks = [
        "Структурированные данные (канонические поля клиента и позиций):\n" + payload,
    ]
    if src:
        blocks.append(
            "Исходный текст заявки / переписки (извлеките из него ВСЕ технические детали для развёрнутых абзацев "
            "(интерфейсы, электропитание, ток, IP, взрывозащита, OEM/комплекты и т.д.); текст может быть на нескольких языках:\n"
            + src
        )
    else:
        blocks.append("Исходный текст заявки отсутствует — опирайтесь только на структурированный JSON.")
    return "\n\n".join(blocks)


def _format_deal_comments_sync(
    parsed: Dict[str, Any],
    source_text: str = "",
    *,
    path_label: str = "deal_draft_format_max_deal",
) -> Tuple[str, str]:
    """CN/RU blocks for deal COMMENTS — JSON + full source text when available."""
    try:
        fmt = _call_chat_json(
            user_prompt=_format_deal_comments_user_prompt(parsed, source_text),
            system_prompt=_FORMAT_SYSTEM,
            path_label=path_label,
        )
        if isinstance(fmt, dict):
            return (
                str(fmt.get("cn_block") or "").strip(),
                str(fmt.get("ru_block") or "").strip(),
            )
    except Exception as e:
        logger.warning("[DealDraft] format LLM failed (deal comments): %s", e)
    return "", ""


def _max_notify_config() -> Tuple[str, str]:
    # Profile-scoped: Bitrix gateway must use this profile's MAX_BOT_TOKEN only; never merge with hermes-main.
    # Notify target: MAX_NOTIFY_CHAT_ID (e.g. operator DM) overrides legacy MAX_NOTIFY_CHAT — see docs/ENV_SEMANTICS.md.
    token = (os.getenv("MAX_BOT_TOKEN") or "").strip()
    chat_id = (os.getenv("MAX_NOTIFY_CHAT_ID") or os.getenv("MAX_NOTIFY_CHAT") or "").strip()
    return token, chat_id


class _MaxDealsDoneStore:
    """Durable idempotency: claim-before-CRM in SQLite; one deal per request_key across restarts."""

    def __init__(self, path: Optional[Any] = None) -> None:
        self._path = Path(path).expanduser() if path else (Path.home() / "deal_draft_max_done.db")
        self._ensure_lock = threading.Lock()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(str(self._path), timeout=30.0)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        return c

    def _create_table_v2(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE max_deals_done (
              request_key TEXT PRIMARY KEY NOT NULL,
              chat_id TEXT NOT NULL,
              deal_id INTEGER,
              status TEXT NOT NULL,
              created_at REAL NOT NULL,
              updated_at REAL NOT NULL,
              error TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_max_deals_done_chat_id ON max_deals_done(chat_id)"
        )

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='max_deals_done'"
        ).fetchone()
        if row is None:
            self._create_table_v2(conn)
            conn.commit()
            return

        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(max_deals_done)")}
        if "request_key" not in cols:
            conn.execute("ALTER TABLE max_deals_done RENAME TO max_deals_done_legacy")
            self._create_table_v2(conn)
            now = time.time()
            conn.execute(
                """
                INSERT INTO max_deals_done (request_key, chat_id, deal_id, status, created_at, updated_at, error)
                SELECT chat_id || ':legacy', chat_id,
                       COALESCE(deal_id, 0), 'done', ?, ?, NULL
                FROM max_deals_done_legacy
                """,
                (now, now),
            )
            conn.execute("DROP TABLE max_deals_done_legacy")
            conn.commit()
            return

        if "status" not in cols:
            now = time.time()
            legacy_rows = conn.execute("SELECT * FROM max_deals_done").fetchall()
            conn.execute("DROP TABLE max_deals_done")
            self._create_table_v2(conn)
            for r in legacy_rows:
                rd = dict(r)
                rk = str(rd.get("request_key") or "").strip()
                cid = str(rd.get("chat_id") or "").strip()
                did = rd.get("deal_id")
                ca = rd.get("created_at")
                ts = _coerce_row_unix_ts(ca, now)
                try:
                    did_i = int(did) if did is not None else None
                except (TypeError, ValueError):
                    did_i = None
                conn.execute(
                    """
                    INSERT INTO max_deals_done (
                      request_key, chat_id, deal_id, status, created_at, updated_at, error
                    ) VALUES (?, ?, ?, 'done', ?, ?, NULL)
                    """,
                    (rk, cid, did_i, ts, ts),
                )
            conn.commit()
            return

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_max_deals_done_chat_id ON max_deals_done(chat_id)"
        )
        conn.commit()

    def _ensure(self) -> None:
        with self._ensure_lock:
            with self._conn() as conn:
                self._migrate_schema(conn)

    def lookup_deal_id(self, request_key: str) -> Optional[int]:
        rk = str(request_key or "").strip()
        if not rk:
            return None
        self._ensure()
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT deal_id FROM max_deals_done
                WHERE request_key=? AND status='done' AND deal_id IS NOT NULL
                LIMIT 1
                """,
                (rk,),
            ).fetchone()
        if row is None:
            return None
        raw = row["deal_id"]
        try:
            return int(raw) if raw is not None else None
        except (TypeError, ValueError):
            return None

    def try_acquire_claim(self, request_key: str, chat_id: str, *, ttl_sec: float) -> bool:
        """INSERT pending (ON CONFLICT DO NOTHING) or reclaim failed / expired pending. True = may call CRM."""
        rk = str(request_key or "").strip()
        cid = str(chat_id or "").strip()
        if not rk or not cid:
            return False
        now = time.time()
        ttl = max(1.0, float(ttl_sec))
        self._ensure()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO max_deals_done (
                  request_key, chat_id, deal_id, status, created_at, updated_at, error
                ) VALUES (?, ?, NULL, 'pending', ?, ?, NULL)
                ON CONFLICT(request_key) DO NOTHING
                """,
                (rk, cid, now, now),
            )
            ins = int(conn.execute("SELECT changes() AS c").fetchone()["c"] or 0)
            conn.commit()
            if ins > 0:
                logger.info(
                    "[DealDraft] claim_acquired chat_id=%s request_key=%s status=pending "
                    "decision=claim path=insert",
                    cid,
                    rk,
                )
                return True

            row = conn.execute(
                """
                SELECT status, deal_id, updated_at, error
                FROM max_deals_done WHERE request_key=? LIMIT 1
                """,
                (rk,),
            ).fetchone()

        if row is None:
            logger.error(
                "[DealDraft] claim_rejected chat_id=%s request_key=%s decision=reject reason=row_missing",
                cid,
                rk,
            )
            return False

        st = str(row["status"] or "").strip().lower()
        if st == "done" and row["deal_id"] is not None:
            logger.info(
                "[DealDraft] claim_rejected chat_id=%s request_key=%s status=done deal_id=%s "
                "decision=reject reason=already_done",
                cid,
                rk,
                row["deal_id"],
            )
            return False

        if st == "failed":
            with self._conn() as conn:
                conn.execute(
                    """
                    UPDATE max_deals_done
                    SET status='pending', deal_id=NULL, updated_at=?, error=NULL, chat_id=?
                    WHERE request_key=? AND status='failed'
                    """,
                    (now, cid, rk),
                )
                ch = int(conn.execute("SELECT changes() AS c").fetchone()["c"] or 0)
                conn.commit()
            if ch > 0:
                logger.info(
                    "[DealDraft] claim_acquired chat_id=%s request_key=%s status=pending "
                    "decision=claim path=reclaim_failed",
                    cid,
                    rk,
                )
                return True
            logger.info(
                "[DealDraft] claim_rejected chat_id=%s request_key=%s status=failed decision=reject reason=reclaim_race",
                cid,
                rk,
            )
            return False

        if st == "pending":
            upd_raw = row["updated_at"]
            upd_f = _coerce_row_unix_ts(upd_raw, 0.0)
            age = now - upd_f
            if age <= ttl:
                logger.info(
                    "[DealDraft] claim_rejected chat_id=%s request_key=%s status=pending "
                    "decision=reject reason=pending_fresh age_sec=%.3f ttl_sec=%.3f",
                    cid,
                    rk,
                    age,
                    ttl,
                )
                return False
            with self._conn() as conn:
                conn.execute(
                    """
                    UPDATE max_deals_done
                    SET updated_at=?, error=NULL, chat_id=?
                    WHERE request_key=? AND status='pending' AND (? - updated_at) > ?
                    """,
                    (now, cid, rk, now, ttl),
                )
                ch = int(conn.execute("SELECT changes() AS c").fetchone()["c"] or 0)
                conn.commit()
            if ch > 0:
                logger.warning(
                    "[DealDraft] claim_acquired chat_id=%s request_key=%s status=pending "
                    "decision=claim path=ttl_reclaim ttl_expired age_sec=%.3f ttl_sec=%.3f",
                    cid,
                    rk,
                    age,
                    ttl,
                )
                return True
            logger.info(
                "[DealDraft] claim_rejected chat_id=%s request_key=%s status=pending "
                "decision=reject reason=ttl_reclaim_race ttl_expired age_sec=%.3f",
                cid,
                rk,
                age,
            )
            return False

        logger.warning(
            "[DealDraft] claim_rejected chat_id=%s request_key=%s status=%r decision=reject reason=unknown_status",
            cid,
            rk,
            st,
        )
        return False

    def finalize_claim_success(self, request_key: str, chat_id: str, deal_id: int) -> bool:
        """pending → done with deal_id."""
        rk = str(request_key or "").strip()
        cid = str(chat_id or "").strip()
        if not rk or not cid:
            return False
        did = int(deal_id)
        now = time.time()
        self._ensure()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE max_deals_done
                SET status='done', deal_id=?, chat_id=?, updated_at=?, error=NULL
                WHERE request_key=? AND status='pending'
                """,
                (did, cid, now, rk),
            )
            ok = int(conn.execute("SELECT changes() AS c").fetchone()["c"] or 0) > 0
            conn.commit()
        if ok:
            logger.info(
                "[DealDraft] claim_finalized chat_id=%s request_key=%s status=done deal_id=%s decision=finalize",
                cid,
                rk,
                did,
            )
        return ok

    def force_set_deal_done(self, request_key: str, chat_id: str, deal_id: int) -> bool:
        """Best-effort done row after CRM success when finalize_claim_success saw a race."""
        rk = str(request_key or "").strip()
        cid = str(chat_id or "").strip()
        if not rk or not cid:
            return False
        did = int(deal_id)
        now = time.time()
        self._ensure()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE max_deals_done
                SET status='done', deal_id=?, chat_id=?, updated_at=?, error=NULL
                WHERE request_key=?
                """,
                (did, cid, now, rk),
            )
            ok = int(conn.execute("SELECT changes() AS c").fetchone()["c"] or 0) > 0
            conn.commit()
            return ok

    def finalize_claim_failure(self, request_key: str, error: str) -> None:
        """pending → failed (CRM error or abort)."""
        rk = str(request_key or "").strip()
        if not rk:
            return
        now = time.time()
        err = (error or "")[:2000]
        self._ensure()
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE max_deals_done
                SET status='failed', error=?, updated_at=?
                WHERE request_key=? AND status='pending'
                """,
                (err, now, rk),
            )
            ch = int(conn.execute("SELECT changes() AS c").fetchone()["c"] or 0)
            conn.commit()
        if ch > 0:
            logger.info(
                "[DealDraft] claim_finalized request_key=%s status=failed decision=finalize error=%s",
                rk,
                err[:500],
            )
        else:
            logger.warning(
                "[DealDraft] claim_finalize_skipped request_key=%s status=failed decision=noop reason=no_pending_row",
                rk,
            )

    def try_record_deal(self, request_key: str, chat_id: str, deal_id: int) -> bool:
        """Upsert a completed row (tests, tooling)."""
        rk = str(request_key or "").strip()
        cid = str(chat_id or "").strip()
        if not rk or not cid:
            return False
        did = int(deal_id)
        now = time.time()
        self._ensure()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO max_deals_done (
                  request_key, chat_id, deal_id, status, created_at, updated_at, error
                ) VALUES (?, ?, ?, 'done', ?, ?, NULL)
                ON CONFLICT(request_key) DO UPDATE SET
                  chat_id=excluded.chat_id,
                  deal_id=excluded.deal_id,
                  status='done',
                  updated_at=excluded.updated_at,
                  error=NULL
                """,
                (rk, cid, did, now, now),
            )
            conn.commit()
        return True

    def count_pending(self) -> int:
        self._ensure()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM max_deals_done WHERE status='pending'",
            ).fetchone()
        return int(row["c"] or 0) if row is not None else 0

    def oldest_pending_age_sec(self) -> Optional[float]:
        """Max age among pending rows (now - min(updated_at)); None if no pending."""
        self._ensure()
        now = time.time()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT MIN(updated_at) AS m FROM max_deals_done WHERE status='pending'",
            ).fetchone()
        if row is None or row["m"] is None:
            return None
        ts = _coerce_row_unix_ts(row["m"], now)
        return max(0.0, float(now - ts))

    def get_pending_chat_id(self, request_key: str) -> Optional[str]:
        rk = str(request_key or "").strip()
        if not rk:
            return None
        self._ensure()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT chat_id FROM max_deals_done WHERE request_key=? AND status='pending' LIMIT 1",
                (rk,),
            ).fetchone()
        if row is None:
            return None
        cid = str(row["chat_id"] or "").strip()
        return cid or None

    def list_stale_pending_claims(self, *, ttl_sec: float, limit: int) -> List[Tuple[str, str]]:
        """Pairs (request_key, chat_id) older than TTL (pending only). Background reconcile only reads CRM."""
        ttl = max(1.0, float(ttl_sec))
        lim = max(1, min(int(limit), 500))
        now = time.time()
        self._ensure()
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT request_key, chat_id FROM max_deals_done
                WHERE status='pending' AND (? - updated_at) > ?
                ORDER BY updated_at ASC
                LIMIT ?
                """,
                (now, ttl, lim),
            ).fetchall()
        out: List[Tuple[str, str]] = []
        for r in rows:
            rk = str(r["request_key"] or "").strip()
            cid = str(r["chat_id"] or "").strip()
            if rk and cid:
                out.append((rk, cid))
        return out


_max_deals_done_singleton: Optional[_MaxDealsDoneStore] = None

_reconcile_worker_lock = threading.Lock()
_reconcile_worker_started = False
_reconcile_worker_thread: Optional[threading.Thread] = None
_reconcile_stop = threading.Event()
_reconcile_last_snapshot_monotonic: float = 0.0
_reconcile_last_lookup_monotonic: float = 0.0
_last_pending_alert_ts: float = 0.0


def stop_reconcile_worker() -> None:
    """Signal the background reconcile thread to exit (daemon; process exit also ends it)."""
    _reconcile_stop.set()
    logger.info("[DealDraft] reconcile_worker_stop_requested")


def _reconcile_throttle_before_background_lookup() -> None:
    """Minimum spacing between CRM lookups in background reconcile (Bitrix protection)."""
    global _reconcile_last_lookup_monotonic
    min_iv = _reconcile_min_lookup_interval_sec()
    if min_iv <= 0:
        _reconcile_last_lookup_monotonic = time.monotonic()
        return
    now = time.monotonic()
    if _reconcile_last_lookup_monotonic > 0:
        elapsed = now - _reconcile_last_lookup_monotonic
        if elapsed < min_iv:
            sleep_d = min_iv - elapsed
            time.sleep(sleep_d)
            logger.info(
                "[DealDraft] reconcile_rate_limited sleep_sec=%.4f min_interval_sec=%.4f",
                sleep_d,
                min_iv,
            )
    _reconcile_last_lookup_monotonic = time.monotonic()


def _reconcile_apply_error_backoff(consecutive_errors: int) -> None:
    """Exponential backoff after CRM lookup error in background reconcile."""
    base = _reconcile_backoff_base_sec()
    cap = _reconcile_backoff_max_sec()
    exp = min(max(0, consecutive_errors - 1), 12)
    delay = min(base * (2**exp), cap)
    logger.info(
        "[DealDraft] reconcile_backoff_applied delay_sec=%.3f consecutive_errors=%s",
        delay,
        consecutive_errors,
    )
    time.sleep(delay)


def reconcile_request_key(request_key: str) -> bool:
    """Manual ops: pending row + CRM find by RK → finalize to done if hit. Never calls create_deal."""
    rk = str(request_key or "").strip()
    if not rk:
        return False
    store = _get_max_deals_done_store()
    cid = store.get_pending_chat_id(rk)
    if not cid:
        logger.info("[DealDraft] reconcile_manual_skip request_key=%s reason=not_pending", rk)
        return False
    from extensions.hermes_spare24.crm.bitrix.service import BitrixService

    svc = BitrixService.from_env()
    if svc is None:
        logger.info("[DealDraft] reconcile_error request_key=%s reason=no_bitrix source=manual", rk)
        return False
    res = svc.find_deal_by_request_key(rk)
    if res.status == "hit" and res.deal_id is not None and int(res.deal_id) > 0:
        ok = store.finalize_claim_success(rk, cid, int(res.deal_id))
        if ok:
            logger.info(
                "[DealDraft] reconcile_hit request_key=%s deal_id=%s source=manual",
                rk,
                res.deal_id,
            )
        return ok
    if res.status == "miss":
        logger.info("[DealDraft] reconcile_miss request_key=%s source=manual", rk)
    else:
        logger.info("[DealDraft] reconcile_error request_key=%s source=manual", rk)
    return False


def _maybe_log_pending_alert(store: _MaxDealsDoneStore) -> None:
    """Warning if too many pending rows or the oldest pending is too old (observability only)."""
    global _last_pending_alert_ts
    cnt_th = _pending_alert_count_threshold()
    age_th = _pending_alert_age_sec()
    if cnt_th <= 0 and age_th <= 0:
        return
    cnt = store.count_pending()
    oldest_age = store.oldest_pending_age_sec()
    by_count = cnt_th > 0 and cnt > cnt_th
    by_age = age_th > 0 and oldest_age is not None and oldest_age > age_th
    if not by_count and not by_age:
        return
    now = time.monotonic()
    cooldown = _pending_alert_cooldown_sec()
    if cooldown > 0 and _last_pending_alert_ts > 0 and (now - _last_pending_alert_ts) < cooldown:
        return
    reasons = [r for r in ("count_exceeded" if by_count else None, "age_exceeded" if by_age else None) if r]
    logger.warning(
        "[DealDraft] pending_alert_triggered pending_count=%s oldest_pending_age_sec=%s threshold_count=%s "
        "threshold_age_sec=%s reason=%s",
        cnt,
        None if oldest_age is None else round(oldest_age, 3),
        cnt_th if cnt_th > 0 else None,
        age_th if age_th > 0 else None,
        ",".join(reasons),
    )
    _last_pending_alert_ts = now


def _reconcile_stale_pending_batch() -> None:
    """One cycle: optional pending snapshot + stale pending rows → CRM find only, finalize on hit."""
    global _reconcile_last_snapshot_monotonic
    now_m = time.monotonic()
    store = _get_max_deals_done_store()
    snap_iv = _reconcile_snapshot_interval_sec()
    if now_m - _reconcile_last_snapshot_monotonic >= snap_iv:
        pc = store.count_pending()
        logger.info("[DealDraft] pending_snapshot pending_count=%s", pc)
        _maybe_log_pending_alert(store)
        _reconcile_last_snapshot_monotonic = now_m

    ttl = _claim_pending_ttl_sec()
    limit = _reconcile_batch_limit()
    stale = store.list_stale_pending_claims(ttl_sec=ttl, limit=limit)
    if not stale:
        return

    logger.info(
        "[DealDraft] pending_reconcile_cycle pending_ttl_exceeded=%s batch_limit=%s",
        len(stale),
        limit,
    )

    from extensions.hermes_spare24.crm.bitrix.service import BitrixService

    svc = BitrixService.from_env()
    if svc is None:
        logger.debug("[DealDraft] pending_reconcile_skip reason=no_bitrix")
        return

    consecutive_lookup_errors = 0
    for rk, cid in stale:
        if _reconcile_stop.is_set():
            break
        _reconcile_throttle_before_background_lookup()
        try:
            res = svc.find_deal_by_request_key(rk)
            if res.status == "hit" and res.deal_id is not None and int(res.deal_id) > 0:
                consecutive_lookup_errors = 0
                if store.finalize_claim_success(rk, cid, int(res.deal_id)):
                    logger.info(
                        "[DealDraft] reconcile_hit request_key=%s deal_id=%s source=background",
                        rk,
                        res.deal_id,
                    )
                else:
                    logger.info("[DealDraft] reconcile_skip request_key=%s reason=finalize_noop", rk)
            elif res.status == "miss":
                consecutive_lookup_errors = 0
                logger.info("[DealDraft] reconcile_miss request_key=%s source=background", rk)
            else:
                consecutive_lookup_errors += 1
                logger.info("[DealDraft] reconcile_error request_key=%s source=background", rk)
                _reconcile_apply_error_backoff(consecutive_lookup_errors)
        except Exception as exc:
            consecutive_lookup_errors += 1
            logger.warning("[DealDraft] reconcile_error request_key=%s error=%s", rk, exc)
            _reconcile_apply_error_backoff(consecutive_lookup_errors)


def _maybe_start_pending_reconcile_worker() -> None:
    global _reconcile_worker_started, _reconcile_worker_thread
    with _reconcile_worker_lock:
        if _reconcile_worker_started:
            return
        raw = (os.getenv("DEAL_DRAFT_PENDING_RECONCILE_ENABLED") or "1").strip().lower()
        if raw in ("0", "false", "no", "off"):
            return
        t = threading.Thread(
            target=_pending_reconcile_worker_main,
            name="hermes_deal_draft_pending_reconcile",
            daemon=True,
        )
        _reconcile_worker_thread = t
        t.start()
        _reconcile_worker_started = True


def _pending_reconcile_worker_main() -> None:
    logger.info("[DealDraft] pending_reconcile_worker_started interval_sec=%s", _reconcile_interval_sec())
    try:
        while not _reconcile_stop.is_set():
            interval = _reconcile_interval_sec()
            if _reconcile_stop.wait(timeout=interval):
                break
            try:
                if not deal_draft_max_enabled():
                    continue
                _reconcile_stale_pending_batch()
            except Exception:
                logger.exception("[DealDraft] pending_reconcile_worker_iteration_failed")
    finally:
        logger.info("[DealDraft] reconcile_worker_stopped")


def _get_max_deals_done_store() -> _MaxDealsDoneStore:
    global _max_deals_done_singleton
    if _max_deals_done_singleton is None:
        _max_deals_done_singleton = _MaxDealsDoneStore()
    _maybe_start_pending_reconcile_worker()
    return _max_deals_done_singleton


class _DealDraftEmailIdempotencyStore:
    def __init__(self, path: Optional[Any] = None) -> None:
        self._path = path or (get_hermes_home() / "deal_draft_email_done.db")

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(str(self._path), timeout=30.0)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL")
        return c

    def _ensure(self) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS deal_draft_email_done (
                  activity_id TEXT PRIMARY KEY NOT NULL,
                  processed_at REAL NOT NULL
                )
                """
            )

    def try_claim(self, activity_id: str) -> bool:
        self._ensure()
        aid = str(activity_id or "").strip()
        if not aid:
            return False
        now = time.time()
        with self._conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO deal_draft_email_done(activity_id, processed_at) VALUES (?, ?)",
                (aid, now),
            )
            conn.commit()
            row = conn.execute("SELECT changes() AS c").fetchone()
            return int(row["c"] if row else 0) > 0


_store: Optional[_DealDraftEmailIdempotencyStore] = None


def _get_store() -> _DealDraftEmailIdempotencyStore:
    global _store
    if _store is None:
        _store = _DealDraftEmailIdempotencyStore()
    return _store


def _strip_json_fence(raw: str) -> str:
    t = (raw or "").strip()
    if t.startswith("```"):
        lines = t.split("\n")
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        t = "\n".join(lines).strip()
    return t


def _call_chat_json(*, user_prompt: str, system_prompt: str, path_label: str) -> Dict[str, Any]:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY missing")

    base_url = (os.getenv("OPENAI_BASE_URL") or "").strip().rstrip("/") or EXPECTED_BASE_URL
    model = (os.getenv("HERMES_MODEL") or os.getenv("OPENAI_MODEL") or "").strip() or EXPECTED_MODEL
    provider = (os.getenv("HERMES_MODEL_PROVIDER") or "custom").strip().lower()

    assert_effective_model_or_raise(
        path=path_label,
        model=model,
        provider=provider,
        base_url=base_url,
        extra={"component": "deal_draft"},
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.1,
    }
    req = urllib.request.Request(
        f"{base_url}/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60.0) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="replace"))
    content = (
        ((((data or {}).get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
    )
    blob = _strip_json_fence(content)
    return json.loads(blob)


_EXTRACT_SYSTEM = """You extract structured RFQ data from industrial B2B email text (RU/CN/EN).
Return STRICT JSON only with this shape (no markdown, no comments):
{
  "items": [
    {
      "brand": "",
      "component_type": "",
      "part_number": "",
      "condition": "",
      "quantity": 0,
      "unit": ""
    }
  ],
  "client": {
    "client_name": "",
    "contact_person": "",
    "phone": ""
  }
}
Use empty string or 0 when unknown. quantity must be integer. unit may be empty (defaults to шт downstream).
condition: extract from email text — look for words like новый, б/у, used, new, refurbished, восстановленный, ремонтный, оригинал, аналог. If not mentioned — use empty string.
quantity: extract the integer number from the email. Look for digits near the part name or unit words (шт, штук, единиц, pcs, pieces). If a range is given (e.g. 2-3 шт) — use the lower bound. If not mentioned — use 0."""


_FORMAT_SYSTEM = """You format RFQ text for China suppliers (cn_block) and Russian colleagues (ru_block).
Return STRICT JSON only:
{"cn_block": "...", "ru_block": "..."}
Plain technical tone. No markdown fences.

The user message has (1) structured JSON and (2) optional "Исходный текст заявки" with the full narrative.
- Use JSON for canonical client name, brands, part numbers, quantities, conditions.
- When the original RFQ text is present, the two dense paragraphs MUST incorporate every relevant technical fact stated there (interfaces, resolution, voltage/current, IP, explosion protection, OEM types, spare kit / article numbers, multi-language duplicates). Do not compress to a one-line gloss if the source lists full specs.
- Do not invent specifications not present in JSON or original text.

cn_block layout (newlines between parts):
- Optional line 1: one official manufacturer homepage URL (https://...) only when the brand is a well-known OEM and the URL is standard public knowledge. If unsure, omit entirely — never guess or invent URLs.
- Next line: 生产商: <brand exactly as in structured JSON>
- Then one dense paragraph in Simplified Chinese covering all specs from the source material.

ru_block layout:
- Line 1: <brand exactly as in structured JSON>
- Then one dense paragraph in Russian mirroring the Chinese technical scope.

Avoid generic bilingual headers such as "RFQ Summary:" / "Сводка запроса на квотацию:" unless the inputs are almost empty."""

_CN_TITLE_UF_SYSTEM = """Return STRICT JSON only: {"cn_title": "..."} (no markdown).

You build ONE short label for Bitrix deal user field "Chinese title" (not COMMENTS).

Rules:
- Single line only. Max 90 characters. No part numbers, model codes, quantities, units, dimensions.
- Avoid long digit sequences; prefer no digits unless unavoidable (max 3 digits total if any).
- For EACH distinct item.brand from input JSON: copy brand in Latin EXACTLY as given, then one space, then a minimal generic Chinese product phrase (2–8 Chinese characters typical), informed by component_type only (e.g. encoder, cable, valve — generic noun, not specs).
- Multiple brands: join segments with " / " (space slash space).
- If you cannot comply, return {"cn_title": ""}.
"""


def _cn_title_uf_field_code() -> str:
    return (os.getenv("DEAL_DRAFT_BITRIX_CN_TITLE_UF") or "UF_CRM_1771851661191").strip()


def _bitrix_rk_uf_field_code() -> str:
    """User field code for request_key (idempotency). When empty, RK is appended to TITLE."""
    return (os.getenv("DEAL_DRAFT_BITRIX_RK_UF") or "").strip()


def _minimal_items_for_cn_title(parsed: Dict[str, Any]) -> List[Dict[str, str]]:
    items = parsed.get("items") if isinstance(parsed.get("items"), list) else []
    out: List[Dict[str, str]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        b = str(it.get("brand") or "").strip()
        ct = str(it.get("component_type") or "").strip()
        if b:
            out.append({"brand": b, "component_type": ct})
    return out


def _validate_cn_title_for_uf(s: str) -> bool:
    """Reject values unsafe or unsuitable for short UF string."""
    t = (s or "").strip()
    if not t:
        return False
    if "\n" in t or "\r" in t:
        return False
    if len(t) > 120:
        return False
    if sum(1 for c in t if c.isdigit()) > 15:
        return False
    return True


def _generate_cn_deal_title_short_sync(parsed: Dict[str, Any]) -> Optional[str]:
    """Separate LLM pass: short Chinese title for UF only. Never use cn_block. On any failure returns None."""
    minimal = _minimal_items_for_cn_title(parsed)
    if not minimal:
        return None
    try:
        raw_obj = _call_chat_json(
            user_prompt=json.dumps({"items": minimal}, ensure_ascii=False),
            system_prompt=_CN_TITLE_UF_SYSTEM,
            path_label="deal_draft_cn_title_uf",
        )
        raw = str((raw_obj or {}).get("cn_title") or "").strip()
        if not raw:
            return None
        one_line = " ".join(raw.split())
        if not _validate_cn_title_for_uf(one_line):
            return None
        return one_line
    except Exception as e:
        logger.warning("[DealDraft] cn_title_uf LLM failed: %s", e)
        return None


def _extract_llm_sync(text_block: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Two attempts; returns (parsed, error_message)."""
    last_err: Optional[str] = None
    for _attempt in range(2):
        try:
            parsed = _call_chat_json(
                user_prompt=f"Текст заявки:\n{(text_block or '')[:24000]}",
                system_prompt=_EXTRACT_SYSTEM,
                path_label="deal_draft_extract",
            )
            return parsed, None
        except Exception as e:
            last_err = str(e)
            logger.warning("[DealDraft] extract attempt failed: %s", last_err)
    return None, last_err or "extract_failed"


_ITEM_LINE_LABELS = {
    "brand": "бренд производителя",
    "component_type": "тип компонента",
    "part_number": "артикул / партномер",
    "condition": "состояние (новый, б/у и т.п.)",
}

_CLIENT_LINE_LABELS = {
    "client_name": "Компания или название клиента",
    "contact_person": "Контактное лицо (ФИО)",
    "phone": "Телефон",
}


def _humanize_missing_key(internal: str) -> str:
    """Map internal validation keys to Russian labels for operators and clients (MAX + email)."""
    s = str(internal).strip()
    if s == "items":
        return "Хотя бы одна номенклатурная позиция (товарные строки)"
    if s == "client":
        return "Данные клиента: компания, контактное лицо, телефон"
    if s == "root_not_object":
        return "Целостная структура заявки — упростите или повторите текст"

    mo = re.match(r"^items\[(\d+)\]$", s)
    if mo:
        n = int(mo.group(1)) + 1
        return f"Позиция {n}: заполните карточку товара целиком"

    mo = re.match(r"^items\[(\d+)\]\.quantity$", s)
    if mo:
        n = int(mo.group(1)) + 1
        return f"Позиция {n}: количество (целое число больше нуля)"

    mo = re.match(r"^items\[(\d+)\]\.(.+)$", s)
    if mo:
        n = int(mo.group(1)) + 1
        sub = mo.group(2)
        lab = _ITEM_LINE_LABELS.get(sub)
        if lab:
            return f"Позиция {n}: {lab}"
        return f"Позиция {n}: {sub}"

    mo = re.match(r"^client\.(.+)$", s)
    if mo:
        sub = mo.group(1)
        lab = _CLIENT_LINE_LABELS.get(sub)
        if lab:
            return lab
        return f"Клиент — поле «{sub}»"

    return s


def _sorted_unique_human_labels(missing: List[str]) -> List[str]:
    labels = [_humanize_missing_key(m) for m in missing]
    return sorted(set(labels))


def _short_missing_msg(missing: List[str]) -> str:
    lines = ["Не хватает данных:"]
    for label in _sorted_unique_human_labels(missing)[:80]:
        lines.append(f"- {label}")
    return "\n".join(lines)


def _validate_parsed(data: Any) -> Tuple[bool, List[str]]:
    logger.info(
        "[DealDraft] %s",
        json.dumps({"draft_before_validation": data}, ensure_ascii=False, default=str),
    )
    missing: List[str] = []
    if not isinstance(data, dict):
        return False, ["root_not_object"]
    items = data.get("items")
    client = data.get("client")
    if not isinstance(items, list) or len(items) == 0:
        missing.append("items")
        return False, missing
    if not isinstance(client, dict):
        missing.append("client")
        return False, missing

    for i, it in enumerate(items):
        if not isinstance(it, dict):
            missing.append(f"items[{i}]")
            continue
        for k in _BLOCKING_ITEM_KEYS:
            val = it.get(k)
            if val is None or str(val).strip() == "":
                missing.append(f"items[{i}].{k}")
        qty = it.get("quantity")
        try:
            qn = int(qty) if qty is not None else 0
        except (TypeError, ValueError):
            qn = 0
        if qn <= 0:
            missing.append(f"items[{i}].quantity")

    for k in _BLOCKING_CLIENT_KEYS:
        val = client.get(k)
        if val is None or str(val).strip() == "":
            missing.append(f"client.{k}")

    return len(missing) == 0, sorted(set(missing))


def _format_dry_run_message(
    *,
    sender_email: str,
    subject: str,
    parsed_ok: bool,
    parsed: Optional[Dict[str, Any]],
    missing: List[str],
    cn_block: str,
    ru_block: str,
    extract_error: Optional[str],
) -> str:
    lines = [
        "[DRY RUN]",
        "",
        "Источник: email",
        f"Клиент (sender): {sender_email or '—'}",
        f"Тема: {subject or '(без темы)'}",
        "",
        "Распознано:",
    ]
    if extract_error:
        lines.append(f"(ошибка извлечения: {extract_error})")
    elif parsed_ok and parsed:
        lines.append(
            json.dumps(_parsed_copy_for_llm(parsed), ensure_ascii=False, indent=2)[:12000]
        )
    else:
        pd = _parsed_copy_for_llm(parsed) if isinstance(parsed, dict) else parsed
        lines.append(json.dumps(pd or {}, ensure_ascii=False, indent=2)[:8000] if parsed else "—")

    prob_lines = ["", "Проблемы / отсутствующие поля:"]
    if missing:
        for lab in _sorted_unique_human_labels(missing):
            prob_lines.append(f"- {lab}")
    else:
        prob_lines.append("—")
    prob_lines.extend(["", "CN/RU:"])
    lines.extend(prob_lines)
    if cn_block or ru_block:
        lines.append((cn_block or "").strip())
        lines.append("")
        lines.append((ru_block or "").strip())
    else:
        lines.append("—")

    lines.extend(["", "Статус: НЕ СОЗДАНА"])
    return "\n".join(lines)


class DealDraftProcessor:
    """Email → DRY RUN in MAX; MAX → interactive completion; optional CRM deal when enabled (MAX only)."""

    @staticmethod
    async def _complete_max_validated(
        sid: str,
        parsed: Dict[str, Any],
        send: Callable[[str], Awaitable[Any]],
        *,
        had_waiting_session: bool,
        request_key: str,
        raw_rfQ_text_for_title: str,
    ) -> bool:
        """COMPLETE only: validated `parsed`; at most one create_deal per request_key (MAX).

        Returns True if idempotency short-circuit (deal already created or create in flight);
        caller should skip generic outer 'complete' log line.
        """
        rk = str(request_key or "").strip()
        kv_ok, ts_ms_log, counter_log = _parse_request_key_parts(rk)
        if not kv_ok:
            ts_ms_log, counter_log = "-", "-"
        items = parsed.get("items") if isinstance(parsed.get("items"), list) else None
        n_items = len(items) if isinstance(items, list) else 0

        with _max_sessions_lock:
            ds = _max_deal_state.setdefault(rk, {})
            if ds.get("deal_created"):
                logger.info(
                    "[DealDraft] channel=max chat_id=%s request_key=%s ts_ms=%s counter=%s "
                    "decision=skip deal_id=%s error=- parsed_items_count=%s reason=already_created",
                    sid,
                    rk,
                    ts_ms_log,
                    counter_log,
                    ds.get("deal_id", "-"),
                    n_items,
                )
                return True
            if ds.get("creating"):
                logger.info(
                    "[DealDraft] channel=max chat_id=%s request_key=%s ts_ms=%s counter=%s "
                    "decision=skip deal_id=- error=- parsed_items_count=%s reason=in_flight",
                    sid,
                    rk,
                    ts_ms_log,
                    counter_log,
                    n_items,
                )
                return True

        summary = json.dumps(_parsed_copy_for_llm(parsed), ensure_ascii=False, indent=2)[
            :3500
        ]

        if not deal_draft_create_enabled():
            await send(f"Данные собраны.\n\n{summary}\n\n(создание сделки отключено)")
            logger.info(
                "[DealDraft] channel=max chat_id=%s request_key=%s ts_ms=%s counter=%s "
                "decision=skip deal_id=- error=- parsed_items_count=%s reason=flag_off",
                sid,
                rk,
                ts_ms_log,
                counter_log,
                n_items,
            )
            if had_waiting_session:
                with _max_sessions_lock:
                    _max_sessions.pop(sid, None)
            return False

        if not rk:
            await send(
                "[DealDraft] Внутренняя ошибка: не задан ключ заявки (request_key). Создание сделки отменено."
            )
            logger.error(
                "[DealDraft] channel=max chat_id=%s request_key=- ts_ms=- counter=- decision=fail deal_id=- "
                "error=missing_request_key parsed_items_count=%s",
                sid,
                n_items,
            )
            return False

        if not kv_ok:
            logger.error(
                "[DealDraft] channel=max chat_id=%s request_key=%r ts_ms=- counter=- decision=fail deal_id=- "
                "error=invalid_request_key_format parsed_items_count=%s",
                sid,
                rk,
                n_items,
            )
            await send(
                "[DealDraft] Внутренняя ошибка: некорректный ключ заявки. Создание сделки отменено."
            )
            return False

        if not str(sid or "").strip():
            logger.error(
                "[DealDraft] channel=max chat_id=- request_key=%s ts_ms=%s counter=%s decision=fail deal_id=- "
                "error=missing_chat_id parsed_items_count=%s",
                rk,
                ts_ms_log,
                counter_log,
                n_items,
            )
            await send("[DealDraft] Внутренняя ошибка: пустой chat_id. Создание сделки отменено.")
            return False

        from extensions.hermes_spare24.crm.bitrix.service import BitrixService

        action_provider = BitrixService.from_env()
        if action_provider is None:
            await send("Ошибка создания сделки: CRM не настроен (нет BITRIX24_WEBHOOK_URL)")
            logger.info(
                "[DealDraft] channel=max chat_id=%s request_key=%s ts_ms=%s counter=%s decision=fail deal_id=- "
                "error=no_bitrix parsed_items_count=%s",
                sid,
                rk,
                ts_ms_log,
                counter_log,
                n_items,
            )
            return False

        store = _get_max_deals_done_store()
        persisted_id = store.lookup_deal_id(rk)
        if persisted_id is not None:
            logger.info(
                "[DealDraft] channel=max chat_id=%s request_key=%s ts_ms=%s counter=%s decision=skip deal_id=%s "
                "error=- parsed_items_count=%s reason=persisted persistent_state=ok",
                sid,
                rk,
                ts_ms_log,
                counter_log,
                persisted_id,
                n_items,
            )
            if had_waiting_session:
                with _max_sessions_lock:
                    _max_sessions.pop(sid, None)
            return True

        cn_block, ru_block = await asyncio.to_thread(
            _format_deal_comments_sync, parsed, raw_rfQ_text_for_title
        )
        comments = (cn_block or "").strip() + "\n\n" + (ru_block or "").strip()
        title = _build_deal_title(parsed, raw_rfQ_text_for_title)
        cn_title_opt = await asyncio.to_thread(_generate_cn_deal_title_short_sync, parsed)

        decision = "fail"
        deal_id_opt: Optional[int] = None
        err_txt: Optional[str] = None
        try:
            chat_lock = _get_chat_lock(sid)
        except ValueError:
            logger.error(
                "[DealDraft] channel=max chat_id=- request_key=%s ts_ms=%s counter=%s decision=fail deal_id=- "
                "error=chat_id_lock_rejected parsed_items_count=%s",
                rk,
                ts_ms_log,
                counter_log,
                n_items,
            )
            await send("[DealDraft] Внутренняя ошибка: пустой chat_id. Создание сделки отменено.")
            return False

        with chat_lock:
            persisted_again = store.lookup_deal_id(rk)
            if persisted_again is not None:
                logger.info(
                    "[DealDraft] channel=max chat_id=%s request_key=%s ts_ms=%s counter=%s decision=skip deal_id=%s "
                    "error=- parsed_items_count=%s reason=persisted_race persistent_state=ok",
                    sid,
                    rk,
                    ts_ms_log,
                    counter_log,
                    persisted_again,
                    n_items,
                )
                if had_waiting_session:
                    with _max_sessions_lock:
                        _max_sessions.pop(sid, None)
                return True

            with _max_sessions_lock:
                ds = _max_deal_state.setdefault(rk, {})
                if ds.get("deal_created") or ds.get("creating"):
                    logger.info(
                        "[DealDraft] channel=max chat_id=%s request_key=%s ts_ms=%s counter=%s decision=skip "
                        "deal_id=- error=- parsed_items_count=%s reason=in_flight_or_done_race",
                        sid,
                        rk,
                        ts_ms_log,
                        counter_log,
                        n_items,
                    )
                    return True

            acquired = store.try_acquire_claim(rk, sid, ttl_sec=_claim_pending_ttl_sec())
            if not acquired:
                deal_after = store.lookup_deal_id(rk)
                if deal_after is not None:
                    logger.info(
                        "[DealDraft] channel=max chat_id=%s request_key=%s ts_ms=%s counter=%s decision=skip deal_id=%s "
                        "error=- parsed_items_count=%s reason=claim_race_done persistent_state=ok",
                        sid,
                        rk,
                        ts_ms_log,
                        counter_log,
                        deal_after,
                        n_items,
                    )
                    if had_waiting_session:
                        with _max_sessions_lock:
                            _max_sessions.pop(sid, None)
                    return True
                logger.info(
                    "[DealDraft] channel=max chat_id=%s request_key=%s ts_ms=%s counter=%s decision=skip deal_id=- "
                    "error=- parsed_items_count=%s reason=claim_reject_pending_in_flight persistent_state=pending",
                    sid,
                    rk,
                    ts_ms_log,
                    counter_log,
                    n_items,
                )
                return True

            with _max_sessions_lock:
                ds = _max_deal_state.setdefault(rk, {})
                ds["creating"] = True

            try:
                rk_res = RKLookupResult("miss", None)
                finder = getattr(action_provider, "find_deal_by_request_key", None)
                if callable(finder):
                    try:
                        rk_res = _normalize_rk_lookup(finder(rk))
                    except Exception as exc:
                        logger.warning(
                            "[DealDraft] crm_lookup_by_request_key_error chat_id=%s request_key=%s error=%s",
                            sid,
                            rk,
                            exc,
                        )
                        rk_res = RKLookupResult("error", None)

                if rk_res.status == "error":
                    logger.error(
                        "[DealDraft] crm_lookup_by_rk_error chat_id=%s request_key=%s ts_ms=%s counter=%s "
                        "decision=skip deal_id=- error=- parsed_items_count=%s reason=crm_lookup_error",
                        sid,
                        rk,
                        ts_ms_log,
                        counter_log,
                        n_items,
                    )
                    with _max_sessions_lock:
                        ds = _max_deal_state.setdefault(rk, {})
                        ds.pop("creating", None)
                    await send(
                        "[DealDraft] Не удалось проверить наличие сделки в CRM (временная ошибка). "
                        "Попробуйте позже или обратитесь к администратору."
                    )
                    logger.info(
                        "[DealDraft] channel=max chat_id=%s request_key=%s ts_ms=%s counter=%s decision=skip "
                        "deal_id=- error=- parsed_items_count=%s reason=crm_lookup_error",
                        sid,
                        rk,
                        ts_ms_log,
                        counter_log,
                        n_items,
                    )
                    return True

                if rk_res.status == "hit":
                    deal_id_opt = rk_res.deal_id
                    if deal_id_opt is None or int(deal_id_opt) <= 0:
                        logger.error(
                            "[DealDraft] crm_lookup_by_rk_hit_invalid chat_id=%s request_key=%s deal_id=%r",
                            sid,
                            rk,
                            deal_id_opt,
                        )
                        with _max_sessions_lock:
                            ds = _max_deal_state.setdefault(rk, {})
                            ds.pop("creating", None)
                        await send(
                            "[DealDraft] Внутренняя ошибка: некорректный ответ CRM при поиске сделки."
                        )
                        return False
                    logger.info(
                        "[DealDraft] crm_lookup_by_rk_hit chat_id=%s request_key=%s deal_id=%s",
                        sid,
                        rk,
                        deal_id_opt,
                    )
                else:
                    logger.info(
                        "[DealDraft] crm_lookup_by_rk_miss chat_id=%s request_key=%s",
                        sid,
                        rk,
                    )
                    # COMMENTS: только технические блоки CN/RU (без RK и без дубля краткого китайского заголовка).
                    comments_body = (comments or "").strip()
                    rk_uf = _bitrix_rk_uf_field_code()
                    deal_title_out = title
                    fields: Dict[str, Any] = {"COMMENTS": comments_body}
                    if rk_uf:
                        fields[rk_uf] = rk
                        logger.info(
                            "event=deal_create_payload_fields rk_storage=uf rk_uf_field_code=%s",
                            rk_uf,
                        )
                    else:
                        deal_title_out = f"{title}\nRK:{rk}"
                        logger.info("event=deal_create_payload_fields rk_storage=title_suffix")

                    uf_key = _cn_title_uf_field_code()
                    has_cn = False
                    if uf_key and cn_title_opt:
                        fields[uf_key] = cn_title_opt
                        has_cn = True
                        logger.info(
                            "event=deal_cn_title_generation generated_value=%r uf_field_code=%s decision=set reason=ok",
                            cn_title_opt,
                            uf_key,
                        )
                    else:
                        skip_reason = "no_uf_key" if not uf_key else "generation_empty_or_invalid"
                        logger.info(
                            "event=deal_cn_title_generation generated_value=%r uf_field_code=%s decision=skip reason=%s",
                            (cn_title_opt or ""),
                            uf_key or "-",
                            skip_reason,
                        )
                    cl = parsed.get("client")
                    if isinstance(cl, dict):
                        bcid = str(cl.get("_bitrix_contact_id") or "").strip()
                        if bcid.isdigit():
                            fields["CONTACT_ID"] = bcid
                            logger.info(
                                "event=deal_create_payload_fields contact_id_from_crm=%s",
                                bcid,
                            )
                    logger.info(
                        "event=deal_create_payload_fields has_comments=true has_cn_title=%s uf_field_code=%s",
                        has_cn,
                        uf_key or "-",
                    )
                    deal_id_opt = action_provider.create_deal(title=deal_title_out, fields=fields)

                persistent_ok = store.finalize_claim_success(rk, sid, deal_id_opt)
                if not persistent_ok:
                    persistent_ok = store.force_set_deal_done(rk, sid, deal_id_opt)
                if not persistent_ok:
                    logger.critical(
                        "[DealDraft] channel=max chat_id=%s request_key=%s ts_ms=%s counter=%s decision=fail deal_id=%s "
                        "parsed_items_count=%s persistent_state=fail error=persist_after_crm",
                        sid,
                        rk,
                        ts_ms_log,
                        counter_log,
                        deal_id_opt,
                        n_items,
                    )
                    with _max_sessions_lock:
                        ds = _max_deal_state.setdefault(rk, {})
                        ds.pop("creating", None)
                    await send(
                        "Ошибка: сделка создана в CRM, но локальный учёт не сохранён. "
                        f"deal_id={deal_id_opt}. Обратитесь к администратору."
                    )
                    logger.info(
                        "[DealDraft] channel=max chat_id=%s request_key=%s ts_ms=%s counter=%s decision=fail deal_id=%s "
                        "error=- parsed_items_count=%s persistent_state=fail",
                        sid,
                        rk,
                        ts_ms_log,
                        counter_log,
                        deal_id_opt,
                        n_items,
                    )
                    return False

                if rk_res.status == "hit":
                    logger.info(
                        "[DealDraft] finalize_after_lookup chat_id=%s request_key=%s deal_id=%s",
                        sid,
                        rk,
                        deal_id_opt,
                    )

                decision = "recover" if rk_res.status == "hit" else "create"
                with _max_sessions_lock:
                    ds = _max_deal_state.setdefault(rk, {})
                    ds["deal_created"] = True
                    ds["deal_id"] = deal_id_opt
                    ds.pop("creating", None)
                    _max_sessions.pop(sid, None)
                await send(f"Сделка создана: {deal_id_opt}")
                logger.info(
                    "[DealDraft] channel=max chat_id=%s request_key=%s ts_ms=%s counter=%s decision=%s deal_id=%s "
                    "error=- parsed_items_count=%s persistent_state=ok",
                    sid,
                    rk,
                    ts_ms_log,
                    counter_log,
                    decision,
                    deal_id_opt,
                    n_items,
                )
                return False
            except Exception as e:
                err_txt = str(e)
                decision = "fail"
                store.finalize_claim_failure(rk, err_txt)
                with _max_sessions_lock:
                    ds = _max_deal_state.setdefault(rk, {})
                    ds.pop("creating", None)
                await send(f"Ошибка создания сделки: {err_txt}")
                logger.info(
                    "[DealDraft] channel=max chat_id=%s request_key=%s ts_ms=%s counter=%s decision=%s deal_id=- "
                    "error=%s parsed_items_count=%s",
                    sid,
                    rk,
                    ts_ms_log,
                    counter_log,
                    decision,
                    err_txt,
                    n_items,
                )
                return False

    @staticmethod
    def handle(
        text: str,
        channel: Literal["email", "max"],
        session_id: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        if channel == "max":
            logger.debug(
                "[DealDraft] handle() for channel=max is not used — call handle_max() from max.py"
            )
            return
        DealDraftProcessor._handle_email(
            text=text,
            session_id=session_id,
            context=context,
        )

    @staticmethod
    def _handle_email(
        text: str,
        session_id: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        correlation_id = str(session_id or "").strip()
        ctx = dict(context or {})
        sender_email = str(ctx.get("sender_email") or "").strip()
        subject = str(ctx.get("subject") or "").strip()

        if not _deal_draft_enabled():
            logger.debug("[DealDraft] disabled DEAL_DRAFT_EMAIL_ENABLED session_id=%s", correlation_id)
            return

        if not correlation_id:
            logger.warning("[DealDraft] missing session_id — skip")
            return

        store = _get_store()
        if not store.try_claim(correlation_id):
            logger.info(
                "[DealDraft] duplicate skip correlation_id=%s activity_id=%s decision=dry_run status=skipped",
                correlation_id,
                correlation_id,
            )
            return

        token, chat_id = _max_notify_config()
        decision = "dry_run"
        status = "fail"
        parsed_items_count = 0
        missing_fields: List[str] = []

        extract_error: Optional[str] = None
        parsed: Optional[Dict[str, Any]] = None
        cn_block = ""
        ru_block = ""

        try:
            parsed, extract_err = _extract_llm_sync((text or ""))
            if parsed is None:
                extract_error = extract_err or "extract_failed"

            ok_struct = False
            if parsed is not None:
                _enrich_client_from_bitrix(parsed)
                ok_struct, missing_fields = _validate_parsed(parsed)
                items = parsed.get("items") if isinstance(parsed, dict) else None
                parsed_items_count = len(items) if isinstance(items, list) else 0

            if parsed is not None and ok_struct:
                try:
                    cn_block, ru_block = _format_deal_comments_sync(
                        parsed,
                        text or "",
                        path_label="deal_draft_format",
                    )
                except Exception as e:
                    logger.warning("[DealDraft] format LLM failed: %s", e)

            msg = _format_dry_run_message(
                sender_email=sender_email,
                subject=subject,
                parsed_ok=bool(parsed is not None and ok_struct),
                parsed=parsed,
                missing=missing_fields,
                cn_block=cn_block,
                ru_block=ru_block,
                extract_error=extract_error,
            )

            if token and chat_id:
                ok_send = send_max_text_sync(msg, chat_id=chat_id, token=token)
                status = "success" if ok_send else "fail"
                if not ok_send:
                    logger.warning(
                        "[DealDraft] MAX send failed correlation_id=%s activity_id=%s",
                        correlation_id,
                        correlation_id,
                    )
            else:
                logger.warning(
                    "[DealDraft] MAX notify skipped (missing MAX_BOT_TOKEN / MAX_NOTIFY_CHAT_ID) "
                    "correlation_id=%s activity_id=%s",
                    correlation_id,
                    correlation_id,
                )
                status = "fail"

            logger.info(
                "[DealDraft] correlation_id=%s activity_id=%s channel=email parsed_items_count=%s "
                "missing_fields=%s decision=%s status=%s",
                correlation_id,
                correlation_id,
                parsed_items_count,
                ",".join(missing_fields) if missing_fields else "-",
                decision,
                status,
            )
        except Exception:
            logger.exception(
                "[DealDraft] fatal correlation_id=%s activity_id=%s decision=%s status=fail",
                correlation_id,
                correlation_id,
                decision,
            )

    @staticmethod
    async def handle_max(
        text: str,
        *,
        chat_id: str,
        send: Callable[[str], Awaitable[Any]],
    ) -> bool:
        """Interactive MAX flow: extract → validate → ask for missing → complete; optional CRM deal.

        Returns True if this message was fully handled here (gateway agent should not run).
        """
        if not deal_draft_max_enabled():
            return False

        sid = str(chat_id or "").strip()
        raw_text = (text or "").strip()
        if not sid or not raw_text:
            return False

        wall_epoch = time.time()
        decision = "unknown"

        try:
            with _max_sessions_lock:
                sess = _max_sessions.get(sid)

            if sess is None:
                parsed, err = await asyncio.to_thread(_extract_llm_sync, raw_text)
                if parsed is None:
                    decision = "fail"
                    await send(
                        "[DealDraft] Не удалось разобрать заявку (извлечение). "
                        "Повторите текст или уточните позиции.\n"
                        f"Деталь: {err or 'ошибка'}"
                    )
                    logger.info(
                        "[DealDraft] channel=max session_id=%s missing_fields=- decision=%s status=fail",
                        sid,
                        decision,
                    )
                    return True

                await _enrich_client_from_bitrix_async(parsed)
                ok, missing = _validate_parsed(parsed)
                items = parsed.get("items") if isinstance(parsed, dict) else None
                n_items = len(items) if isinstance(items, list) else 0

                if ok:
                    decision = "complete"
                    ts_ms = int(wall_epoch * 1000)
                    nonce = _next_counter()
                    idempotent_skip = await DealDraftProcessor._complete_max_validated(
                        sid,
                        parsed,
                        send,
                        had_waiting_session=False,
                        request_key=_make_request_key(sid, ts_ms, nonce),
                        raw_rfQ_text_for_title=raw_text,
                    )
                    if not idempotent_skip:
                        logger.info(
                            "[DealDraft] channel=max session_id=%s parsed_items_count=%s missing_fields=- "
                            "decision=%s status=success",
                            sid,
                            n_items,
                            decision,
                        )
                    return True

                accumulated = raw_text
                started_ms = int(wall_epoch * 1000)
                nonce = _next_counter()
                with _max_sessions_lock:
                    _max_sessions[sid] = {
                        "phase": "waiting_operator",
                        "accumulated": accumulated,
                        "last_parsed": parsed,
                        "missing_fields": missing,
                        "session_started_at": wall_epoch,
                        "session_started_ms": started_ms,
                        "request_nonce": nonce,
                    }
                await send(_short_missing_msg(missing))
                decision = "ask"
                logger.info(
                    "[DealDraft] channel=max session_id=%s parsed_items_count=%s missing_fields=%s "
                    "decision=%s status=success",
                    sid,
                    n_items,
                    ",".join(missing),
                    decision,
                )
                return True

            # Existing session (waiting for operator input)
            phase = str(sess.get("phase") or "")
            if phase != "waiting_operator":
                with _max_sessions_lock:
                    _max_sessions.pop(sid, None)
                return await DealDraftProcessor.handle_max(text, chat_id=chat_id, send=send)

            accumulated = str(sess.get("accumulated") or "") + "\n---\n" + raw_text
            parsed, err = await asyncio.to_thread(_extract_llm_sync, accumulated)
            if parsed is None:
                decision = "fail"
                await send(
                    "[DealDraft] Не удалось обновить черновик. Повторите ответ или начните новое сообщение.\n"
                    f"Деталь: {err or 'ошибка'}"
                )
                logger.info(
                    "[DealDraft] channel=max session_id=%s missing_fields=- decision=%s status=fail",
                    sid,
                    decision,
                )
                return True

            prior_parsed = sess.get("last_parsed")
            if isinstance(parsed, dict) and isinstance(prior_parsed, dict):
                _merge_parsed_with_prior(parsed, prior_parsed)
            pm = sess.get("missing_fields")
            if isinstance(parsed, dict) and isinstance(pm, list):
                _apply_followup_reply_to_missing(parsed, raw_text, pm)

            await _enrich_client_from_bitrix_async(parsed)
            ok, missing = _validate_parsed(parsed)
            items = parsed.get("items") if isinstance(parsed, dict) else None
            n_items = len(items) if isinstance(items, list) else 0

            if ok:
                decision = "complete"
                with _max_sessions_lock:
                    s = _max_sessions.get(sid)
                    if s is None:
                        st = float(sess.get("session_started_at", wall_epoch))
                        ts_ms = int(st * 1000)
                        nonce = _next_counter()
                    else:
                        if s.get("request_nonce") is None:
                            s["request_nonce"] = _next_counter()
                        if s.get("session_started_ms") is None:
                            st = float(s.get("session_started_at", wall_epoch))
                            s["session_started_ms"] = int(st * 1000)
                        ts_ms = int(s["session_started_ms"])
                        nonce = int(s["request_nonce"])
                idempotent_skip = await DealDraftProcessor._complete_max_validated(
                    sid,
                    parsed,
                    send,
                    had_waiting_session=True,
                    request_key=_make_request_key(sid, ts_ms, nonce),
                    raw_rfQ_text_for_title=accumulated,
                )
                if not idempotent_skip:
                    logger.info(
                        "[DealDraft] channel=max session_id=%s parsed_items_count=%s missing_fields=- "
                        "decision=%s status=success",
                        sid,
                        n_items,
                        decision,
                    )
                return True

            started = float(sess.get("session_started_at", wall_epoch))
            started_ms = int(sess.get("session_started_ms", int(started * 1000)))
            rn = sess.get("request_nonce")
            if rn is None:
                nonce = _next_counter()
            else:
                nonce = int(rn)
            with _max_sessions_lock:
                _max_sessions[sid] = {
                    "phase": "waiting_operator",
                    "accumulated": accumulated,
                    "last_parsed": parsed,
                    "missing_fields": missing,
                    "session_started_at": started,
                    "session_started_ms": started_ms,
                    "request_nonce": nonce,
                }
            await send(_short_missing_msg(missing))
            decision = "update"
            logger.info(
                "[DealDraft] channel=max session_id=%s parsed_items_count=%s missing_fields=%s "
                "decision=%s status=success",
                sid,
                n_items,
                ",".join(missing),
                decision,
            )
            return True

        except Exception:
            logger.exception("[DealDraft] handle_max fatal session_id=%s", sid)
            try:
                await send("[DealDraft] Внутренняя ошибка. Попробуйте ещё раз.")
            except Exception:
                logger.exception("[DealDraft] handle_max fallback send failed session_id=%s", sid)
            return True


__all__ = [
    "DealDraftProcessor",
    "deal_draft_max_enabled",
    "deal_draft_max_session_waiting",
    "deal_draft_create_enabled",
]
