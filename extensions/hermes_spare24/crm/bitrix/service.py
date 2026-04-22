from __future__ import annotations

import logging
import os
import re
import time
import urllib.error
from typing import Any, Dict, List, Optional

from .open_entities import (
    OPEN_DEAL_STAGE_SEMANTIC_ID,
    OPEN_LEAD_STATUS_SEMANTIC_ID,
    deal_entity_is_open,
    filter_open_deals,
    filter_open_leads,
)

from .action_provider import ActionProvider
from .client import Bitrix24Client
from .errors import BitrixAPIError
from .types import RKLookupResult

logger = logging.getLogger(__name__)


class BitrixService(ActionProvider):
    def __init__(self, client: Bitrix24Client):
        self.client = client

    @staticmethod
    def _deal_text_contains_exact_rk_marker(text: Any, request_key: str) -> bool:
        """True if text has a line that is exactly RK:<request_key> (no substring false positives)."""
        body = str(text or "")
        rk = str(request_key or "").strip()
        if not rk:
            return False
        pat = re.compile(
            rf"(?:^|\n)\s*RK:{re.escape(rk)}\s*(?:\n|$)",
            flags=re.MULTILINE,
        )
        return pat.search(body) is not None

    def find_deal_by_request_key(self, request_key: str) -> RKLookupResult:
        """Resolve existing deal id by idempotency key (UF field or RK: marker in COMMENTS/TITLE)."""
        rk = str(request_key or "").strip()
        if not rk:
            return RKLookupResult("miss", None)
        uf = (os.getenv("DEAL_DRAFT_BITRIX_RK_UF") or "").strip()
        try:
            if uf:
                raw = self.client.call(
                    "crm.deal.list",
                    {
                        f"filter[{uf}]": rk,
                        "select[0]": "ID",
                        f"select[1]": uf,
                        "limit": 1,
                    },
                )
                rows = raw.get("result") if isinstance(raw, dict) else None
                if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                    row = rows[0]
                    if str(row.get(uf) or "").strip() == rk:
                        did = row.get("ID")
                        if did is not None and str(did).strip().isdigit():
                            iid = int(str(did).strip())
                            if iid > 0:
                                logger.info(
                                    "action=find_deal_by_request_key status=hit deal_id=%s path=uf",
                                    iid,
                                )
                                return RKLookupResult("hit", iid)

            marker = f"RK:{rk}"
            raw = self.client.call(
                "crm.deal.list",
                {
                    "filter[%COMMENTS]": f"%{marker}%",
                    "select[0]": "ID",
                    "select[1]": "COMMENTS",
                    "select[2]": "TITLE",
                    "limit": 25,
                    "order[ID]": "DESC",
                },
            )
            rows = raw.get("result") if isinstance(raw, dict) else None
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    for fld in ("COMMENTS", "TITLE"):
                        if self._deal_text_contains_exact_rk_marker(row.get(fld), rk):
                            did = row.get("ID")
                            if did is not None and str(did).strip().isdigit():
                                iid = int(str(did).strip())
                                if iid > 0:
                                    logger.info(
                                        "action=find_deal_by_request_key status=hit deal_id=%s path=comments_or_title",
                                        iid,
                                    )
                                    return RKLookupResult("hit", iid)

            raw_t = self.client.call(
                "crm.deal.list",
                {
                    "filter[%TITLE]": f"%{marker}%",
                    "select[0]": "ID",
                    "select[1]": "COMMENTS",
                    "select[2]": "TITLE",
                    "limit": 25,
                    "order[ID]": "DESC",
                },
            )
            rows_t = raw_t.get("result") if isinstance(raw_t, dict) else None
            if isinstance(rows_t, list):
                for row in rows_t:
                    if not isinstance(row, dict):
                        continue
                    for fld in ("COMMENTS", "TITLE"):
                        if self._deal_text_contains_exact_rk_marker(row.get(fld), rk):
                            did = row.get("ID")
                            if did is not None and str(did).strip().isdigit():
                                iid = int(str(did).strip())
                                if iid > 0:
                                    logger.info(
                                        "action=find_deal_by_request_key status=hit deal_id=%s path=title_search",
                                        iid,
                                    )
                                    return RKLookupResult("hit", iid)

            logger.info("action=find_deal_by_request_key status=miss request_key=%s", rk)
            return RKLookupResult("miss", None)
        except BitrixAPIError as exc:
            logger.warning(
                "action=find_deal_by_request_key status=error request_key=%s error=%s",
                rk,
                exc,
            )
            return RKLookupResult("error", None)

    @classmethod
    def from_env(cls) -> Optional["BitrixService"]:
        webhook = ((os.environ.get("BITRIX24_WEBHOOK_URL") or os.environ.get("BITRIX_WEBHOOK_URL") or "").strip().rstrip("/"))
        if not webhook:
            return None
        rate = float(os.getenv("BITRIX24_RATE_LIMITER_RATE", "2.0"))
        burst = float(os.getenv("BITRIX24_RATE_LIMITER_BURST", "10"))
        client = Bitrix24Client(webhook, timeout=10, max_attempts=5, rate_per_sec=rate, burst=burst)
        return cls(client)

    def find_duplicate_by_email(self, email: str) -> Dict[str, Any]:
        return self.client.call("crm.duplicate.findbycomm", {"type": "EMAIL", "values[0]": email})

    def create_lead(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        for key, value in fields.items():
            params[f"fields[{key}]"] = value
        return self.client.call("crm.lead.add", params)

    def get_lead(self, lead_id: str) -> Dict[str, Any]:
        return self.client.call("crm.lead.get", {"id": str(lead_id)})

    def get_deal(self, deal_id: str) -> Dict[str, Any]:
        raw = self.client.call("crm.deal.get", {"id": str(deal_id)})
        ent = raw.get("result") if isinstance(raw, dict) else None
        if not isinstance(ent, dict):
            return raw
        if deal_entity_is_open(ent):
            return raw
        logger.debug("get_deal returned CLOSED deal id=%s", deal_id)
        return {**raw, "result": None}

    def get_deal_raw(self, deal_id: str) -> Dict[str, Any]:
        return self.client.call("crm.deal.get", {"id": str(deal_id)})

    @staticmethod
    def _sanitize_lead_list_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        result = payload.get("result")
        if not isinstance(result, list):
            return payload
        filtered = filter_open_leads(result, log=logger)
        return {**payload, "result": filtered}

    @staticmethod
    def _sanitize_deal_list_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
        result = payload.get("result")
        if not isinstance(result, list):
            return payload
        filtered = filter_open_deals(result, log=logger)
        return {**payload, "result": filtered}

    def list_leads_by_email(self, email: str) -> Dict[str, Any]:
        raw = self.client.call(
            "crm.lead.list",
            {
                "limit": 50,
                "filter[EMAIL]": email,
                "filter[STATUS_SEMANTIC_ID]": OPEN_LEAD_STATUS_SEMANTIC_ID,
                "order[DATE_CREATE]": "DESC",
                "select[0]": "ID",
                "select[1]": "STATUS_SEMANTIC_ID",
                "select[2]": "EMAIL",
            },
        )
        return self._sanitize_lead_list_payload(raw)

    def list_leads_by_contact(self, contact_id: str) -> Dict[str, Any]:
        raw = self.client.call(
            "crm.lead.list",
            {
                "limit": 50,
                "filter[CONTACT_ID]": str(contact_id),
                "filter[STATUS_SEMANTIC_ID]": OPEN_LEAD_STATUS_SEMANTIC_ID,
                "order[DATE_CREATE]": "DESC",
                "select[0]": "ID",
                "select[1]": "STATUS_SEMANTIC_ID",
                "select[2]": "CONTACT_ID",
            },
        )
        return self._sanitize_lead_list_payload(raw)

    def list_deals_by_contact(self, contact_id: str) -> Dict[str, Any]:
        raw = self.client.call(
            "crm.deal.list",
            {
                "filter[CONTACT_IDS]": [str(contact_id)],
                "filter[STAGE_SEMANTIC_ID]": OPEN_DEAL_STAGE_SEMANTIC_ID,
                "order[DATE_CREATE]": "DESC",
                "select[0]": "ID",
                "select[1]": "STAGE_SEMANTIC_ID",
                "select[2]": "CLOSED",
                "limit": 50,
            },
        )
        return self._sanitize_deal_list_payload(raw)

    def list_deals_by_company(self, company_id: str) -> Dict[str, Any]:
        raw = self.client.call(
            "crm.deal.list",
            {
                "filter[COMPANY_ID]": company_id,
                "filter[STAGE_SEMANTIC_ID]": OPEN_DEAL_STAGE_SEMANTIC_ID,
                "order[DATE_CREATE]": "DESC",
                "select[0]": "ID",
                "select[1]": "STAGE_SEMANTIC_ID",
                "select[2]": "CLOSED",
                "limit": 50,
            },
        )
        return self._sanitize_deal_list_payload(raw)

    def add_timeline_comment(self, *, lead_id: str, comment: str) -> Dict[str, Any]:
        return self.client.call(
            "crm.timeline.comment.add",
            {
                "fields[ENTITY_ID]": str(lead_id),
                "fields[ENTITY_TYPE]": "lead",
                "fields[COMMENT]": comment,
            },
        )

    def create_contact(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        for key, value in fields.items():
            params[f"fields[{key}]"] = value
        return self.client.call("crm.contact.add", params)

    def list_contacts_by_name_guess(self, query: str) -> List[Dict[str, Any]]:
        """Best-effort contact search by display name for DealDraft CRM enrichment.

        Tries exact NAME+LAST_NAME (first + last token), then substring filters.
        Returns deduplicated rows with ID, NAME, LAST_NAME, PHONE when available.
        """
        q = " ".join(str(query).split()).strip()
        if len(q) < 2:
            return []

        parts = q.split()
        seen: set[int] = set()
        merged: List[Dict[str, Any]] = []

        def add_rows(raw: Any) -> None:
            rows = raw.get("result") if isinstance(raw, dict) else None
            if not isinstance(rows, list):
                return
            for row in rows:
                if not isinstance(row, dict):
                    continue
                rid = row.get("ID")
                try:
                    iid = int(str(rid).strip()) if rid is not None else 0
                except (TypeError, ValueError):
                    continue
                if iid <= 0 or iid in seen:
                    continue
                seen.add(iid)
                merged.append(row)

        base = {
            "select[0]": "ID",
            "select[1]": "NAME",
            "select[2]": "LAST_NAME",
            "select[3]": "SECOND_NAME",
            "select[4]": "PHONE",
            "limit": 25,
        }
        try:
            if len(parts) >= 2:
                raw = self.client.call(
                    "crm.contact.list",
                    {
                        **base,
                        "filter[NAME]": parts[0],
                        "filter[LAST_NAME]": parts[-1],
                    },
                )
                add_rows(raw)
            if not merged and len(parts) >= 2:
                raw = self.client.call(
                    "crm.contact.list",
                    {
                        **base,
                        "filter[NAME]": parts[0],
                        "filter[LAST_NAME]": " ".join(parts[1:]),
                    },
                )
                add_rows(raw)
            if not merged:
                raw = self.client.call(
                    "crm.contact.list",
                    {**base, "filter[%NAME]": q},
                )
                add_rows(raw)
            if not merged and parts:
                raw = self.client.call(
                    "crm.contact.list",
                    {**base, "filter[%LAST_NAME]": parts[-1]},
                )
                add_rows(raw)
        except BitrixAPIError as exc:
            logger.warning("list_contacts_by_name_guess failed query=%r error=%s", q, exc)
            return []
        return merged

    def add_deal_timeline_comment(self, *, deal_id: str, comment: str) -> Dict[str, Any]:
        return self.client.call(
            "crm.timeline.comment.add",
            {
                "fields[ENTITY_ID]": str(deal_id),
                "fields[ENTITY_TYPE]": "deal",
                "fields[COMMENT]": comment,
            },
        )

    def add_lead_todo(
        self,
        *,
        lead_id: str,
        title: str,
        description: str,
        responsible_id: str,
        deadline: str,
    ) -> Dict[str, Any]:
        return self.client.call(
            "crm.activity.todo.add",
            {
                "ownerTypeId": "1",
                "ownerId": str(lead_id),
                "deadline": deadline,
                "title": title,
                "description": description,
                "responsibleId": str(responsible_id),
                "pingOffsets[0]": "0",
                "colorId": "2",
            },
        )

    def create_deal(self, title: str, fields: Dict[str, Any]) -> int:
        """crm.deal.add — returns new deal ID. Initial attempt + up to 2 retries on retryable errors."""
        t = str(title or "").strip()
        if not t:
            raise ValueError("create_deal: title must be non-empty")
        if not isinstance(fields, dict):
            raise TypeError("create_deal: fields must be a dict")

        merged: Dict[str, Any] = dict(fields)
        merged["TITLE"] = t

        params: Dict[str, Any] = {}
        for key, value in merged.items():
            params[f"fields[{key}]"] = value

        for attempt in range(3):
            try:
                raw = self.client.call("crm.deal.add", params)
                deal_raw = raw.get("result") if isinstance(raw, dict) else None
                if deal_raw is None:
                    logger.error(
                        "action=create_deal status=fail deal_id=- error=no_result attempt=%s",
                        attempt + 1,
                    )
                    raise BitrixAPIError(
                        "crm.deal.add returned empty result",
                        code="EMPTY_RESULT",
                        payload=raw if isinstance(raw, dict) else {},
                    )
                sid = str(deal_raw).strip()
                if not sid.isdigit():
                    logger.error(
                        "action=create_deal status=fail deal_id=- error=invalid_id raw=%r",
                        deal_raw,
                    )
                    raise BitrixAPIError(
                        f"crm.deal.add returned invalid id: {deal_raw!r}",
                        code="INVALID_RESULT",
                        payload=raw if isinstance(raw, dict) else {},
                    )
                did = int(sid)
                if did <= 0:
                    raise BitrixAPIError(
                        "crm.deal.add returned non-positive id",
                        code="INVALID_RESULT",
                        payload=raw if isinstance(raw, dict) else {},
                    )
                logger.info(
                    "action=create_deal status=success deal_id=%s attempt=%s",
                    did,
                    attempt + 1,
                )
                return did
            except BitrixAPIError as exc:
                if exc.fatal or not exc.retryable:
                    logger.error(
                        "action=create_deal status=fail deal_id=- error=%s attempt=%s",
                        exc,
                        attempt + 1,
                    )
                    raise
                if attempt >= 2:
                    logger.error(
                        "action=create_deal status=fail deal_id=- error=%s attempts_exhausted",
                        exc,
                    )
                    raise
                logger.warning(
                    "action=create_deal status=retry deal_id=- error=%s attempt=%s",
                    exc,
                    attempt + 1,
                )
                time.sleep(min(0.25 * (2**attempt), 4.0))
            except urllib.error.URLError as exc:
                if attempt >= 2:
                    logger.error(
                        "action=create_deal status=fail deal_id=- error=%s attempts_exhausted",
                        exc,
                    )
                    raise BitrixAPIError(
                        f"Network error: {exc}",
                        status=0,
                        code="NETWORK_ERROR",
                    ) from exc
                logger.warning(
                    "action=create_deal status=retry deal_id=- error=%s attempt=%s",
                    exc,
                    attempt + 1,
                )
                time.sleep(min(0.25 * (2**attempt), 4.0))

        raise BitrixAPIError("create_deal: retries exhausted", code="RETRIES_EXHAUSTED")

    def create_activity(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self.client.call("crm.activity.add", params)

    def get_activity(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self.client.call("crm.activity.get", params)

    def send_email(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self.client.call("crm.activity.add", params)

    def duplicate_find_by_email(self, email: str) -> Dict[str, Any]:
        return self.find_duplicate_by_email(email)

    def list_open_leads_by_email(self, email: str) -> Dict[str, Any]:
        return self.list_leads_by_email(email)

    def list_open_deals_by_contact(self, contact_id: str) -> Dict[str, Any]:
        return self.list_deals_by_contact(contact_id)

    def list_open_deals_by_company(self, company_id: str) -> Dict[str, Any]:
        return self.list_deals_by_company(company_id)
