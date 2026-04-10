from __future__ import annotations

import os
from typing import Any, Dict, Optional

from .action_provider import ActionProvider
from .client import Bitrix24Client


class BitrixService(ActionProvider):
    def __init__(self, client: Bitrix24Client):
        self.client = client

    @classmethod
    def from_env(cls) -> Optional["BitrixService"]:
        webhook = (os.getenv("BITRIX24_WEBHOOK_URL") or "").strip().rstrip("/")
        if not webhook:
            return None
        rate = float(os.getenv("BITRIX24_RATE_LIMITER_RATE", "2.0"))
        burst = float(os.getenv("BITRIX24_RATE_LIMITER_BURST", "10"))
        client = Bitrix24Client(
            webhook,
            timeout=10,
            max_attempts=5,
            rate_per_sec=rate,
            burst=burst,
        )
        return cls(client)

    def find_duplicate_by_email(self, email: str) -> Dict[str, Any]:
        return self.client.call(
            "crm.duplicate.findbycomm",
            {"type": "EMAIL", "values[0]": email},
        )

    def create_lead(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        params: Dict[str, Any] = {}
        for key, value in fields.items():
            params[f"fields[{key}]"] = value
        return self.client.call("crm.lead.add", params)

    def get_lead(self, lead_id: str) -> Dict[str, Any]:
        return self.client.call("crm.lead.get", {"id": str(lead_id)})

    def list_leads_by_email(self, email: str) -> Dict[str, Any]:
        return self.client.call(
            "crm.lead.list",
            {
                "limit": 50,
                "filter[EMAIL]": email,
                "filter[STATUS_SEMANTIC_ID]": "P",
                "order[DATE_CREATE]": "DESC",
            },
        )

    def list_deals_by_contact(self, contact_id: str) -> Dict[str, Any]:
        return self.client.call(
            "crm.deal.list",
            {
                "filter[CONTACT_ID]": contact_id,
                "filter[STAGE_SEMANTIC_ID]": "P",
                "filter[CLOSED]": "N",
                "order[DATE_CREATE]": "DESC",
                "select[0]": "ID",
                "limit": 50,
            },
        )

    def list_deals_by_company(self, company_id: str) -> Dict[str, Any]:
        return self.client.call(
            "crm.deal.list",
            {
                "filter[COMPANY_ID]": company_id,
                "filter[STAGE_SEMANTIC_ID]": "P",
                "filter[CLOSED]": "N",
                "order[DATE_CREATE]": "DESC",
                "select[0]": "ID",
                "limit": 50,
            },
        )

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

    # Phase-1 abstraction placeholders for later rollout.
    def create_activity(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self.client.call("crm.activity.add", params)

    def get_activity(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self.client.call("crm.activity.get", params)

    def send_email(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return self.client.call("crm.activity.add", params)

    # Backward-compatible aliases used in Phase-1 call sites.
    def duplicate_find_by_email(self, email: str) -> Dict[str, Any]:
        return self.find_duplicate_by_email(email)

    def list_open_leads_by_email(self, email: str) -> Dict[str, Any]:
        return self.list_leads_by_email(email)

    def list_open_deals_by_contact(self, contact_id: str) -> Dict[str, Any]:
        return self.list_deals_by_contact(contact_id)

    def list_open_deals_by_company(self, company_id: str) -> Dict[str, Any]:
        return self.list_deals_by_company(company_id)
