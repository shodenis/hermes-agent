from __future__ import annotations

import logging
import re
from typing import Any, Dict

from ..action_provider import ActionProvider
from ..service import BitrixService
from ..types import DuplicateResponse, LeadFields, LeadResponse

logger = logging.getLogger(__name__)

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
ISO8601_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$")


class BitrixServiceAdapter(ActionProvider):
    """Adapter exposing BitrixService through the ActionProvider contract."""

    def __init__(self, service: BitrixService):
        """Initialize adapter with an underlying BitrixService."""
        self._service = service

    def find_duplicate_by_email(self, email: str) -> DuplicateResponse:
        """Find duplicates in CRM by validated email."""
        if not EMAIL_RE.fullmatch(email):
            raise ValueError(f"Invalid email format: {email!r}")
        try:
            return self._service.find_duplicate_by_email(email)
        except Exception as e:
            logger.error("BitrixServiceAdapter.find_duplicate_by_email failed: %s", e)
            raise

    def create_lead(self, fields: LeadFields) -> LeadResponse:
        """Create a lead with typed lead fields."""
        try:
            return self._service.create_lead(fields)
        except Exception as e:
            logger.error("BitrixServiceAdapter.create_lead failed: %s", e)
            raise

    def get_lead(self, lead_id: str) -> Dict[str, Any]:
        """Get lead details by lead ID."""
        try:
            return self._service.get_lead(lead_id)
        except Exception as e:
            logger.error("BitrixServiceAdapter.get_lead failed: %s", e)
            raise

    def list_leads_by_email(self, email: str) -> Dict[str, Any]:
        """List leads by email."""
        try:
            return self._service.list_leads_by_email(email)
        except Exception as e:
            logger.error("BitrixServiceAdapter.list_leads_by_email failed: %s", e)
            raise

    def list_deals_by_contact(self, contact_id: str) -> Dict[str, Any]:
        """List deals attached to a contact."""
        try:
            return self._service.list_deals_by_contact(contact_id)
        except Exception as e:
            logger.error("BitrixServiceAdapter.list_deals_by_contact failed: %s", e)
            raise

    def list_deals_by_company(self, company_id: str) -> Dict[str, Any]:
        """List deals attached to a company."""
        try:
            return self._service.list_deals_by_company(company_id)
        except Exception as e:
            logger.error("BitrixServiceAdapter.list_deals_by_company failed: %s", e)
            raise

    def add_timeline_comment(self, *, lead_id: str, comment: str) -> Dict[str, Any]:
        """Add a comment into lead timeline."""
        try:
            return self._service.add_timeline_comment(lead_id=lead_id, comment=comment)
        except Exception as e:
            logger.error("BitrixServiceAdapter.add_timeline_comment failed: %s", e)
            raise

    def create_contact(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        """Create a contact with typed contact fields."""
        try:
            return self._service.create_contact(fields)
        except Exception as e:
            logger.error("BitrixServiceAdapter.create_contact failed: %s", e)
            raise

    def add_deal_timeline_comment(self, *, deal_id: str, comment: str) -> Dict[str, Any]:
        """Add a comment into deal timeline."""
        try:
            return self._service.add_deal_timeline_comment(deal_id=deal_id, comment=comment)
        except Exception as e:
            logger.error("BitrixServiceAdapter.add_deal_timeline_comment failed: %s", e)
            raise

    def add_lead_todo(
        self,
        *,
        lead_id: str,
        title: str,
        description: str,
        responsible_id: str,
        deadline: str,
    ) -> Dict[str, Any]:
        """Add a TODO for a lead with validated ISO 8601 datetime deadline."""
        if not ISO8601_DT_RE.fullmatch(deadline):
            raise ValueError(
                f"Invalid deadline format (expected ISO 8601 datetime): {deadline!r}"
            )
        try:
            return self._service.add_lead_todo(
                lead_id=lead_id,
                title=title,
                description=description,
                responsible_id=responsible_id,
                deadline=deadline,
            )
        except Exception as e:
            logger.error("BitrixServiceAdapter.add_lead_todo failed: %s", e)
            raise
