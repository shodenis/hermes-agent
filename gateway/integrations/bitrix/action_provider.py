from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

from .types import DuplicateResponse, LeadFields, LeadResponse


class ActionProvider(ABC):
    """Abstract interface for CRM actions performed by the digital employee."""

    @abstractmethod
    def find_duplicate_by_email(self, email: str) -> DuplicateResponse:
        """Find CRM duplicates by email."""
        raise NotImplementedError

    @abstractmethod
    def create_lead(self, fields: LeadFields) -> LeadResponse:
        """Create a lead in Bitrix24 CRM."""
        raise NotImplementedError

    @abstractmethod
    def get_lead(self, lead_id: str) -> Dict[str, Any]:
        """Get a lead by ID."""
        raise NotImplementedError

    @abstractmethod
    def list_leads_by_email(self, email: str) -> Dict[str, Any]:
        """List leads filtered by email."""
        raise NotImplementedError

    @abstractmethod
    def list_deals_by_contact(self, contact_id: str) -> Dict[str, Any]:
        """List deals linked to a contact."""
        raise NotImplementedError

    @abstractmethod
    def list_deals_by_company(self, company_id: str) -> Dict[str, Any]:
        """List deals linked to a company."""
        raise NotImplementedError

    @abstractmethod
    def add_timeline_comment(self, *, lead_id: str, comment: str) -> Dict[str, Any]:
        """Add a timeline comment to a lead."""
        raise NotImplementedError

    @abstractmethod
    def create_contact(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        """Create a contact in Bitrix24 CRM."""
        raise NotImplementedError

    @abstractmethod
    def add_deal_timeline_comment(self, *, deal_id: str, comment: str) -> Dict[str, Any]:
        """Add a timeline comment to a deal."""
        raise NotImplementedError

    @abstractmethod
    def add_lead_todo(
        self,
        *,
        lead_id: str,
        title: str,
        description: str,
        responsible_id: str,
        deadline: str,
    ) -> Dict[str, Any]:
        """Add a TODO activity to a lead."""
        raise NotImplementedError
