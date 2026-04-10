from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, cast

from .action_provider import ActionProvider
from .types import LeadFields

logger = logging.getLogger(__name__)


class CRMUseCases:
    """Application use-case layer for common CRM business operations."""

    def __init__(self, actions: ActionProvider, responsible_id: str):
        """Initialize use cases with ActionProvider and default responsible user ID."""
        self._actions = actions
        self._responsible_id = responsible_id

    def upsert_lead_by_email(self, email: str, fields: LeadFields) -> Dict[str, Any]:
        """Get existing lead by email duplicates or create a new one.

        The method checks duplicates by email first. If any lead duplicates are found,
        it returns the existing lead details. Otherwise it creates a new lead and
        returns creation metadata.
        """
        logger.info("CRMUseCases.upsert_lead_by_email started email=%s", email)
        try:
            duplicate_payload = self._actions.find_duplicate_by_email(email)
            lead_ids = self._extract_duplicate_entity_ids(duplicate_payload, "LEAD")
            if lead_ids:
                lead_id = str(lead_ids[0])
                existing = self._actions.get_lead(lead_id)
                logger.info(
                    "CRMUseCases.upsert_lead_by_email duplicate found email=%s lead_id=%s",
                    email,
                    lead_id,
                )
                return {
                    "status": "existing",
                    "lead_id": lead_id,
                    "lead": existing,
                }

            create_result = self._actions.create_lead(fields)
            created_id = create_result.get("result")
            logger.info(
                "CRMUseCases.upsert_lead_by_email created new lead email=%s lead_id=%s",
                email,
                created_id,
            )
            return {
                "status": "created",
                "lead_id": created_id,
                "lead": create_result,
            }
        except Exception:
            logger.exception("CRMUseCases.upsert_lead_by_email failed email=%s", email)
            raise

    def precheck_by_email(self, email: str) -> Dict[str, str]:
        """Collect duplicate/contact/company/open lead/deal IDs for an email."""
        logger.info("CRMUseCases.precheck_by_email started email=%s", email)
        out = {
            "contact_id": "",
            "company_id": "",
            "open_lead_id": "",
            "open_deal_id": "",
        }
        if not email:
            return out
        try:
            duplicate_payload = self._actions.find_duplicate_by_email(email)
            dup_result = duplicate_payload.get("result") if isinstance(duplicate_payload, dict) else {}
            if isinstance(dup_result, dict):
                contacts = dup_result.get("CONTACT") if isinstance(dup_result.get("CONTACT"), list) else []
                companies = dup_result.get("COMPANY") if isinstance(dup_result.get("COMPANY"), list) else []
                if contacts:
                    out["contact_id"] = str(contacts[0])
                if companies:
                    out["company_id"] = str(companies[0])

            lead_data = self._actions.list_leads_by_email(email)
            lead_result = lead_data.get("result") if isinstance(lead_data, dict) else None
            if isinstance(lead_result, list) and lead_result:
                out["open_lead_id"] = str((lead_result[0] or {}).get("ID") or "")

            if out["contact_id"]:
                deal_data = self._actions.list_deals_by_contact(out["contact_id"])
                deal_result = deal_data.get("result") if isinstance(deal_data, dict) else None
                if isinstance(deal_result, list) and deal_result:
                    out["open_deal_id"] = str((deal_result[0] or {}).get("ID") or "")
            if not out["open_deal_id"] and out["company_id"]:
                deal_data = self._actions.list_deals_by_company(out["company_id"])
                deal_result = deal_data.get("result") if isinstance(deal_data, dict) else None
                if isinstance(deal_result, list) and deal_result:
                    out["open_deal_id"] = str((deal_result[0] or {}).get("ID") or "")

            logger.info("CRMUseCases.precheck_by_email completed email=%s", email)
            return out
        except Exception:
            logger.exception("CRMUseCases.precheck_by_email failed email=%s", email)
            raise

    def ensure_contact_exists(self, email: str, name: str = "") -> Dict[str, Any]:
        """Ensure a contact exists for email, creating it if absent.

        Contact lookup uses duplicate search by email and checks CONTACT entries.
        If no contact exists, creates a new one.
        """
        logger.info("CRMUseCases.ensure_contact_exists started email=%s", email)
        try:
            duplicate_payload = self._actions.find_duplicate_by_email(email)
            contact_ids = self._extract_duplicate_entity_ids(duplicate_payload, "CONTACT")
            if contact_ids:
                contact_id = str(contact_ids[0])
                logger.info(
                    "CRMUseCases.ensure_contact_exists found existing contact email=%s contact_id=%s",
                    email,
                    contact_id,
                )
                return {
                    "status": "existing",
                    "contact_id": contact_id,
                }

            created = self._actions.create_contact({"NAME": name, "EMAIL": email})
            logger.info(
                "CRMUseCases.ensure_contact_exists created contact email=%s result=%s",
                email,
                created,
            )
            return {
                "status": "created",
                "contact": created,
            }
        except Exception:
            logger.exception("CRMUseCases.ensure_contact_exists failed email=%s", email)
            raise

    def add_lead_with_reminder(
        self,
        lead_data: LeadFields,
        reminder_text: str,
        deadline_days: int,
    ) -> Dict[str, Any]:
        """Create/upsert lead, add timeline comment, then add TODO reminder.

        Deadline is calculated as current UTC datetime + `deadline_days`, formatted
        in ISO 8601 datetime representation.
        """
        logger.info(
            "CRMUseCases.add_lead_with_reminder started deadline_days=%s",
            deadline_days,
        )
        try:
            email = str(lead_data.get("EMAIL", "")).strip()
            if email:
                lead_result = self.upsert_lead_by_email(email=email, fields=lead_data)
            else:
                created = self._actions.create_lead(lead_data)
                lead_result = {
                    "status": "created",
                    "lead_id": created.get("result"),
                    "lead": created,
                }

            lead_id = str(lead_result.get("lead_id", "")).strip()
            if not lead_id:
                lead_id = str(cast(Dict[str, Any], lead_result.get("lead", {})).get("result", "")).strip()
            if not lead_id or (not lead_id.isdigit()) or int(lead_id) <= 0:
                raise ValueError(f"Invalid lead_id resolved for reminder workflow: {lead_id!r}")

            self._actions.add_timeline_comment(lead_id=lead_id, comment=reminder_text)

            deadline_dt = datetime.now(timezone.utc) + timedelta(days=deadline_days)
            deadline_iso = deadline_dt.isoformat()
            todo_result = self._actions.add_lead_todo(
                lead_id=lead_id,
                title="Follow up",
                description=reminder_text,
                responsible_id=self._responsible_id,
                deadline=deadline_iso,
            )

            response = {
                "lead": lead_result,
                "todo": todo_result,
                "deadline": deadline_iso,
            }
            logger.info(
                "CRMUseCases.add_lead_with_reminder completed lead_id=%s",
                lead_id,
            )
            return response
        except Exception:
            logger.exception("CRMUseCases.add_lead_with_reminder failed")
            raise

    def log_communication(self, entity_type: str, entity_id: str, message: str) -> Dict[str, Any]:
        """Log communication message on supported entity timeline.

        Supports both `lead` and `deal` entities via ActionProvider methods.
        """
        logger.info(
            "CRMUseCases.log_communication started entity_type=%s entity_id=%s",
            entity_type,
            entity_id,
        )
        try:
            normalized = entity_type.strip().lower()
            if normalized == "lead":
                result = self._actions.add_timeline_comment(lead_id=entity_id, comment=message)
            elif normalized == "deal":
                result = self._actions.add_deal_timeline_comment(deal_id=entity_id, comment=message)
            else:
                raise ValueError("entity_type must be 'lead' or 'deal'")

            logger.info(
                "CRMUseCases.log_communication completed entity_type=%s entity_id=%s",
                normalized,
                entity_id,
            )
            return {"status": "ok", "result": result}
        except Exception:
            logger.exception(
                "CRMUseCases.log_communication failed entity_type=%s entity_id=%s",
                entity_type,
                entity_id,
            )
            raise

    def add_lead_todo(
        self,
        *,
        lead_id: str,
        title: str,
        description: str,
        deadline_iso: str,
        responsible_id: str | None = None,
    ) -> Dict[str, Any]:
        """Create a lead TODO activity via ActionProvider."""
        logger.info("CRMUseCases.add_lead_todo started lead_id=%s", lead_id)
        try:
            rid = responsible_id or self._responsible_id
            result = self._actions.add_lead_todo(
                lead_id=lead_id,
                title=title,
                description=description,
                responsible_id=rid,
                deadline=deadline_iso,
            )
            logger.info("CRMUseCases.add_lead_todo completed lead_id=%s", lead_id)
            return result
        except Exception:
            logger.exception("CRMUseCases.add_lead_todo failed lead_id=%s", lead_id)
            raise

    @staticmethod
    def _extract_duplicate_entity_ids(payload: Dict[str, Any], entity_key: str) -> List[str]:
        result = payload.get("result")
        if not isinstance(result, dict):
            return []
        raw_ids = result.get(entity_key)
        if not isinstance(raw_ids, list):
            return []
        return [str(item) for item in raw_ids if isinstance(item, (int, str))]
