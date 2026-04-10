from __future__ import annotations

from typing import Dict, Optional, TypedDict


class LeadFields(TypedDict, total=False):
    TITLE: str
    NAME: str
    EMAIL: str
    PHONE: str


class ContactFields(TypedDict, total=False):
    NAME: str
    EMAIL: str
    PHONE: str


class LeadResponse(TypedDict):
    result: int


class DuplicateResponse(TypedDict):
    result: Optional[Dict[str, object]]
