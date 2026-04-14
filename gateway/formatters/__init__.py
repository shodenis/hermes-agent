"""Gateway text formatters (e.g. MAX channel HTML posts)."""

from .post_formatter import format_post
from .spare24_formatter import (
    SPARE24_CHANNEL_CHAT_ID,
    apply_post_formatting_rules,
    apply_spare24_formatting_for_max_outbound,
    is_manual_review_chat_id,
    is_spare24_channel_chat_id,
    manual_review_chat_ids,
)

__all__ = [
    "SPARE24_CHANNEL_CHAT_ID",
    "apply_post_formatting_rules",
    "apply_spare24_formatting_for_max_outbound",
    "format_post",
    "is_manual_review_chat_id",
    "is_spare24_channel_chat_id",
    "manual_review_chat_ids",
]
