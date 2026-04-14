"""Session NDJSON debug log (debug mode). Do not log secrets."""
import json
import time
from typing import Any, Dict

_DEBUG_LOG_PATH = "/root/.hermes/.cursor/debug-079763.log"
_SESSION_ID = "079763"


def agent_debug_log(
    hypothesis_id: str,
    location: str,
    message: str,
    data: Dict[str, Any],
) -> None:
    try:
        with open(_DEBUG_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(
                json.dumps(
                    {
                        "sessionId": _SESSION_ID,
                        "hypothesisId": hypothesis_id,
                        "location": location,
                        "message": message,
                        "data": data,
                        "timestamp": int(time.time() * 1000),
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
    except Exception:
        pass
