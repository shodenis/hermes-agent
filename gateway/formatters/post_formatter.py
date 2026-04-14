"""
LLM-based HTML post formatting for MAX messenger (HTML only — no Markdown).

MAX accepts: <b>, <i>, <a href="...">, <code>.
"""

from __future__ import annotations

import html
import logging
import os
import re
from html.parser import HTMLParser
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

SPARE24_TAGLINE = (
    "Наша сеть международных партнёров позволяет решать задачи любой сложности — "
    "от редких позиций до поставок в условиях ограничений."
)

_SYSTEM_PROMPT = """Ты форматируешь короткий пост для мессенджера MAX (только HTML).

Правила:
- Язык: русский. Кратко, по делу, без воды.
- Структура (по возможности): <b>Заголовок</b>, краткое описание, при необходимости <a href="URL">Источник</a>, хештеги в конце строки при уместности.
- Подводка бренда (один абзац в конце), если её ещё нет в исходном тексте:
  «""" + SPARE24_TAGLINE + """»
  Если исходный текст уже содержит эту подводку или очень близкий по смыслу абзац — НЕ дублируй.
- Разрешены ТОЛЬКО теги: <b>, <i>, <a href="...">, <code>. Закрывай теги корректно.
- Запрещено: markdown (**, ##, [], - списки markdown), любые другие HTML-теги, <script>, стили.
- Выведи только готовый HTML фрагмент текста поста, без преамбулы и без обёртки ```."""


def _resolve_llm_credentials() -> Tuple[str, str, str]:
    """api_key, base_url, model_id — как у основного агента Hermes."""
    from hermes_cli.config import load_config
    from hermes_cli.runtime_provider import resolve_runtime_provider

    runtime: Dict[str, Any] = resolve_runtime_provider(
        requested=os.getenv("HERMES_INFERENCE_PROVIDER"),
    )
    api_key = str(runtime.get("api_key") or "").strip()
    base_url = str(runtime.get("base_url") or "").strip()

    cfg = load_config()
    mc = cfg.get("model") or {}
    if isinstance(mc, str):
        model = mc.strip()
        if not base_url:
            base_url = ""
    elif isinstance(mc, dict):
        model = str(mc.get("default") or mc.get("model") or "").strip()
        if not base_url:
            base_url = str(mc.get("base_url") or "").strip()
    else:
        model = ""

    return api_key, base_url, model


def _chat_completions_url(base_url: str) -> str:
    b = base_url.rstrip("/")
    if not b:
        return ""
    if b.endswith("/chat/completions"):
        return b
    if b.endswith("/v1"):
        return f"{b}/chat/completions"
    return f"{b}/v1/chat/completions"


def _strip_markdown_artifacts(s: str) -> str:
    s = re.sub(r"^```(?:html)?\s*", "", s.strip(), flags=re.I)
    s = re.sub(r"\s*```\s*$", "", s)
    s = re.sub(r"\*\*([^*]+)\*\*", r"<b>\1</b>", s)
    s = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"<i>\1</i>", s)
    return s.strip()


class _MaxHTMLSanitizer(HTMLParser):
    """Keep only MAX-allowed tags; strip everything else; escape text nodes."""

    _ALLOW = frozenset({"b", "i", "code", "a"})

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._out: List[str] = []
        self._skip: int = 0

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, str | None]]) -> None:
        t = tag.lower()
        if t in ("script", "style"):
            self._skip += 1
            return
        if self._skip:
            return
        if t not in self._ALLOW:
            return
        if t in ("b", "i", "code"):
            self._out.append(f"<{t}>")
            return
        if t == "a":
            href = ""
            for k, v in attrs:
                if (k or "").lower() == "href" and v:
                    href = v.strip()
                    break
            if href.startswith(("http://", "https://")):
                self._out.append(f'<a href="{html.escape(href, quote=True)}">')

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t in ("script", "style"):
            self._skip = max(0, self._skip - 1)
            return
        if self._skip:
            return
        if t in ("b", "i", "code", "a"):
            self._out.append(f"</{t}>")

    def handle_data(self, data: str) -> None:
        if self._skip:
            return
        self._out.append(html.escape(data))

    def parsed(self) -> str:
        return "".join(self._out)


def _sanitize_max_html(fragment: str) -> str:
    """Allow only <b>, <i>, <code>, <a href=\"http(s):...\">; strip other tags."""
    cleaned = re.sub(r"(?is)<script[^>]*>.*?</script>", "", fragment)
    cleaned = re.sub(r"(?is)<style[^>]*>.*?</style>", "", cleaned)
    parser = _MaxHTMLSanitizer()
    parser.feed(cleaned)
    parser.close()
    return parser.parsed()


def format_post(raw_text: str) -> str:
    """Return HTML-formatted post body for MAX (LLM + light sanitization).

    On any failure, returns a safe minimal HTML fragment with escaped source text.
    """
    raw = (raw_text or "").strip()
    if not raw:
        return ""

    if os.getenv("MAX_POST_FORMAT_DISABLE", "").strip().lower() in ("1", "true", "yes", "on"):
        logger.info("post_formatter: skipped (MAX_POST_FORMAT_DISABLE), escaped text only")
        return html.escape(raw)

    api_key, base_url, model = _resolve_llm_credentials()
    if not api_key or not model:
        logger.warning("post_formatter: missing api_key or model — falling back to escaped text")
        return html.escape(raw)

    url = _chat_completions_url(base_url)
    if not url:
        logger.warning("post_formatter: empty base_url — falling back to escaped text")
        return html.escape(raw)

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": raw},
        ],
        "temperature": 0.35,
        "max_tokens": 1200,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        import httpx

        logger.info("post_formatter: calling LLM for HTML channel post (model=%s)", model)
        with httpx.Client(timeout=60.0) as client:
            r = client.post(url, headers=headers, json=payload)
            if r.status_code >= 400:
                logger.warning(
                    "post_formatter: chat/completions HTTP %s: %s",
                    r.status_code,
                    r.text[:400],
                )
                return html.escape(raw)
            data = r.json()
            choices = data.get("choices") or []
            if not choices:
                return html.escape(raw)
            content = (choices[0].get("message") or {}).get("content") or ""
            content = _strip_markdown_artifacts(content)
            if not content.strip():
                return html.escape(raw)
            return _sanitize_max_html(content)
    except Exception as exc:
        logger.warning("post_formatter: LLM error: %s", exc)
        return html.escape(raw)
