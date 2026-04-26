"""Tests for MAX gateway integration and transport adapter."""

import json
from types import SimpleNamespace

import pytest

from gateway.config import HomeChannel, Platform, PlatformConfig, _apply_env_overrides
from gateway.platforms.max import MAX_MESSAGE_LENGTH, MaxAdapter, _MessageDedup


def test_platform_enum_has_max():
    assert Platform.MAX.value == "max"


def test_env_overrides_load_max_token_and_home(monkeypatch):
    cfg = SimpleNamespace(platforms={})
    monkeypatch.setenv("MAX_BOT_TOKEN", "token-123")
    monkeypatch.setenv("MAX_HOME_CHANNEL", "-100200300")
    monkeypatch.setenv("MAX_HOME_CHANNEL_NAME", "Ops")

    _apply_env_overrides(cfg)

    assert Platform.MAX in cfg.platforms
    p = cfg.platforms[Platform.MAX]
    assert p.enabled is True
    assert p.token == "token-123"
    assert isinstance(p.home_channel, HomeChannel)
    assert p.home_channel.chat_id == "-100200300"
    assert p.home_channel.name == "Ops"


def test_adapter_init_reads_config_and_dedup_path(tmp_path):
    cfg = PlatformConfig(
        enabled=True,
        token="tok",
        extra={"dedup_db_path": str(tmp_path / "dedup.db")},
    )
    adapter = MaxAdapter(cfg)
    assert adapter.platform == Platform.MAX
    assert adapter.token == "tok"
    assert adapter._dedup is not None


def test_message_dedup_detects_duplicates(tmp_path):
    dedup = _MessageDedup(tmp_path / "seen.db", ttl=300)
    assert dedup.is_duplicate("mid-1") is False
    assert dedup.is_duplicate("mid-1") is True
    assert dedup.is_duplicate("mid-2") is False


@pytest.mark.asyncio
async def test_handle_update_skips_duplicate_message(monkeypatch, tmp_path):
    cfg = PlatformConfig(enabled=True, token="tok", extra={"dedup_db_path": str(tmp_path / "dedup.db")})
    adapter = MaxAdapter(cfg)
    calls = []

    async def _fake_handle_message(_update):
        calls.append("handled")

    adapter._handle_message = _fake_handle_message  # type: ignore[assignment]

    update = {
        "update_type": "message_created",
        "message": {
            "body": {"mid": "m-1", "text": "hi"},
            "recipient": {"chat_id": "10"},
            "sender": {"user_id": 20},
        },
    }
    await adapter._handle_update(update)
    await adapter._handle_update(update)

    assert calls == ["handled"]


@pytest.mark.asyncio
async def test_handle_message_filters_self_messages(tmp_path):
    cfg = PlatformConfig(enabled=True, token="tok", extra={"dedup_db_path": str(tmp_path / "dedup.db")})
    adapter = MaxAdapter(cfg)
    adapter._bot_id = 42
    seen = []

    async def _fake_handle_event(event):
        seen.append(event)

    adapter.handle_message = _fake_handle_event  # type: ignore[assignment]
    update = {
        "message": {
            "sender": {"user_id": 42, "name": "bot"},
            "recipient": {"chat_id": "123", "chat_type": "dialog"},
            "body": {"text": "loop"},
        }
    }
    await adapter._handle_message(update)
    assert seen == []


class _FakeResponse:
    def __init__(self, status, payload):
        self.status = status
        self._payload = payload

    async def read(self):
        import json

        return json.dumps(self._payload).encode("utf-8")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSession:
    def __init__(self):
        self.payloads = []

    def post(self, _url, **kwargs):
        self.payloads.append(kwargs.get("json", {}))
        return _FakeResponse(200, {"ok": True, "message": {"mid": "x"}})


@pytest.mark.asyncio
async def test_send_chunks_long_messages(tmp_path):
    cfg = PlatformConfig(enabled=True, token="tok", extra={"dedup_db_path": str(tmp_path / "dedup.db")})
    adapter = MaxAdapter(cfg)
    adapter._session = _FakeSession()
    text = "a" * (MAX_MESSAGE_LENGTH + 10)
    result = await adapter.send("123", text)
    assert result.success is True
    assert len(adapter._session.payloads) == 2


@pytest.mark.asyncio
async def test_send_detects_markdown_format(tmp_path):
    cfg = PlatformConfig(enabled=True, token="tok", extra={"dedup_db_path": str(tmp_path / "dedup.db")})
    adapter = MaxAdapter(cfg)
    adapter._session = _FakeSession()
    await adapter.send("123", "**bold** text")
    assert adapter._session.payloads[0]["format"] == "markdown"


def test_update_allowed_platforms_contains_max():
    from gateway.run import GatewayRunner

    assert Platform.MAX in GatewayRunner._UPDATE_ALLOWED_PLATFORMS


def test_send_message_tool_supports_max_target():
    import gateway.config as gateway_config
    import model_tools
    import tools.send_message_tool as send_tool

    cfg = SimpleNamespace(
        platforms={Platform.MAX: SimpleNamespace(enabled=True, token="tok", extra={})},
        get_home_channel=lambda _platform: None,
    )
    called = {}

    async def _fake_send(platform, _pconfig, chat_id, message, thread_id=None, media_files=None):
        called["platform"] = platform
        called["chat_id"] = chat_id
        called["message"] = message
        return {"success": True}

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(gateway_config, "load_gateway_config", lambda: cfg)
    monkeypatch.setattr(model_tools, "_run_async", lambda coro: __import__("asyncio").run(coro))
    monkeypatch.setattr(send_tool, "_send_to_platform", _fake_send)
    monkeypatch.setattr(send_tool, "_get_cron_auto_delivery_target", lambda: None)
    monkeypatch.setattr(send_tool, "_maybe_skip_cron_duplicate_send", lambda *args, **kwargs: None)
    monkeypatch.setattr(send_tool, "is_interrupted", lambda: False, raising=False)
    try:
        payload = send_tool.send_message_tool(
            {"action": "send", "target": "max:12345", "message": "hello"}
        )
    finally:
        monkeypatch.undo()

    data = json.loads(payload)
    assert data["success"] is True
    assert called["platform"] == Platform.MAX
    assert called["chat_id"] == "12345"


def test_cron_scheduler_resolves_max_delivery_target(monkeypatch):
    from cron.scheduler import _resolve_delivery_target

    monkeypatch.setenv("MAX_HOME_CHANNEL", "-9001")
    assert _resolve_delivery_target({"deliver": "max"}) == {
        "platform": "max",
        "chat_id": "-9001",
        "thread_id": None,
    }
