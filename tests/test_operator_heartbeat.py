"""Real HTTP classification and canonical availability with a controlled clock."""

from datetime import datetime

import pytest
from fastapi.testclient import TestClient
from src.cancellation import availability
from src.web import app as web_app


class ClockRedis:
    """Controlled Redis transport: retain the TTL supplied by real middleware."""

    def __init__(self):
        self.now = 0
        self.deadline = 0
        self.override = None
        self.failed = False

    async def set(self, key, value, *, ex):
        assert (key, value, ex) == ("operator_heartbeat", "1", 300)
        if self.failed:
            raise ConnectionError("controlled outage")
        self.deadline = self.now + ex

    async def get(self, key):
        return self.override

    async def exists(self, key):
        if self.failed:
            raise ConnectionError("controlled outage")
        return int(self.now < self.deadline)


@pytest.fixture
def presence_client(tmp_path, monkeypatch):
    config = tmp_path / "config.yml"
    config.write_text("repositories: []\ndaemon:\n  operator_timezone: UTC\n")
    monkeypatch.setattr(web_app, "CONFIG_PATH", str(config))
    redis = ClockRedis()
    monkeypatch.setattr(web_app.app.state, "redis", redis, raising=False)

    class FixedDatetime:
        hour = 3

        @classmethod
        def now(cls, tz):
            return datetime(2026, 10, 27, cls.hour, tzinfo=tz)

    monkeypatch.setattr(availability, "datetime", FixedDatetime)
    # No lifespan: do not connect to runtime Redis or launch discovery tasks.
    yield TestClient(web_app.app), redis, FixedDatetime


def test_idle_polls_expire_but_interaction_renews(presence_client):
    client, redis, _clock = presence_client
    url = "/api/availability"
    assert client.get(url).json()["composed_state"] == "AWAY"
    assert client.get(url, headers={"X-Operator-Interaction": "true"}).status_code == 200
    assert redis.deadline == 300
    for second in (60, 120, 180, 240, 299, 300, 360):
        redis.now = second
        response = client.get(url, headers={
            "HX-Request": "true", "Sec-Fetch-Site": "same-origin",
        })
        assert response.json()["composed_state"] == ("AVAILABLE" if second < 300 else "AWAY")
        assert redis.deadline == 300
    client.get(url, headers={"X-Operator-Interaction": "true"})
    assert redis.deadline == 660
    assert client.get(url).json()["composed_state"] == "AVAILABLE"


def test_navigation_active_hours_and_manual_precedence(presence_client):
    client, redis, clock = presence_client
    url = "/api/availability"
    client.get(url, headers={
        "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Dest": "document",
        "Sec-Fetch-User": "?1", "Sec-Fetch-Site": "none",
    })
    assert redis.deadline == 300
    redis.override = "AWAY"
    assert client.get(url).json()["composed_state"] == "AWAY"
    redis.now = 300
    redis.override = "AVAILABLE"
    assert client.get(url).json()["composed_state"] == "AVAILABLE"
    redis.override = None
    assert client.get(url).json()["composed_state"] == "AWAY"
    clock.hour = 12
    assert client.get(url).json()["composed_state"] == "AVAILABLE"
    assert redis.deadline == 300


def test_unknown_heartbeat_keeps_failure_safe_policy_and_override(presence_client, caplog):
    client, redis, _clock = presence_client
    redis.failed = True
    response = client.get("/api/availability", headers={"X-Operator-Interaction": "true"})
    assert response.status_code == 200
    assert response.json()["composed_state"] == "AVAILABLE"
    assert "presence unknown" in caplog.text
    redis.override = "AWAY"
    assert client.get("/api/availability").json()["composed_state"] == "AWAY"
