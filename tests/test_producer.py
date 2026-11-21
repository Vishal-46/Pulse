"""Producer unit tests. The Kafka admin client is replaced by a fake, so no broker is needed."""

from types import SimpleNamespace
from typing import ClassVar

from pulse.config import Settings
from pulse.ingestion import producer


class FakeAdmin:
    created: ClassVar[list] = []

    def __init__(self, existing):
        self._existing = existing

    def list_topics(self, timeout):
        return SimpleNamespace(topics={name: None for name in self._existing})

    def create_topics(self, topics):
        if not topics:
            raise ValueError("Expected non-empty list of NewTopic objects")  # real client behaviour
        FakeAdmin.created.extend(t.topic for t in topics)
        return {t.topic: SimpleNamespace(result=lambda: None) for t in topics}


def run_with(monkeypatch, existing):
    FakeAdmin.created = []
    monkeypatch.setattr(producer, "AdminClient", lambda conf: FakeAdmin(existing))
    producer.ensure_topics(Settings())
    return FakeAdmin.created


def test_creates_missing_topics(monkeypatch):
    assert run_with(monkeypatch, existing=[]) == ["market.bars.1m", "market.bars.dlq"]


def test_ensure_topics_is_idempotent(monkeypatch):
    # Second run: both topics exist -> must do nothing (this crashed before the fix).
    assert run_with(monkeypatch, existing=["market.bars.1m", "market.bars.dlq"]) == []


def test_creates_only_the_missing_one(monkeypatch):
    assert run_with(monkeypatch, existing=["market.bars.1m"]) == ["market.bars.dlq"]
