"""Lightweight test doubles for Layer 2 agent-logic tests (spec §9.3): no real network."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import trio
from pydantic import BaseModel


class FakeRawMessage:
    def __init__(self, data: bytes):
        self.data = data


class FakeSubscription:
    def __init__(self) -> None:
        self._send, self._recv = trio.open_memory_channel(32)

    async def get(self) -> FakeRawMessage:
        return await self._recv.receive()

    async def put(self, data: bytes) -> None:
        await self._send.send(FakeRawMessage(data))


class FakePubsub:
    """Stands in for `libp2p.pubsub.pubsub.Pubsub` -- just subscribe/publish/unsubscribe."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []
        self._subscriptions: dict[str, FakeSubscription] = {}

    async def subscribe(self, topic: str) -> FakeSubscription:
        return self._subscriptions.setdefault(topic, FakeSubscription())

    async def unsubscribe(self, topic: str) -> None:
        self._subscriptions.pop(topic, None)

    async def publish(self, topic: str, data: bytes) -> None:
        self.published.append((topic, data))

    async def deliver(self, topic: str, message: BaseModel) -> None:
        """Push `message` onto `topic` as if it had arrived from the network."""
        sub = self._subscriptions.setdefault(topic, FakeSubscription())
        await sub.put(message.model_dump_json().encode("utf-8"))

    def published_on(self, topic: str) -> list[bytes]:
        return [data for t, data in self.published if t == topic]


def fake_llm_client(*responses: str) -> AsyncMock:
    """An AsyncAnthropic-shaped mock whose `messages.create` returns each `responses[i]`
    (as the text of a single content block) in turn, one per call.
    """
    client = AsyncMock()
    client.messages.create.side_effect = [
        SimpleNamespace(content=[SimpleNamespace(type="text", text=text)]) for text in responses
    ]
    return client


def fake_llm_client_raising(exc: Exception, then: str | None = None) -> AsyncMock:
    """An AsyncAnthropic-shaped mock whose `messages.create` raises `exc` every call,
    unless `then` is given, in which case it raises `exc` once and then succeeds with `then`.
    """
    client = AsyncMock()
    if then is None:
        client.messages.create.side_effect = exc
    else:
        client.messages.create.side_effect = [
            exc,
            SimpleNamespace(content=[SimpleNamespace(type="text", text=then)]),
        ]
    return client
