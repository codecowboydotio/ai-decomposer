"""GossipSub wiring + schema-validated pubsub helpers (spec §6, §6a).

Every incoming message is parsed against its expected Pydantic schema
before being acted on; malformed messages (or ones that fail the
content-hash integrity check) are logged and dropped, never processed.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TypeVar

import trio
from libp2p.abc import IHost
from libp2p.custom_types import TProtocol
from libp2p.network.stream.exceptions import StreamError
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
from pydantic import BaseModel, ValidationError

from decentralized_decomposer import config

logger = logging.getLogger(__name__)

GOSSIPSUB_PROTOCOL_ID = TProtocol("/meshsub/1.0.0")

M = TypeVar("M", bound=BaseModel)


def build_pubsub(host: IHost) -> tuple[GossipSub, Pubsub]:
    """Construct GossipSub tuned for a handful of local-laptop peers (spec §2 scale)."""
    gossipsub = GossipSub(
        protocols=[GOSSIPSUB_PROTOCOL_ID],
        degree=3,
        degree_low=1,
        degree_high=6,
        heartbeat_initial_delay=0.5,
        heartbeat_interval=2,
    )
    pubsub = Pubsub(host, gossipsub)
    return gossipsub, pubsub


def parse_and_validate(raw: bytes, model_cls: type[M]) -> M | None:
    """Parse `raw` JSON bytes against `model_cls`, returning None on any failure.

    Also enforces the content-hash integrity check (`verify_integrity`) when
    the model defines one, per spec §6a.
    """
    try:
        message = model_cls.model_validate_json(raw)
    except ValidationError:
        logger.warning("Dropping malformed %s message", model_cls.__name__, exc_info=True)
        return None

    verify = getattr(message, "verify_integrity", None)
    if callable(verify) and not verify():
        logger.warning("Dropping %s with failed integrity check", model_cls.__name__)
        return None

    return message


async def _retry_stream_broadcast(op: Callable[[], Awaitable[M]], description: str) -> M:
    """Retry a `pubsub.subscribe`/`unsubscribe` call against a dead-peer race.

    Both calls broadcast an announcement to every currently-connected peer
    (`Pubsub.message_all_peers`); if writing to any *one* of them raises
    `StreamReset` (the peer's process died rather than closing cleanly), the
    whole broadcast aborts with an uncaught exception -- py-libp2p's own
    broadcast loop only catches the sibling `StreamClosed` and prunes that
    peer gracefully, not `StreamReset` (they're siblings under `StreamError`,
    not a subclass relationship). Left alone, this crashes the calling
    agent's entire trio nursery over one stale connection. The backoff here
    gives py-libp2p's own dead-peer sweep (`Pubsub.handle_dead_peer_queue`,
    driven by swarm connection-loss notifications) a chance to prune the
    stale peer before the next attempt.
    """
    delay = config.PUBSUB_STREAM_RETRY_BASE_DELAY
    for attempt in range(config.MAX_PUBSUB_STREAM_RETRIES + 1):
        try:
            return await op()
        except StreamError as exc:
            if attempt >= config.MAX_PUBSUB_STREAM_RETRIES:
                raise
            logger.warning(
                "%s hit a dead peer connection (attempt %d/%d): %s; retrying",
                description,
                attempt + 1,
                config.MAX_PUBSUB_STREAM_RETRIES + 1,
                exc,
            )
            await trio.sleep(delay)
            delay *= 2
    raise AssertionError("unreachable")  # loop always returns or raises above


async def subscribe_and_validate(pubsub: Pubsub, topic: str, model_cls: type[M]) -> AsyncIterator[M]:
    """Subscribe to `topic` and yield validated `model_cls` instances, dropping bad ones.

    `pubsub.subscribe(topic)` hands back a shared subscription object when more
    than one caller subscribes to the same topic concurrently (e.g. two
    in-flight handlers for the same node, or a duplicate message triggering a
    second handler before the first exits). If one of those callers finishes
    and unsubscribes, the underlying channel closes out from under the other
    caller, which sees `trio.EndOfChannel` from `subscription.get()`. Treat
    that the same as "no more messages" instead of letting it blow up the
    whole process.
    """
    subscription = await _retry_stream_broadcast(
        lambda: pubsub.subscribe(topic), f"subscribe to {topic}"
    )
    try:
        while True:
            try:
                raw_message = await subscription.get()
            except trio.EndOfChannel:
                return
            parsed = parse_and_validate(raw_message.data, model_cls)
            if parsed is not None:
                yield parsed
    finally:
        try:
            await _retry_stream_broadcast(
                lambda: pubsub.unsubscribe(topic), f"unsubscribe from {topic}"
            )
        except StreamError:
            # Local subscription state is already torn down by this point
            # (Pubsub.unsubscribe clears it before broadcasting) -- only the
            # "tell peers we left" announcement was missed, not worth
            # crashing (or masking whatever exception is already propagating
            # through this generator's teardown) over.
            logger.warning("Giving up on cleanly announcing unsubscribe from %s", topic)


async def publish_model(pubsub: Pubsub, topic: str, message: BaseModel) -> None:
    await pubsub.publish(topic, message.model_dump_json().encode("utf-8"))
