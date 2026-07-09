"""GossipSub wiring + schema-validated pubsub helpers (spec §6, §6a).

Every incoming message is parsed against its expected Pydantic schema
before being acted on; malformed messages (or ones that fail the
content-hash integrity check) are logged and dropped, never processed.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import TypeVar

import trio
from libp2p.abc import IHost
from libp2p.custom_types import TProtocol
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
from pydantic import BaseModel, ValidationError

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
    subscription = await pubsub.subscribe(topic)
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
        await pubsub.unsubscribe(topic)


async def publish_model(pubsub: Pubsub, topic: str, message: BaseModel) -> None:
    await pubsub.publish(topic, message.model_dump_json().encode("utf-8"))
