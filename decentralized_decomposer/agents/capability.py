"""Executor capability advertisement + matching (spec §3.3).

Capabilities are announced once via gossip on a well-known topic that
subscribed peers pick up directly -- no DHT lookup, consistent with the
mDNS-only discovery model (spec §6, §7).
"""

from __future__ import annotations

import logging

from libp2p.pubsub.pubsub import Pubsub
from pydantic import BaseModel, Field

from decentralized_decomposer.config import Capability
from decentralized_decomposer.protocol.topics import CAPABILITIES_ANNOUNCE_TOPIC

logger = logging.getLogger(__name__)


class CapabilityAnnounce(BaseModel):
    peer_id: str
    capabilities: list[Capability] = Field(min_length=1)


async def announce_capabilities(
    pubsub: Pubsub, peer_id: str, capabilities: list[Capability]
) -> None:
    from decentralized_decomposer.p2p.pubsub import publish_model

    await publish_model(
        pubsub, CAPABILITIES_ANNOUNCE_TOPIC, CapabilityAnnounce(peer_id=peer_id, capabilities=capabilities)
    )


def can_handle(own_capabilities: list[Capability], required_capability: str | None) -> bool:
    """Whether an executor advertising `own_capabilities` should claim a node.

    A node with no `required_capability` (spec §4 base schema doesn't have
    one; see PromptNode docstring) is treated as handleable by anyone --
    conservative capability-gating only kicks in when a capability is
    actually specified.
    """
    if required_capability is None:
        return True
    return required_capability in {c.value for c in own_capabilities}


class CapabilityRegistry:
    """Tracks peer_id -> advertised capabilities, learned from gossip announcements."""

    def __init__(self) -> None:
        self._by_peer: dict[str, list[Capability]] = {}

    def record(self, announce: CapabilityAnnounce) -> None:
        self._by_peer[announce.peer_id] = announce.capabilities

    def peers_with(self, capability: Capability) -> list[str]:
        return [peer for peer, caps in self._by_peer.items() if capability in caps]

    def capabilities_of(self, peer_id: str) -> list[Capability]:
        return self._by_peer.get(peer_id, [])
