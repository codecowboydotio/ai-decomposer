"""libp2p host setup with persistent identity (spec §5, §6, §7).

A fresh PeerId on every restart breaks peer discovery continuity, so each
agent's ed25519 keypair is generated once and persisted to a local file.
"""

from __future__ import annotations

import logging
from pathlib import Path

from libp2p import new_host
from libp2p.abc import IHost
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.crypto.keys import KeyPair
from libp2p.crypto.serialization import deserialize_private_key
from libp2p.utils.address_validation import find_free_port, get_available_interfaces
from multiaddr import Multiaddr

from decentralized_decomposer.config import IDENTITY_DIR

logger = logging.getLogger(__name__)


def identity_path(identity_name: str) -> Path:
    return Path(IDENTITY_DIR) / f"{identity_name}.key"


def load_or_create_key_pair(identity_name: str) -> KeyPair:
    """Load a persisted ed25519 keypair for `identity_name`, creating one on first run."""
    path = identity_path(identity_name)
    if path.exists():
        private_key = deserialize_private_key(path.read_bytes())
        key_pair = KeyPair(private_key, private_key.get_public_key())
        logger.info("Loaded existing identity for %s", identity_name)
        return key_pair

    key_pair = create_new_key_pair()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(key_pair.private_key.serialize())
    logger.info("Created new identity for %s at %s", identity_name, path)
    return key_pair


def build_host(identity_name: str, port: int | None = None) -> tuple[IHost, list[Multiaddr], int]:
    """Build a libp2p host with a persistent identity and mDNS discovery enabled.

    Returns (host, listen_addrs, port). TCP transport only — WebSocket
    transport is avoided per spec §6 (still experimental upstream); QUIC
    is left disabled by default since it is not required for the
    single-laptop deployment target (spec §7) and TCP keeps the local
    setup simplest to debug.
    """
    key_pair = load_or_create_key_pair(identity_name)
    if port is None or port == 0:
        port = find_free_port()
    listen_addrs = get_available_interfaces(port)
    # enable_mDNS=False: BasicHost wires its built-in MDNSDiscovery with a
    # hardcoded default port (8000) rather than the actual listen port
    # (see libp2p/host/basic_host.py), which breaks discovery for any agent
    # not listening on 8000. p2p/discovery.py builds MDNSDiscovery itself
    # with the correct port instead.
    host = new_host(key_pair=key_pair, enable_mDNS=False)
    return host, listen_addrs, port
