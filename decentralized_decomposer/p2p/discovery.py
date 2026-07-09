"""mDNS-only peer discovery (spec §6, §7): no Kademlia DHT bootstrap node.

Newly discovered peers are auto-connected so GossipSub has a mesh to work
with. `MDNSDiscovery` is constructed here (rather than via
`new_host(enable_mDNS=True)`) so it advertises the agent's *actual* listen
port -- see the comment in `p2p/host.py`.
"""

from __future__ import annotations

import logging

import trio
from libp2p.abc import IHost, PeerInfo
from libp2p.discovery.events.peerDiscovery import peerDiscovery
from libp2p.discovery.mdns.mdns import MDNSDiscovery

logger = logging.getLogger(__name__)


class Discovery:
    def __init__(self, host: IHost, port: int):
        self.host = host
        self.mdns = MDNSDiscovery(host.get_network(), port=port)
        self._send_channel, self._receive_channel = trio.open_memory_channel[PeerInfo](64)
        self._trio_token: trio.lowlevel.TrioToken | None = None

    def _on_peer_discovered(self, peer_info: PeerInfo) -> None:
        if peer_info.peer_id == self.host.get_id():
            return
        if self._trio_token is None:
            return
        try:
            trio.from_thread.run_sync(
                self._send_channel.send_nowait, peer_info, trio_token=self._trio_token
            )
        except (trio.WouldBlock, trio.RunFinishedError):
            pass

    async def start(self, nursery: trio.Nursery) -> None:
        self._trio_token = trio.lowlevel.current_trio_token()
        peerDiscovery.register_peer_discovered_handler(self._on_peer_discovered)
        self.mdns.start()
        nursery.start_soon(self._connect_loop)

    async def _connect_loop(self) -> None:
        async for peer_info in self._receive_channel:
            if peer_info.peer_id in self.host.get_network().connections:
                continue
            try:
                await self.host.connect(peer_info)
                logger.info("Connected to discovered peer %s", peer_info.peer_id)
            except Exception:
                logger.debug("Failed to connect to %s", peer_info.peer_id, exc_info=True)

    def stop(self) -> None:
        self.mdns.stop()
