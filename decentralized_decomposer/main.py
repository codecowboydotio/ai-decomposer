"""CLI entrypoint (spec §5, §5a, §7): run a single agent as a standalone OS process.

Concurrency note: the spec (§5a) describes each agent running its own
`asyncio` event loop. py-libp2p 0.6 (the only maintained Python libp2p
implementation, and the one with native GossipSub + mDNS support this
project relies on) is built entirely on `trio`, not `asyncio`, so this
implementation uses trio throughout instead. It fills the same role
described in §5a: one structured-concurrency loop per process that keeps
handling gossip/heartbeats while LLM calls are awaited (httpx, which the
Anthropic SDK is built on, works the same under trio as under asyncio).

Example (see spec §7, Option 1 -- one terminal per agent):
    python -m decentralized_decomposer.main --role decomposer --port 4001 \\
        --submit-goal "Plan a two-week trip to Japan"
    python -m decentralized_decomposer.main --role scorer --port 4002
    python -m decentralized_decomposer.main --role executor --port 4003 \\
        --capabilities can_write_text,can_query_api
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import trio
from anthropic import AsyncAnthropic
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.tools.async_service.trio_service import background_trio_service

from decentralized_decomposer import config as dd_config
from decentralized_decomposer.agents.dashboard_agent import DashboardAgent
from decentralized_decomposer.agents.decomposer_agent import DecomposerAgent
from decentralized_decomposer.agents.executor_agent import ExecutorAgent
from decentralized_decomposer.agents.observer_agent import ObserverAgent
from decentralized_decomposer.agents.scorer_agent import ScorerAgent
from decentralized_decomposer.config import Capability
from decentralized_decomposer.dashboard_server import EventBus, start_dashboard_server
from decentralized_decomposer.p2p.crdt_state import PlanState
from decentralized_decomposer.p2p.discovery import Discovery
from decentralized_decomposer.p2p.host import build_host
from decentralized_decomposer.p2p.pubsub import build_pubsub, publish_model
from decentralized_decomposer.protocol.messages import Goal
from decentralized_decomposer.protocol.topics import NEW_GOAL_TOPIC

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Decentralized goal decomposer agent")
    parser.add_argument(
        "--role",
        required=True,
        choices=["decomposer", "scorer", "executor", "observer", "dashboard"],
    )
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument(
        "--identity", default=None, help="Identity file name (default: <role>-<port>)"
    )
    parser.add_argument(
        "--capabilities",
        default=Capability.CAN_WRITE_TEXT.value,
        help="Comma-separated capabilities this executor advertises (executor role only)",
    )
    parser.add_argument(
        "--submit-goal",
        default=None,
        help="After startup, publish a new top-level goal with this text and continue running",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=None,
        help=(
            "Recursion depth cap (decomposer + scorer roles; default: config.py's MAX_DEPTH, "
            "3). Set to 1 so subgoals of the initial goal are never split further."
        ),
    )
    parser.add_argument(
        "--num-splits",
        type=int,
        default=None,
        help=(
            "Exact number of subgoals to split the *initial* (depth-0) goal into "
            "(decomposer role only; default: let the LLM choose within "
            "--min-splits/--max-splits)"
        ),
    )
    parser.add_argument(
        "--min-splits",
        type=int,
        default=None,
        help="Lower bound on subgoal count when --num-splits isn't set (decomposer role only)",
    )
    parser.add_argument(
        "--max-splits",
        type=int,
        default=None,
        help=(
            "Upper bound on subgoal count for any goal without an exact count pinned -- "
            "i.e. the initial goal when --num-splits isn't set, and any deeper goal if "
            "--max-depth allows recursion past depth 1 (decomposer role only)"
        ),
    )
    parser.add_argument(
        "--report-file",
        type=Path,
        default=None,
        help=(
            "Observer role only: also write the formatted plan-tree report to this file "
            "(overwritten in full) every time the plan state changes"
        ),
    )
    parser.add_argument(
        "--dashboard-port",
        type=int,
        default=8765,
        help="Dashboard role only: local HTTP port to serve the live UI on (default: 8765)",
    )
    parser.add_argument(
        "--dashboard-host",
        default="127.0.0.1",
        help="Dashboard role only: host/interface to bind the UI's HTTP server to",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args()


async def _submit_goal_after_delay(gossipsub: GossipSub, pubsub, peer_id: str, text: str) -> None:
    """Wait for a real mesh peer on NEW_GOAL_TOPIC before publishing.

    GossipSub never replays missed messages to a late subscriber, so a
    fixed sleep is really just a guess about *other* processes' startup
    time (host init, discovery, subscribing) -- there's no coordinator to
    ask instead (spec's decentralized design). We ourselves subscribe to
    NEW_GOAL_TOPIC too (for recursion), so `gossipsub.mesh[NEW_GOAL_TOPIC]`
    is a locally-observable, real signal that at least one other
    subscriber (scorer/observer/another decomposer) is mesh-joined and
    would actually receive this publish. Falls back to publishing anyway
    after GOAL_MESH_WAIT_TIMEOUT so a solo run (tests, a lone decomposer)
    doesn't hang forever.
    """
    start = trio.current_time()
    while len(gossipsub.mesh.get(NEW_GOAL_TOPIC, set())) < dd_config.MIN_GOAL_MESH_PEERS:
        if trio.current_time() - start >= dd_config.GOAL_MESH_WAIT_TIMEOUT:
            logger.warning(
                "Timed out after %.0fs waiting for a mesh peer on %s; publishing goal "
                "anyway -- any subscriber not yet mesh-joined will silently miss it",
                dd_config.GOAL_MESH_WAIT_TIMEOUT,
                NEW_GOAL_TOPIC,
            )
            break
        await trio.sleep(dd_config.GOAL_MESH_POLL_INTERVAL)

    goal = Goal.create(text=text, origin_peer=peer_id)
    await publish_model(pubsub, NEW_GOAL_TOPIC, goal)
    logger.info("Submitted top-level goal %s: %s", goal.goal_id, text)


async def amain(args: argparse.Namespace) -> None:
    if args.max_depth is not None:
        dd_config.MAX_DEPTH = args.max_depth
    if args.num_splits is not None:
        dd_config.ROOT_SPLIT_COUNT = args.num_splits
    if args.min_splits is not None:
        dd_config.MIN_SUBGOAL_SPLITS = args.min_splits
    if args.max_splits is not None:
        dd_config.MAX_SUBGOAL_SPLITS = args.max_splits

    identity_name = args.identity or f"{args.role}-{args.port or 'auto'}"
    host, listen_addrs, port = build_host(identity_name, args.port)
    peer_id = host.get_id().to_string()
    logger.info("Starting %s agent, peer id %s, port %d", args.role, peer_id, port)

    gossipsub, pubsub = build_pubsub(host)
    discovery = Discovery(host, port)

    event_bus = EventBus()
    if args.role == "dashboard":
        start_dashboard_server(event_bus, host=args.dashboard_host, port=args.dashboard_port)

    async with AsyncAnthropic() as llm_client:
        async with host.run(listen_addrs=listen_addrs):
            try:
                async with trio.open_nursery() as nursery:
                    # Deliberately not running peerstore.start_cleanup_task here: its
                    # expiry sweep deletes a peer's *entire* PeerData entry (pubkey +
                    # privkey included) once its TTL lapses, and something in the
                    # swarm/mDNS bookkeeping ends up giving our own peer id a
                    # short-lived TTL -- after ~60s that wipes our own keypair out of
                    # our own peerstore, and the next publish() crashes trying to
                    # sign with a public key that's no longer there. For a handful of
                    # local-laptop peers (spec §2), the memory this task reclaims
                    # isn't worth that risk.
                    await discovery.start(nursery)

                    async with background_trio_service(pubsub):
                        async with background_trio_service(gossipsub):
                            await pubsub.wait_until_ready()
                            logger.info("Pubsub ready")

                            if args.role == "decomposer":
                                await DecomposerAgent(pubsub, peer_id, llm_client).run(nursery)
                            elif args.role == "scorer":
                                await ScorerAgent(pubsub, peer_id, llm_client, PlanState()).run(
                                    nursery
                                )
                            elif args.role == "executor":
                                capabilities = [
                                    Capability(c.strip())
                                    for c in args.capabilities.split(",")
                                    if c.strip()
                                ]
                                await ExecutorAgent(pubsub, peer_id, capabilities, llm_client).run(
                                    nursery
                                )
                            elif args.role == "observer":
                                await ObserverAgent(
                                    pubsub, PlanState(), report_file=args.report_file
                                ).run(nursery)
                            else:
                                await DashboardAgent(pubsub, event_bus).run(nursery)

                            if args.submit_goal:
                                nursery.start_soon(
                                    _submit_goal_after_delay,
                                    gossipsub,
                                    pubsub,
                                    peer_id,
                                    args.submit_goal,
                                )

                            await trio.sleep_forever()
            finally:
                discovery.stop()


def main() -> None:
    args = parse_args()
    # LLM output routinely contains emoji (markdown headers, etc.), and on
    # Windows stdout/stderr default to the console codepage (cp1252), which
    # can't encode most of them. Left alone, the *logging* call itself throws
    # a UnicodeEncodeError -- non-fatal (logging swallows handler errors) but
    # it silently drops the log line and spams a "Logging error" traceback.
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(errors="backslashreplace")
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        trio.run(amain, args)
    except KeyboardInterrupt:
        logger.info("Shutting down")


if __name__ == "__main__":
    main()
