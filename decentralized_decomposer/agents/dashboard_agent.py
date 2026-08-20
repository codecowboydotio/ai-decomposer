"""Dashboard agent: observe every gossip message and stream it to the live UI.

Read-only, like `ObserverAgent` -- no LLM calls, no capabilities, never
publishes anything back onto the network. Unlike `ObserverAgent` (which only
tracks the *terminal* state needed to reconstruct the plan tree: one accepted
split, one result per node), this agent surfaces every intermediate message
-- each proposal, each score, each claim, capability announcements, failures
-- since the point (dashboard_server.py + dashboard/index.html) is to
visualize the interactions themselves, not just their outcome.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import trio
from libp2p.pubsub.pubsub import Pubsub

from decentralized_decomposer.agents.capability import CapabilityAnnounce
from decentralized_decomposer.dashboard_server import EventBus
from decentralized_decomposer.p2p.pubsub import subscribe_and_validate
from decentralized_decomposer.protocol.messages import (
    AcceptedSplit,
    ClaimMsg,
    FailureMsg,
    Goal,
    PromptNode,
    ResultMsg,
    ScoreMsg,
    SubgoalProposal,
)
from decentralized_decomposer.protocol.topics import (
    CAPABILITIES_ANNOUNCE_TOPIC,
    NEW_GOAL_TOPIC,
    NEW_NODE_TOPIC,
    accepted_topic,
    claim_topic,
    node_failed_topic,
    node_prefix,
    propose_failed_topic,
    propose_topic,
    result_topic,
    root_prefix,
    score_failed_topic,
    score_topic,
)

logger = logging.getLogger(__name__)


def _truncate(text: str, limit: int = 80) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


class DashboardAgent:
    def __init__(self, pubsub: Pubsub, bus: EventBus):
        self.pubsub = pubsub
        self.bus = bus
        self._seen_goals: set[str] = set()
        self._seen_nodes: set[str] = set()
        self._nursery: trio.Nursery | None = None

    async def run(self, nursery: trio.Nursery) -> None:
        self._nursery = nursery
        nursery.start_soon(self._handle_capabilities)
        nursery.start_soon(self._handle_new_goals)
        nursery.start_soon(self._handle_new_nodes)

    def _emit(self, **fields: Any) -> None:
        # Stamped here (message-observed time), not left for the browser to fill
        # in on arrival -- the UI replays backlog history on connect, and a
        # client-side "now" would show every backlogged event as having just
        # happened.
        self.bus.publish({"ts": datetime.now(timezone.utc).isoformat(), **fields})

    def _emit_failure(
        self, topic: str, kind: str, failure: FailureMsg, node_id: str | None = None
    ) -> None:
        self._emit(
            kind=kind,
            topic=topic,
            goal_id=failure.goal_id,
            node_id=node_id or failure.node_id,
            role_hint=failure.role,
            summary=f"{failure.role} failure: {_truncate(failure.reason, 120)}",
            data=failure.model_dump(mode="json"),
        )

    async def _handle_capabilities(self) -> None:
        async for announce in subscribe_and_validate(
            self.pubsub, CAPABILITIES_ANNOUNCE_TOPIC, CapabilityAnnounce
        ):
            self._emit(
                kind="capability",
                topic=CAPABILITIES_ANNOUNCE_TOPIC,
                peer=announce.peer_id,
                role_hint="executor",
                summary=(
                    f"{announce.peer_id} announced capabilities: "
                    f"{', '.join(c.value for c in announce.capabilities)}"
                ),
                data=announce.model_dump(mode="json"),
            )

    async def _handle_new_goals(self) -> None:
        assert self._nursery is not None
        async for goal in subscribe_and_validate(self.pubsub, NEW_GOAL_TOPIC, Goal):
            if goal.goal_id in self._seen_goals:
                # GossipSub gives no exactly-once delivery guarantee (see
                # ExecutorAgent's identical guard on node/new): without this,
                # a duplicate delivery of the same goal spins up a second
                # concurrent watcher sharing the same underlying topic
                # subscriptions as the first.
                continue
            self._seen_goals.add(goal.goal_id)
            self._emit(
                kind="goal",
                topic=NEW_GOAL_TOPIC,
                goal_id=goal.goal_id,
                peer=goal.origin_peer,
                role_hint="decomposer" if goal.depth == 0 else "scorer",
                summary=f'New goal (depth {goal.depth}): "{_truncate(goal.text)}"',
                data=goal.model_dump(mode="json"),
            )
            self._nursery.start_soon(self._watch_goal, goal)

    async def _watch_goal(self, goal: Goal) -> None:
        prefix = root_prefix(goal.goal_id)
        send_accepted, recv_accepted = trio.open_memory_channel[AcceptedSplit](1)

        async def watch_propose() -> None:
            async for proposal in subscribe_and_validate(
                self.pubsub, propose_topic(prefix), SubgoalProposal
            ):
                self._emit(
                    kind="proposal",
                    topic=propose_topic(prefix),
                    goal_id=goal.goal_id,
                    peer=proposal.proposer_peer,
                    role_hint="decomposer",
                    summary=f"Proposed {len(proposal.subgoals)} subgoals",
                    data=proposal.model_dump(mode="json"),
                )

        async def watch_score() -> None:
            async for score in subscribe_and_validate(self.pubsub, score_topic(prefix), ScoreMsg):
                self._emit(
                    kind="score",
                    topic=score_topic(prefix),
                    goal_id=goal.goal_id,
                    peer=score.scorer_peer,
                    role_hint="scorer",
                    summary=f"Scored proposal {score.proposal_id[:8]} = {score.score:.2f}",
                    data=score.model_dump(mode="json"),
                )

        async def watch_propose_failed() -> None:
            async for failure in subscribe_and_validate(
                self.pubsub, propose_failed_topic(prefix), FailureMsg
            ):
                self._emit_failure(propose_failed_topic(prefix), "propose_failed", failure)

        async def watch_score_failed() -> None:
            async for failure in subscribe_and_validate(
                self.pubsub, score_failed_topic(prefix), FailureMsg
            ):
                self._emit_failure(score_failed_topic(prefix), "score_failed", failure)

        async def watch_accepted() -> None:
            async for accepted in subscribe_and_validate(
                self.pubsub, accepted_topic(prefix), AcceptedSplit
            ):
                self._emit(
                    kind="accepted",
                    topic=accepted_topic(prefix),
                    goal_id=goal.goal_id,
                    summary=(
                        f"Split accepted (score {accepted.final_score:.2f}, "
                        f"{len(accepted.subgoals)} subgoals)"
                    ),
                    data=accepted.model_dump(mode="json"),
                )
                try:
                    send_accepted.send_nowait(accepted)
                except trio.WouldBlock:
                    pass
                return  # one accepted split per goal

        async with trio.open_nursery() as sub_nursery:
            sub_nursery.start_soon(watch_propose)
            sub_nursery.start_soon(watch_score)
            sub_nursery.start_soon(watch_propose_failed)
            sub_nursery.start_soon(watch_score_failed)
            sub_nursery.start_soon(watch_accepted)
            await recv_accepted.receive()
            sub_nursery.cancel_scope.cancel()

    async def _handle_new_nodes(self) -> None:
        assert self._nursery is not None
        async for node in subscribe_and_validate(self.pubsub, NEW_NODE_TOPIC, PromptNode):
            if node.node_id in self._seen_nodes:
                continue
            self._seen_nodes.add(node.node_id)
            self._emit(
                kind="node",
                topic=NEW_NODE_TOPIC,
                goal_id=node.goal_id,
                node_id=node.node_id,
                summary=f'New atomic node: "{_truncate(node.text)}"',
                data=node.model_dump(mode="json"),
            )
            self._nursery.start_soon(self._watch_node, node)

    async def _watch_node(self, node: PromptNode) -> None:
        prefix = node_prefix(node.goal_id, node.node_id)
        send_done, recv_done = trio.open_memory_channel[None](1)

        async def watch_claim() -> None:
            async for claim in subscribe_and_validate(self.pubsub, claim_topic(prefix), ClaimMsg):
                self._emit(
                    kind="claim",
                    topic=claim_topic(prefix),
                    goal_id=node.goal_id,
                    node_id=node.node_id,
                    peer=claim.claimer_peer,
                    role_hint="executor",
                    summary=f"{claim.claimer_peer} claimed this node",
                    data=claim.model_dump(mode="json"),
                )

        async def watch_result() -> None:
            async for result in subscribe_and_validate(self.pubsub, result_topic(prefix), ResultMsg):
                self._emit(
                    kind="result",
                    topic=result_topic(prefix),
                    goal_id=node.goal_id,
                    node_id=node.node_id,
                    peer=result.executor_peer,
                    role_hint="executor",
                    summary=f"Result ({'success' if result.success else 'FAILED'})",
                    data=result.model_dump(mode="json"),
                )
                try:
                    send_done.send_nowait(None)
                except trio.WouldBlock:
                    pass
                return  # first result wins any claim race

        async def watch_failed() -> None:
            async for failure in subscribe_and_validate(
                self.pubsub, node_failed_topic(prefix), FailureMsg
            ):
                self._emit_failure(
                    node_failed_topic(prefix), "node_failed", failure, node_id=node.node_id
                )
                try:
                    send_done.send_nowait(None)
                except trio.WouldBlock:
                    pass
                return

        async with trio.open_nursery() as sub_nursery:
            sub_nursery.start_soon(watch_claim)
            sub_nursery.start_soon(watch_result)
            sub_nursery.start_soon(watch_failed)
            await recv_done.receive()
            sub_nursery.cancel_scope.cancel()
