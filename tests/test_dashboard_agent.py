from datetime import datetime, timezone

import trio

from decentralized_decomposer.agents.capability import CapabilityAnnounce
from decentralized_decomposer.agents.dashboard_agent import DashboardAgent
from decentralized_decomposer.config import Capability
from decentralized_decomposer.dashboard_server import EventBus
from decentralized_decomposer.protocol.messages import (
    AcceptedSplit,
    ClaimMsg,
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
    node_prefix,
    propose_topic,
    result_topic,
    root_prefix,
    score_topic,
)
from tests.fakes import FakePubsub


def _kinds(bus: EventBus) -> list[str]:
    _, _, backlog = bus.subscribe()
    return [e["kind"] for e in backlog]


async def test_capability_announcement_is_emitted_with_executor_role_hint():
    pubsub = FakePubsub()
    bus = EventBus()
    agent = DashboardAgent(pubsub, bus)

    async with trio.open_nursery() as nursery:
        await agent.run(nursery)
        await pubsub.deliver(
            CAPABILITIES_ANNOUNCE_TOPIC,
            CapabilityAnnounce(peer_id="executor-1", capabilities=[Capability.CAN_RUN_CODE]),
        )
        await trio.sleep(0.05)
        nursery.cancel_scope.cancel()

    _, _, backlog = bus.subscribe()
    assert len(backlog) == 1
    event = backlog[0]
    assert event["kind"] == "capability"
    assert event["peer"] == "executor-1"
    assert event["role_hint"] == "executor"
    assert event["data"]["capabilities"] == ["can_run_code"]


async def test_goal_propose_score_accepted_round_trip_is_captured_in_order():
    pubsub = FakePubsub()
    bus = EventBus()
    agent = DashboardAgent(pubsub, bus)

    goal = Goal.create(text="Build a todo app", origin_peer="decomposer-1")
    prefix = root_prefix(goal.goal_id)
    proposal = SubgoalProposal.create(
        goal_id=goal.goal_id,
        proposer_peer="decomposer-1",
        subgoals=["Design schema", "Build API"],
        rationale="split by layer",
    )
    score = ScoreMsg(
        goal_id=goal.goal_id, proposal_id=proposal.proposal_id, scorer_peer="scorer-1", score=0.9
    )
    accepted = AcceptedSplit(
        goal_id=goal.goal_id,
        proposal_id=proposal.proposal_id,
        subgoals=proposal.subgoals,
        final_score=0.9,
    )

    async with trio.open_nursery() as nursery:
        await agent.run(nursery)
        await pubsub.deliver(NEW_GOAL_TOPIC, goal)
        await trio.sleep(0.02)
        await pubsub.deliver(propose_topic(prefix), proposal)
        await pubsub.deliver(score_topic(prefix), score)
        await pubsub.deliver(accepted_topic(prefix), accepted)
        await trio.sleep(0.05)
        nursery.cancel_scope.cancel()

    kinds = _kinds(bus)
    assert kinds == ["goal", "proposal", "score", "accepted"]

    _, _, backlog = bus.subscribe()
    goal_event, proposal_event, score_event, accepted_event = backlog
    assert goal_event["role_hint"] == "decomposer"  # depth 0
    assert proposal_event["peer"] == "decomposer-1"
    assert score_event["peer"] == "scorer-1"
    assert accepted_event["data"]["final_score"] == 0.9


async def test_node_claim_result_round_trip_is_captured_in_order():
    pubsub = FakePubsub()
    bus = EventBus()
    agent = DashboardAgent(pubsub, bus)

    node = PromptNode.create(goal_id="goal-1", text="Write the README", is_atomic=True)
    prefix = node_prefix(node.goal_id, node.node_id)
    claim = ClaimMsg(
        node_id=node.node_id,
        claimer_peer="executor-1",
        claimed_at=datetime.now(timezone.utc),
        capability="can_write_text",
    )
    result = ResultMsg(
        node_id=node.node_id, executor_peer="executor-1", output="the readme text", success=True
    )

    async with trio.open_nursery() as nursery:
        await agent.run(nursery)
        await pubsub.deliver(NEW_NODE_TOPIC, node)
        await trio.sleep(0.02)
        await pubsub.deliver(claim_topic(prefix), claim)
        await pubsub.deliver(result_topic(prefix), result)
        await trio.sleep(0.05)
        nursery.cancel_scope.cancel()

    assert _kinds(bus) == ["node", "claim", "result"]

    _, _, backlog = bus.subscribe()
    node_event, claim_event, result_event = backlog
    assert node_event["node_id"] == node.node_id
    assert claim_event["peer"] == "executor-1"
    assert result_event["data"]["output"] == "the readme text"


async def test_duplicate_goal_delivery_is_deduplicated():
    pubsub = FakePubsub()
    bus = EventBus()
    agent = DashboardAgent(pubsub, bus)

    goal = Goal.create(text="Plan a trip", origin_peer="decomposer-1")

    async with trio.open_nursery() as nursery:
        await agent.run(nursery)
        await pubsub.deliver(NEW_GOAL_TOPIC, goal)
        await pubsub.deliver(NEW_GOAL_TOPIC, goal)
        await trio.sleep(0.05)
        nursery.cancel_scope.cancel()

    assert _kinds(bus) == ["goal"]


async def test_duplicate_node_delivery_is_deduplicated():
    pubsub = FakePubsub()
    bus = EventBus()
    agent = DashboardAgent(pubsub, bus)

    node = PromptNode.create(goal_id="goal-1", text="Write the README", is_atomic=True)

    async with trio.open_nursery() as nursery:
        await agent.run(nursery)
        await pubsub.deliver(NEW_NODE_TOPIC, node)
        await pubsub.deliver(NEW_NODE_TOPIC, node)
        await trio.sleep(0.05)
        nursery.cancel_scope.cancel()

    assert _kinds(bus) == ["node"]
