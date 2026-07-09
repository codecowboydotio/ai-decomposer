from datetime import datetime, timedelta, timezone

import trio

from decentralized_decomposer.agents.executor_agent import ExecutorAgent
from decentralized_decomposer.config import Capability
from decentralized_decomposer.protocol.messages import ClaimMsg, PromptNode, ResultMsg
from decentralized_decomposer.protocol.topics import (
    NEW_NODE_TOPIC,
    claim_topic,
    node_prefix,
    result_topic,
)
from tests.fakes import FakePubsub, fake_llm_client


def _node(required_capability: str | None = None) -> PromptNode:
    return PromptNode.create(
        goal_id="goal-1", text="Write the README", is_atomic=True, required_capability=required_capability
    )


async def test_executor_attempts_claim_when_capability_matches():
    pubsub = FakePubsub()
    llm = fake_llm_client("Here is the README.")
    agent = ExecutorAgent(
        pubsub, peer_id="executor-1", capabilities=[Capability.CAN_WRITE_TEXT], llm_client=llm
    )

    node = _node(required_capability=Capability.CAN_WRITE_TEXT.value)
    prefix = node_prefix(node.goal_id, node.node_id)

    async with trio.open_nursery() as nursery:
        await agent._handle_node(node, nursery)

    claims = [ClaimMsg.model_validate_json(d) for d in pubsub.published_on(claim_topic(prefix))]
    assert len(claims) == 1
    assert claims[0].claimer_peer == "executor-1"

    results = [ResultMsg.model_validate_json(d) for d in pubsub.published_on(result_topic(prefix))]
    assert len(results) == 1
    assert results[0].success is True


async def test_executor_does_not_claim_when_capability_does_not_match():
    pubsub = FakePubsub()
    llm = fake_llm_client()
    agent = ExecutorAgent(
        pubsub, peer_id="executor-1", capabilities=[Capability.CAN_QUERY_API], llm_client=llm
    )

    node = _node(required_capability=Capability.CAN_RUN_CODE.value)
    prefix = node_prefix(node.goal_id, node.node_id)

    async with trio.open_nursery() as nursery:
        await agent._handle_node(node, nursery)
        await trio.sleep(0.05)
        nursery.cancel_scope.cancel()

    assert not pubsub.published_on(claim_topic(prefix))
    assert not pubsub.published_on(result_topic(prefix))
    assert llm.messages.create.call_count == 0


async def test_executor_backs_off_when_beaten_by_earlier_claim(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.agents.executor_agent.CLAIM_SETTLE_WINDOW", 0.05)
    pubsub = FakePubsub()
    llm = fake_llm_client("should not be called")
    agent = ExecutorAgent(
        pubsub, peer_id="executor-late", capabilities=[Capability.CAN_WRITE_TEXT], llm_client=llm
    )

    node = _node(required_capability=Capability.CAN_WRITE_TEXT.value)
    prefix = node_prefix(node.goal_id, node.node_id)

    earlier_claim = ClaimMsg(
        node_id=node.node_id,
        claimer_peer="executor-early",
        claimed_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        capability=Capability.CAN_WRITE_TEXT.value,
    )

    async def seed_competing_claim():
        await trio.sleep(0.01)  # let the executor subscribe first
        await pubsub.deliver(claim_topic(prefix), earlier_claim)

    async with trio.open_nursery() as nursery:
        nursery.start_soon(seed_competing_claim)
        won = await agent._try_claim(node)

    assert won is False
    assert llm.messages.create.call_count == 0
    assert not pubsub.published_on(result_topic(prefix))


async def test_executor_ignores_duplicate_node_delivery(monkeypatch):
    """GossipSub gives no exactly-once guarantee: the same node/new message can
    arrive twice. Without dedup, two concurrent _handle_node calls for the same
    node race to claim it, and (in the real libp2p pubsub, modeled here by
    FakePubsub's shared per-topic subscription) the first one to finish tears
    down the claim-topic subscription out from under the second.
    """
    monkeypatch.setattr("decentralized_decomposer.agents.executor_agent.CLAIM_SETTLE_WINDOW", 0.05)
    pubsub = FakePubsub()
    llm = fake_llm_client("Here is the README.")
    agent = ExecutorAgent(
        pubsub, peer_id="executor-1", capabilities=[Capability.CAN_WRITE_TEXT], llm_client=llm
    )

    node = _node(required_capability=Capability.CAN_WRITE_TEXT.value)
    prefix = node_prefix(node.goal_id, node.node_id)

    async with trio.open_nursery() as nursery:
        await agent.run(nursery)
        await pubsub.deliver(NEW_NODE_TOPIC, node)
        await pubsub.deliver(NEW_NODE_TOPIC, node)
        await trio.sleep(0.2)
        nursery.cancel_scope.cancel()

    claims = [ClaimMsg.model_validate_json(d) for d in pubsub.published_on(claim_topic(prefix))]
    assert len(claims) == 1

    results = [ResultMsg.model_validate_json(d) for d in pubsub.published_on(result_topic(prefix))]
    assert len(results) == 1
    assert llm.messages.create.call_count == 1
