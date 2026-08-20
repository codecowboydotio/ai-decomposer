import json

import trio

from decentralized_decomposer.agents.decomposer_agent import DecomposerAgent
from decentralized_decomposer.protocol.messages import (
    AcceptedSplit,
    FailureMsg,
    Goal,
    ScoreMsg,
    SubgoalProposal,
)
from decentralized_decomposer.protocol.topics import (
    NEW_GOAL_TOPIC,
    accepted_topic,
    propose_failed_topic,
    propose_topic,
    root_prefix,
    score_topic,
)
from tests.fakes import FakePubsub, fake_llm_client


def _goal() -> Goal:
    return Goal.create(text="Build a todo app", origin_peer="peer-origin")


async def test_decomposer_publishes_well_formed_proposal_on_valid_response():
    pubsub = FakePubsub()
    llm = fake_llm_client(
        json.dumps({"subgoals": ["Design schema", "Build API"], "rationale": "split by layer"})
    )
    agent = DecomposerAgent(pubsub, peer_id="decomposer-1", llm_client=llm)

    goal = _goal()
    proposal = await agent.propose_split(goal)

    assert proposal is not None
    assert proposal.subgoals == ["Design schema", "Build API"]
    assert proposal.verify_integrity()

    published = pubsub.published_on(propose_topic(root_prefix(goal.goal_id)))
    assert len(published) == 1
    assert SubgoalProposal.model_validate_json(published[0]) == proposal


async def test_decomposer_retries_and_feeds_parse_error_back_into_prompt(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.LLM_RETRY_BASE_DELAY", 0.001)
    pubsub = FakePubsub()
    llm = fake_llm_client(
        "not json at all",
        json.dumps({"subgoals": ["a", "b"], "rationale": "ok now"}),
    )
    agent = DecomposerAgent(pubsub, peer_id="decomposer-1", llm_client=llm)

    proposal = await agent.propose_split(_goal())

    assert proposal is not None
    assert llm.messages.create.call_count == 2
    second_call_prompt = llm.messages.create.call_args_list[1].kwargs["messages"][0]["content"]
    assert "didn't match the required JSON schema" in second_call_prompt


async def test_decomposer_exhausts_retries_publishes_failure_notice_not_a_guess(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.LLM_RETRY_BASE_DELAY", 0.001)
    monkeypatch.setattr("decentralized_decomposer.config.MAX_LLM_RETRIES", 1)
    pubsub = FakePubsub()
    llm = fake_llm_client("still not json", "also not json")
    agent = DecomposerAgent(pubsub, peer_id="decomposer-1", llm_client=llm)

    goal = _goal()
    proposal = await agent.propose_split(goal)

    assert proposal is None
    assert llm.messages.create.call_count == 2  # initial attempt + 1 retry, then give up

    failed = pubsub.published_on(propose_failed_topic(root_prefix(goal.goal_id)))
    assert len(failed) == 1
    failure = FailureMsg.model_validate_json(failed[0])
    assert failure.role == "decomposer"
    assert failure.goal_id == goal.goal_id

    assert not pubsub.published_on(propose_topic(root_prefix(goal.goal_id)))


async def test_decomposer_skips_goals_already_at_max_depth(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.MAX_DEPTH", 2)
    pubsub = FakePubsub()
    llm = fake_llm_client(json.dumps({"subgoals": ["x"], "rationale": "r"}))
    agent = DecomposerAgent(pubsub, peer_id="decomposer-1", llm_client=llm)

    goal = Goal.create(text="deep goal", origin_peer="peer-origin", depth=2)

    async with trio.open_nursery() as nursery:
        await agent.run(nursery)
        await pubsub.deliver(NEW_GOAL_TOPIC, goal)
        await trio.sleep(0.05)
        nursery.cancel_scope.cancel()

    assert llm.messages.create.call_count == 0
    assert not pubsub.published_on(propose_topic(root_prefix(goal.goal_id)))


async def test_decomposer_retries_when_root_split_count_is_wrong(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.LLM_RETRY_BASE_DELAY", 0.001)
    monkeypatch.setattr("decentralized_decomposer.config.ROOT_SPLIT_COUNT", 4)
    pubsub = FakePubsub()
    llm = fake_llm_client(
        json.dumps({"subgoals": ["a", "b"], "rationale": "too few"}),  # wrong count: 2, not 4
        json.dumps({"subgoals": ["a", "b", "c", "d"], "rationale": "correct now"}),
    )
    agent = DecomposerAgent(pubsub, peer_id="decomposer-1", llm_client=llm)

    proposal = await agent.propose_split(_goal())  # depth 0

    assert proposal is not None
    assert len(proposal.subgoals) == 4
    assert llm.messages.create.call_count == 2
    second_call_prompt = llm.messages.create.call_args_list[1].kwargs["messages"][0]["content"]
    assert "expected exactly 4 subgoals, got 2" in second_call_prompt


async def test_decomposer_does_not_enforce_root_split_count_on_deeper_goals(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.ROOT_SPLIT_COUNT", 4)
    pubsub = FakePubsub()
    llm = fake_llm_client(json.dumps({"subgoals": ["a", "b"], "rationale": "fine at depth 1"}))
    agent = DecomposerAgent(pubsub, peer_id="decomposer-1", llm_client=llm)

    goal = Goal.create(text="subgoal", origin_peer="peer-origin", parent_id="root-1", depth=1)
    proposal = await agent.propose_split(goal)

    assert proposal is not None
    assert len(proposal.subgoals) == 2
    assert llm.messages.create.call_count == 1


async def test_decomposer_does_not_repropose_when_goal_converges(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.ACCEPT_TIMEOUT", 0.05)
    monkeypatch.setattr("decentralized_decomposer.config.REPROPOSAL_GRACE", 0.05)
    pubsub = FakePubsub()
    llm = fake_llm_client(json.dumps({"subgoals": ["a", "b"], "rationale": "r"}))
    agent = DecomposerAgent(pubsub, peer_id="decomposer-1", llm_client=llm)

    goal = _goal()
    accepted = AcceptedSplit(goal_id=goal.goal_id, proposal_id="p1", subgoals=["a", "b"], final_score=0.9)

    async with trio.open_nursery() as nursery:
        await agent.run(nursery)
        await pubsub.deliver(NEW_GOAL_TOPIC, goal)
        await trio.sleep(0.01)
        await pubsub.deliver(accepted_topic(root_prefix(goal.goal_id)), accepted)
        await trio.sleep(0.3)  # well past the (shortened) timeout window
        nursery.cancel_scope.cancel()

    assert llm.messages.create.call_count == 1
    assert not pubsub.published_on(NEW_GOAL_TOPIC)


async def test_decomposer_reproposes_with_scorer_feedback_then_gives_up(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.ACCEPT_TIMEOUT", 0.03)
    monkeypatch.setattr("decentralized_decomposer.config.REPROPOSAL_GRACE", 0.02)
    monkeypatch.setattr("decentralized_decomposer.config.MAX_REPROPOSAL_ATTEMPTS", 1)
    pubsub = FakePubsub()
    llm = fake_llm_client(
        json.dumps({"subgoals": ["a", "b"], "rationale": "first try"}),
        json.dumps({"subgoals": ["a", "b"], "rationale": "second try"}),
    )
    agent = DecomposerAgent(pubsub, peer_id="decomposer-1", llm_client=llm)

    goal = _goal()

    async def deliver_a_low_score():
        # Delivered while the decomposer is watching, before its timeout window
        # closes -- should show up as feedback on the retry's prompt.
        await trio.sleep(0.01)
        await pubsub.deliver(
            score_topic(root_prefix(goal.goal_id)),
            ScoreMsg(
                goal_id=goal.goal_id,
                proposal_id="does-not-matter-for-this-test",
                scorer_peer="scorer-1",
                score=0.3,
                notes="subgoals overlap too much",
            ),
        )

    async with trio.open_nursery() as nursery:
        await agent.run(nursery)
        nursery.start_soon(deliver_a_low_score)
        await pubsub.deliver(NEW_GOAL_TOPIC, goal)
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()

    assert llm.messages.create.call_count == 2
    second_call_prompt = llm.messages.create.call_args_list[1].kwargs["messages"][0]["content"]
    assert "0.30 (subgoals overlap too much)" in second_call_prompt
    assert "was not accepted in time" in second_call_prompt

    retried_goals = [Goal.model_validate_json(d) for d in pubsub.published_on(NEW_GOAL_TOPIC)]
    assert len(retried_goals) == 1
    retry_goal = retried_goals[0]
    assert retry_goal.text == goal.text
    assert retry_goal.goal_id != goal.goal_id
    assert retry_goal.depth == goal.depth
    assert retry_goal.parent_id == goal.parent_id

    failed = [
        FailureMsg.model_validate_json(d)
        for d in pubsub.published_on(propose_failed_topic(root_prefix(retry_goal.goal_id)))
    ]
    assert len(failed) == 1
    assert "exhausted 1 reproposal attempt" in failed[0].reason


async def test_decomposer_retry_feedback_notes_absence_of_scorer_feedback(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.ACCEPT_TIMEOUT", 0.02)
    monkeypatch.setattr("decentralized_decomposer.config.REPROPOSAL_GRACE", 0.01)
    monkeypatch.setattr("decentralized_decomposer.config.MAX_REPROPOSAL_ATTEMPTS", 1)
    pubsub = FakePubsub()
    llm = fake_llm_client(
        json.dumps({"subgoals": ["a", "b"], "rationale": "first try"}),
        json.dumps({"subgoals": ["a", "b"], "rationale": "second try"}),
    )
    agent = DecomposerAgent(pubsub, peer_id="decomposer-1", llm_client=llm)

    goal = _goal()  # no scorers running at all -- never any ScoreMsg

    async with trio.open_nursery() as nursery:
        await agent.run(nursery)
        await pubsub.deliver(NEW_GOAL_TOPIC, goal)
        await trio.sleep(0.2)
        nursery.cancel_scope.cancel()

    second_call_prompt = llm.messages.create.call_args_list[1].kwargs["messages"][0]["content"]
    assert "No scorer feedback arrived" in second_call_prompt


async def test_decomposer_ignores_duplicate_goal_delivery(monkeypatch):
    pubsub = FakePubsub()
    llm = fake_llm_client(json.dumps({"subgoals": ["a", "b"], "rationale": "r"}))
    agent = DecomposerAgent(pubsub, peer_id="decomposer-1", llm_client=llm)

    goal = _goal()

    async with trio.open_nursery() as nursery:
        await agent.run(nursery)
        await pubsub.deliver(NEW_GOAL_TOPIC, goal)
        await pubsub.deliver(NEW_GOAL_TOPIC, goal)
        await trio.sleep(0.05)
        nursery.cancel_scope.cancel()

    assert llm.messages.create.call_count == 1
    assert len(pubsub.published_on(propose_topic(root_prefix(goal.goal_id)))) == 1
