import json

from decentralized_decomposer.agents.scorer_agent import ScorerAgent
from decentralized_decomposer.p2p.crdt_state import PlanState
from decentralized_decomposer.protocol.messages import Goal, ScoreMsg, SubgoalProposal
from decentralized_decomposer.protocol.topics import root_prefix, score_topic
from tests.fakes import FakePubsub, fake_llm_client


def _goal() -> Goal:
    return Goal.create(text="Build a todo app", origin_peer="peer-origin")


def _proposal(goal: Goal) -> SubgoalProposal:
    return SubgoalProposal.create(
        goal_id=goal.goal_id,
        proposer_peer="decomposer-1",
        subgoals=["Design schema", "Build API"],
        rationale="split by layer",
    )


async def test_scorer_publishes_score_in_valid_range_for_a_proposal():
    pubsub = FakePubsub()
    llm = fake_llm_client(json.dumps({"score": 0.9, "notes": "covers the goal well"}))
    agent = ScorerAgent(pubsub, peer_id="scorer-1", llm_client=llm, plan_state=PlanState())

    goal = _goal()
    proposal = _proposal(goal)
    prefix = root_prefix(goal.goal_id)

    await agent._score_proposal(goal, proposal, prefix)

    published = pubsub.published_on(score_topic(prefix))
    assert len(published) == 1
    score_msg = ScoreMsg.model_validate_json(published[0])
    assert 0.0 <= score_msg.score <= 1.0
    assert score_msg.proposal_id == proposal.proposal_id
    assert score_msg.scorer_peer == "scorer-1"


async def test_scorer_clamps_out_of_range_llm_score():
    pubsub = FakePubsub()
    llm = fake_llm_client(json.dumps({"score": 1.4, "notes": "overconfident"}))
    agent = ScorerAgent(pubsub, peer_id="scorer-1", llm_client=llm, plan_state=PlanState())

    goal = _goal()
    proposal = _proposal(goal)
    prefix = root_prefix(goal.goal_id)

    await agent._score_proposal(goal, proposal, prefix)

    score_msg = ScoreMsg.model_validate_json(pubsub.published_on(score_topic(prefix))[0])
    assert score_msg.score == 1.0


async def test_classify_atomicity_returns_llm_verdict():
    pubsub = FakePubsub()
    llm = fake_llm_client(json.dumps({"is_atomic": True, "reason": "single deliverable"}))
    agent = ScorerAgent(pubsub, peer_id="scorer-1", llm_client=llm, plan_state=PlanState())

    assert await agent._classify_atomicity("Write the README") is True


async def test_classify_atomicity_defaults_to_atomic_on_llm_failure(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.LLM_RETRY_BASE_DELAY", 0.001)
    monkeypatch.setattr("decentralized_decomposer.config.MAX_LLM_RETRIES", 1)
    pubsub = FakePubsub()
    llm = fake_llm_client("not json", "still not json")
    agent = ScorerAgent(pubsub, peer_id="scorer-1", llm_client=llm, plan_state=PlanState())

    assert await agent._classify_atomicity("Write the README") is True


async def test_recurse_creates_node_for_atomic_and_goal_for_composite_subgoal():
    from decentralized_decomposer.protocol.messages import AcceptedSplit, PromptNode
    from decentralized_decomposer.protocol.topics import NEW_GOAL_TOPIC, NEW_NODE_TOPIC

    pubsub = FakePubsub()
    llm = fake_llm_client(
        json.dumps({"is_atomic": True, "reason": "leaf"}),
        json.dumps({"is_atomic": False, "reason": "needs more breakdown"}),
    )
    plan_state = PlanState()
    agent = ScorerAgent(pubsub, peer_id="scorer-1", llm_client=llm, plan_state=plan_state)

    goal = _goal()  # depth 0, MAX_DEPTH default 3 -> depth+1=1, not forced atomic
    accepted = AcceptedSplit(
        goal_id=goal.goal_id,
        proposal_id="p1",
        subgoals=["Write the README", "Design the whole backend"],
        final_score=0.9,
    )

    await agent._recurse(goal, accepted)

    node_msgs = [PromptNode.model_validate_json(d) for d in pubsub.published_on(NEW_NODE_TOPIC)]
    assert len(node_msgs) == 1
    assert node_msgs[0].text == "Write the README"
    assert node_msgs[0].is_atomic is True

    goal_msgs = [Goal.model_validate_json(d) for d in pubsub.published_on(NEW_GOAL_TOPIC)]
    assert len(goal_msgs) == 1
    assert goal_msgs[0].text == "Design the whole backend"
    assert goal_msgs[0].depth == 1
    assert goal_msgs[0].parent_id == goal.goal_id

    assert len(plan_state.atomic_nodes) == 1
    assert len(plan_state.goals) == 1


async def test_recurse_forces_atomic_at_max_depth_without_calling_classifier(monkeypatch):
    from decentralized_decomposer.protocol.messages import AcceptedSplit, PromptNode
    from decentralized_decomposer.protocol.topics import NEW_NODE_TOPIC

    monkeypatch.setattr("decentralized_decomposer.config.MAX_DEPTH", 2)
    pubsub = FakePubsub()
    llm = fake_llm_client()  # should never be called
    agent = ScorerAgent(pubsub, peer_id="scorer-1", llm_client=llm, plan_state=PlanState())

    goal = Goal.create(text="deep goal", origin_peer="peer-1", depth=1)  # depth+1 == MAX_DEPTH
    accepted = AcceptedSplit(goal_id=goal.goal_id, proposal_id="p1", subgoals=["x"], final_score=0.9)

    await agent._recurse(goal, accepted)

    assert llm.messages.create.call_count == 0
    node_msgs = [PromptNode.model_validate_json(d) for d in pubsub.published_on(NEW_NODE_TOPIC)]
    assert len(node_msgs) == 1
    assert node_msgs[0].is_atomic is True
