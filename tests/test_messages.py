import pytest
from pydantic import ValidationError

from decentralized_decomposer.protocol.messages import (
    AcceptedSplit,
    ClaimMsg,
    Goal,
    PromptNode,
    ScoreMsg,
    SubgoalProposal,
)


def test_goal_create_round_trips_and_verifies():
    goal = Goal.create(text="Build a todo app", origin_peer="peer-1")
    assert goal.verify_integrity()


def test_goal_tampered_text_fails_integrity_check():
    goal = Goal.create(text="Build a todo app", origin_peer="peer-1")
    tampered = goal.model_copy(update={"text": "Build a chat app"})
    assert not tampered.verify_integrity()


def test_goal_missing_required_field_rejected():
    with pytest.raises(ValidationError):
        Goal(goal_id="abc", text="x")  # missing origin_peer


def test_subgoal_proposal_round_trips_and_verifies():
    proposal = SubgoalProposal.create(
        goal_id="goal-1", proposer_peer="peer-1", subgoals=["a", "b"], rationale="because"
    )
    assert proposal.verify_integrity()


def test_subgoal_proposal_tampered_subgoals_fails_integrity_check():
    proposal = SubgoalProposal.create(
        goal_id="goal-1", proposer_peer="peer-1", subgoals=["a", "b"], rationale="because"
    )
    tampered = proposal.model_copy(update={"subgoals": ["a", "b", "c"]})
    assert not tampered.verify_integrity()


def test_subgoal_proposal_requires_at_least_one_subgoal():
    with pytest.raises(ValidationError):
        SubgoalProposal(
            goal_id="g", proposer_peer="p", subgoals=[], rationale="r", proposal_id="x"
        )


def test_score_msg_rejects_out_of_range_score():
    with pytest.raises(ValidationError):
        ScoreMsg(goal_id="g", proposal_id="p", scorer_peer="s", score=1.5)
    with pytest.raises(ValidationError):
        ScoreMsg(goal_id="g", proposal_id="p", scorer_peer="s", score=-0.1)


def test_score_msg_accepts_boundary_scores():
    ScoreMsg(goal_id="g", proposal_id="p", scorer_peer="s", score=0.0)
    ScoreMsg(goal_id="g", proposal_id="p", scorer_peer="s", score=1.0)


def test_accepted_split_rejects_out_of_range_score():
    with pytest.raises(ValidationError):
        AcceptedSplit(goal_id="g", proposal_id="p", subgoals=["a"], final_score=2.0)


def test_prompt_node_round_trips_and_verifies():
    node = PromptNode.create(goal_id="goal-1", text="Write the README", is_atomic=True)
    assert node.verify_integrity()
    assert node.required_capability is None


def test_prompt_node_tampered_atomicity_fails_integrity_check():
    node = PromptNode.create(goal_id="goal-1", text="Write the README", is_atomic=True)
    tampered = node.model_copy(update={"is_atomic": False})
    assert not tampered.verify_integrity()


def test_prompt_node_capability_not_part_of_identity_hash():
    a = PromptNode.create(goal_id="goal-1", text="x", is_atomic=True, required_capability="can_write_text")
    b = PromptNode.create(goal_id="goal-1", text="x", is_atomic=True, required_capability="can_query_api")
    assert a.node_id == b.node_id


def test_claim_msg_malformed_missing_field_rejected():
    with pytest.raises(ValidationError):
        ClaimMsg(node_id="n", claimer_peer="p", capability="can_write_text")  # missing claimed_at
