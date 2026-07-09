from decentralized_decomposer.agents.scorer_agent import next_depth_forces_atomic
from decentralized_decomposer.config import MAX_DEPTH
from decentralized_decomposer.protocol.messages import Goal


def test_depth_below_cutoff_does_not_force_atomic():
    assert next_depth_forces_atomic(MAX_DEPTH - 2) is False


def test_depth_at_cutoff_forces_atomic():
    assert next_depth_forces_atomic(MAX_DEPTH - 1) is True


def test_depth_past_cutoff_forces_atomic():
    assert next_depth_forces_atomic(MAX_DEPTH) is True


def test_goal_create_increments_depth_and_sets_parent():
    root = Goal.create(text="root", origin_peer="peer-1")
    assert root.depth == 0
    assert root.parent_id is None

    child = Goal.create(text="child", origin_peer="peer-1", parent_id=root.goal_id, depth=root.depth + 1)
    assert child.depth == 1
    assert child.parent_id == root.goal_id
    assert child.goal_id != root.goal_id
