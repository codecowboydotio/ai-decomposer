import random

from decentralized_decomposer.p2p.crdt_state import GSet, PlanState
from decentralized_decomposer.protocol.messages import AcceptedSplit, Goal, PromptNode, ResultMsg


def test_gset_add_is_idempotent():
    gset: GSet[str, int] = GSet()
    assert gset.add("a", 1) is True
    assert gset.add("a", 999) is False  # duplicate key, ignored
    assert gset.get("a") == 1
    assert len(gset) == 1


def test_gset_merge_is_a_union_and_order_independent():
    a: GSet[str, int] = GSet()
    a.add("x", 1)
    a.add("y", 2)

    b: GSet[str, int] = GSet()
    b.add("y", 2)  # duplicate of a's entry
    b.add("z", 3)

    merged_ab: GSet[str, int] = GSet()
    merged_ab.merge(a)
    merged_ab.merge(b)

    merged_ba: GSet[str, int] = GSet()
    merged_ba.merge(b)
    merged_ba.merge(a)

    assert merged_ab == merged_ba
    assert set(merged_ab.values()) == {1, 2, 3}


def test_gset_merge_handles_duplicate_and_out_of_order_deltas():
    deltas = [("a", 1), ("b", 2), ("a", 1), ("c", 3), ("b", 2)]
    shuffled = deltas[:]
    random.shuffle(shuffled)

    gset_in_order: GSet[str, int] = GSet()
    for k, v in deltas:
        gset_in_order.add(k, v)

    gset_shuffled: GSet[str, int] = GSet()
    for k, v in shuffled:
        gset_shuffled.add(k, v)

    assert gset_in_order == gset_shuffled
    assert len(gset_in_order) == 3


def _goal(text: str, parent_id: str | None = None, depth: int = 0) -> Goal:
    return Goal.create(text=text, origin_peer="peer-1", parent_id=parent_id, depth=depth)


def test_plan_state_reconstruct_pending_when_no_accepted_split():
    state = PlanState()
    root = _goal("Build a todo app")
    state.apply_goal(root)

    tree = state.reconstruct(root.goal_id)
    assert tree["status"] == "pending_decomposition"


def test_plan_state_reconstruct_full_tree_leaf_and_recursive_child():
    state = PlanState()
    root = _goal("Build a todo app")
    state.apply_goal(root)

    accepted = AcceptedSplit(
        goal_id=root.goal_id,
        proposal_id="p1",
        subgoals=["Design the schema", "Write the API"],
        final_score=0.9,
    )
    state.apply_accepted(accepted)

    leaf = PromptNode.create(goal_id=root.goal_id, text="Design the schema", is_atomic=True)
    state.apply_node(leaf)
    state.apply_result(ResultMsg(node_id=leaf.node_id, executor_peer="p2", output="done", success=True))

    child = _goal("Write the API", parent_id=root.goal_id, depth=1)
    state.apply_goal(child)

    tree = state.reconstruct(root.goal_id)
    assert tree["status"] == "decomposed"
    assert len(tree["subgoals"]) == 2

    leaf_view = next(s for s in tree["subgoals"] if s.get("text") == "Design the schema")
    assert leaf_view["status"] == "done"
    assert leaf_view["result"] == "done"

    child_view = next(s for s in tree["subgoals"] if s.get("goal_id") == child.goal_id)
    assert child_view["status"] == "pending_decomposition"


def test_plan_state_reconstruct_prefers_accepted_goal_among_retried_duplicates():
    """The decomposer's timeout-retry (agents/decomposer_agent.py) republishes a
    stalled goal as a fresh goal_id with the same (parent_id, text) -- reconstruct
    must resolve to whichever one actually converged, not whichever was seen first.
    """
    state = PlanState()
    root = _goal("Build a todo app")
    state.apply_goal(root)
    state.apply_accepted(
        AcceptedSplit(goal_id=root.goal_id, proposal_id="p1", subgoals=["Write the API"], final_score=0.9)
    )

    stale = _goal("Write the API", parent_id=root.goal_id, depth=1)  # never converges
    retry = _goal("Write the API", parent_id=root.goal_id, depth=1)  # the reproposal
    state.apply_goal(stale)
    state.apply_goal(retry)
    state.apply_accepted(
        AcceptedSplit(
            goal_id=retry.goal_id, proposal_id="p2", subgoals=["Write the endpoint"], final_score=0.85
        )
    )

    tree = state.reconstruct(root.goal_id)
    child_view = tree["subgoals"][0]
    assert child_view["goal_id"] == retry.goal_id
    assert child_view["status"] == "decomposed"


def test_plan_state_merge_combines_independently_seen_deltas():
    root = _goal("Build a todo app")

    state_a = PlanState()
    state_a.apply_goal(root)

    state_b = PlanState()
    state_b.apply_goal(root)
    state_b.apply_accepted(
        AcceptedSplit(goal_id=root.goal_id, proposal_id="p1", subgoals=["x"], final_score=0.8)
    )

    state_a.merge(state_b)
    assert root.goal_id in state_a.accepted_splits
