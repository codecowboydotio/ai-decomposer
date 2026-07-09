"""Replicated plan state: a grow-only set (G-Set) of accepted splits + results (spec §6).

No peer ever holds the "whole plan" (spec §1) -- each peer applies whatever
deltas it has seen via gossip, in whatever order they arrive, and
`PlanState.reconstruct` builds the best local view from that. G-Sets merge
by taking the union of entries keyed by content-derived id, so the result
is identical regardless of delta order or duplicates (add is idempotent).

Starting with a custom G-Set rather than `automerge` per spec §10 (deferred
decision) -- the state here is simple enough (four flat id-keyed sets, no
concurrent edits to the same key) that CRDT merge complexity doesn't
warrant the extra dependency yet.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar

from decentralized_decomposer.protocol.messages import (
    AcceptedSplit,
    Goal,
    PromptNode,
    ResultMsg,
)

K = TypeVar("K")
V = TypeVar("V")


class GSet(Generic[K, V]):
    """A grow-only set of (key, value) pairs. Adds are idempotent; merge is a union."""

    def __init__(self) -> None:
        self._items: dict[K, V] = {}

    def add(self, key: K, value: V) -> bool:
        """Insert `value` under `key` if not already present. Returns True if newly added."""
        if key in self._items:
            return False
        self._items[key] = value
        return True

    def merge(self, other: "GSet[K, V]") -> None:
        for key, value in other._items.items():
            self._items.setdefault(key, value)

    def __contains__(self, key: K) -> bool:
        return key in self._items

    def get(self, key: K) -> V | None:
        return self._items.get(key)

    def values(self) -> list[V]:
        return list(self._items.values())

    def __len__(self) -> int:
        return len(self._items)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, GSet) and self._items == other._items


@dataclass
class PlanState:
    """Local reconstruction of decomposition-tree state from replicated deltas."""

    goals: GSet[str, Goal] = field(default_factory=GSet)
    accepted_splits: GSet[str, AcceptedSplit] = field(default_factory=GSet)  # keyed by goal_id
    atomic_nodes: GSet[str, PromptNode] = field(default_factory=GSet)  # keyed by node_id
    results: GSet[str, ResultMsg] = field(default_factory=GSet)  # keyed by node_id

    def apply_goal(self, goal: Goal) -> bool:
        return self.goals.add(goal.goal_id, goal)

    def apply_accepted(self, accepted: AcceptedSplit) -> bool:
        return self.accepted_splits.add(accepted.goal_id, accepted)

    def apply_node(self, node: PromptNode) -> bool:
        return self.atomic_nodes.add(node.node_id, node)

    def apply_result(self, result: ResultMsg) -> bool:
        return self.results.add(result.node_id, result)

    def merge(self, other: "PlanState") -> None:
        self.goals.merge(other.goals)
        self.accepted_splits.merge(other.accepted_splits)
        self.atomic_nodes.merge(other.atomic_nodes)
        self.results.merge(other.results)

    def is_resolved(self, node_id: str) -> bool:
        return node_id in self.results

    def reconstruct(self, goal_id: str) -> dict:
        """Best-effort local view of the decomposition subtree rooted at `goal_id`.

        Branches for which the accepted split, a child goal, or a result
        hasn't arrived yet are simply omitted -- this is expected under
        eventual consistency, not an error.
        """
        node: dict = {"goal_id": goal_id}

        accepted = self.accepted_splits.get(goal_id)
        if accepted is None:
            node["status"] = "pending_decomposition"
            return node

        children = []
        for subgoal_text in accepted.subgoals:
            child_goal = next(
                (
                    g
                    for g in self.goals.values()
                    if g.parent_id == goal_id and g.text == subgoal_text
                ),
                None,
            )
            if child_goal is not None:
                children.append(self.reconstruct(child_goal.goal_id))
                continue

            atomic_node = next(
                (
                    n
                    for n in self.atomic_nodes.values()
                    if n.goal_id == goal_id and n.text == subgoal_text
                ),
                None,
            )
            if atomic_node is not None:
                result = self.results.get(atomic_node.node_id)
                children.append(
                    {
                        "node_id": atomic_node.node_id,
                        "text": atomic_node.text,
                        "status": "done" if result else "pending_execution",
                        "result": result.output if result else None,
                    }
                )
                continue

            children.append({"text": subgoal_text, "status": "unknown"})

        node["status"] = "decomposed"
        node["subgoals"] = children
        return node
