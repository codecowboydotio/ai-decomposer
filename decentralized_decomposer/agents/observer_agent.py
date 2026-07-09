"""Observer: watches gossip and prints the reconstructed plan tree.

Not one of the three core roles in spec §3 -- it's a read-only convenience
so a human can actually *see* the decomposition + results (spec §1's "any
peer can reconstruct current plan state locally" is otherwise true but
invisible without something printing `PlanState.reconstruct`). Needs no
LLM calls and no capabilities; it just listens.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import trio
from libp2p.pubsub.pubsub import Pubsub

from decentralized_decomposer.p2p.crdt_state import PlanState
from decentralized_decomposer.p2p.pubsub import subscribe_and_validate
from decentralized_decomposer.protocol.messages import AcceptedSplit, Goal, PromptNode, ResultMsg
from decentralized_decomposer.protocol.topics import (
    NEW_GOAL_TOPIC,
    NEW_NODE_TOPIC,
    accepted_topic,
    node_prefix,
    result_topic,
    root_prefix,
)
from decentralized_decomposer.report import format_plan_tree, format_report

logger = logging.getLogger(__name__)

PRINT_INTERVAL = 3.0


class ObserverAgent:
    def __init__(self, pubsub: Pubsub, plan_state: PlanState, report_file: Path | None = None):
        self.pubsub = pubsub
        self.plan_state = plan_state
        self.report_file = report_file
        self._roots: set[str] = set()
        self._snapshots: dict[str, str] = {}
        self._nursery: trio.Nursery | None = None

    async def run(self, nursery: trio.Nursery) -> None:
        self._nursery = nursery
        nursery.start_soon(self._handle_new_goals)
        nursery.start_soon(self._handle_new_nodes)
        nursery.start_soon(self._print_loop)

    async def _handle_new_goals(self) -> None:
        assert self._nursery is not None
        async for goal in subscribe_and_validate(self.pubsub, NEW_GOAL_TOPIC, Goal):
            self.plan_state.apply_goal(goal)
            if goal.parent_id is None:
                self._roots.add(goal.goal_id)
            self._nursery.start_soon(self._watch_accepted, goal.goal_id)

    async def _watch_accepted(self, goal_id: str) -> None:
        prefix = root_prefix(goal_id)
        async for accepted in subscribe_and_validate(self.pubsub, accepted_topic(prefix), AcceptedSplit):
            self.plan_state.apply_accepted(accepted)
            return  # one accepted split per goal

    async def _handle_new_nodes(self) -> None:
        assert self._nursery is not None
        async for node in subscribe_and_validate(self.pubsub, NEW_NODE_TOPIC, PromptNode):
            self.plan_state.apply_node(node)
            self._nursery.start_soon(self._watch_result, node)

    async def _watch_result(self, node: PromptNode) -> None:
        prefix = node_prefix(node.goal_id, node.node_id)
        async for result in subscribe_and_validate(self.pubsub, result_topic(prefix), ResultMsg):
            self.plan_state.apply_result(result)
            return  # one result per node (first executor to finish wins any race)

    async def _print_loop(self) -> None:
        while True:
            await trio.sleep(PRINT_INTERVAL)
            changed = False
            for root_id in sorted(self._roots):
                tree = self.plan_state.reconstruct(root_id)
                snapshot = json.dumps(tree, sort_keys=True)
                if self._snapshots.get(root_id) != snapshot:
                    self._snapshots[root_id] = snapshot
                    changed = True
                    print(f"\n=== plan state for goal {root_id} ===")
                    print(format_plan_tree(self.plan_state, root_id))
            if changed and self.report_file is not None:
                self.report_file.write_text(
                    format_report(self.plan_state, list(self._roots)) + "\n", encoding="utf-8"
                )
