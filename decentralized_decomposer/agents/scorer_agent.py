"""Scorer agent (spec §3.2, §3a, §6): score competing splits, decide acceptance, recurse.

Recursion (spec §6): once a split is accepted, each subgoal is classified
atomic-or-composite by a dedicated LLM pass (never a heuristic string
check). Atomic subgoals become `PromptNode`s announced on `NEW_NODE_TOPIC`
for executors to claim; composite ones become new `Goal`s announced on
`NEW_GOAL_TOPIC` with depth+1, re-triggering the same cycle -- unless
depth+1 has already hit `MAX_DEPTH`, in which case the subgoal is forced
atomic regardless of the classifier, to guarantee recursion terminates.
"""

from __future__ import annotations

import logging

import trio
from anthropic import AsyncAnthropic
from libp2p.pubsub.pubsub import Pubsub
from pydantic import BaseModel

from decentralized_decomposer import config
from decentralized_decomposer.agents.acceptance import AcceptanceTracker
from decentralized_decomposer.config import ACCEPT_TIMEOUT
from decentralized_decomposer.llm import LLMCallFailed, call_structured
from decentralized_decomposer.p2p.crdt_state import PlanState
from decentralized_decomposer.p2p.pubsub import publish_model, subscribe_and_validate
from decentralized_decomposer.protocol.messages import (
    AcceptedSplit,
    FailureMsg,
    Goal,
    PromptNode,
    ScoreMsg,
    SubgoalProposal,
)
from decentralized_decomposer.protocol.topics import (
    NEW_GOAL_TOPIC,
    NEW_NODE_TOPIC,
    accepted_topic,
    propose_topic,
    root_prefix,
    score_failed_topic,
    score_topic,
)

logger = logging.getLogger(__name__)

SCORER_SYSTEM_PROMPT = (
    "You are evaluating a proposed task decomposition against a fixed rubric. Score "
    "0.0-1.0 on: (a) does the split fully cover the original goal, (b) are subgoals "
    "non-overlapping, (c) is each subgoal actually atomic/actionable or still too "
    'coarse. Return ONLY valid JSON matching: {"score": float, "notes": "string"}'
)

ATOMICITY_SYSTEM_PROMPT = (
    "Classify whether this subgoal is atomic (a single concrete deliverable requiring "
    "no further decomposition) or composite (still needs breaking down). Return ONLY: "
    '{"is_atomic": bool, "reason": "string"}'
)


class ScoreDraft(BaseModel):
    score: float
    notes: str | None = None


class AtomicityDraft(BaseModel):
    is_atomic: bool
    reason: str


def next_depth_forces_atomic(current_depth: int) -> bool:
    """Spec §6: recursion stops at MAX_DEPTH regardless of the atomicity classifier."""
    return current_depth + 1 >= config.MAX_DEPTH


class ScorerAgent:
    def __init__(
        self, pubsub: Pubsub, peer_id: str, llm_client: AsyncAnthropic, plan_state: PlanState
    ):
        self.pubsub = pubsub
        self.peer_id = peer_id
        self.llm = llm_client
        self.plan_state = plan_state
        self._active_goals: set[str] = set()
        self._nursery: trio.Nursery | None = None

    async def run(self, nursery: trio.Nursery) -> None:
        self._nursery = nursery
        nursery.start_soon(self._handle_new_goals)

    async def _handle_new_goals(self) -> None:
        async for goal in subscribe_and_validate(self.pubsub, NEW_GOAL_TOPIC, Goal):
            logger.info("Observed goal %s (depth=%d): %s", goal.goal_id, goal.depth, goal.text)
            self.plan_state.apply_goal(goal)
            if goal.goal_id in self._active_goals:
                continue
            self._active_goals.add(goal.goal_id)
            assert self._nursery is not None
            self._nursery.start_soon(self._track_goal, goal)

    async def _track_goal(self, goal: Goal) -> None:
        prefix = root_prefix(goal.goal_id)
        tracker = AcceptanceTracker()
        start = trio.current_time()
        send_decision, recv_decision = trio.open_memory_channel[AcceptedSplit](1)

        def _maybe_signal(elapsed: float) -> None:
            decision = tracker.check(elapsed)
            if decision is not None:
                try:
                    send_decision.send_nowait(decision)
                except trio.WouldBlock:
                    pass

        async def watch_proposals() -> None:
            async for proposal in subscribe_and_validate(
                self.pubsub, propose_topic(prefix), SubgoalProposal
            ):
                tracker.add_proposal(proposal)
                await self._score_proposal(goal, proposal, prefix)
                _maybe_signal(trio.current_time() - start)

        async def watch_scores() -> None:
            async for score in subscribe_and_validate(self.pubsub, score_topic(prefix), ScoreMsg):
                tracker.add_score(score)
                _maybe_signal(trio.current_time() - start)

        async def watch_timeout() -> None:
            await trio.sleep(ACCEPT_TIMEOUT)
            _maybe_signal(ACCEPT_TIMEOUT)

        async with trio.open_nursery() as sub_nursery:
            sub_nursery.start_soon(watch_proposals)
            sub_nursery.start_soon(watch_scores)
            sub_nursery.start_soon(watch_timeout)
            accepted = await recv_decision.receive()
            sub_nursery.cancel_scope.cancel()

        self.plan_state.apply_accepted(accepted)
        await publish_model(self.pubsub, accepted_topic(prefix), accepted)
        logger.info("Accepted split for %s (score=%.2f)", goal.goal_id, accepted.final_score)
        await self._recurse(goal, accepted)
        self._active_goals.discard(goal.goal_id)

    async def _score_proposal(self, goal: Goal, proposal: SubgoalProposal, prefix: str) -> None:
        user_prompt = (
            f'Original goal: "{goal.text}"\n'
            f"Proposed split: {proposal.subgoals}\n"
            f"Rationale given: {proposal.rationale}"
        )
        try:
            draft = await call_structured(
                self.llm, system=SCORER_SYSTEM_PROMPT, user=user_prompt, response_model=ScoreDraft
            )
        except LLMCallFailed as exc:
            logger.warning("Scoring of proposal %s failed: %s", proposal.proposal_id, exc)
            await publish_model(
                self.pubsub,
                score_failed_topic(prefix),
                FailureMsg(goal_id=goal.goal_id, role="scorer", reason=str(exc)),
            )
            return

        score = max(0.0, min(1.0, draft.score))
        score_msg = ScoreMsg(
            goal_id=goal.goal_id,
            proposal_id=proposal.proposal_id,
            scorer_peer=self.peer_id,
            score=score,
            notes=draft.notes,
        )
        await publish_model(self.pubsub, score_topic(prefix), score_msg)

    async def _classify_atomicity(self, subgoal_text: str) -> bool:
        try:
            draft = await call_structured(
                self.llm,
                system=ATOMICITY_SYSTEM_PROMPT,
                user=f'Subgoal: "{subgoal_text}"',
                response_model=AtomicityDraft,
            )
        except LLMCallFailed as exc:
            logger.warning(
                "Atomicity classification failed for %r, defaulting to atomic: %s",
                subgoal_text,
                exc,
            )
            return True
        return draft.is_atomic

    async def _recurse(self, goal: Goal, accepted: AcceptedSplit) -> None:
        force_atomic = next_depth_forces_atomic(goal.depth)
        for subgoal_text in accepted.subgoals:
            is_atomic = True if force_atomic else await self._classify_atomicity(subgoal_text)

            if is_atomic:
                node = PromptNode.create(goal_id=goal.goal_id, text=subgoal_text, is_atomic=True)
                self.plan_state.apply_node(node)
                await publish_model(self.pubsub, NEW_NODE_TOPIC, node)
            else:
                child_goal = Goal.create(
                    text=subgoal_text,
                    origin_peer=self.peer_id,
                    parent_id=goal.goal_id,
                    depth=goal.depth + 1,
                )
                self.plan_state.apply_goal(child_goal)
                await publish_model(self.pubsub, NEW_GOAL_TOPIC, child_goal)
