"""Decomposer agent (spec §3.1, §3a): goal -> LLM -> structured subgoal split -> publish.

Topic model note: rather than requiring decomposers to dynamically
discover and subscribe to an ever-deeper set of nested per-goal subtopics
(GossipSub has no wildcard subscription), every `Goal` -- top-level or a
recursively-spawned subgoal -- is announced on the single well-known
`NEW_GOAL_TOPIC`. Each goal's own propose/score/accepted/claim/result
topics then hang off `root_prefix(goal.goal_id)`. This realizes spec
§3.1's "subscribes to goal/* (and recursively to accepted subgoal
topics)" with one static subscription instead of a growing one, while
keeping the exact `goal/<goal_id>/propose` etc. topic shapes from §4.

Split count control: not part of the original spec, added on request.
The top-level (depth 0) goal can be pinned to an exact subgoal count via
`config.ROOT_SPLIT_COUNT`; anything else falls back to the
[MIN_SUBGOAL_SPLITS, MAX_SUBGOAL_SPLITS] range. Both are read from the
`config` module dynamically (not imported by name) so `main.py` can
override them per-process from a CLI flag before any goals arrive.
"""

from __future__ import annotations

import logging

import trio
from anthropic import AsyncAnthropic
from libp2p.pubsub.pubsub import Pubsub
from pydantic import BaseModel

from decentralized_decomposer import config
from decentralized_decomposer.llm import LLMCallFailed, call_structured
from decentralized_decomposer.p2p.pubsub import publish_model, subscribe_and_validate
from decentralized_decomposer.protocol.messages import FailureMsg, Goal, SubgoalProposal
from decentralized_decomposer.protocol.topics import (
    NEW_GOAL_TOPIC,
    propose_failed_topic,
    propose_topic,
    root_prefix,
)

logger = logging.getLogger(__name__)

DECOMPOSER_SYSTEM_PROMPT_TEMPLATE = (
    "You are a task decomposition agent. Given a goal, break it into {count_text} "
    "that are each independently actionable. Do not produce subgoals that require "
    "judgment calls beyond what's stated in the goal. Return ONLY valid JSON matching "
    'this schema: {{"subgoals": ["string", ...], "rationale": "string"}}'
)


class SubgoalDraft(BaseModel):
    subgoals: list[str]
    rationale: str


def exact_split_count_for(depth: int) -> int | None:
    """Exact subgoal count required at `depth`, or None if any count in
    [MIN_SUBGOAL_SPLITS, MAX_SUBGOAL_SPLITS] is acceptable.
    """
    if depth == 0:
        return config.ROOT_SPLIT_COUNT
    return None


def build_decomposer_system_prompt(depth: int) -> str:
    exact = exact_split_count_for(depth)
    if exact is not None:
        count_text = f"exactly {exact} subgoals"
    else:
        count_text = f"{config.MIN_SUBGOAL_SPLITS}-{config.MAX_SUBGOAL_SPLITS} subgoals"
    return DECOMPOSER_SYSTEM_PROMPT_TEMPLATE.format(count_text=count_text)


def validate_split_count(depth: int, draft: SubgoalDraft) -> None:
    """Raise ValueError (caught by `call_structured`'s retry loop) if the
    split doesn't respect the exact/ranged count constraint for `depth`.
    """
    n = len(draft.subgoals)
    exact = exact_split_count_for(depth)
    if exact is not None:
        if n != exact:
            raise ValueError(f"expected exactly {exact} subgoals, got {n}")
    elif not (config.MIN_SUBGOAL_SPLITS <= n <= config.MAX_SUBGOAL_SPLITS):
        raise ValueError(
            f"expected between {config.MIN_SUBGOAL_SPLITS} and {config.MAX_SUBGOAL_SPLITS} "
            f"subgoals, got {n}"
        )


class DecomposerAgent:
    def __init__(self, pubsub: Pubsub, peer_id: str, llm_client: AsyncAnthropic):
        self.pubsub = pubsub
        self.peer_id = peer_id
        self.llm = llm_client

    async def run(self, nursery: trio.Nursery) -> None:
        nursery.start_soon(self._handle_new_goals)

    async def _handle_new_goals(self) -> None:
        async for goal in subscribe_and_validate(self.pubsub, NEW_GOAL_TOPIC, Goal):
            if goal.depth >= config.MAX_DEPTH:
                logger.debug("Skipping decomposition of %s: already at MAX_DEPTH", goal.goal_id)
                continue
            await self.propose_split(goal)

    async def propose_split(self, goal: Goal) -> SubgoalProposal | None:
        """Propose a split for `goal`. Returns None (and publishes a failure notice)
        if the LLM call/parse fails after retries -- never a guessed result (spec §6b).
        """
        user_prompt = (
            f'Goal: "{goal.text}"\n'
            f"Depth: {goal.depth} (max {config.MAX_DEPTH})\n"
            f"Parent context: {goal.parent_id or 'none (top-level goal)'}"
        )
        try:
            draft = await call_structured(
                self.llm,
                system=build_decomposer_system_prompt(goal.depth),
                user=user_prompt,
                response_model=SubgoalDraft,
                validate=lambda d: validate_split_count(goal.depth, d),
            )
        except LLMCallFailed as exc:
            logger.warning("Decomposition of %s failed: %s", goal.goal_id, exc)
            await publish_model(
                self.pubsub,
                propose_failed_topic(root_prefix(goal.goal_id)),
                FailureMsg(goal_id=goal.goal_id, role="decomposer", reason=str(exc)),
            )
            return None

        proposal = SubgoalProposal.create(
            goal_id=goal.goal_id,
            proposer_peer=self.peer_id,
            subgoals=draft.subgoals,
            rationale=draft.rationale,
        )
        await publish_model(self.pubsub, propose_topic(root_prefix(goal.goal_id)), proposal)
        logger.info("Proposed split for %s: %d subgoals", goal.goal_id, len(draft.subgoals))
        return proposal
