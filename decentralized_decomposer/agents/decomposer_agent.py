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


def _rejection_feedback(proposal: SubgoalProposal, scores: list[ScoreMsg]) -> str:
    """Summarize why a proposal didn't converge, for the next attempt's prompt."""
    subgoals_text = "; ".join(proposal.subgoals)
    header = (
        f"Your previous split into {len(proposal.subgoals)} subgoals ({subgoals_text}) "
        "was not accepted in time."
    )
    if not scores:
        return f"{header} No scorer feedback arrived -- try a clearer, more actionable split."
    notes = [f"{s.score:.2f}" + (f" ({s.notes})" if s.notes else "") for s in scores]
    return f"{header} Scorer feedback: {'; '.join(notes)}. Produce a different, improved split."


class DecomposerAgent:
    def __init__(self, pubsub: Pubsub, peer_id: str, llm_client: AsyncAnthropic):
        self.pubsub = pubsub
        self.peer_id = peer_id
        self.llm = llm_client
        self._seen_goals: set[str] = set()
        self._nursery: trio.Nursery | None = None

    async def run(self, nursery: trio.Nursery) -> None:
        self._nursery = nursery
        nursery.start_soon(self._handle_new_goals)

    async def _handle_new_goals(self) -> None:
        assert self._nursery is not None
        async for goal in subscribe_and_validate(self.pubsub, NEW_GOAL_TOPIC, Goal):
            if goal.depth >= config.MAX_DEPTH:
                logger.debug("Skipping decomposition of %s: already at MAX_DEPTH", goal.goal_id)
                continue
            if goal.goal_id in self._seen_goals:
                # GossipSub gives no exactly-once delivery guarantee (see the
                # identical guard on ExecutorAgent's node/new handling): without
                # this, a duplicate delivery would spawn a second concurrent
                # _propose_with_retry for the same goal_id, and both would end
                # up watching the same accepted_topic subscription out from
                # under each other.
                continue
            self._seen_goals.add(goal.goal_id)
            self._nursery.start_soon(self._propose_with_retry, goal, 0)

    async def _propose_with_retry(
        self, goal: Goal, attempt: int, feedback: str | None = None
    ) -> None:
        """Propose a split for `goal`, then watch for it to actually converge.

        Not part of the original spec: a proposal that never reaches
        `N_CONFIRMATIONS` scorers at `MIN_SCORE` (or has too few scorers
        running to ever confirm at all) otherwise leaves the goal stuck in
        `pending_decomposition` forever. Here, if no accepted split shows up
        within `ACCEPT_TIMEOUT` (+ a small grace buffer so we don't race a
        scorer's own timeout-fallback accept), republish the same goal text
        as a fresh `Goal` (a new goal_id, so this is a clean independent
        acceptance cycle -- see README) and try again, up to
        `MAX_REPROPOSAL_ATTEMPTS` times before giving up with a failure notice.

        Retries aren't a blind re-roll: whatever scorer notes arrived for the
        rejected proposal (or their absence) are fed back into the next
        attempt's prompt, the same way `call_structured` feeds a schema error
        back on retry -- otherwise a goal that's genuinely hard to split well
        would likely just get re-scored the same way every time, burning all
        its attempts without ever improving.
        """
        proposal = await self.propose_split(goal, feedback=feedback)
        if proposal is None:
            return  # propose_split already published a propose_failed notice

        accepted, scores = await self._wait_for_outcome(goal.goal_id)
        if accepted:
            return

        if attempt >= config.MAX_REPROPOSAL_ATTEMPTS:
            logger.warning(
                "Giving up on %s after %d proposal attempt(s) with no accepted split",
                goal.goal_id,
                attempt + 1,
            )
            await publish_model(
                self.pubsub,
                propose_failed_topic(root_prefix(goal.goal_id)),
                FailureMsg(
                    goal_id=goal.goal_id,
                    role="decomposer",
                    reason=(
                        f"exhausted {config.MAX_REPROPOSAL_ATTEMPTS} reproposal attempt(s) "
                        "without an accepted split"
                    ),
                ),
            )
            return

        retry_goal = Goal.create(
            text=goal.text,
            origin_peer=self.peer_id,
            parent_id=goal.parent_id,
            depth=goal.depth,
        )
        logger.info(
            "No accepted split for %s within %.0fs; re-proposing as %s (attempt %d/%d)",
            goal.goal_id,
            config.ACCEPT_TIMEOUT + config.REPROPOSAL_GRACE,
            retry_goal.goal_id,
            attempt + 2,
            config.MAX_REPROPOSAL_ATTEMPTS + 1,
        )
        self._seen_goals.add(retry_goal.goal_id)
        await publish_model(self.pubsub, NEW_GOAL_TOPIC, retry_goal)
        assert self._nursery is not None
        self._nursery.start_soon(
            self._propose_with_retry,
            retry_goal,
            attempt + 1,
            _rejection_feedback(proposal, scores),
        )

    async def _wait_for_outcome(self, goal_id: str) -> tuple[bool, list[ScoreMsg]]:
        """Wait for `goal_id` to converge on an accepted split, or time out.

        Returns (converged, scores observed for it in the meantime) -- the
        scores are collected regardless of outcome so a timeout still has
        something concrete to feed back into the next attempt's prompt.
        """
        prefix = root_prefix(goal_id)
        send_done, recv_done = trio.open_memory_channel[None](1)
        scores: list[ScoreMsg] = []

        async def watch_accepted() -> None:
            async for _ in subscribe_and_validate(self.pubsub, accepted_topic(prefix), AcceptedSplit):
                try:
                    send_done.send_nowait(None)
                except trio.WouldBlock:
                    pass
                return

        async def watch_scores() -> None:
            async for score in subscribe_and_validate(self.pubsub, score_topic(prefix), ScoreMsg):
                scores.append(score)

        async with trio.open_nursery() as sub_nursery:
            sub_nursery.start_soon(watch_accepted)
            sub_nursery.start_soon(watch_scores)
            with trio.move_on_after(config.ACCEPT_TIMEOUT + config.REPROPOSAL_GRACE) as scope:
                await recv_done.receive()
            sub_nursery.cancel_scope.cancel()
            return not scope.cancelled_caught, scores

    async def propose_split(
        self, goal: Goal, feedback: str | None = None
    ) -> SubgoalProposal | None:
        """Propose a split for `goal`. Returns None (and publishes a failure notice)
        if the LLM call/parse fails after retries -- never a guessed result (spec §6b).
        """
        user_prompt = (
            f'Goal: "{goal.text}"\n'
            f"Depth: {goal.depth} (max {config.MAX_DEPTH})\n"
            f"Parent context: {goal.parent_id or 'none (top-level goal)'}"
        )
        if feedback:
            user_prompt += f"\n\n{feedback}"
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
