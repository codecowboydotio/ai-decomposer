"""Pydantic message schemas for the decentralized decomposer gossip protocol.

See spec §4. All ids (`goal_id`, `proposal_id`, `node_id`) are content-derived
hashes so a receiving agent can independently recompute and verify them
rather than trusting the sender's label (spec §6a).
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field


def _hash(*parts: str) -> str:
    h = hashlib.sha256()
    for part in parts:
        h.update(part.encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()[:16]


def compute_goal_id(text: str, created_at: datetime, origin_peer: str) -> str:
    return _hash(text, created_at.isoformat(), origin_peer)


def compute_proposal_id(goal_id: str, subgoals: list[str], proposer_peer: str) -> str:
    return _hash(goal_id, *subgoals, proposer_peer)


def compute_node_id(goal_id: str, text: str, is_atomic: bool) -> str:
    return _hash(goal_id, text, str(is_atomic))


class Goal(BaseModel):
    goal_id: str
    text: str
    parent_id: str | None = None
    depth: int = 0
    origin_peer: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @classmethod
    def create(
        cls, text: str, origin_peer: str, parent_id: str | None = None, depth: int = 0
    ) -> "Goal":
        created_at = datetime.now(timezone.utc)
        goal_id = compute_goal_id(text, created_at, origin_peer)
        return cls(
            goal_id=goal_id,
            text=text,
            parent_id=parent_id,
            depth=depth,
            origin_peer=origin_peer,
            created_at=created_at,
        )

    def verify_integrity(self) -> bool:
        return self.goal_id == compute_goal_id(self.text, self.created_at, self.origin_peer)


class SubgoalProposal(BaseModel):
    goal_id: str
    proposer_peer: str
    subgoals: list[str] = Field(min_length=1)
    rationale: str
    proposal_id: str

    @classmethod
    def create(
        cls, goal_id: str, proposer_peer: str, subgoals: list[str], rationale: str
    ) -> "SubgoalProposal":
        proposal_id = compute_proposal_id(goal_id, subgoals, proposer_peer)
        return cls(
            goal_id=goal_id,
            proposer_peer=proposer_peer,
            subgoals=subgoals,
            rationale=rationale,
            proposal_id=proposal_id,
        )

    def verify_integrity(self) -> bool:
        return self.proposal_id == compute_proposal_id(
            self.goal_id, self.subgoals, self.proposer_peer
        )


class ScoreMsg(BaseModel):
    goal_id: str
    proposal_id: str
    scorer_peer: str
    score: float = Field(ge=0.0, le=1.0)
    notes: str | None = None


class AcceptedSplit(BaseModel):
    goal_id: str
    proposal_id: str
    subgoals: list[str]
    final_score: float = Field(ge=0.0, le=1.0)


class PromptNode(BaseModel):
    node_id: str
    goal_id: str
    text: str
    is_atomic: bool
    depends_on: list[str] = Field(default_factory=list)
    # Not in the original spec §4 schema; added so executors can filter
    # subgoal topics by capability per spec §3.3 without every executor
    # having to run an LLM classification pass on every node it sees.
    # Excluded from the node_id hash: it's a routing hint, not identity.
    required_capability: str | None = None

    @classmethod
    def create(
        cls,
        goal_id: str,
        text: str,
        is_atomic: bool,
        depends_on: list[str] | None = None,
        required_capability: str | None = None,
    ) -> "PromptNode":
        node_id = compute_node_id(goal_id, text, is_atomic)
        return cls(
            node_id=node_id,
            goal_id=goal_id,
            text=text,
            is_atomic=is_atomic,
            depends_on=depends_on or [],
            required_capability=required_capability,
        )

    def verify_integrity(self) -> bool:
        return self.node_id == compute_node_id(self.goal_id, self.text, self.is_atomic)


class ClaimMsg(BaseModel):
    node_id: str
    claimer_peer: str
    claimed_at: datetime
    capability: str


class ResultMsg(BaseModel):
    node_id: str
    executor_peer: str
    output: str
    success: bool


class FailureMsg(BaseModel):
    """Published when an agent exhausts retries without producing a result (spec §6b)."""

    goal_id: str
    role: Literal["decomposer", "scorer", "executor"]
    reason: str
    node_id: str | None = None
