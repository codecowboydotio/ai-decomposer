"""Pure acceptance-rule logic (spec §6): decide when a proposal is accepted.

Deliberately separated from `ScorerAgent` (no pubsub/trio here) so the rule
itself -- N-confirmation, timeout fallback -- is unit-testable
in isolation (spec §9.2, §9.3). Given the same sequence of observed
proposals/scores, every scorer peer reaches the same decision regardless
of delivery order; that determinism is what lets independently-running
scorers converge on one accepted split without a coordinator (spec §2a).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from decentralized_decomposer.config import ACCEPT_TIMEOUT, MIN_SCORE, N_CONFIRMATIONS
from decentralized_decomposer.protocol.messages import AcceptedSplit, ScoreMsg, SubgoalProposal


@dataclass
class AcceptanceTracker:
    proposals: dict[str, SubgoalProposal] = field(default_factory=dict)
    scores: list[ScoreMsg] = field(default_factory=list)

    def add_proposal(self, proposal: SubgoalProposal) -> None:
        self.proposals.setdefault(proposal.proposal_id, proposal)

    def add_score(self, score: ScoreMsg) -> None:
        self.scores.append(score)

    def _scores_for(self, proposal_id: str) -> list[ScoreMsg]:
        return [s for s in self.scores if s.proposal_id == proposal_id]

    def _accepted(self, proposal_id: str, final_score: float) -> AcceptedSplit:
        proposal = self.proposals[proposal_id]
        return AcceptedSplit(
            goal_id=proposal.goal_id,
            proposal_id=proposal_id,
            subgoals=proposal.subgoals,
            final_score=final_score,
        )

    def check(self, elapsed: float) -> AcceptedSplit | None:
        """Return the accepted split if criteria are met yet, else None.

        Order: (a) first proposal to reach N_CONFIRMATIONS distinct scorers
        each >= MIN_SCORE wins -- no proposal is ever accepted on a single
        scorer's say-so, since independently-running scorers must actually
        agree; (b) once `elapsed` >= ACCEPT_TIMEOUT, fall back to the
        highest mean-scored proposal among those with at least
        N_CONFIRMATIONS scores (single-scored proposals are never accepted
        via fallback). Proposal insertion order is the tie-break for (a) so
        the result is deterministic given an identical observed message
        sequence.
        """
        for proposal_id in self.proposals:
            proposal_scores = self._scores_for(proposal_id)
            confirmations = {s.scorer_peer for s in proposal_scores if s.score >= MIN_SCORE}
            if len(confirmations) >= N_CONFIRMATIONS:
                mean_score = sum(s.score for s in proposal_scores) / len(proposal_scores)
                return self._accepted(proposal_id, mean_score)

        if elapsed >= ACCEPT_TIMEOUT:
            best: tuple[str, float] | None = None
            for proposal_id in self.proposals:
                proposal_scores = self._scores_for(proposal_id)
                if len(proposal_scores) < N_CONFIRMATIONS:
                    continue
                mean_score = sum(s.score for s in proposal_scores) / len(proposal_scores)
                if best is None or mean_score > best[1]:
                    best = (proposal_id, mean_score)
            if best is not None:
                return self._accepted(best[0], best[1])

        return None
