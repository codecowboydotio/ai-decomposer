from decentralized_decomposer.agents.acceptance import AcceptanceTracker
from decentralized_decomposer.config import ACCEPT_TIMEOUT, MIN_SCORE, N_CONFIRMATIONS
from decentralized_decomposer.protocol.messages import ScoreMsg, SubgoalProposal


def _proposal(pid_suffix: str, subgoals: list[str] | None = None) -> SubgoalProposal:
    return SubgoalProposal.create(
        goal_id="g1",
        proposer_peer=f"proposer-{pid_suffix}",
        subgoals=subgoals or [f"sub-{pid_suffix}-1", f"sub-{pid_suffix}-2"],
        rationale="because",
    )


def test_no_acceptance_before_any_scores():
    tracker = AcceptanceTracker()
    tracker.add_proposal(_proposal("a"))
    assert tracker.check(elapsed=0.0) is None


def test_single_score_never_accepts_even_at_max_score():
    # A lone scorer's opinion -- however high -- must never be enough on
    # its own; acceptance requires independently-running scorers to agree.
    tracker = AcceptanceTracker()
    proposal = _proposal("a")
    tracker.add_proposal(proposal)
    tracker.add_score(
        ScoreMsg(
            goal_id="g1",
            proposal_id=proposal.proposal_id,
            scorer_peer="scorer-1",
            score=1.0,
        )
    )
    assert tracker.check(elapsed=0.1) is None


def test_below_min_score_single_score_does_not_accept():
    tracker = AcceptanceTracker()
    proposal = _proposal("a")
    tracker.add_proposal(proposal)
    tracker.add_score(
        ScoreMsg(
            goal_id="g1",
            proposal_id=proposal.proposal_id,
            scorer_peer="scorer-1",
            score=MIN_SCORE - 0.1,
        )
    )
    assert tracker.check(elapsed=0.1) is None


def test_n_confirmations_above_min_score_accepts():
    tracker = AcceptanceTracker()
    proposal = _proposal("a")
    tracker.add_proposal(proposal)
    for i in range(N_CONFIRMATIONS):
        tracker.add_score(
            ScoreMsg(
                goal_id="g1",
                proposal_id=proposal.proposal_id,
                scorer_peer=f"scorer-{i}",
                score=MIN_SCORE,
            )
        )
    result = tracker.check(elapsed=0.1)
    assert result is not None
    assert result.proposal_id == proposal.proposal_id


def test_n_confirmations_from_the_same_scorer_do_not_count_twice():
    tracker = AcceptanceTracker()
    proposal = _proposal("a")
    tracker.add_proposal(proposal)
    for _ in range(N_CONFIRMATIONS + 2):
        tracker.add_score(
            ScoreMsg(
                goal_id="g1",
                proposal_id=proposal.proposal_id,
                scorer_peer="scorer-only-one",
                score=MIN_SCORE,
            )
        )
    assert tracker.check(elapsed=0.1) is None


def test_timeout_falls_back_to_highest_mean_scored_proposal():
    # Scores are kept below MIN_SCORE so tier (b) confirmation never fires,
    # isolating the tier (c) timeout fallback behavior being tested here.
    tracker = AcceptanceTracker()
    weak = _proposal("weak")
    strong = _proposal("strong")
    tracker.add_proposal(weak)
    tracker.add_proposal(strong)
    for i in range(N_CONFIRMATIONS):
        tracker.add_score(
            ScoreMsg(goal_id="g1", proposal_id=weak.proposal_id, scorer_peer=f"s{i}", score=0.3)
        )
        tracker.add_score(
            ScoreMsg(goal_id="g1", proposal_id=strong.proposal_id, scorer_peer=f"s{i}", score=0.5)
        )

    assert tracker.check(elapsed=ACCEPT_TIMEOUT - 0.01) is None  # not timed out yet
    result = tracker.check(elapsed=ACCEPT_TIMEOUT)
    assert result is not None
    assert result.proposal_id == strong.proposal_id


def test_timeout_with_no_scores_at_all_yields_no_acceptance():
    tracker = AcceptanceTracker()
    tracker.add_proposal(_proposal("a"))
    assert tracker.check(elapsed=ACCEPT_TIMEOUT + 10) is None


def test_timeout_fallback_ignores_proposals_with_fewer_than_n_confirmations():
    tracker = AcceptanceTracker()
    single = _proposal("single")
    tracker.add_proposal(single)
    tracker.add_score(
        ScoreMsg(goal_id="g1", proposal_id=single.proposal_id, scorer_peer="s1", score=0.5)
    )
    assert tracker.check(elapsed=ACCEPT_TIMEOUT + 10) is None
