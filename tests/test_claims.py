from datetime import datetime, timedelta, timezone

import pytest

from decentralized_decomposer.agents.claims import is_claim_timestamp_sane, resolve_claim_winner
from decentralized_decomposer.protocol.messages import ClaimMsg


def _claim(peer: str, when: datetime, capability: str = "can_write_text") -> ClaimMsg:
    return ClaimMsg(node_id="n1", claimer_peer=peer, claimed_at=when, capability=capability)


def test_resolve_claim_winner_earliest_timestamp_wins():
    now = datetime.now(timezone.utc)
    early = _claim("peer-a", now)
    late = _claim("peer-b", now + timedelta(seconds=1))

    assert resolve_claim_winner([late, early]) == early
    assert resolve_claim_winner([early, late]) == early


def test_resolve_claim_winner_tie_break_is_deterministic_by_peer_id():
    now = datetime.now(timezone.utc)
    a = _claim("peer-a", now)
    b = _claim("peer-b", now)  # exact tie on timestamp

    winner_1 = resolve_claim_winner([a, b])
    winner_2 = resolve_claim_winner([b, a])
    assert winner_1 == winner_2 == a  # "peer-a" < "peer-b" lexicographically


def test_resolve_claim_winner_near_tie_within_tolerance_window():
    now = datetime.now(timezone.utc)
    a = _claim("peer-a", now)
    b = _claim("peer-b", now + timedelta(seconds=0.001))
    assert resolve_claim_winner([a, b]) == a


def test_resolve_claim_winner_requires_at_least_one_claim():
    with pytest.raises(ValueError):
        resolve_claim_winner([])


def test_claim_timestamp_sanity_check_within_tolerance():
    now = datetime.now(timezone.utc)
    claim = _claim("peer-a", now - timedelta(seconds=3))
    assert is_claim_timestamp_sane(claim, now=now, tolerance=5.0)


def test_claim_timestamp_sanity_check_rejects_skewed_timestamp():
    now = datetime.now(timezone.utc)
    claim = _claim("peer-a", now - timedelta(seconds=30))
    assert not is_claim_timestamp_sane(claim, now=now, tolerance=5.0)
