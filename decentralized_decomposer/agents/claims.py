"""Pure claim-resolution logic (spec §6 claim protocol, §6a timestamp sanity check).

Separated from `ExecutorAgent` (no pubsub/trio here) so the race-resolution
rule is unit-testable in isolation (spec §9.2).
"""

from __future__ import annotations

from datetime import datetime, timezone

from decentralized_decomposer.config import CLAIM_TIMESTAMP_TOLERANCE
from decentralized_decomposer.protocol.messages import ClaimMsg


def is_claim_timestamp_sane(
    claim: ClaimMsg, now: datetime | None = None, tolerance: float = CLAIM_TIMESTAMP_TOLERANCE
) -> bool:
    """Spec §6a: an incoming `claimed_at` must be within `tolerance` seconds of receipt time."""
    now = now or datetime.now(timezone.utc)
    return abs((now - claim.claimed_at).total_seconds()) <= tolerance


def resolve_claim_winner(claims: list[ClaimMsg]) -> ClaimMsg:
    """First-published-timestamp wins (spec §6); `claimer_peer` breaks exact ties
    deterministically so every peer that sees the same claim set agrees on the winner.
    """
    if not claims:
        raise ValueError("resolve_claim_winner requires at least one claim")
    return min(claims, key=lambda c: (c.claimed_at, c.claimer_peer))
