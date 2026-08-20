"""Tunables for depth limits, scoring thresholds, timeouts, and retries (spec §6, §6b)."""

from __future__ import annotations

import os
from enum import StrEnum

# --- Recursion / decomposition ---
MAX_DEPTH = int(os.environ.get("DD_MAX_DEPTH", 3))

# Exact subgoal count for the top-level (depth 0) goal. None = let the LLM
# choose a count within [MIN_SUBGOAL_SPLITS, MAX_SUBGOAL_SPLITS], same as
# any other goal. Read dynamically (via the `config` module, not imported
# by name) so `main.py` can override it per-process from a CLI flag.
_root_split_count_env = os.environ.get("DD_ROOT_SPLIT_COUNT")
ROOT_SPLIT_COUNT: int | None = int(_root_split_count_env) if _root_split_count_env else None

# Bounds on subgoal count for any decomposition that doesn't have an exact
# count pinned (the root when ROOT_SPLIT_COUNT is None, and always for
# deeper levels if MAX_DEPTH > 1 lets recursion go past depth 1).
MIN_SUBGOAL_SPLITS = int(os.environ.get("DD_MIN_SUBGOAL_SPLITS", 2))
MAX_SUBGOAL_SPLITS = int(os.environ.get("DD_MAX_SUBGOAL_SPLITS", 6))

# --- Acceptance rule (spec §6) ---
MIN_SCORE = float(os.environ.get("DD_MIN_SCORE", 0.6))
N_CONFIRMATIONS = int(os.environ.get("DD_N_CONFIRMATIONS", 2))
ACCEPT_TIMEOUT = float(os.environ.get("DD_ACCEPT_TIMEOUT", 30.0))  # seconds

# --- Decomposer-side reproposal (not part of the spec; see README) ---
# If a goal never converges on an accepted split (too-low scores, or too few
# scorers running), the decomposer that proposed it republishes a fresh Goal
# (new goal_id, same text) rather than leaving it stuck forever. Bounded so
# a persistently-unscoreable goal fails loudly instead of looping forever.
MAX_REPROPOSAL_ATTEMPTS = int(os.environ.get("DD_MAX_REPROPOSAL_ATTEMPTS", 2))
# Extra wait past ACCEPT_TIMEOUT before giving up on convergence, so the
# decomposer doesn't retry right as a scorer's own timeout-fallback accept
# (which starts its clock slightly later) is about to land.
REPROPOSAL_GRACE = float(os.environ.get("DD_REPROPOSAL_GRACE", 5.0))  # seconds

# --- Claim protocol (spec §6, §6a) ---
CLAIM_TIMESTAMP_TOLERANCE = float(os.environ.get("DD_CLAIM_TOLERANCE", 5.0))  # seconds
CLAIM_TIMEOUT = float(os.environ.get("DD_CLAIM_TIMEOUT", 60.0))  # seconds, local-laptop default
CLAIM_SETTLE_WINDOW = float(os.environ.get("DD_CLAIM_SETTLE_WINDOW", 2.0))  # time to wait for competing claims

# --- Pubsub subscribe/unsubscribe resilience (not spec; py-libp2p workaround) ---
# pubsub.subscribe()/unsubscribe() broadcast to every connected peer, and abort
# the whole call if writing to any ONE peer whose connection died raises
# StreamReset -- py-libp2p's own broadcast loop only catches the sibling
# StreamClosed. See p2p/pubsub.py's subscribe_resilient/unsubscribe_resilient.
MAX_PUBSUB_STREAM_RETRIES = int(os.environ.get("DD_MAX_PUBSUB_STREAM_RETRIES", 5))
PUBSUB_STREAM_RETRY_BASE_DELAY = float(
    os.environ.get("DD_PUBSUB_STREAM_RETRY_BASE_DELAY", 0.2)
)  # seconds, doubles each retry

# --- LLM retry policy (spec §6b) ---
MAX_LLM_RETRIES = int(os.environ.get("DD_MAX_LLM_RETRIES", 3))
LLM_RETRY_BASE_DELAY = float(os.environ.get("DD_LLM_RETRY_BASE_DELAY", 1.0))  # seconds, doubles each retry

# --- LLM model ---
ANTHROPIC_MODEL = os.environ.get("DD_ANTHROPIC_MODEL", "claude-sonnet-4-6")

# --- Identity persistence (spec §6, §7) ---
IDENTITY_DIR = os.environ.get("DD_IDENTITY_DIR", ".identity")

# --- Goal submission mesh wait (spec §5a/§7) ---
# GossipSub never replays missed messages to a late subscriber, so
# --submit-goal must wait for at least one other NEW_GOAL_TOPIC subscriber
# (scorer/observer/another decomposer) to actually mesh-join before
# publishing -- see main.py's `_submit_goal_after_delay`. The timeout is a
# fallback for solo runs (tests, a lone decomposer) so it doesn't wait forever.
MIN_GOAL_MESH_PEERS = int(os.environ.get("DD_MIN_GOAL_MESH_PEERS", 1))
GOAL_MESH_WAIT_TIMEOUT = float(os.environ.get("DD_GOAL_MESH_WAIT_TIMEOUT", 20.0))  # seconds
GOAL_MESH_POLL_INTERVAL = float(os.environ.get("DD_GOAL_MESH_POLL_INTERVAL", 0.25))  # seconds


class Capability(StrEnum):
    """Executor capability taxonomy (spec §3.3, resolved open question §10).

    Kept intentionally small for v1 — extend only when an executor actually
    needs to advertise a capability not covered here.
    """

    CAN_RUN_CODE = "can_run_code"
    CAN_QUERY_API = "can_query_api"
    CAN_WRITE_TEXT = "can_write_text"
