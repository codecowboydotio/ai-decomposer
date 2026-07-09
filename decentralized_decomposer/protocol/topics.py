"""GossipSub topic naming per spec §4.

A topic *prefix* identifies a position in the decomposition tree:
  - the root goal's prefix is ``goal/<goal_id>``
  - each subgoal's prefix is ``<parent_prefix>/sub/<index>``

Each prefix has associated suffix topics: propose / score / accepted /
propose-failed / score-failed / claim / result. GossipSub has no native
wildcard subscription, so the spec's ``goal/*`` is realized as a single
well-known topic (`NEW_GOAL_TOPIC`) that all top-level goals are announced
on; recursively-spawned subgoals are announced on their own `<prefix>`
topic instead (see `agents/decomposer_agent.py`).
"""

from __future__ import annotations

from dataclasses import dataclass, field

NEW_GOAL_TOPIC = "goal/new"
NEW_NODE_TOPIC = "node/new"
CAPABILITIES_ANNOUNCE_TOPIC = "capabilities/announce"

_SUFFIXES = (
    "propose",
    "score",
    "accepted",
    "propose/failed",
    "score/failed",
    "claim",
    "result",
)


def root_prefix(goal_id: str) -> str:
    return f"goal/{goal_id}"


def sub_prefix(parent_prefix: str, index: int) -> str:
    return f"{parent_prefix}/sub/{index}"


def propose_topic(prefix: str) -> str:
    return f"{prefix}/propose"


def score_topic(prefix: str) -> str:
    return f"{prefix}/score"


def accepted_topic(prefix: str) -> str:
    return f"{prefix}/accepted"


def propose_failed_topic(prefix: str) -> str:
    return f"{prefix}/propose/failed"


def score_failed_topic(prefix: str) -> str:
    return f"{prefix}/score/failed"


def node_prefix(goal_id: str, node_id: str) -> str:
    """Prefix for an atomic leaf node's claim/result channel.

    Addressed by (goal_id, node_id) directly rather than by ancestor-chain
    position (`.../sub/<n>/...`) -- executors learn of new leaves via
    `NEW_NODE_TOPIC`, which carries the `PromptNode` itself, so they always
    have both ids on hand to recompute this prefix without needing to track
    each leaf's index within its parent's accepted split.

    Note: topics built on `node_prefix` are a separate, flatter addressing
    scheme from the `.../sub/<n>/...` shape `parse_topic` below handles --
    they are not accepted by `parse_topic`.
    """
    return f"goal/{goal_id}/node/{node_id}"


def node_failed_topic(prefix: str) -> str:
    """Capability-gap / unresolved-node failure notice for a `node_prefix` (spec §6b)."""
    return f"{prefix}/failed"


def claim_topic(prefix: str) -> str:
    return f"{prefix}/claim"


def result_topic(prefix: str) -> str:
    return f"{prefix}/result"


@dataclass(frozen=True)
class ParsedTopic:
    root_goal_id: str
    path: tuple[int, ...] = field(default_factory=tuple)
    suffix: str | None = None  # one of _SUFFIXES, or None for a bare prefix

    @property
    def prefix(self) -> str:
        p = root_prefix(self.root_goal_id)
        for index in self.path:
            p = sub_prefix(p, index)
        return p

    def to_topic(self) -> str:
        return f"{self.prefix}/{self.suffix}" if self.suffix else self.prefix


def parse_topic(topic: str) -> ParsedTopic:
    """Parse any topic produced by the `*_topic`/`*_prefix` helpers above.

    Raises ValueError if the topic doesn't match the ``goal/...`` shape.
    """
    parts = topic.split("/")
    if len(parts) < 2 or parts[0] != "goal":
        raise ValueError(f"not a goal topic: {topic!r}")

    root_goal_id = parts[1]
    rest = parts[2:]

    path: list[int] = []
    i = 0
    while i + 1 < len(rest) and rest[i] == "sub":
        path.append(int(rest[i + 1]))
        i += 2

    remaining = rest[i:]
    suffix = "/".join(remaining) if remaining else None
    if suffix is not None and suffix not in _SUFFIXES:
        raise ValueError(f"unrecognized topic suffix {suffix!r} in {topic!r}")

    return ParsedTopic(root_goal_id=root_goal_id, path=tuple(path), suffix=suffix)
