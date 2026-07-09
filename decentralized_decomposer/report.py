"""Human-readable formatting of a `PlanState.reconstruct` snapshot.

`PlanState.reconstruct` returns nested dicts meant for machine consumption
(the observer used to just `json.dumps` them). This renders the same data
as an indented dash list -- box-drawing tree connectors (`├── └── │`) read
as garbage in non-UTF-8-aware terminals/editors, so indentation alone
carries the nesting -- resolving goal_id -> goal text via the `PlanState`
(`reconstruct`'s dicts don't carry a goal's own text, only its id).
"""

from __future__ import annotations

import string

from decentralized_decomposer.p2p.crdt_state import PlanState

RESULT_TRUNCATE = 200

_PRINTABLE = set(string.printable)


def _sanitize(text: str) -> str:
    """Collapse to plain-ASCII printable characters.

    LLM-authored text (goal/subgoal wording, results) routinely contains
    emoji, smart quotes, and other unicode that renders as mojibake in
    non-UTF-8-aware terminals/editors on Windows -- the same class of
    issue `main.py` works around for logging. The report file needs to
    stay readable regardless of what opens it, so strip anything outside
    `string.printable` rather than assume a viewer's encoding.
    """
    cleaned = "".join(ch if ch in _PRINTABLE else " " for ch in text)
    return " ".join(cleaned.split())


def _label_for(plan_state: PlanState, node: dict) -> str:
    if "goal_id" in node:
        goal = plan_state.goals.get(node["goal_id"])
        text = goal.text if goal is not None else node["goal_id"]
    else:
        text = node.get("text", node.get("node_id", "?"))
    return _sanitize(text)


def _format_node(plan_state: PlanState, node: dict, depth: int, lines: list[str]) -> None:
    indent = "  " * depth
    bullet = "" if depth == 0 else "* "
    status = node.get("status", "?")
    lines.append(f"{indent}{bullet}{_label_for(plan_state, node)} [{status}]")

    result = node.get("result")
    if result:
        text = _sanitize(result)
        if len(text) > RESULT_TRUNCATE:
            text = text[:RESULT_TRUNCATE] + "..."
        lines.append(f"{indent}  * Result: {text}")

    for child in node.get("subgoals") or []:
        _format_node(plan_state, child, depth + 1, lines)


def format_plan_tree(plan_state: PlanState, root_id: str) -> str:
    """Render the decomposition subtree rooted at `root_id` as an indented dash list."""
    tree = plan_state.reconstruct(root_id)
    lines: list[str] = []
    _format_node(plan_state, tree, depth=0, lines=lines)
    return "\n".join(lines)


def format_report(plan_state: PlanState, root_ids: list[str]) -> str:
    """Render every root goal's tree, in a stable order, as one combined report."""
    sections = [
        f"=== plan state for goal {root_id} ===\n{format_plan_tree(plan_state, root_id)}"
        for root_id in sorted(root_ids)
    ]
    return "\n\n".join(sections)
