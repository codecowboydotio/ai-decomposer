import pytest

from decentralized_decomposer.agents.decomposer_agent import (
    SubgoalDraft,
    build_decomposer_system_prompt,
    exact_split_count_for,
    validate_split_count,
)


def test_root_uses_exact_count_when_configured(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.ROOT_SPLIT_COUNT", 4)
    assert exact_split_count_for(depth=0) == 4


def test_root_falls_back_to_range_when_not_configured(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.ROOT_SPLIT_COUNT", None)
    assert exact_split_count_for(depth=0) is None


def test_non_root_never_uses_exact_count(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.ROOT_SPLIT_COUNT", 4)
    assert exact_split_count_for(depth=1) is None
    assert exact_split_count_for(depth=2) is None


def test_prompt_mentions_exact_count_for_root(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.ROOT_SPLIT_COUNT", 4)
    prompt = build_decomposer_system_prompt(depth=0)
    assert "exactly 4 subgoals" in prompt


def test_prompt_mentions_range_when_no_exact_count(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.ROOT_SPLIT_COUNT", None)
    monkeypatch.setattr("decentralized_decomposer.config.MIN_SUBGOAL_SPLITS", 2)
    monkeypatch.setattr("decentralized_decomposer.config.MAX_SUBGOAL_SPLITS", 6)
    prompt = build_decomposer_system_prompt(depth=0)
    assert "2-6 subgoals" in prompt


def test_validate_split_count_accepts_exact_match(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.ROOT_SPLIT_COUNT", 3)
    validate_split_count(0, SubgoalDraft(subgoals=["a", "b", "c"], rationale="r"))


def test_validate_split_count_rejects_wrong_exact_count(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.ROOT_SPLIT_COUNT", 3)
    with pytest.raises(ValueError, match="expected exactly 3"):
        validate_split_count(0, SubgoalDraft(subgoals=["a", "b"], rationale="r"))


def test_validate_split_count_rejects_out_of_range_when_no_exact_count(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.ROOT_SPLIT_COUNT", None)
    monkeypatch.setattr("decentralized_decomposer.config.MIN_SUBGOAL_SPLITS", 2)
    monkeypatch.setattr("decentralized_decomposer.config.MAX_SUBGOAL_SPLITS", 6)
    with pytest.raises(ValueError, match="between 2 and 6"):
        validate_split_count(1, SubgoalDraft(subgoals=["a"], rationale="r"))


def test_validate_split_count_ignores_root_exact_count_at_deeper_depth(monkeypatch):
    monkeypatch.setattr("decentralized_decomposer.config.ROOT_SPLIT_COUNT", 4)
    monkeypatch.setattr("decentralized_decomposer.config.MIN_SUBGOAL_SPLITS", 2)
    monkeypatch.setattr("decentralized_decomposer.config.MAX_SUBGOAL_SPLITS", 6)
    # depth=1 subgoal split into 2 -- within range, should not raise even though
    # ROOT_SPLIT_COUNT=4 is set (that only pins the depth-0 goal).
    validate_split_count(1, SubgoalDraft(subgoals=["a", "b"], rationale="r"))
