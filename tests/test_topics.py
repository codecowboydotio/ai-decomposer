import pytest

from decentralized_decomposer.protocol.topics import (
    accepted_topic,
    claim_topic,
    node_failed_topic,
    node_prefix,
    parse_topic,
    propose_failed_topic,
    propose_topic,
    result_topic,
    root_prefix,
    score_failed_topic,
    score_topic,
    sub_prefix,
)


def test_root_prefix_round_trips():
    prefix = root_prefix("abc123")
    parsed = parse_topic(prefix)
    assert parsed.root_goal_id == "abc123"
    assert parsed.path == ()
    assert parsed.suffix is None
    assert parsed.to_topic() == prefix


@pytest.mark.parametrize(
    "builder",
    [propose_topic, score_topic, accepted_topic, propose_failed_topic, score_failed_topic, claim_topic, result_topic],
)
def test_suffix_topics_round_trip(builder):
    prefix = root_prefix("goal-xyz")
    topic = builder(prefix)
    parsed = parse_topic(topic)
    assert parsed.to_topic() == topic
    assert parsed.root_goal_id == "goal-xyz"


def test_nested_sub_prefix_round_trips():
    prefix = sub_prefix(sub_prefix(root_prefix("g1"), 0), 2)
    topic = propose_topic(prefix)
    parsed = parse_topic(topic)
    assert parsed.path == (0, 2)
    assert parsed.suffix == "propose"
    assert parsed.to_topic() == topic


def test_parse_topic_rejects_non_goal_topic():
    with pytest.raises(ValueError):
        parse_topic("capabilities/announce")


def test_parse_topic_rejects_unrecognized_suffix():
    with pytest.raises(ValueError):
        parse_topic("goal/g1/bogus")


def test_node_prefix_topics_are_not_parsed_by_parse_topic():
    prefix = node_prefix("goal-1", "node-1")
    topic = claim_topic(prefix)
    assert topic == "goal/goal-1/node/node-1/claim"
    with pytest.raises(ValueError):
        parse_topic(topic)


def test_node_failed_topic_shape():
    prefix = node_prefix("goal-1", "node-1")
    assert node_failed_topic(prefix) == "goal/goal-1/node/node-1/failed"
