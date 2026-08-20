import queue

import pytest

from decentralized_decomposer.dashboard_server import EventBus


def test_publish_stamps_monotonic_seq_and_records_history():
    bus = EventBus()
    bus.publish({"kind": "goal", "goal_id": "g1"})
    bus.publish({"kind": "node", "node_id": "n1"})

    _, _, backlog = bus.subscribe()
    assert [e["kind"] for e in backlog] == ["goal", "node"]
    assert backlog[0]["seq"] < backlog[1]["seq"]


def test_history_is_capped_at_configured_size():
    bus = EventBus(history_size=3)
    for i in range(5):
        bus.publish({"kind": "score", "i": i})

    _, _, backlog = bus.subscribe()
    assert [e["i"] for e in backlog] == [2, 3, 4]


def test_subscriber_receives_events_published_after_it_subscribed():
    bus = EventBus()
    bus.publish({"kind": "before"})
    sub_id, q, backlog = bus.subscribe()
    bus.publish({"kind": "after"})

    assert [e["kind"] for e in backlog] == ["before"]
    delivered = q.get(timeout=1)
    assert delivered["kind"] == "after"


def test_unsubscribe_stops_further_delivery():
    bus = EventBus()
    sub_id, q, _ = bus.subscribe()
    bus.unsubscribe(sub_id)

    bus.publish({"kind": "after-unsub"})

    with pytest.raises(queue.Empty):
        q.get_nowait()


def test_multiple_subscribers_each_get_every_event():
    bus = EventBus()
    _, q1, _ = bus.subscribe()
    _, q2, _ = bus.subscribe()

    bus.publish({"kind": "broadcast"})

    assert q1.get_nowait()["kind"] == "broadcast"
    assert q2.get_nowait()["kind"] == "broadcast"
