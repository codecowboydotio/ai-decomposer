"""Thread-safe event fan-out + local HTTP/SSE server for the live dashboard UI.

Not part of the gossip protocol (spec §4) -- this is a debugging/visualization
side-channel. `DashboardAgent` (agents/dashboard_agent.py) runs inside the
process's single trio event loop and observes every topic; `stdlib`'s
`http.server` has no trio integration, so the HTTP/SSE server instead runs in
a plain OS thread. `EventBus` is the thread-safe hand-off between the two:
`publish()` is called from the trio thread, `subscribe`/`unsubscribe`/queue
draining happen on HTTP handler threads. A `queue.Queue` per subscriber plus
a `threading.Lock` around the subscriber dict is the only shared state
needed -- no async/await crosses the thread boundary.
"""

from __future__ import annotations

import itertools
import json
import logging
import queue
import threading
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_INDEX_HTML_PATH = Path(__file__).parent / "dashboard" / "index.html"

# Bound on live SSE subscriber queues: if a browser tab stalls badly enough to
# fill this, drop new events for it rather than block the trio publish path.
_SUBSCRIBER_QUEUE_MAXSIZE = 2000

# How long an /events handler thread blocks on its queue before writing a
# comment-only keepalive chunk, so idle proxies/browsers don't time out the
# connection and so a dead client is noticed on the next write.
_SSE_KEEPALIVE_SECONDS = 15.0


class EventBus:
    """Thread-safe pub/sub of small JSON-serializable event dicts, with replay."""

    def __init__(self, history_size: int = 1000) -> None:
        self._lock = threading.Lock()
        self._history: deque[dict[str, Any]] = deque(maxlen=history_size)
        self._subscribers: dict[int, "queue.Queue[dict[str, Any]]"] = {}
        self._next_sub_id = itertools.count()
        self._next_seq = itertools.count(1)

    def publish(self, event: dict[str, Any]) -> None:
        """Stamp `event` with a monotonic `seq` + seen order, record it, and fan it out."""
        stamped = {**event, "seq": next(self._next_seq)}
        with self._lock:
            self._history.append(stamped)
            subscribers = list(self._subscribers.values())
        for q in subscribers:
            try:
                q.put_nowait(stamped)
            except queue.Full:
                logger.warning("Dashboard subscriber queue full, dropping event")

    def subscribe(self) -> tuple[int, "queue.Queue[dict[str, Any]]", list[dict[str, Any]]]:
        """Register a new subscriber; returns (id, its queue, a snapshot of past events)."""
        q: "queue.Queue[dict[str, Any]]" = queue.Queue(maxsize=_SUBSCRIBER_QUEUE_MAXSIZE)
        with self._lock:
            sub_id = next(self._next_sub_id)
            self._subscribers[sub_id] = q
            backlog = list(self._history)
        return sub_id, q, backlog

    def unsubscribe(self, sub_id: int) -> None:
        with self._lock:
            self._subscribers.pop(sub_id, None)


class _DashboardRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "DDDashboard/1.0"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 (stdlib signature)
        logger.debug("%s - %s", self.address_string(), format % args)

    def do_GET(self) -> None:
        if self.path in ("/", "/index.html"):
            self._serve_index()
        elif self.path == "/events":
            self._serve_events()
        else:
            self.send_error(404)

    def _serve_index(self) -> None:
        body = self.server.index_html  # type: ignore[attr-defined]
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_events(self) -> None:
        """Server-Sent Events stream: replay history, then forward live events."""
        bus: EventBus = self.server.event_bus  # type: ignore[attr-defined]
        sub_id, q, backlog = bus.subscribe()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()

            for event in backlog:
                self._write_event(event)
            # Lets the client tell "replayed history" apart from "just happened"
            # (e.g. to skip flow-animation pulses for the initial backlog dump).
            self._write_event({"kind": "_replay_complete", "seq": -1})
            while True:
                try:
                    event = q.get(timeout=_SSE_KEEPALIVE_SECONDS)
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    continue
                self._write_event(event)
        except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
            pass
        finally:
            bus.unsubscribe(sub_id)

    def _write_event(self, event: dict[str, Any]) -> None:
        chunk = f"data: {json.dumps(event)}\n\n".encode("utf-8")
        self.wfile.write(chunk)
        self.wfile.flush()


class DashboardHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], event_bus: EventBus, index_html: bytes):
        super().__init__(server_address, _DashboardRequestHandler)
        self.event_bus = event_bus
        self.index_html = index_html


def start_dashboard_server(
    event_bus: EventBus, host: str = "127.0.0.1", port: int = 8765
) -> tuple[threading.Thread, DashboardHTTPServer]:
    """Start the dashboard's HTTP/SSE server in a background daemon thread."""
    index_html = _INDEX_HTML_PATH.read_bytes()
    httpd = DashboardHTTPServer((host, port), event_bus, index_html)
    thread = threading.Thread(target=httpd.serve_forever, name="dashboard-http", daemon=True)
    thread.start()
    logger.info("Dashboard UI listening at http://%s:%d", host, port)
    return thread, httpd
