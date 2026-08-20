# Decentralized Goal Decomposer

Peer-to-peer goal decomposition over libp2p: decomposer, scorer, and
executor agents gossip proposals, votes, and results to turn a high-level
goal into executable atomic prompts, with no central coordinator. See
[`SPEC.md`](SPEC.md) (or the original spec you were given) for the full
design; this README covers what actually got built and how to run it.

## Requirements

- Python 3.11+
- An `ANTHROPIC_API_KEY` environment variable (the agents call the
  Anthropic API for decomposition, scoring, atomicity classification, and
  execution)

## Install

```bash
python -m venv .venv
.venv/Scripts/activate        # or `source .venv/bin/activate` on macOS/Linux
pip install -e ".[dev]"
```

## Run (spec §7, Option 1 -- one terminal per agent)

```bash
export ANTHROPIC_API_KEY=sk-...

# terminal 1
python -m decentralized_decomposer.main --role decomposer --port 4001 \
    --submit-goal "Plan a two-week trip to Japan"

# terminal 2
python -m decentralized_decomposer.main --role scorer --port 4002

# terminal 3
python -m decentralized_decomposer.main --role executor --port 4003 \
    --capabilities can_write_text,can_query_api
```

Peers find each other via mDNS on localhost/LAN automatically -- no
bootstrap node needed. `--submit-goal` waits until at least one other
`goal/new` subscriber (scorer/observer/another decomposer) has actually
mesh-joined GossipSub before publishing the top-level goal, then the
process keeps running as a normal agent of its `--role`. This matters
because GossipSub never replays missed messages to a late subscriber: if
the goal were published before, say, the scorer has finished starting up
and subscribing, it would be silently dropped with no error anywhere and
the whole plan would stall forever. The wait gives up after
`DD_GOAL_MESH_WAIT_TIMEOUT` seconds (default 20) and publishes anyway --
that fallback is what lets a lone decomposer (tests, or genuinely running
solo) start without hanging; you'll see a "Timed out ... publishing goal
anyway" warning in that case. Run more decomposer/scorer/executor
processes on different `--port`s to see competing proposals and claim
races.

Each agent persists its libp2p identity to `.identity/<name>.key` (default
name is `<role>-<port>`; override with `--identity`) so restarting a
process doesn't give it a new PeerId.

## Controlling how many splits happen

Not part of the original spec, added on request. Three flags, all
optional and all defaulting to the original behavior (LLM picks 2-6
subgoals, recursion goes up to depth 3):

- `--num-splits N` -- split the *initial* goal into exactly `N` subgoals
  (decomposer role only). If the LLM returns the wrong count, that's
  treated the same as a malformed response: retried with the count error
  fed back into the prompt (spec §6b's retry/backoff path), up to
  `MAX_LLM_RETRIES`.
- `--max-depth N` -- caps recursion depth (decomposer + scorer roles).
  **`--max-depth 1` is what makes "subgoals are never split further"
  literal**: the initial goal (depth 0) splits once, and every resulting
  subgoal is forced atomic immediately, skipping the atomicity-classifier
  call entirely.
- `--min-splits N` / `--max-splits N` -- bounds on subgoal count for any
  goal that doesn't have an exact count pinned (the initial goal when
  `--num-splits` isn't given, and any deeper goal if `--max-depth` allows
  recursion past depth 1).

Example -- split the initial goal into exactly 4 pieces, none of which
get split further:

```bash
python -m decentralized_decomposer.main --role decomposer --port 4001 \
    --num-splits 4 --max-depth 1 \
    --submit-goal "Plan a two-week trip to Japan"

python -m decentralized_decomposer.main --role scorer --port 4002 --max-depth 1
```

**Important:** each of these is a per-process CLI flag, and each role
runs as its own OS process (spec §5a) -- there's no shared config. Pass
`--num-splits` to the decomposer (it builds the prompt) and `--max-depth`
to *both* the decomposer (skips a wasted LLM call on over-depth goals)
and the scorer (it's the one that actually decides atomic-vs-recurse). If
you run multiple decomposers/scorers, give them all the same flags --
this is a decentralized system, so nothing enforces that they agree.

## Seeing results

There's no central place a "whole plan" lives (spec §1), so there's no
single log to tail either. Two ways to actually see what's happening:

**Run a `--role observer` process.** It needs no LLM/API key and no
capabilities -- it just listens to every topic, reconstructs the plan
tree locally (`PlanState.reconstruct`, spec §6), and prints it to stdout
whenever it changes:

```bash
python -m decentralized_decomposer.main --role observer --port 4004
```

You'll see JSON snapshots like:

```json
=== plan state for goal 1fb7d8c... ===
{
  "goal_id": "1fb7d8c...",
  "status": "decomposed",
  "subgoals": [
    {"node_id": "...", "text": "Book flights", "status": "done", "result": "..."},
    {"goal_id": "...", "status": "pending_decomposition"}
  ]
}
```

`"status"` moves from `pending_decomposition` -> `decomposed` once a
split is accepted; each leaf moves from `pending_execution` -> `done`
(with its `result` text filled in) once an executor finishes it. Nested,
not-yet-decomposed subgoals show up as their own `pending_decomposition`
entries -- run the observer alongside the other roles and watch it fill in.

**Or just read the executor's own log.** Pass `-v` and every executor
process logs the full output text the moment it publishes a result:
`Result for <node_id> [<task text>] (success):` followed by the
generated content. That's the fastest way to see one specific answer;
the observer is better for watching the whole tree converge.

## Design deviations from the spec worth knowing about

- **trio, not asyncio.** The spec (§5a) describes an `asyncio` event loop
  per agent. The only maintained Python libp2p implementation
  ([`py-libp2p`](https://github.com/libp2p/py-libp2p) 0.6, which is what
  provides GossipSub + mDNS here) is built entirely on `trio`. This
  project uses `trio` throughout instead; it fills the same role (one
  structured-concurrency loop per process, LLM calls awaited without
  blocking gossip/heartbeats -- `httpx`, which the Anthropic SDK sits on,
  works the same under trio as under asyncio).
- **mDNS port fix.** `py-libp2p`'s `new_host(enable_mDNS=True)` wires its
  built-in `MDNSDiscovery` with a hardcoded default port (8000) instead of
  the host's actual listen port (see `libp2p/host/basic_host.py`), which
  breaks discovery for any agent not listening on 8000. `p2p/discovery.py`
  builds `MDNSDiscovery` itself with the correct port instead of relying
  on the built-in wiring.
- **No `peerstore.start_cleanup_task`.** py-libp2p's own pubsub demo
  starts this task to garbage-collect stale peer records. Its expiry
  sweep deletes a peer's *entire* record -- pubkey and privkey included
  -- once its TTL lapses, and something in the swarm/mDNS bookkeeping
  ends up giving your own peer id a short-lived TTL too; after ~60s the
  sweep wipes your own keypair out of your own peerstore, and the next
  `publish()` crashes (`PeerStoreError: peer ID not found`) trying to
  sign with a key that's no longer there. `main.py` deliberately doesn't
  start it. You may still see occasional harmless
  `PeerStoreError: peer ID is expired` tracebacks in the logs from
  py-libp2p's mDNS listener (its discovered-peer address TTL is
  hardcoded to 10s) -- those fire on zeroconf's own background thread
  and don't affect the agent.
- **Retrying past `StreamReset` on subscribe/unsubscribe.** `Pubsub.subscribe`
  and `.unsubscribe` broadcast an announcement to every currently-connected
  peer; if any *one* of them has a dead connection, py-libp2p's own
  broadcast loop only catches `StreamClosed` and prunes that peer
  gracefully -- the sibling `StreamReset` (same base class, not a
  subclass) is left uncaught, aborting the whole call and crashing the
  calling agent's process. Every agent goes through
  `p2p/pubsub.py`'s `subscribe_and_validate`, so this fixes it once,
  centrally: retry both calls with backoff (`DD_MAX_PUBSUB_STREAM_RETRIES`,
  default 5; `DD_PUBSUB_STREAM_RETRY_BASE_DELAY`, default 0.2s, doubling),
  which gives py-libp2p's own dead-peer sweep time to prune the stale
  connection before the next attempt. If unsubscribe still can't get its
  announcement out after retrying, that's logged and swallowed rather than
  raised -- local subscription state is already torn down by that point,
  only the "tell peers we left" broadcast was missed.
- **Retrying past the `message_all_peers` dict-mutation race.** The same
  `subscribe`/`unsubscribe` broadcast also does a plain
  `for stream in self.peers.values(): await stream.write(...)` -- if a
  sibling coroutine connects or drops a peer while a write is suspended,
  `self.peers` changes size mid-iteration and Python raises `RuntimeError:
  dictionary changed size during iteration`, uncaught, crashing the whole
  process. This hit hardest on `--role dashboard`, which subscribes to a
  new topic per goal and so churns peers/subscriptions the most. Fixed the
  same way as the `StreamReset` case above: `p2p/pubsub.py` patches
  `Pubsub.message_all_peers` to retry the whole broadcast (up to 5 times)
  when this specific `RuntimeError` fires. A retry may re-message a peer
  the first, partial pass already reached, which is harmless at the
  gossipsub level.
- **Topic addressing is flatter than the literal §4 diagram.** GossipSub
  has no wildcard subscriptions, so a decomposer can't literally subscribe
  to `goal/*`. Every `Goal` -- top-level or a recursively-spawned subgoal
  -- is announced on one well-known `goal/new` topic instead; from there,
  a goal's own propose/score/accepted topics hang off
  `goal/<goal_id>/...` (exactly the §4 shape), keyed by that goal's own
  id rather than nested under its ancestor's `sub/<n>/...` path. Atomic
  leaf nodes are similarly announced on one `node/new` topic, and their
  claim/result channels are addressed by `(goal_id, node_id)` rather than
  by index-in-parent. `protocol/topics.py` documents both schemes.
- **`PromptNode.required_capability` was added.** The spec's §4 schema
  doesn't give executors a way to know which capability a leaf node needs,
  even though §3.3 has them filter by capability. This field is additive
  and excluded from the node's identity hash (it's a routing hint, not
  part of the node's content identity).
- **No sandboxed code execution.** `can_run_code` names an executor's
  *willingness* to take on code-shaped tasks; v1 does not actually run
  model-generated code on the host. Every capability's execution is a
  plain LLM completion returning text output. Building a real sandbox is
  future work, not a v1 scope call the spec makes explicitly.
- **Decomposer-side reproposal on timeout.** Not part of the spec, added
  on request. Without it, a goal whose proposal never reaches
  `N_CONFIRMATIONS` scorers at `MIN_SCORE` -- too strict a `DD_MIN_SCORE`,
  or just too few scorers running -- sits in `pending_decomposition`
  forever with no way out. Now, the decomposer that proposed a split
  watches for it to actually converge; if `DD_ACCEPT_TIMEOUT` (+ a
  `DD_REPROPOSAL_GRACE` buffer, default 5s, so it doesn't race a scorer's
  own timeout-fallback accept) passes with no accepted split, it
  republishes the same goal text as a **fresh goal** (a new content-hashed
  `goal_id`, so this is a clean independent acceptance cycle, not a
  mutation of the stuck one) and tries again -- feeding whatever scorer
  notes came back (or their absence) into the next attempt's prompt, the
  same way `call_structured` already feeds a schema error back on retry.
  Bounded by `DD_MAX_REPROPOSAL_ATTEMPTS` (default 2, so 3 proposals
  total) before giving up with a `propose_failed` notice.
  Because a retried goal shares `(parent_id, text)` with the original
  stuck one, `PlanState.reconstruct` and the dashboard's tree view
  deliberately prefer whichever of the two duplicates actually has an
  accepted split, rather than whichever was seen first.

## Testing

```bash
pytest                              # Layers 1-2: fast, deterministic, no network/API calls
pytest --cov=decentralized_decomposer --cov-report=term-missing
```

- **Layer 1** (`tests/test_messages.py`, `test_topics.py`,
  `test_crdt_state.py`, `test_claims.py`, `test_acceptance.py`,
  `test_depth.py`): pure logic, no network, no LLM.
- **Layer 2** (`tests/test_decomposer_agent.py`, `test_scorer_agent.py`,
  `test_executor_agent.py`): agent decision logic against a fake in-memory
  pubsub (`tests/fakes.py`) and a mocked Anthropic client -- no real
  libp2p host.
- **Layer 3** (real libp2p processes, mocked LLM at the network boundary)
  and **Layer 4** (real API calls, manual) from spec §9.4-9.5 are not
  implemented here. They need real multi-process/multi-port orchestration
  (claim races, split-brain convergence, crash recovery) that's
  meaningfully harder to get right than the agent logic itself --
  worth a dedicated pass rather than folding in at the end of this one.
  `p2p/host.py` and `p2p/discovery.py` are exercised by running the CLI
  directly (see above), not by the automated suite.

## Layout

```
decentralized_decomposer/
├── p2p/            host.py (identity + host), discovery.py (mDNS), pubsub.py (GossipSub),
│                   crdt_state.py (G-Set plan state)
├── agents/         decomposer_agent.py, scorer_agent.py, executor_agent.py,
│                   observer_agent.py (prints reconstructed plan state, spec §1),
│                   acceptance.py + claims.py (pure decision logic), capability.py
├── protocol/       messages.py (Pydantic schemas), topics.py (topic naming)
├── llm.py          shared structured-output call w/ retry+backoff
├── config.py       tunables (MAX_DEPTH, score thresholds, timeouts, capability taxonomy)
└── main.py         CLI entrypoint
```
