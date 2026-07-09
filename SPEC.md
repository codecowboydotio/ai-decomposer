# Decentralized Goal Decomposer — Technical Spec

## 1. Overview

A peer-to-peer system that takes a high-level goal and decomposes it into
actionable, executable prompts — without any central supervisor. Agents
discover each other over libp2p, propose competing decompositions, score
them, and execute the resulting atomic prompts collaboratively.

This is a decentralized counterpart to a centralized generate → score →
refine pipeline: instead of one process owning the full plan, the plan
emerges from gossip-based negotiation among independent agent processes.

**Core principle:** no single peer ever holds the "whole plan." Plan state
is reconstructed locally by each peer from replicated CRDT deltas.

---

## 2. Goals / Non-goals

**Goals**
- Goal → recursively decomposed subgoals → atomic, executable prompts
- Fully decentralized: no bootstrap-dependent coordinator, no single point
  of failure for decomposition or execution logic
- Fault-tolerant: peers can join/leave without halting in-progress plans
- Reuses existing scoring/refinement patterns from prior agent work

**Non-goals (for v1)**
- Not solving Byzantine fault tolerance / adversarial peers
- Not targeting massive scale (design for 3–8 peer processes on one
  laptop, not thousands — ceiling is local machine resources, not
  network design)
- Not shipping production NAT traversal/relay hardening in v1 — a
  single local laptop is the initial deployment target (see §7)
- Not replacing existing centralized pipeline — this is a parallel
  architecture, not a rewrite

---

## 2a. Tradeoffs vs. Centralized Architecture

Relative to a single-process generate → score → refine pipeline:

**Gains**
- Fault tolerance: no single point of failure for decomposition,
  scoring, or execution — any peer can drop out without halting the
  system
- Natural horizontal scaling: add more decomposer/scorer/executor peers
  independently as load grows

**Costs**
- Consensus overhead: voting/scoring rounds across peers add latency
  compared to one LLM call in a centralized decomposer
- No guaranteed global optimality: the decomposition is locally
  negotiated among whichever peers happen to be subscribed, not
  centrally optimized
- New failure mode — **split-brain on plan state**: if CRDT deltas lag
  or arrive out of order, different peers can temporarily hold
  divergent views of the plan. Mitigate with a per-topic convergence
  rule (accept after N seconds or M peer confirmations — see §6).

---

## 3. Agent Types

### 3.1 Decomposer Agent
- Subscribes to `goal/*` (and recursively to accepted subgoal topics)
- On receiving a goal broadcast, makes an LLM call to propose a subgoal
  split (structured output via Pydantic schema)
- Publishes candidate split to `goal/<hash>/propose`
- Multiple decomposer agents may propose competing splits for the same
  goal — this is expected and desired (diversity of decompositions)

### 3.2 Scorer Agent
- Subscribes to `goal/<hash>/propose`
- Evaluates competing splits against a rubric (clarity, atomicity,
  coverage of the original goal, no redundant overlap)
- Publishes a score to `goal/<hash>/score`
- Splits are accepted once N scorer confirmations are met, or (failing
  that) at timeout — see §6
- Accepted split published to `goal/<hash>/accepted`

### 3.3 Executor Agent
- Advertises capabilities on join (e.g. `can_run_code`, `can_query_api`,
  `can_write_text`) by announcing once via gossip on a well-known
  `capabilities/announce` topic, which subscribed peers pick up
  directly (no DHT lookup — consistent with the mDNS-only discovery
  model in §6/§7)
- Subscribes to subgoal topics matching its advertised capability
- On seeing an atomic (leaf) prompt-node, attempts to **claim** it via a
  lease protocol (first-seen-timestamp wins; late claims back off)
- Executes the prompt, publishes result to `goal/<hash>/sub/<n>/result`

---

## 3a. LLM Prompt Templates

Each LLM-calling agent role needs a defined prompt shape and output
contract — not just "make an LLM call." Placeholder structure below;
actual wording should go through the same iterative scoring process
used for prior agent/content work before being finalized.

### Decomposer prompt

```
System: You are a task decomposition agent. Given a goal, break it into
2-6 subgoals that are each independently actionable. Do not produce
subgoals that require judgment calls beyond what's stated in the goal.
Return ONLY valid JSON matching this schema: {schema for SubgoalProposal}

User: Goal: "{goal.text}"
Depth: {goal.depth} (max {MAX_DEPTH})
Parent context: {goal.parent_id and its known context, if any}
```

Output must validate against `SubgoalProposal` on receipt (see §6a) —
reject and retry (see §6b) if it doesn't parse.

### Scorer prompt

```
System: You are evaluating a proposed task decomposition against a
fixed rubric. Score 0.0-1.0 on: (a) does the split fully cover the
original goal, (b) are subgoals non-overlapping, (c) is each subgoal
actually atomic/actionable or still too coarse. Return ONLY valid JSON
matching: {schema for ScoreMsg}

User: Original goal: "{goal.text}"
Proposed split: {proposal.subgoals}
Rationale given: {proposal.rationale}
```

### Atomicity classifier prompt (used by scorer, see §6 Atomicity check)

```
System: Classify whether this subgoal is atomic (a single concrete
deliverable requiring no further decomposition) or composite (still
needs breaking down). Return ONLY: {"is_atomic": bool, "reason": str}

User: Subgoal: "{subgoal_text}"
```

**Open item:** exact wording, few-shot examples, and the scoring rubric
weightings are not finalized — iterate on these the same way prior
content was scored/refined to ~9.8/10, rather than treating first draft
as final.

---

## 4. Message Protocol (Pydantic Schemas)

```python
from pydantic import BaseModel
from typing import Literal
from datetime import datetime

class Goal(BaseModel):
    goal_id: str          # hash of goal text + timestamp
    text: str
    parent_id: str | None = None   # None if top-level goal
    depth: int = 0
    origin_peer: str      # PeerId of originator

class SubgoalProposal(BaseModel):
    goal_id: str
    proposer_peer: str
    subgoals: list[str]
    rationale: str
    proposal_id: str      # hash of subgoals + proposer

class ScoreMsg(BaseModel):
    goal_id: str
    proposal_id: str
    scorer_peer: str
    score: float           # 0.0–1.0
    notes: str | None = None

class AcceptedSplit(BaseModel):
    goal_id: str
    proposal_id: str
    subgoals: list[str]
    final_score: float

class PromptNode(BaseModel):
    node_id: str
    goal_id: str
    text: str
    is_atomic: bool
    depends_on: list[str] = []   # node_ids this depends on

class ClaimMsg(BaseModel):
    node_id: str
    claimer_peer: str
    claimed_at: datetime
    capability: str

class ResultMsg(BaseModel):
    node_id: str
    executor_peer: str
    output: str
    success: bool
```

### Topic naming convention

```
goal/<goal_id>/propose
goal/<goal_id>/score
goal/<goal_id>/accepted
goal/<goal_id>/sub/<subgoal_index>/...      # recursive, same topic shape
goal/<goal_id>/sub/<subgoal_index>/claim
goal/<goal_id>/sub/<subgoal_index>/result
```

---

## 5. Module Layout

```
decentralized_decomposer/
├── p2p/
│   ├── host.py            # libp2p host setup, identity, transport (TCP/QUIC)
│   ├── discovery.py       # mDNS peer discovery (primary); Kademlia DHT deferred until beyond single-laptop scope
│   ├── pubsub.py          # GossipSub topic subscribe/publish helpers
│   └── crdt_state.py      # replicated plan-state (G-Set or automerge)
├── agents/
│   ├── decomposer_agent.py
│   ├── scorer_agent.py
│   ├── executor_agent.py
│   └── capability.py      # capability advertisement + matching
├── protocol/
│   ├── messages.py        # Pydantic schemas (§4)
│   └── topics.py          # topic naming helpers
├── models.py               # Goal, SubgoalProposal, PromptNode, etc. (re-export)
├── config.py                # depth limit, score threshold, timeouts
└── main.py                  # CLI entrypoint: role, port (mDNS discovery, no bootstrap peer needed — see §7)
```

---

## 5a. Runtime / Process Model

Each agent is a standalone OS process — there is no shared runtime.
Every peer runs its own `asyncio` event loop with a libp2p host embedded
in it. Agents are started independently, e.g.:

```bash
python -m agents.decomposer_agent --port 4001
python -m agents.scorer_agent --port 4002
python -m agents.executor_agent --port 4003
```

Per-agent process lifecycle:

```
main.py
 ├── build libp2p host (identity keypair, transports: TCP/QUIC)
 ├── start mDNS discovery (no bootstrap peer needed on a single laptop)
 ├── subscribe to GossipSub topics (goal/*, capability-matched subtopics)
 ├── run agent loop (asyncio task):
 │     - on message → decompose/score/execute
 │     - publish result back to topic
 └── keep event loop alive until SIGINT
```

LLM calls happen inside each agent's own process. They block that
agent's own work but not the network — `asyncio` makes the API call
awaitable so the host keeps handling gossip/heartbeats concurrently.

---

## 6. Key Design Decisions

- **Atomicity check:** a prompt-node is atomic if it names a concrete
  deliverable and requires no further judgment calls. Implement as a
  dedicated scorer classification pass, not a heuristic string check.
- **Depth limit:** cap recursion at `MAX_DEPTH = 3` (configurable) to
  prevent runaway decomposition.
- **Recursion mechanism:** each subgoal in an `AcceptedSplit` that is
  not yet atomic is re-published as a new `Goal` message (with
  `parent_id` set and `depth` incremented) onto its own
  `goal/<goal_id>/sub/<n>` topic. This re-triggers the same
  decomposer → scorer → accept cycle on that subgoal, recursively,
  until nodes are atomic or `MAX_DEPTH` is hit.
- **Acceptance rule:** a proposal is accepted once it receives `N`
  scorer confirmations above `MIN_SCORE` within `ACCEPT_TIMEOUT` seconds
  — no single scorer can accept a proposal on its own, since
  independently-running scorers must actually agree. Falls back to the
  highest mean-scored proposal (among those with at least `N`
  confirmations) at timeout if nothing reaches `N` confirmations.
- **Claim protocol:** first published `ClaimMsg` (by timestamp) for a
  given `node_id` wins; other executors that see a competing claim with
  an earlier timestamp back off. No central lock — accept the small risk
  of rare duplicate execution over adding a coordinator.
- **Plan state:** CRDT-replicated (grow-only set of accepted proposals +
  results) so any peer can reconstruct current plan state without
  querying a central store. Start with a simple custom G-Set; move to
  `automerge` only if merge complexity grows.
- **Persistent identity:** each agent's libp2p keypair must persist
  across restarts (store in a local file/volume) — a new PeerId on every
  restart breaks peer discovery continuity.
- **Peer discovery:** mDNS only for the local-laptop deployment target
  (see §7) — no Kademlia DHT bootstrap node required. Capability
  records (§3.3) are announced via gossip rather than DHT lookup for
  this same reason; DHT-based capability lookup can be revisited if the
  system later moves beyond a single machine.
- **Transport:** use TCP or QUIC. Avoid WebSocket transport in this
  system for now — it remains explicitly marked experimental upstream
  (proxy support and production examples still in development).

---

## 6a. Message Validation & Trust Model

Scope note: for the local-laptop prototype, all peers are trusted (no
adversarial actors — see §2 Non-goals). This section covers basic
correctness validation, not adversarial defense.

- **Schema validation on receipt:** every incoming pubsub message is
  parsed against its expected Pydantic schema before being acted on.
  Malformed messages are logged and dropped, not processed — an agent
  should never crash on a bad message from a peer.
- **Claim timestamp sanity check:** since the claim protocol (§6) relies
  on `claimed_at` timestamps to resolve races, an executor validates
  that an incoming `ClaimMsg.claimed_at` is within a small tolerance
  window (e.g. ±5s) of local receipt time before treating it as
  authoritative. Wildly skewed timestamps are treated as a validation
  failure and the message is dropped.
- **Node ID / goal ID integrity:** `node_id`, `goal_id`, and
  `proposal_id` are content-derived hashes (see §4 comments), so a
  receiving agent can independently recompute and check them rather
  than trusting the sender's label. Mismatches are dropped.
- **Not in scope for v1:** peer authentication/signing, rate limiting
  against a misbehaving peer, or defending against a peer publishing
  intentionally false results. Revisit if this system moves beyond a
  trusted, single-operator, single-laptop context.

---

## 6b. Error Handling & Retry Policy

- **LLM call failures (timeout, API error):** retry up to
  `MAX_LLM_RETRIES` (default 3) with exponential backoff. If all
  retries fail, the agent publishes nothing for that goal/subgoal —
  it does not publish a partial or guessed result.
- **Unparseable LLM output:** if a response fails schema validation
  (see §6a), treat it the same as a call failure — retry with the same
  backoff policy, feeding the parse error back into the prompt on retry
  (e.g. "your last response didn't match the required JSON schema:
  {error}. Try again.").
- **Exhausted retries — failure signaling:** after `MAX_LLM_RETRIES` is
  exhausted, the agent publishes a lightweight failure notice to the
  relevant topic (e.g. `goal/<hash>/propose/failed` for a decomposer)
  rather than staying silent. This lets other peers (or a human
  watching logs) distinguish "no one has answered yet" from "an agent
  tried and gave up" — silent failure is harder to debug in a
  gossip-based system where there's no central log of who was supposed
  to respond.
- **Stuck/unclaimed atomic nodes:** if an atomic `PromptNode` sits
  unclaimed for longer than `CLAIM_TIMEOUT` (default 60s on a local
  laptop — no network latency to account for), it's re-announced once.
  If it remains unclaimed after a second timeout, treat it as a
  capability gap (no executor advertises the needed capability) and
  surface it rather than looping indefinitely.
- **Process crash recovery:** since each agent is a standalone OS
  process (§5a), a crashed agent simply drops off gossip topics. No
  special recovery logic is required for v1 — restarting the process
  (with its persisted keypair, §6) is sufficient to rejoin. In-flight
  claims held by a crashed executor will eventually be treated as stale
  once `CLAIM_TIMEOUT` passes and re-announced.

---

## 7. Deployment Model (local laptop)

Target environment: single laptop, all peers on the same machine.
No bootstrap node needed — mDNS handles peer discovery on localhost/LAN
automatically. Three ways to run it, in order of setup effort:

| Option | How | Best for |
|---|---|---|
| **1. Separate terminal processes** | Run each agent directly: `python -m agents.decomposer_agent --port 4001`, etc., one per terminal tab | Fastest to start, easiest to read logs and debug a single agent live |
| **2. Process manager script** | A small script (`honcho`, `overmind`, or a plain `Procfile` + shell loop) launches all agents as background processes from one command, tags log output by agent name | Convenient once the agent set stabilizes; still easy to kill/restart individually |
| **3. Docker Compose** | One container per agent type on a shared bridge network, `docker-compose up` | Closest to eventual multi-machine deployment; isolates dependencies; adds Docker overhead for local iteration |

**Recommended starting point:** Option 1 while building/debugging
individual agents, moving to Option 2 or 3 once the full loop
(decompose → score → execute) is working end-to-end and you want to
run all agents together repeatably.

What's required regardless of option:
- Persistent peer identity (keypair) per agent, stored to a local file
  — if regenerated on every restart, the agent gets a new PeerId and
  loses its place in others' peerstores
- mDNS discovery only (no DHT bootstrap needed on a single machine)
- TCP or QUIC transport (see §6 WebSocket caveat)
- Distinct ports per agent process on `localhost`

NAT traversal, relay support, and multi-machine bootstrap nodes are out
of scope for this deployment target — revisit if/when moving beyond a
single laptop.

---

## 8. Phased Build Plan

| Phase | Scope | Est. time |
|---|---|---|
| 1. Core P2P plumbing | host setup, identity persistence, mDNS discovery, GossipSub wiring | 2–3 days |
| 2. Message protocol | Pydantic schemas, serialization, topic conventions | 1 day |
| 3. Decomposer agent | LLM call → structured split → publish | 1 day |
| 4. Scorer agent | scoring logic, vote aggregation, acceptance rule | 1 day |
| 5. Executor agent | capability matching, claim/lease protocol, execution | 1–2 days |
| 6. CRDT plan state | replicated DAG, merge logic, local reconstruction | 2–3 days |
| 7. Recursion + depth control | re-broadcast subgoals, depth tracking, atomicity checks | 1 day |
| 8. Testing (Layers 1–4, §9) | unit + agent-logic tests written alongside phases 1-7; integration tests (race conditions, duplicate claims, split-brain merges, crash recovery) | 3–5 days dedicated, plus ongoing unit/agent-logic tests during phases 1-7 |

**Total: ~2–3 weeks solo** for a working local-laptop prototype
(Deployment Option 1 or 2 from §7).

**Range depending on scope:**
- **Faster (~1–1.5 weeks):** skip the CRDT and use a simpler
  eventual-consistency hack (periodic full-state gossip instead of
  proper merge semantics) — fine for a prototype, not for real fault
  tolerance.
- **Slower (~+3–5 days):** add Docker Compose packaging (Option 3) once
  the core loop works, for easier repeatable local runs.

Phase 8 (distributed testing/debugging) is the phase most commonly
underestimated — claim races and gossip timing issues don't surface
until running 4+ real peers, and they are genuinely fiddly to
reproduce even on a single machine. Budget real time here rather than
treating it as routine wiring, especially if this is a first
CRDT/gossip-consensus implementation.

**Note on estimate calibration:** Phase 6 and Phase 8 estimates above
were originally calibrated assuming real network conditions (multiple
physical hosts, genuine latency/partition behavior). Running all peers
as loopback processes on one laptop removes real network unreliability
from the picture, so split-brain and gossip-timing bugs may surface
less often — these two phases could run faster in practice than
estimated. Treat the numbers above as an upper bound for this
deployment target, not a recalibrated one.

---

## 9. Testing Strategy

### 9.1 Tooling

- **Test runner:** `pytest` with `pytest-asyncio` (all agent/network code
  is async)
- **Mocking:** `unittest.mock` / `pytest-mock` for LLM calls; never hit
  the real Anthropic API in unit or agent-logic tests — only in a
  clearly separate, manually-run "live" test tier (see 9.5)
- **Test libp2p hosts:** use ephemeral in-process hosts on random free
  ports (`port=0`) for integration tests, torn down in fixture teardown
  — avoids port collisions between test runs and leftover processes
- **Coverage:** `pytest-cov`, tracked per-module; no fixed target
  enforced initially, but `p2p/crdt_state.py` and the claim protocol
  logic should be the highest-coverage modules given their bug history
  risk (§2a split-brain, §6 claim races)

### 9.2 Layer 1 — Unit tests (no network, no LLM)

Pure logic, fast, run on every save.

| Module | What's tested |
|---|---|
| `protocol/messages.py` | Schema validation accepts valid payloads, rejects malformed ones (missing fields, wrong types) |
| `protocol/topics.py` | Topic string construction/parsing round-trips correctly for all topic shapes in §4 |
| `p2p/crdt_state.py` | Given known delta sequences (including out-of-order and duplicate deltas), final merged state is correct and order-independent |
| Claim resolution logic | Given two `ClaimMsg`s with known timestamps (including a near-tie within the §6a tolerance window), the correct winner is deterministically selected |
| Atomicity/depth logic | `MAX_DEPTH` cutoff triggers correctly; depth increments correctly on recursion (§6 Recursion mechanism) |

### 9.3 Layer 2 — Agent logic tests (mocked LLM, no real network)

Each agent's decision logic tested in isolation by mocking the LLM
client to return canned responses.

| Scenario | Expected behavior |
|---|---|
| Decomposer gets valid LLM response | Publishes a well-formed `SubgoalProposal` |
| Decomposer gets malformed/unparseable response | Retries per §6b, includes the parse error in the retry prompt |
| Decomposer exhausts retries | Publishes failure notice to `.../propose/failed`, does not publish a guessed result |
| Scorer receives a proposal | Publishes a `ScoreMsg` in range [0.0, 1.0] |
| Scorer + acceptance rule | Given a sequence of incoming scores, correctly determines when N-confirmation/timeout acceptance fires (§6 Acceptance rule) |
| Executor sees atomic node matching its capability | Attempts claim |
| Executor sees atomic node NOT matching its capability | Does not attempt claim |
| Executor's claim is beaten by an earlier timestamp | Backs off, does not execute |

### 9.4 Layer 3 — Integration tests (real local processes, mocked LLM)

Spin up real agent processes via mDNS on `localhost` (§7 Option 1
style), with the LLM call mocked at the network boundary so tests are
fast and deterministic. These are the tests that actually exercise
libp2p/GossipSub.

1. **Single goal, full loop:** one of each agent type, submit one goal,
   assert it flows propose → score → accept → recurse → atomic → claim
   → execute → result end to end.
2. **Convergence under concurrency:** multiple decomposers + scorers,
   one goal, assert all peers converge on the same `AcceptedSplit`
   (validates §2a split-brain mitigation and §6a CRDT merge).
3. **Claim race:** 2+ executors advertising the same capability, one
   atomic node, assert exactly one result is accepted and the other
   backs off cleanly.
4. **Stuck node / capability gap:** submit an atomic node with no
   matching executor present, assert it's re-announced once then
   surfaced as a capability gap per `CLAIM_TIMEOUT` (§6b).
5. **Late-joining peer:** start a goal's propagation, then start a new
   agent process partway through, assert it correctly reconstructs
   current plan state from replicated CRDT deltas rather than missing
   context (this is the core "no peer holds the whole plan" claim from
   §1 — worth testing explicitly, not just assuming it works).
6. **Process crash mid-claim:** kill an executor process after it
   claims a node but before it publishes a result, assert the node is
   eventually re-announced and picked up by another executor once
   `CLAIM_TIMEOUT` passes (§6b crash recovery).

### 9.5 Layer 4 — Live/manual tests (real LLM, real API calls)

Run manually, not in CI, since they cost tokens and are non-deterministic:

- Run Deployment Option 1 (§7) end-to-end with real Anthropic API calls
  for a handful of representative goals; manually review whether
  decompositions are actually sensible (this is a judgment call the
  automated tests can't make — same as the manual scoring iteration
  used for prior content work)
- Deliberately kill agent processes mid-run during a live session to
  sanity-check the crash-recovery behavior from 9.4.6 under real
  (non-mocked) timing

### 9.6 What CI should run

Layers 1–3 on every push (fast, deterministic, no API cost). Layer 4
stays manual/on-demand. If Docker Compose (§7 Option 3) is adopted
later, add a CI job that runs the Layer 3 integration suite inside
Compose as a closer approximation of the real deployment shape.

---

## 10. Open Questions / Decisions Needed Before Coding

- [ ] CRDT library choice: custom G-Set vs. `automerge-py` — decide in
      phase 6, not before (avoid premature dependency lock-in)
- [ ] Scorer diversity: single scorer type, or multiple scorer agents
      with different rubrics competing/averaging?
- [ ] Executor capability taxonomy — needs a concrete list before
      phase 5 (what capabilities actually exist in this system?)
