"""Executor agent (spec §3.3, §6, §6a, §6b): claim + execute atomic prompt nodes.

Execution safety note: `_run_prompt` answers the atomic prompt with a
plain LLM completion regardless of the node's advertised capability. The
spec's `can_run_code` capability names an intent (this executor is willing
to take on code-shaped tasks), but v1 does not sandbox or actually execute
arbitrary model-generated code on the host machine -- that's a real
blast-radius/security concern the spec doesn't address, so the "code" a
`can_run_code` executor produces is text output (e.g. the code itself),
not something this process runs. Revisit only behind an explicit sandbox.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import trio
from anthropic import AsyncAnthropic
from libp2p.pubsub.pubsub import Pubsub

from decentralized_decomposer.agents.capability import announce_capabilities, can_handle
from decentralized_decomposer.agents.claims import is_claim_timestamp_sane, resolve_claim_winner
from decentralized_decomposer.config import (
    ANTHROPIC_MODEL,
    CLAIM_SETTLE_WINDOW,
    CLAIM_TIMEOUT,
    Capability,
)
from decentralized_decomposer.p2p.pubsub import publish_model, subscribe_and_validate
from decentralized_decomposer.protocol.messages import ClaimMsg, FailureMsg, PromptNode, ResultMsg
from decentralized_decomposer.protocol.topics import (
    NEW_NODE_TOPIC,
    claim_topic,
    node_failed_topic,
    node_prefix,
    result_topic,
)

logger = logging.getLogger(__name__)

EXECUTION_SYSTEM_PROMPT = (
    "You are an execution agent. Complete the following atomic task and return only "
    "the deliverable -- no preamble, no meta-commentary."
)


class ExecutorAgent:
    def __init__(
        self,
        pubsub: Pubsub,
        peer_id: str,
        capabilities: list[Capability],
        llm_client: AsyncAnthropic,
    ):
        self.pubsub = pubsub
        self.peer_id = peer_id
        self.capabilities = capabilities
        self.llm = llm_client
        self._seen_nodes: set[str] = set()

    async def run(self, nursery: trio.Nursery) -> None:
        await announce_capabilities(self.pubsub, self.peer_id, self.capabilities)
        nursery.start_soon(self._handle_new_nodes, nursery)

    async def _handle_new_nodes(self, nursery: trio.Nursery) -> None:
        async for node in subscribe_and_validate(self.pubsub, NEW_NODE_TOPIC, PromptNode):
            logger.info("Observed node %s: %s", node.node_id, node.text)
            if not node.is_atomic:
                continue
            if node.node_id in self._seen_nodes:
                # GossipSub gives no exactly-once delivery guarantee -- the same
                # node/new message can legitimately arrive twice (e.g. relayed via
                # more than one mesh peer). Without this guard, two concurrent
                # _handle_node calls for the same node both subscribe to the same
                # claim topic, and the first to finish tears down that shared
                # subscription out from under the second (crashes the process).
                logger.debug("Ignoring duplicate delivery of node %s", node.node_id)
                continue
            self._seen_nodes.add(node.node_id)
            nursery.start_soon(self._handle_node, node, nursery)

    async def _handle_node(self, node: PromptNode, parent_nursery: trio.Nursery) -> None:
        if not can_handle(self.capabilities, node.required_capability):
            logger.debug("Skipping node %s: capability mismatch", node.node_id)
            parent_nursery.start_soon(self._watch_for_stuck, node)
            return

        won = await self._try_claim(node)
        if not won:
            parent_nursery.start_soon(self._watch_for_stuck, node)
            return

        await self._execute(node)

    async def _try_claim(self, node: PromptNode) -> bool:
        """Publish a claim and wait out the settle window; return True if we won."""
        prefix = node_prefix(node.goal_id, node.node_id)
        my_claim = ClaimMsg(
            node_id=node.node_id,
            claimer_peer=self.peer_id,
            claimed_at=datetime.now(timezone.utc),
            capability=node.required_capability or self.capabilities[0].value,
        )
        claims_seen = [my_claim]

        async with trio.open_nursery() as sub_nursery:
            send_ch, recv_ch = trio.open_memory_channel[ClaimMsg](32)

            async def watch_claims() -> None:
                async for claim in subscribe_and_validate(self.pubsub, claim_topic(prefix), ClaimMsg):
                    if claim.claimer_peer == self.peer_id or not is_claim_timestamp_sane(claim):
                        continue
                    try:
                        send_ch.send_nowait(claim)
                    except trio.WouldBlock:
                        pass

            sub_nursery.start_soon(watch_claims)
            await publish_model(self.pubsub, claim_topic(prefix), my_claim)

            with trio.move_on_after(CLAIM_SETTLE_WINDOW):
                async for claim in recv_ch:
                    claims_seen.append(claim)

            sub_nursery.cancel_scope.cancel()

        winner = resolve_claim_winner(claims_seen)
        if winner.claimer_peer != self.peer_id:
            logger.info(
                "Backing off claim on %s: %s claimed earlier", node.node_id, winner.claimer_peer
            )
            return False
        return True

    async def _execute(self, node: PromptNode) -> None:
        prefix = node_prefix(node.goal_id, node.node_id)
        try:
            output = await self._run_prompt(node.text)
            success = True
        except Exception as exc:
            logger.exception("Execution failed for node %s", node.node_id)
            output = str(exc)
            success = False

        result = ResultMsg(
            node_id=node.node_id, executor_peer=self.peer_id, output=output, success=success
        )
        await publish_model(self.pubsub, result_topic(prefix), result)
        logger.info(
            "Result for %s [%s] (%s):\n%s",
            node.node_id,
            node.text,
            "success" if success else "FAILED",
            output,
        )

    async def _run_prompt(self, text: str) -> str:
        response = await self.llm.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=2048,
            system=EXECUTION_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": text}],
        )
        return "".join(
            block.text for block in response.content if getattr(block, "type", None) == "text"
        )

    async def _watch_for_stuck(self, node: PromptNode) -> None:
        """Re-announce an unclaimed/unresolved node once, then flag a capability gap (spec §6b)."""
        prefix = node_prefix(node.goal_id, node.node_id)

        async with trio.open_nursery() as sub_nursery:
            send_ch, recv_ch = trio.open_memory_channel[ResultMsg](1)

            async def watch_result() -> None:
                async for result in subscribe_and_validate(self.pubsub, result_topic(prefix), ResultMsg):
                    try:
                        send_ch.send_nowait(result)
                    except trio.WouldBlock:
                        pass
                    return

            sub_nursery.start_soon(watch_result)

            with trio.move_on_after(CLAIM_TIMEOUT) as scope1:
                await recv_ch.receive()
            if not scope1.cancelled_caught:
                sub_nursery.cancel_scope.cancel()
                return

            logger.warning("Node %s unresolved after %.0fs, re-announcing", node.node_id, CLAIM_TIMEOUT)
            await publish_model(self.pubsub, NEW_NODE_TOPIC, node)

            with trio.move_on_after(CLAIM_TIMEOUT) as scope2:
                await recv_ch.receive()
            sub_nursery.cancel_scope.cancel()

            if scope2.cancelled_caught:
                logger.error("Node %s still unresolved: capability gap", node.node_id)
                await publish_model(
                    self.pubsub,
                    node_failed_topic(prefix),
                    FailureMsg(
                        goal_id=node.goal_id,
                        role="executor",
                        reason="capability gap: no executor claimed/resolved this node",
                        node_id=node.node_id,
                    ),
                )
