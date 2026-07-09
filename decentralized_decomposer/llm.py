"""Shared LLM call helper: structured output + retry/backoff (spec §6b).

Used by both the decomposer and scorer agents. On a malformed/unparseable
response, retries with exponential backoff and feeds the parse error back
into the prompt. Raises `LLMCallFailed` once retries are exhausted --
callers publish a failure notice (spec §6b) rather than a guessed result.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

import trio
from anthropic import AsyncAnthropic
from pydantic import BaseModel, ValidationError

from decentralized_decomposer import config

logger = logging.getLogger(__name__)


class LLMCallFailed(Exception):
    pass


def _extract_json(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:]
        text = text.strip()
    return text


async def call_structured(
    client: AsyncAnthropic,
    *,
    system: str,
    user: str,
    response_model: type[BaseModel],
    max_retries: int | None = None,
    validate: Callable[[BaseModel], None] | None = None,
) -> BaseModel:
    """`validate` (optional): raise ValueError from it to reject an otherwise
    schema-valid response (e.g. wrong subgoal count) and trigger the same
    retry-with-feedback path as a schema validation failure.
    """
    if max_retries is None:
        max_retries = config.MAX_LLM_RETRIES
    last_error: str | None = None
    for attempt in range(max_retries + 1):
        prompt = user
        if last_error is not None:
            prompt = (
                f"{user}\n\nYour last response didn't match the required JSON "
                f"schema: {last_error}. Try again."
            )
        try:
            response = await client.messages.create(
                model=config.ANTHROPIC_MODEL,
                max_tokens=2048,
                system=system,
                messages=[{"role": "user", "content": prompt}],
            )
        except Exception as exc:  # network / API failure
            last_error = str(exc)
            logger.warning(
                "LLM call failed (attempt %d/%d): %s", attempt + 1, max_retries + 1, exc
            )
        else:
            text = "".join(
                block.text for block in response.content if getattr(block, "type", None) == "text"
            )
            try:
                parsed = response_model.model_validate_json(_extract_json(text))
                if validate is not None:
                    validate(parsed)
                return parsed
            except (ValidationError, ValueError) as exc:
                last_error = str(exc)
                logger.warning(
                    "LLM response failed schema validation (attempt %d/%d): %s",
                    attempt + 1,
                    max_retries + 1,
                    exc,
                )

        if attempt < max_retries:
            await trio.sleep(config.LLM_RETRY_BASE_DELAY * (2**attempt))

    raise LLMCallFailed(last_error or "unknown error")
