"""Parsing helpers for one-shot LLM replies."""

import json
import re

_FENCED_BLOCK = re.compile(r"```(?:json)?\s*\n?(.*?)```", re.DOTALL)


def parse_json_reply(raw: str) -> dict:
    """Parse a model's JSON reply, tolerating a ```json code fence.

    Handles models that append reasoning text after the closing fence.
    Raises like json.loads on anything else — every llm_calls caller is
    fail-open and treats a parse failure as "no result"."""
    text = raw.strip()
    if text.startswith("```"):
        match = _FENCED_BLOCK.search(text)
        if match:
            text = match.group(1).strip()
        else:
            text = text.strip("`").lstrip("json").strip()
    return json.loads(text)
