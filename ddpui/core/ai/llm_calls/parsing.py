"""Parsing helpers for one-shot LLM replies."""

import json


def parse_json_reply(raw: str) -> dict:
    """Parse a model's JSON reply, tolerating a ```json code fence.

    Raises like json.loads on anything else — every llm_calls caller is
    fail-open and treats a parse failure as "no result"."""
    text = raw.strip()
    if text.startswith("```"):
        text = text.strip("`").lstrip("json").strip()
    return json.loads(text)
