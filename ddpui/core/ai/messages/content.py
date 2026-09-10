"""Extract user-facing text from LangChain message content.

Claude models with thinking enabled (the default on claude-sonnet-5) return
content as a LIST of blocks — e.g. [{"type": "thinking", "thinking": "",
"signature": "..."}, {"type": "text", "text": "the answer"}]. The signature is
the API's tamper-proof stamp on the thinking block; it must stay in the stored
message (replays require it) but must never reach the user.
"""

from typing import Any


def extract_text(content: Any) -> str:
    """Only the human-readable text: plain strings pass through, block lists
    are reduced to their text blocks, everything else renders as nothing."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and block.get("type") == "text":
                parts.append(block.get("text", ""))
        return "".join(parts)
    return ""
