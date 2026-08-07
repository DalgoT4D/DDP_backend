"""PII masking for the chat agent — declarative rules over PIIMiddleware.

PII_RULES is the single place to decide what gets masked; each rule becomes one
langchain PIIMiddleware in the agent's middleware stack. Masking applies to
what the MODEL sees — the user's typed message and tool results (query rows) —
before any of it reaches a model provider. The result table the chat UI renders
comes from the tool artifact, which does not pass through the model and is not
masked here.

Strategies: redact ([REDACTED_TYPE]), mask (keep a readable tail, e.g.
****-****-****-1234), hash (pseudonymous), block (refuse the turn).

Masking rewrites the conversation STATE, not just the model request: redacted
values are what get checkpointed and traced, so PII is never persisted — and
history replay shows the placeholders, even for text the user typed themselves.

This list is deployment-wide for now; if orgs need different rules, move it
into org settings and build the middleware from the org's choices instead.
"""

from langchain.agents.middleware import PIIMiddleware

# Indian mobile numbers: optional +91 / 0 prefix, then 10 digits starting 6-9.
# The look-arounds stop it firing inside longer numeric ids.
INDIAN_PHONE_REGEX = r"(?<!\d)(?:\+91[\-\s]?|0)?[6-9]\d{9}(?!\d)"

PII_RULES: list[dict] = [
    {"pii_type": "email", "strategy": "redact"},
    {"pii_type": "credit_card", "strategy": "mask"},
    {"pii_type": "indian_phone", "strategy": "redact", "detector": INDIAN_PHONE_REGEX},
]


def build_pii_middleware() -> list[PIIMiddleware]:
    """One PIIMiddleware per rule, masking user input and tool results."""
    return [
        PIIMiddleware(
            rule["pii_type"],
            strategy=rule.get("strategy", "redact"),
            detector=rule.get("detector"),
            apply_to_input=True,
            apply_to_tool_results=True,
        )
        for rule in PII_RULES
    ]
