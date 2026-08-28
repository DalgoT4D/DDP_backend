"""PII masking for the chat agent — declarative rules over PIIMiddleware.

Two layers of rules, both applied to what the MODEL sees (the user's typed
message and tool results / query rows) before any of it reaches a model
provider:

- DEFAULT_PII_RULES — deployment-wide and immovable. Orgs can never remove
  or weaken these.
- Org rules — extra regex detectors an org admin adds in
  ChatWithDataOrgConfig.pii_rules (e.g. their case-id format). Additive only:
  an org rule may not reuse a default rule's pii_type. Validated at save time
  by validate_org_pii_rules(); anything invalid that still reaches
  build_pii_middleware() is skipped with a loud log, never crashes the turn.

The result table the chat UI renders comes from the tool artifact, which does
not pass through the model and is not masked here.

Aadhaar and PAN use detector FUNCTIONS, not bare regexes: NGO tables are full
of long numeric beneficiary ids, so a 12-digit pattern alone would misfire
constantly. Aadhaar numbers carry a Verhoeff check digit and PAN a fixed
structure — the detectors validate those, so only real identifiers match.

Strategies: redact ([REDACTED_TYPE]), mask (keep a readable tail, e.g.
****-****-****-1234), hash (stable pseudonym), block (refuse the turn).

Masking rewrites the conversation STATE, not just the model request: redacted
values are what get checkpointed and traced, so PII is never persisted — and
history replay shows the placeholders, even for text the user typed themselves.
"""

import re

from langchain.agents.middleware import PIIMiddleware

from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

# ── Indian phone ─────────────────────────────────────────────────────────────
# Optional +91 / 0 prefix, then 10 digits starting 6-9. The look-arounds stop
# it firing inside longer numeric ids.
INDIAN_PHONE_REGEX = r"(?<!\d)(?:\+91[\-\s]?|0)?[6-9]\d{9}(?!\d)"

# ── Aadhaar (Verhoeff-validated) ─────────────────────────────────────────────
# 12 digits, first digit 2-9, optionally grouped 4-4-4 by a space or dash
# (the backreference forces the same separator in both places).
_AADHAAR_REGEX = re.compile(r"(?<!\d)([2-9]\d{3})([ -]?)(\d{4})\2(\d{4})(?!\d)")

# Verhoeff dihedral-group tables (multiplication d, permutation p).
_VERHOEFF_D = (
    (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
    (1, 2, 3, 4, 0, 6, 7, 8, 9, 5),
    (2, 3, 4, 0, 1, 7, 8, 9, 5, 6),
    (3, 4, 0, 1, 2, 8, 9, 5, 6, 7),
    (4, 0, 1, 2, 3, 9, 5, 6, 7, 8),
    (5, 9, 8, 7, 6, 0, 4, 3, 2, 1),
    (6, 5, 9, 8, 7, 1, 0, 4, 3, 2),
    (7, 6, 5, 9, 8, 2, 1, 0, 4, 3),
    (8, 7, 6, 5, 9, 3, 2, 1, 0, 4),
    (9, 8, 7, 6, 5, 4, 3, 2, 1, 0),
)
_VERHOEFF_P = (
    (0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
    (1, 5, 7, 6, 2, 8, 3, 0, 9, 4),
    (5, 8, 0, 3, 7, 9, 6, 1, 4, 2),
    (8, 9, 1, 6, 0, 4, 3, 5, 2, 7),
    (9, 4, 5, 3, 1, 2, 6, 8, 7, 0),
    (4, 2, 8, 6, 5, 7, 3, 9, 0, 1),
    (2, 7, 9, 3, 8, 0, 6, 4, 1, 5),
    (7, 0, 4, 6, 9, 1, 3, 2, 5, 8),
)


def verhoeff_valid(digits: str) -> bool:
    """True when the number's Verhoeff check digit is consistent (Aadhaar uses
    this; a random 12-digit id passes only 1 time in 10)."""
    check = 0
    for position, char in enumerate(reversed(digits)):
        check = _VERHOEFF_D[check][_VERHOEFF_P[position % 8][int(char)]]
    return check == 0


def detect_aadhaar(content: str) -> list[dict]:
    """Aadhaar detector: 12-digit shape AND a valid Verhoeff checksum."""
    matches = []
    for match in _AADHAAR_REGEX.finditer(content):
        digits = match.group(1) + match.group(3) + match.group(4)
        if verhoeff_valid(digits):
            matches.append({"text": match.group(0), "start": match.start(), "end": match.end()})
    return matches


# ── PAN (structure-validated) ────────────────────────────────────────────────
# AAAPA1234A: 5 letters, 4 digits, 1 letter — and the 4th letter encodes the
# holder type (P=person, C=company, …). Random uppercase codes fail that check.
_PAN_REGEX = re.compile(r"\b[A-Z]{5}\d{4}[A-Z]\b")
_PAN_HOLDER_TYPES = frozenset("ABCFGHJLPT")


def detect_pan(content: str) -> list[dict]:
    """PAN detector: 5-letters/4-digits/1-letter AND a real holder-type char."""
    return [
        {"text": match.group(0), "start": match.start(), "end": match.end()}
        for match in _PAN_REGEX.finditer(content)
        if match.group(0)[3] in _PAN_HOLDER_TYPES
    ]


# ── The immovable defaults ───────────────────────────────────────────────────
DEFAULT_PII_RULES: list[dict] = [
    {"pii_type": "email", "strategy": "redact"},
    {"pii_type": "credit_card", "strategy": "mask"},
    {"pii_type": "indian_phone", "strategy": "redact", "detector": INDIAN_PHONE_REGEX},
    {"pii_type": "aadhaar", "strategy": "redact", "detector": detect_aadhaar},
    {"pii_type": "pan", "strategy": "redact", "detector": detect_pan},
]
DEFAULT_PII_TYPES = frozenset(rule["pii_type"] for rule in DEFAULT_PII_RULES)

# ── Org rules: validation + merge ────────────────────────────────────────────
ORG_RULE_STRATEGIES = frozenset({"redact", "mask", "hash", "block"})
_PII_TYPE_SLUG = re.compile(r"^[a-z][a-z0-9_]{0,39}$")
MAX_DETECTOR_LENGTH = 500


def validate_org_pii_rules(rules) -> None:
    """Validate an org's pii_rules list; raises ValueError with a message an
    admin can act on. Called at save time (ChatWithDataOrgConfig.clean)."""
    if not isinstance(rules, list):
        raise ValueError("pii_rules must be a list of rules")
    seen_types: set[str] = set()
    for index, rule in enumerate(rules):
        label = f"pii_rules[{index}]"
        if not isinstance(rule, dict):
            raise ValueError(f"{label}: each rule must be an object")
        unknown = set(rule) - {"pii_type", "detector", "strategy"}
        if unknown:
            raise ValueError(f"{label}: unknown keys {sorted(unknown)}")

        pii_type = rule.get("pii_type")
        if not isinstance(pii_type, str) or not _PII_TYPE_SLUG.match(pii_type):
            raise ValueError(
                f"{label}: pii_type must be a short lowercase slug (letters, digits, _)"
            )
        if pii_type in DEFAULT_PII_TYPES:
            raise ValueError(f"{label}: '{pii_type}' is a built-in rule and cannot be overridden")
        if pii_type in seen_types:
            raise ValueError(f"{label}: duplicate pii_type '{pii_type}'")
        seen_types.add(pii_type)

        detector = rule.get("detector")
        if not isinstance(detector, str) or not detector.strip():
            raise ValueError(f"{label}: detector must be a non-empty regex string")
        if len(detector) > MAX_DETECTOR_LENGTH:
            raise ValueError(f"{label}: detector longer than {MAX_DETECTOR_LENGTH} characters")
        try:
            re.compile(detector)
        except re.error as err:
            raise ValueError(f"{label}: detector is not a valid regex ({err})") from err

        strategy = rule.get("strategy", "redact")
        if strategy not in ORG_RULE_STRATEGIES:
            raise ValueError(f"{label}: strategy must be one of {sorted(ORG_RULE_STRATEGIES)}")


def _to_middleware(pii_type: str, detector, strategy: str) -> PIIMiddleware:
    return PIIMiddleware(
        pii_type,
        strategy=strategy,
        detector=detector,
        apply_to_input=True,
        apply_to_tool_results=True,
    )


def build_pii_middleware(org_rules: list[dict] | None = None) -> list[PIIMiddleware]:
    """One PIIMiddleware per rule: the immovable defaults, then the org's own
    rules. Org regex strings are re-validated (and compiled by PIIMiddleware)
    here, so a bad pattern fails at agent build, not mid-conversation; a rule
    that fails despite save-time validation is skipped with an error log
    rather than breaking the turn."""
    middlewares = [
        _to_middleware(rule["pii_type"], rule.get("detector"), rule.get("strategy", "redact"))
        for rule in DEFAULT_PII_RULES
    ]
    for rule in org_rules or []:
        try:
            validate_org_pii_rules([rule])
            middlewares.append(
                _to_middleware(rule["pii_type"], rule["detector"], rule.get("strategy", "redact"))
            )
        except (ValueError, KeyError, TypeError) as err:
            logger.error(f"skipping invalid org pii rule {rule!r}: {err}")
    return middlewares
