"""PII middleware tests — the model must never see raw PII.

Real agent graph + scripted model (records every request it receives), so
these prove masking behaviorally for both surfaces: the user's typed message
and execute_sql tool results.
"""

import os
from typing import Any

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

import pytest
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from ddpui.core.ai.agent.chat_data_agent import build_agent
from ddpui.core.ai.agent.pii import (
    DEFAULT_PII_RULES,
    build_pii_middleware,
    detect_aadhaar,
    detect_pan,
    validate_org_pii_rules,
    verhoeff_valid,
)
from ddpui.tests.core.ai.test_agent_loop import ScriptedChatModel, make_context, sql_call
from ddpui.tests.core.ai.test_tools import FakeWarehouse

# Verhoeff-valid Aadhaar-format test numbers (generated, not real identifiers)
VALID_AADHAAR = "234567890124"
INVALID_AADHAAR = "234567890125"  # same digits, wrong check digit


class RecordingScriptedModel(ScriptedChatModel):
    """Scripted model that also records the messages of every request."""

    requests: list = []

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        self.requests.append(list(messages))
        return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)


def run_agent(model, question, warehouse=None, pii_rules=None):
    # PII masking is what's under test — approvals would pause the scripted run
    agent = build_agent(model=model, human_in_the_loop=False, pii_rules=pii_rules)
    return agent.invoke(
        {"messages": [("user", question)]},
        context=make_context(warehouse=warehouse),
    )


def all_text(messages) -> str:
    return "\n".join(str(m.content) for m in messages)


def test_rules_build_one_middleware_each_covering_input_and_tool_results():
    middlewares = build_pii_middleware()
    assert len(middlewares) == len(DEFAULT_PII_RULES)


def test_email_and_phone_in_user_message_are_masked_before_the_model():
    model = RecordingScriptedModel(script=[AIMessage(content="Noted.")], requests=[])

    run_agent(model, "email priya@ngo.org or call 9876543210 about the surveys")

    seen = all_text(model.requests[0])
    assert "priya@ngo.org" not in seen
    assert "9876543210" not in seen
    assert "REDACTED" in seen  # placeholders replace the raw values


def test_query_results_are_masked_before_the_model_narrates_them():
    warehouse = FakeWarehouse(
        rows=[{"name": "Priya", "email": "priya@ngo.org", "phone": "+91 9876543210"}]
    )
    model = RecordingScriptedModel(
        script=[
            sql_call("SELECT name, email, phone FROM prod.contacts", "c1"),
            AIMessage(content="One contact found."),
        ],
        requests=[],
    )

    run_agent(model, "list the field coordinator contacts", warehouse=warehouse)

    # the model's second request contains the tool result — masked
    narration_request = all_text(model.requests[1])
    assert "priya@ngo.org" not in narration_request
    assert "9876543210" not in narration_request


def test_ordinary_numbers_and_ids_are_not_mangled():
    """The phone regex must not fire on counts or long numeric ids."""
    model = RecordingScriptedModel(script=[AIMessage(content="Noted.")], requests=[])

    run_agent(model, "why are there 1284 surveys and id 123456789012 in prod?")

    seen = all_text(model.requests[0])
    assert "1284" in seen
    assert "123456789012" in seen


# ── Aadhaar / PAN detector functions ─────────────────────────────────────────


def test_verhoeff_checksum_canonical_vectors():
    assert verhoeff_valid("2363")  # the textbook Verhoeff example
    assert not verhoeff_valid("2364")
    assert verhoeff_valid(VALID_AADHAAR)
    assert not verhoeff_valid(INVALID_AADHAAR)


def test_aadhaar_detector_requires_a_valid_checksum():
    spaced = f"{VALID_AADHAAR[:4]} {VALID_AADHAAR[4:8]} {VALID_AADHAAR[8:]}"
    assert [m["text"] for m in detect_aadhaar(f"aadhaar {VALID_AADHAAR} ok")] == [VALID_AADHAAR]
    assert [m["text"] for m in detect_aadhaar(f"aadhaar {spaced} ok")] == [spaced]
    # a 12-digit beneficiary id with a wrong check digit is NOT flagged
    assert detect_aadhaar(f"beneficiary {INVALID_AADHAAR} enrolled") == []


def test_pan_detector_requires_a_real_holder_type():
    assert [m["text"] for m in detect_pan("PAN ABCPE1234F on file")] == ["ABCPE1234F"]
    # 4th char 'D' is not a PAN holder type — looks like a PAN, isn't one
    assert detect_pan("code ABCDE1234F is a program id") == []


def test_aadhaar_in_query_results_is_masked_before_the_model():
    warehouse = FakeWarehouse(rows=[{"name": "Mohan", "aadhaar": VALID_AADHAAR}])
    model = RecordingScriptedModel(
        script=[
            sql_call("SELECT name, aadhaar FROM prod.farmers", "c1"),
            AIMessage(content="One farmer found."),
        ],
        requests=[],
    )

    run_agent(model, "show farmer identifiers", warehouse=warehouse)

    assert VALID_AADHAAR not in all_text(model.requests[1])


# ── Org-defined rules (dynamic, additive) ────────────────────────────────────

CASE_ID_RULE = {"pii_type": "case_id", "detector": r"CASE-\d{6}", "strategy": "redact"}


def test_org_rule_masks_its_pattern_alongside_the_defaults():
    model = RecordingScriptedModel(script=[AIMessage(content="Noted.")], requests=[])

    run_agent(
        model,
        "what happened to CASE-482910? email me at priya@ngo.org",
        pii_rules=[CASE_ID_RULE],
    )

    seen = all_text(model.requests[0])
    assert "CASE-482910" not in seen  # the org's own rule fired
    assert "priya@ngo.org" not in seen  # defaults still apply


def test_invalid_stored_org_rule_is_skipped_not_fatal():
    middlewares = build_pii_middleware([{"pii_type": "bad", "detector": "("}])
    assert len(middlewares) == len(DEFAULT_PII_RULES)  # skipped, defaults intact


def test_org_rules_cannot_override_or_duplicate():
    with pytest.raises(ValueError, match="built-in"):
        validate_org_pii_rules([{"pii_type": "email", "detector": "x"}])
    with pytest.raises(ValueError, match="duplicate"):
        validate_org_pii_rules([CASE_ID_RULE, CASE_ID_RULE])


def test_org_rule_validation_rejects_bad_shapes():
    with pytest.raises(ValueError, match="valid regex"):
        validate_org_pii_rules([{"pii_type": "case_id", "detector": "("}])
    with pytest.raises(ValueError, match="strategy"):
        validate_org_pii_rules([{**CASE_ID_RULE, "strategy": "shout"}])
    with pytest.raises(ValueError, match="slug"):
        validate_org_pii_rules([{"pii_type": "Case ID!", "detector": "x"}])
    with pytest.raises(ValueError, match="non-empty regex"):
        validate_org_pii_rules([{"pii_type": "case_id"}])
