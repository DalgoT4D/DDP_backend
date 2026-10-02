"""Tests for parse_json_reply — the shared JSON extractor for LLM replies."""

import json
import os

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from ddpui.core.ai.llm_calls.parsing import parse_json_reply


def test_plain_json():
    assert parse_json_reply('{"intent": "data_question"}') == {"intent": "data_question"}


def test_fenced_json():
    raw = '```json\n{"intent": "data_question"}\n```'
    assert parse_json_reply(raw) == {"intent": "data_question"}


def test_fenced_without_json_tag():
    raw = '```\n{"intent": "data_question"}\n```'
    assert parse_json_reply(raw) == {"intent": "data_question"}


def test_fenced_json_with_trailing_reasoning():
    raw = (
        '```json\n{"intent": "data_question", "complexity": "simple"}\n```\n\n'
        "**Reasoning:** This is a follow-up about data analysis."
    )
    assert parse_json_reply(raw) == {"intent": "data_question", "complexity": "simple"}


def test_fenced_json_with_multiline_trailing_text():
    raw = (
        '```json\n{"ok": true}\n```\n\n'
        "**Reasoning:** The query looks correct.\n\n"
        "The JOIN is appropriate here because..."
    )
    assert parse_json_reply(raw) == {"ok": True}


def test_no_closing_fence_falls_back():
    raw = '```json\n{"intent": "small_talk"}'
    assert parse_json_reply(raw) == {"intent": "small_talk"}


def test_whitespace_around_fences():
    raw = '  ```json\n  {"a": 1}  \n```  '
    assert parse_json_reply(raw) == {"a": 1}


def test_invalid_json_raises():
    with pytest.raises(json.JSONDecodeError):
        parse_json_reply("not json at all")
