"""Tests for the report executive-summary generator.

The generator fetches each frozen component's data through ReportService and
composes ONE model call — both are mocked here; no warehouse, no LLM.
"""

import os
from types import SimpleNamespace
from unittest.mock import patch

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
django.setup()

from ddpui.core.ai.agent.report_summary_agent import SummaryGenerationError, generate_report_summary


class FakeModel:
    """Records the prompt and returns a canned summary."""

    def __init__(self, reply="**Great quarter.**"):
        self.reply = reply
        self.prompts = []

    def invoke(self, prompt):
        self.prompts.append(prompt)
        return SimpleNamespace(content=self.reply)


def make_snapshot():
    return SimpleNamespace(
        id=11,
        title="Q1 Field Report",
        period_start="2026-01-01",
        period_end="2026-03-31",
        org=SimpleNamespace(id=1),
        frozen_chart_configs={
            "3": {
                "component_type": "chart",
                "title": "Surveys by district",
                "chart_type": "bar",
                "schema_name": "prod",
                "table_name": "surveys",
            },
            "9": {
                "component_type": "kpi",
                "title": "Donations this quarter",
                "metric": {"schema_name": "prod", "table_name": "donations"},
            },
        },
    )


@patch("ddpui.core.ai.agent.report_summary_agent.ReportService")
def test_prompt_carries_title_period_and_every_components_data(mock_service):
    mock_service.get_report_chart_data.return_value = {
        "data": {"xAxisData": ["Pune", "Nagpur"], "seriesData": [120, 80]}
    }
    mock_service.get_report_kpi_data.return_value = {"current_value": 54000}
    model = FakeModel()

    summary = generate_report_summary(make_snapshot(), model=model)

    assert summary == "**Great quarter.**"
    prompt = model.prompts[0]
    assert "Q1 Field Report" in prompt
    assert "2026-01-01" in prompt and "2026-03-31" in prompt
    assert "Surveys by district" in prompt and "Pune" in prompt
    assert "Donations this quarter" in prompt and "54000" in prompt
    # chart components go through the chart path, KPIs through the KPI path
    assert mock_service.get_report_chart_data.call_count == 1
    assert mock_service.get_report_kpi_data.call_count == 1


@patch("ddpui.core.ai.agent.report_summary_agent.ReportService")
def test_one_broken_component_becomes_a_note_not_a_failure(mock_service):
    mock_service.get_report_chart_data.side_effect = RuntimeError("column gone")
    mock_service.get_report_kpi_data.return_value = {"current_value": 54000}
    model = FakeModel()

    generate_report_summary(make_snapshot(), model=model)

    prompt = model.prompts[0]
    assert "(data unavailable)" in prompt
    assert "54000" in prompt  # the healthy component still contributes


@patch("ddpui.core.ai.agent.report_summary_agent.ReportService")
def test_all_components_failing_raises(mock_service):
    mock_service.get_report_chart_data.side_effect = RuntimeError("x")
    mock_service.get_report_kpi_data.side_effect = RuntimeError("x")

    with pytest.raises(SummaryGenerationError, match="couldn't read any"):
        generate_report_summary(make_snapshot(), model=FakeModel())


@patch("ddpui.core.ai.agent.report_summary_agent.ReportService")
def test_thinking_blocks_never_reach_the_draft(mock_service):
    # claude-sonnet-5 with thinking enabled returns content as a block list —
    # only the text block belongs in the draft (caught by browser smoke test)
    mock_service.get_report_chart_data.return_value = {"data": {"seriesData": [1]}}
    mock_service.get_report_kpi_data.return_value = {"current_value": 1}
    model = FakeModel(
        reply=[
            {"type": "thinking", "thinking": "", "signature": "Ev4OC..."},
            {"type": "text", "text": "**A strong quarter.**"},
        ]
    )

    summary = generate_report_summary(make_snapshot(), model=model)

    assert summary == "**A strong quarter.**"


def test_report_with_no_components_raises():
    snapshot = make_snapshot()
    snapshot.frozen_chart_configs = {}
    with pytest.raises(SummaryGenerationError, match="no charts"):
        generate_report_summary(snapshot, model=FakeModel())
