import os
import django

from sqlalchemy import create_engine

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.alerts.alert_query_builder import build_alert_query_builder, compile_query
from ddpui.core.alerts.rendering import render_alert_message
from ddpui.models.alert import AlertMessagePlaceholderConfig, AlertQueryConfig


class FakeWarehouseClient:
    engine = create_engine("sqlite://")


def test_alert_query_builder_selects_placeholder_aggregates_without_replacing_alert_value():
    config = AlertQueryConfig(
        schema_name="analytics",
        table_name="attendance_fact",
        aggregation="SUM",
        measure_column="attendance",
        group_by_column="district_name",
        condition_operator="<",
        condition_value=10,
        filters=[],
    )
    placeholders = [
        AlertMessagePlaceholderConfig(
            key="sum_enrolled",
            aggregation="SUM",
            column="enrolled",
        ),
        AlertMessagePlaceholderConfig(
            key="count_rows",
            aggregation="COUNT",
            column=None,
        ),
    ]

    sql = compile_query(
        build_alert_query_builder(config, placeholders),
        FakeWarehouseClient(),
    ).lower()

    assert "alert_value" in sql
    assert "sum_enrolled" in sql
    assert "count_rows" in sql
    assert "group by district_name" in sql
    assert "where alert_value < 10" in sql


def test_grouped_alert_rendering_outputs_one_email_with_one_section_per_failing_group():
    placeholders = [
        AlertMessagePlaceholderConfig(
            key="sum_enrolled",
            aggregation="SUM",
            column="enrolled",
        )
    ]
    rendered = render_alert_message(
        alert_name="Attendance alert",
        message="The following {{group_by_column}} values failed {{alert_name}}.",
        group_message="{{group_by_value}} | Attendance: {{alert_value}} | Enrolled: {{sum_enrolled}}",
        rows=[
            {"district_name": "North", "alert_value": 8, "sum_enrolled": 24},
            {"district_name": "South", "alert_value": 6, "sum_enrolled": 19},
        ],
        placeholders=placeholders,
        table_name="attendance_fact",
        metric_name="Attendance metric",
        group_by_column="district_name",
    )

    assert "The following district_name values failed Attendance alert." in rendered
    assert "North | Attendance: 8 | Enrolled: 24" in rendered
    assert "South | Attendance: 6 | Enrolled: 19" in rendered
    assert rendered.count("Attendance:") == 2
