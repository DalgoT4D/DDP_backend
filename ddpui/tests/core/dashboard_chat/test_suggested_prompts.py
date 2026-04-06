from ddpui.core.dashboard_chat.suggested_prompts import build_dashboard_suggested_prompts


def test_build_dashboard_suggested_prompts_returns_three_grounded_questions():
    prompts = build_dashboard_suggested_prompts(
        dashboard_export={
            "dashboard": {
                "title": "Facilitator Effectiveness Studio",
                "description": "Facilitator performance and district literacy efficiency by quarter",
            },
            "charts": [
                {
                    "id": 7,
                    "title": "Facilitator Outcomes",
                    "description": "Quarterly facilitator effectiveness across learner outcomes",
                    "chart_type": "line",
                    "schema_name": "analytics",
                    "table_name": "facilitator_effectiveness_quarterly",
                    "extra_config": {
                        "dimension_column": "quarter_label",
                        "extra_dimension_column": "facilitator_name",
                        "metrics": [
                            {
                                "column": "improved_literacy_students",
                                "aggregation": "sum",
                                "alias": "outcomes",
                            }
                        ],
                    },
                },
                {
                    "id": 3,
                    "title": "District Literacy Efficiency",
                    "description": "Improved literacy students per spend by district",
                    "chart_type": "bar",
                    "schema_name": "analytics",
                    "table_name": "district_funding_efficiency_quarterly",
                    "extra_config": {
                        "dimension_column": "district_name",
                        "metrics": [
                            {
                                "column": "literacy_efficiency",
                                "aggregation": "avg",
                                "alias": "literacy efficiency",
                            }
                        ],
                    },
                },
                {
                    "id": 9,
                    "title": "Total Facilitators",
                    "description": "Count of facilitators on the dashboard",
                    "chart_type": "number",
                    "schema_name": "analytics",
                    "table_name": "facilitator_effectiveness_quarterly",
                    "extra_config": {
                        "aggregate_column": "facilitator_name",
                        "aggregate_function": "count_distinct",
                    },
                },
            ],
        },
        org_context_markdown="Facilitators and districts are the main operating units.",
        dashboard_context_markdown="Use this dashboard to compare outcomes over time.",
    )

    assert prompts == [
        "How have outcomes changed by quarter?",
        "Which districts have the highest literacy efficiency?",
        'What does the "Total Facilitators" metric represent?',
    ]


def test_build_dashboard_suggested_prompts_backfills_with_explanations_when_only_number_charts_exist():
    prompts = build_dashboard_suggested_prompts(
        dashboard_export={
            "dashboard": {
                "title": "Impact Snapshot",
                "description": "Headline metrics for the current program cycle",
            },
            "charts": [
                {
                    "id": 1,
                    "title": "Total Learners Reached",
                    "description": "Unique learners supported",
                    "chart_type": "number",
                    "schema_name": "analytics",
                    "table_name": "learner_rollup",
                    "extra_config": {
                        "aggregate_column": "learner_id",
                        "aggregate_function": "count_distinct",
                    },
                },
                {
                    "id": 2,
                    "title": "Average Attendance Rate",
                    "description": "Average attendance across all learners",
                    "chart_type": "number",
                    "schema_name": "analytics",
                    "table_name": "learner_rollup",
                    "extra_config": {
                        "aggregate_column": "attendance_rate",
                        "aggregate_function": "avg",
                    },
                },
            ],
        },
        org_context_markdown="Learners are the key population.",
        dashboard_context_markdown="Use this dashboard for quick snapshot metrics.",
    )

    assert prompts == [
        'What does the "Total Learners Reached" metric represent?',
        'What does the "Average Attendance Rate" metric represent?',
    ]
