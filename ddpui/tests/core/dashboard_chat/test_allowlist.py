"""Tests for dashboard chat table allowlist construction."""

from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import (
    DashboardChatAllowlistBuilder,
    DashboardChatManifestDependencyError,
)


def test_allowlist_excludes_manifest_source_tables_from_queryable_tables():
    """Raw dbt source tables should not become queryable dashboard-chat tables."""
    export_payload = {
        "charts": [
            {"schema_name": "analytics", "table_name": "program_summary"},
            {"schema_name": "raw", "table_name": "student_feed"},
        ]
    }
    manifest_json = {
        "nodes": {
            "model.project.program_summary": {
                "resource_type": "model",
                "name": "program_summary",
                "schema": "analytics",
                "alias": "program_summary",
                "depends_on": {"nodes": ["model.project.student_rollup"]},
            },
            "model.project.student_rollup": {
                "resource_type": "model",
                "name": "student_rollup",
                "schema": "analytics",
                "alias": "student_rollup",
                "depends_on": {"nodes": ["source.project.raw.student_feed"]},
            },
        },
        "sources": {
            "source.project.raw.student_feed": {
                "resource_type": "source",
                "name": "student_feed",
                "schema": "raw",
                "identifier": "student_feed",
                "depends_on": {"nodes": []},
            }
        },
        "parent_map": {},
        "child_map": {},
    }

    allowlist = DashboardChatAllowlistBuilder.build(
        export_payload,
        manifest_json=manifest_json,
    )
    dbt_index = DashboardChatAllowlistBuilder.build_dbt_index(manifest_json, allowlist)

    assert allowlist.chart_tables == {"analytics.program_summary"}
    assert allowlist.allowed_tables == {
        "analytics.program_summary",
        "analytics.student_rollup",
    }
    assert "raw.student_feed" not in allowlist.allowed_tables
    assert "source.project.raw.student_feed" not in allowlist.allowed_unique_ids
    assert "source.project.raw.student_feed" not in dbt_index["resources_by_unique_id"]


def test_allowlist_excludes_staging_models_from_queryable_tables_and_dbt_index():
    """Staging lineage models should stay out of dashboard-chat table/model discovery."""
    export_payload = {
        "charts": [
            {"schema_name": "analytics", "table_name": "program_summary"},
        ]
    }
    manifest_json = {
        "nodes": {
            "model.project.program_summary": {
                "resource_type": "model",
                "name": "program_summary",
                "schema": "analytics",
                "alias": "program_summary",
                "depends_on": {"nodes": ["model.project.student_metrics_stg"]},
            },
            "model.project.student_metrics_stg": {
                "resource_type": "model",
                "name": "student_metrics_stg",
                "schema": "dev_staging",
                "alias": "student_metrics_stg",
                "depends_on": {"nodes": []},
            },
        },
        "sources": {},
        "parent_map": {
            "model.project.program_summary": ["model.project.student_metrics_stg"],
            "model.project.student_metrics_stg": [],
        },
        "child_map": {
            "model.project.program_summary": [],
            "model.project.student_metrics_stg": ["model.project.program_summary"],
        },
    }

    allowlist = DashboardChatAllowlistBuilder.build(
        export_payload,
        manifest_json=manifest_json,
    )
    dbt_index = DashboardChatAllowlistBuilder.build_dbt_index(manifest_json, allowlist)

    assert allowlist.allowed_tables == {"analytics.program_summary"}
    assert "dev_staging.student_metrics_stg" not in allowlist.allowed_tables
    assert "model.project.student_metrics_stg" not in allowlist.allowed_unique_ids
    assert "model.project.student_metrics_stg" not in dbt_index["resources_by_unique_id"]


def test_required_manifest_loader_raises_clear_build_dependency_error():
    try:
        DashboardChatAllowlistBuilder.load_required_manifest_json(None)
    except DashboardChatManifestDependencyError as error:
        message = str(error)
    else:
        raise AssertionError("Expected DashboardChatManifestDependencyError")

    assert "requires dbt target/manifest.json" in message
    assert "before rebuilding dashboard chat metadata" in message
