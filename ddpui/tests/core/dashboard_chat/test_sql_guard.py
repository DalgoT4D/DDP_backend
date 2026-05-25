from ddpui.core.dashboard_chat.context.dashboard_table_allowlist import DashboardChatAllowlist
from ddpui.core.dashboard_chat.warehouse.sql_guard import DashboardChatSqlGuard


def test_extract_table_names_ignores_from_inside_extract() -> None:
    sql = """
        SELECT SUM(silt_achieved) AS total_silt_excavated
        FROM dev_analytics_niti_2025_reports_cleaned.ss_work_order_metric_niti_25
        WHERE EXTRACT(YEAR FROM date_time) = 2023;
    """

    assert DashboardChatSqlGuard._extract_table_names(sql) == [
        "dev_analytics_niti_2025_reports_cleaned.ss_work_order_metric_niti_25"
    ]


def test_validate_allows_extract_query_on_allowlisted_table() -> None:
    allowlist = DashboardChatAllowlist(
        allowed_tables={
            "dev_analytics_niti_2025_reports_cleaned.ss_work_order_metric_niti_25",
        }
    )
    sql = """
        SELECT SUM(silt_achieved) AS total_silt_excavated
        FROM dev_analytics_niti_2025_reports_cleaned.ss_work_order_metric_niti_25
        WHERE EXTRACT(YEAR FROM date_time) = 2023;
    """

    validation = DashboardChatSqlGuard(allowlist).validate(sql)

    assert validation.is_valid is True
    assert validation.errors == []
    assert validation.tables == [
        "dev_analytics_niti_2025_reports_cleaned.ss_work_order_metric_niti_25"
    ]


def test_extract_table_names_handles_join_and_ignores_cte_names() -> None:
    sql = """
        WITH ranked AS (
            SELECT district, SUM(silt_achieved) AS total_silt
            FROM dev_analytics_niti_2025_reports_cleaned.ss_work_order_metric_niti_25
            GROUP BY district
        )
        SELECT ranked.district, farmer.verified_farmers
        FROM ranked
        JOIN dev_analytics_niti_2025_reports_aggregated.ss_farmer_agg_niti_25 AS farmer
          ON ranked.district = farmer.district;
    """

    assert DashboardChatSqlGuard._extract_table_names(sql) == [
        "dev_analytics_niti_2025_reports_aggregated.ss_farmer_agg_niti_25"
    ]
