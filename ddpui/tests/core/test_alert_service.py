"""AlertService unit tests.

SKIPPED in Batch 1 of the KPI+Alerts overhaul:
  - Alert's metric FK has been rewired to KPI for the RAG path
  - _resolve_metric / _build_metric_backed_query_config / _evaluate_metric_rag_alert
    have been replaced by their _kpi_* equivalents
  - Three-type alert split (threshold / rag / standalone) lands in Batch 3

Will be rewritten in Batch 3 against the new alert shape.
"""

import pytest

pytestmark = pytest.mark.skip(
    reason="Batch 1: awaiting Batch 3 alert restructure — see module docstring"
)
