"""Tests for ddpui/utils/transform_workflow_helpers.py

Covers:
  - from_orgdbtoperation (with and without chain_length, extra kwargs)
  - from_orgdbtmodel
"""

from unittest.mock import MagicMock, patch
import uuid

from ddpui.utils.transform_workflow_helpers import from_orgdbtoperation, from_orgdbtmodel
from ddpui.models.dbt_workflow import OrgDbtNodeType


class TestFromOrgdbtoperation:
    def _make_mock_op(self, seq=1, chain_length_in_db=3):
        """Create a mock OrgDbtOperation."""
        op = MagicMock()
        op.uuid = uuid.uuid4()
        op.output_cols = ["col1", "col2"]
        op.config = {"type": "rename"}
        op.seq = seq
        op.dbtmodel = MagicMock()
        op.dbtmodel.uuid = uuid.uuid4()
        op.dbtmodel.name = "model_name"
        op.dbtmodel.schema = "public"
        return op

    @patch("ddpui.utils.transform_workflow_helpers.OrgDbtOperation")
    def test_with_chain_length_provided(self, MockOrgDbtOperation):
        op = self._make_mock_op(seq=2)
        result = from_orgdbtoperation(op, chain_length=5)

        assert result["id"] == op.uuid
        assert result["output_cols"] == ["col1", "col2"]
        assert result["config"] == {"type": "rename"}
        assert result["type"] == OrgDbtNodeType.OPERATION_NODE
        assert result["target_model_id"] == op.dbtmodel.uuid
        assert result["target_model_name"] == "model_name"
        assert result["target_model_schema"] == "public"
        assert result["seq"] == 2
        assert result["chain_length"] == 5
        assert result["is_last_in_chain"] is False

    @patch("ddpui.utils.transform_workflow_helpers.OrgDbtOperation")
    def test_without_chain_length_queries_db(self, MockOrgDbtOperation):
        op = self._make_mock_op(seq=3)
        MockOrgDbtOperation.objects.filter.return_value.count.return_value = 3

        result = from_orgdbtoperation(op, chain_length=None)

        assert result["chain_length"] == 3
        assert result["is_last_in_chain"] is True  # seq=3 == chain_length=3
        MockOrgDbtOperation.objects.filter.assert_called_once_with(dbtmodel=op.dbtmodel)

    @patch("ddpui.utils.transform_workflow_helpers.OrgDbtOperation")
    def test_is_last_in_chain_true(self, MockOrgDbtOperation):
        op = self._make_mock_op(seq=5)
        result = from_orgdbtoperation(op, chain_length=5)
        assert result["is_last_in_chain"] is True

    @patch("ddpui.utils.transform_workflow_helpers.OrgDbtOperation")
    def test_is_last_in_chain_false(self, MockOrgDbtOperation):
        op = self._make_mock_op(seq=1)
        result = from_orgdbtoperation(op, chain_length=5)
        assert result["is_last_in_chain"] is False

    @patch("ddpui.utils.transform_workflow_helpers.OrgDbtOperation")
    def test_extra_kwargs_merged(self, MockOrgDbtOperation):
        op = self._make_mock_op(seq=1)
        result = from_orgdbtoperation(op, chain_length=2, canvas_x=100, canvas_y=200)

        assert result["canvas_x"] == 100
        assert result["canvas_y"] == 200

    @patch("ddpui.utils.transform_workflow_helpers.OrgDbtOperation")
    def test_chain_length_zero_queries_db(self, MockOrgDbtOperation):
        """chain_length=0 is falsy, so the DB query path should be taken."""
        op = self._make_mock_op(seq=1)
        MockOrgDbtOperation.objects.filter.return_value.count.return_value = 4

        result = from_orgdbtoperation(op, chain_length=0)
        assert result["chain_length"] == 4


class TestFromOrgdbtmodel:
    def test_returns_correct_dict(self):
        model = MagicMock()
        model.uuid = uuid.uuid4()
        model.source_name = "source1"
        model.name = "table1"
        model.type = "source"
        model.schema = "raw"

        result = from_orgdbtmodel(model)

        assert result["id"] == model.uuid
        assert result["source_name"] == "source1"
        assert result["input_name"] == "table1"
        assert result["input_type"] == "source"
        assert result["schema"] == "raw"
        assert result["type"] == OrgDbtNodeType.SRC_MODEL_NODE
