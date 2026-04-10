import os
import uuid
from unittest.mock import Mock, patch, MagicMock
import pytest
from ninja.errors import HttpError

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.transformfunctions import validate_operation_config
from ddpui.models.dbt_workflow import OrgDbtModel

pytestmark = pytest.mark.django_db


def _make_payload(
    config=None,
    op_type="flatten_json",
    input_uuid="",
    source_columns=None,
    other_inputs=None,
):
    """Create a mock payload matching CreateDbtModelPayload."""
    payload = Mock()
    payload.config = config if config is not None else {"key": "value"}
    payload.op_type = op_type
    payload.input_uuid = input_uuid
    payload.source_columns = source_columns if source_columns is not None else ["col1"]
    payload.other_inputs = other_inputs if other_inputs is not None else []
    return payload


def _make_orgdbtmodel(**kwargs):
    """Build a mock OrgDbtModel."""
    m = Mock(spec=OrgDbtModel)
    m.uuid = kwargs.get("uuid", str(uuid.uuid4()))
    m.name = kwargs.get("name", "test_model")
    m.display_name = kwargs.get("display_name", "Test Model")
    m.source_name = kwargs.get("source_name", "src")
    m.schema = kwargs.get("schema", "public")
    m.type = kwargs.get("type", "source")
    return m


class TestValidateOperationConfig:
    """Tests for validate_operation_config"""

    def test_input_required_when_no_chained_before(self):
        """When operation_chained_before == 0, input_uuid is required."""
        payload = _make_payload(input_uuid="")
        target_model = Mock()
        with pytest.raises(HttpError) as excinfo:
            validate_operation_config(payload, target_model, is_multi_input_op=False)
        assert excinfo.value.status_code == 422
        assert "input is required" in str(excinfo.value)

    @patch("ddpui.core.transformfunctions.OrgDbtModel.objects")
    def test_input_not_found(self, mock_objects):
        """When the provided input_uuid doesn't match any model, 404 is raised."""
        mock_objects.filter.return_value.first.return_value = None
        payload = _make_payload(input_uuid="some-uuid")
        target_model = Mock()
        with pytest.raises(HttpError) as excinfo:
            validate_operation_config(payload, target_model, is_multi_input_op=False)
        assert excinfo.value.status_code == 404
        assert "input not found" in str(excinfo.value)

    @patch("ddpui.core.transformfunctions.OrgDbtModel.objects")
    def test_single_input_success(self, mock_objects):
        """Single-input operation succeeds and returns correct config."""
        model = _make_orgdbtmodel()
        mock_objects.filter.return_value.first.return_value = model

        payload = _make_payload(
            input_uuid="valid-uuid",
            config={"key": "val"},
            op_type="flatten_json",
            source_columns=["col1", "col2"],
        )
        target_model = Mock()

        result_config, all_inputs = validate_operation_config(
            payload, target_model, is_multi_input_op=False
        )

        assert result_config["type"] == "flatten_json"
        assert result_config["config"]["source_columns"] == ["col1", "col2"]
        assert result_config["config"]["other_inputs"] == []
        assert len(result_config["input_models"]) == 1
        assert result_config["input_models"][0]["name"] == model.name
        assert all_inputs == [model]

    @patch("ddpui.core.transformfunctions.OrgDbtModel.objects")
    def test_multi_input_no_other_inputs_raises(self, mock_objects):
        """Multi-input op with empty other_inputs raises 422."""
        model = _make_orgdbtmodel()
        mock_objects.filter.return_value.first.return_value = model

        payload = _make_payload(input_uuid="valid-uuid", other_inputs=[])
        target_model = Mock()

        with pytest.raises(HttpError) as excinfo:
            validate_operation_config(payload, target_model, is_multi_input_op=True)
        assert excinfo.value.status_code == 422
        assert "atleast 2 inputs" in str(excinfo.value)

    @patch("ddpui.core.transformfunctions.OrgDbtModel.objects")
    def test_multi_input_other_input_not_found(self, mock_objects):
        """Multi-input op when an other_input uuid doesn't match a model."""
        primary_model = _make_orgdbtmodel()

        # First call returns the primary model, second call returns None
        mock_objects.filter.return_value.first.side_effect = [primary_model, None]

        other_input = Mock()
        other_input.uuid = "bad-uuid"
        other_input.seq = 1
        other_input.columns = ["col_a"]

        payload = _make_payload(input_uuid="valid-uuid", other_inputs=[other_input])
        target_model = Mock()

        with pytest.raises(HttpError) as excinfo:
            validate_operation_config(payload, target_model, is_multi_input_op=True)
        assert excinfo.value.status_code == 404

    @patch("ddpui.core.transformfunctions.OrgDbtModel.objects")
    def test_multi_input_success(self, mock_objects):
        """Multi-input operation succeeds with multiple inputs."""
        primary_model = _make_orgdbtmodel(name="primary")
        other_model = _make_orgdbtmodel(name="other", type="model", source_name="src2")

        mock_objects.filter.return_value.first.side_effect = [primary_model, other_model]

        other_input = Mock()
        other_input.uuid = "other-uuid"
        other_input.seq = 2
        other_input.columns = ["col_x"]

        payload = _make_payload(
            input_uuid="primary-uuid",
            config={"join": "left"},
            op_type="join",
            source_columns=["col1"],
            other_inputs=[other_input],
        )
        target_model = Mock()

        result_config, all_inputs = validate_operation_config(
            payload, target_model, is_multi_input_op=True
        )

        assert len(all_inputs) == 2
        assert all_inputs[0].name == "primary"
        assert all_inputs[1].name == "other"
        assert len(result_config["config"]["other_inputs"]) == 1
        assert result_config["config"]["other_inputs"][0]["input"]["input_name"] == "other"
        assert result_config["config"]["other_inputs"][0]["seq"] == 2
        assert len(result_config["input_models"]) == 2

    @patch("ddpui.core.transformfunctions.OrgDbtModel.objects")
    def test_chained_operation_no_input_uuid_required(self, mock_objects):
        """When operation_chained_before > 0, input_uuid is NOT required."""
        payload = _make_payload(
            input_uuid="",
            config={"key": "val"},
            op_type="cast",
            source_columns=["c1"],
        )
        target_model = Mock()

        result_config, all_inputs = validate_operation_config(
            payload, target_model, is_multi_input_op=False, operation_chained_before=1
        )

        # Should succeed with no primary input model
        assert result_config["type"] == "cast"
        assert all_inputs == []
        assert len(result_config["input_models"]) == 0

    @patch("ddpui.core.transformfunctions.OrgDbtModel.objects")
    def test_multi_input_sorts_other_inputs_by_seq(self, mock_objects):
        """Verifies that other_inputs are sorted by seq."""
        primary_model = _make_orgdbtmodel(name="primary")
        model_a = _make_orgdbtmodel(name="model_a", source_name="src_a")
        model_b = _make_orgdbtmodel(name="model_b", source_name="src_b")

        mock_objects.filter.return_value.first.side_effect = [primary_model, model_a, model_b]

        other_input_b = Mock()
        other_input_b.uuid = "uuid-b"
        other_input_b.seq = 2
        other_input_b.columns = ["col_b"]

        other_input_a = Mock()
        other_input_a.uuid = "uuid-a"
        other_input_a.seq = 1
        other_input_a.columns = ["col_a"]

        # Pass in reverse order
        payload = _make_payload(
            input_uuid="primary-uuid",
            config={"type": "union"},
            op_type="union",
            source_columns=["c"],
            other_inputs=[other_input_b, other_input_a],
        )
        target_model = Mock()

        result_config, all_inputs = validate_operation_config(
            payload, target_model, is_multi_input_op=True
        )

        # The other_inputs should be ordered by seq after sort
        assert result_config["config"]["other_inputs"][0]["seq"] == 1
        assert result_config["config"]["other_inputs"][1]["seq"] == 2

    @patch("ddpui.core.transformfunctions.OrgDbtModel.objects")
    def test_edit_flag_passes(self, mock_objects):
        """Passing edit=True should not affect validation logic."""
        model = _make_orgdbtmodel()
        mock_objects.filter.return_value.first.return_value = model

        payload = _make_payload(
            input_uuid="valid-uuid",
            config={"key": "val"},
            op_type="rename",
            source_columns=["c1"],
        )
        target_model = Mock()

        result_config, all_inputs = validate_operation_config(
            payload, target_model, is_multi_input_op=False, edit=True
        )

        assert result_config["type"] == "rename"
        assert all_inputs == [model]
