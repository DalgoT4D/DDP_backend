import os
from unittest.mock import Mock, patch, MagicMock
import pytest
from ninja.errors import HttpError

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from ddpui.core.visualizationfunctions import generate_chart_data

pytestmark = pytest.mark.django_db


@pytest.fixture
def mock_org_warehouse():
    wh = Mock()
    wh.wtype = "postgres"
    return wh


class TestGenerateChartData:
    """Tests for generate_chart_data"""

    @patch("ddpui.core.visualizationfunctions.WarehouseFactory")
    @patch("ddpui.core.visualizationfunctions.secretsmanager")
    def test_generate_chart_data_success(
        self, mock_secretsmanager, mock_factory, mock_org_warehouse
    ):
        """Test successful chart data generation."""
        mock_secretsmanager.retrieve_warehouse_credentials.return_value = {"host": "localhost"}

        mock_wclient = Mock()
        mock_engine = Mock()
        mock_wclient.engine = mock_engine
        mock_factory.connect.return_value = mock_wclient

        # Mock rows returned by warehouse execute
        mock_wclient.execute.return_value = [
            {"city": "NY", "count": 10},
            {"city": "LA", "count": 20},
        ]

        result = generate_chart_data(
            org_warehouse=mock_org_warehouse,
            schema_name="public",
            table_name="sales",
            xaxis_col="city",
            yaxis_col="count",
            offset=0,
            limit=10,
        )

        assert "xaxis_data" in result
        assert "yaxis_data" in result
        assert result["xaxis_data"]["city"] == ["NY", "LA"]
        assert result["yaxis_data"]["count"] == [10, 20]

    @patch("ddpui.core.visualizationfunctions.WarehouseFactory")
    @patch("ddpui.core.visualizationfunctions.secretsmanager")
    def test_generate_chart_data_empty_rows(
        self, mock_secretsmanager, mock_factory, mock_org_warehouse
    ):
        """Test chart data generation with no rows returned."""
        mock_secretsmanager.retrieve_warehouse_credentials.return_value = {"host": "localhost"}

        mock_wclient = Mock()
        mock_wclient.engine = Mock()
        mock_factory.connect.return_value = mock_wclient
        mock_wclient.execute.return_value = []

        result = generate_chart_data(
            org_warehouse=mock_org_warehouse,
            schema_name="public",
            table_name="empty_table",
            xaxis_col="x",
            yaxis_col="y",
        )

        assert result["xaxis_data"]["x"] == []
        assert result["yaxis_data"]["y"] == []

    @patch("ddpui.core.visualizationfunctions.WarehouseFactory")
    @patch("ddpui.core.visualizationfunctions.secretsmanager")
    def test_generate_chart_data_warehouse_error(
        self, mock_secretsmanager, mock_factory, mock_org_warehouse
    ):
        """Test that warehouse errors are wrapped in HttpError 500."""
        mock_secretsmanager.retrieve_warehouse_credentials.return_value = {"host": "localhost"}
        mock_factory.connect.side_effect = Exception("Connection refused")

        with pytest.raises(HttpError) as excinfo:
            generate_chart_data(
                org_warehouse=mock_org_warehouse,
                schema_name="public",
                table_name="table1",
                xaxis_col="x",
                yaxis_col="y",
            )
        assert excinfo.value.status_code == 500
        assert "Failed to fetch chart data" in str(excinfo.value)

    @patch("ddpui.core.visualizationfunctions.WarehouseFactory")
    @patch("ddpui.core.visualizationfunctions.secretsmanager")
    def test_generate_chart_data_credentials_error(
        self, mock_secretsmanager, mock_factory, mock_org_warehouse
    ):
        """Test error when credentials retrieval fails."""
        mock_secretsmanager.retrieve_warehouse_credentials.side_effect = Exception(
            "Secret not found"
        )

        with pytest.raises(HttpError) as excinfo:
            generate_chart_data(
                org_warehouse=mock_org_warehouse,
                schema_name="public",
                table_name="table1",
                xaxis_col="x",
                yaxis_col="y",
            )
        assert excinfo.value.status_code == 500

    @patch("ddpui.core.visualizationfunctions.WarehouseFactory")
    @patch("ddpui.core.visualizationfunctions.secretsmanager")
    def test_generate_chart_data_with_offset_and_limit(
        self, mock_secretsmanager, mock_factory, mock_org_warehouse
    ):
        """Test that offset and limit params are passed through."""
        mock_secretsmanager.retrieve_warehouse_credentials.return_value = {"host": "localhost"}

        mock_wclient = Mock()
        mock_wclient.engine = Mock()
        mock_factory.connect.return_value = mock_wclient
        mock_wclient.execute.return_value = [{"a": 1, "b": 2}]

        result = generate_chart_data(
            org_warehouse=mock_org_warehouse,
            schema_name="analytics",
            table_name="metrics",
            xaxis_col="a",
            yaxis_col="b",
            offset=5,
            limit=3,
        )

        assert result["xaxis_data"]["a"] == [1]
        assert result["yaxis_data"]["b"] == [2]
        mock_wclient.execute.assert_called_once()

    @patch("ddpui.core.visualizationfunctions.WarehouseFactory")
    @patch("ddpui.core.visualizationfunctions.secretsmanager")
    def test_generate_chart_data_execute_error(
        self, mock_secretsmanager, mock_factory, mock_org_warehouse
    ):
        """Test that SQL execution errors raise HttpError."""
        mock_secretsmanager.retrieve_warehouse_credentials.return_value = {"host": "localhost"}

        mock_wclient = Mock()
        mock_wclient.engine = Mock()
        mock_factory.connect.return_value = mock_wclient
        mock_wclient.execute.side_effect = Exception("SQL error")

        with pytest.raises(HttpError) as excinfo:
            generate_chart_data(
                org_warehouse=mock_org_warehouse,
                schema_name="public",
                table_name="broken",
                xaxis_col="x",
                yaxis_col="y",
            )
        assert excinfo.value.status_code == 500
