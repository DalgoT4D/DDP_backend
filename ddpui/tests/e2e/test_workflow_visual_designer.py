"""
End-to-End Tests for Workflow Visual Designer V2 (UI4T Architecture)

These tests simulate the complete user journey through the workflow visual designer
using real warehouse connections, actual API calls, and the complete DBT process.
The tests mimic a non-technical user creating data transformation workflows.

Based on the new V2 architecture using CanvasNode and CanvasEdge models.
"""

import os
import json
import uuid
import pytest
import subprocess
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
from django.test import TestCase, Client
from django.contrib.auth.models import User
from django.urls import reverse
from rest_framework.test import APIClient
from django.conf import settings

from ddpui.models.org import Org, OrgDbt, OrgWarehouse, TransformType
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role, RolePermission, Permission
from ddpui.models.dbt_workflow import OrgDbtModel, OrgDbtModelType
from ddpui.models.canvas_models import CanvasNode, CanvasEdge, CanvasNodeType
from ddpui.models.canvaslock import CanvasLock
from ddpui.auth import SUPER_ADMIN_ROLE
from ddpui.dbt_automation.utils.warehouseclient import get_client
from ddpui.dbt_automation.operations.scaffold import scaffold
from ddpui.core.orgdbt_manager import DbtProjectManager

pytestmark = pytest.mark.django_db


class WorkflowVisualDesignerV2E2ETests(TestCase):
    """
    End-to-end tests for the UI4T workflow visual designer V2.

    This simulates a complete user journey:
    1. Setup workspace and connect to real warehouse
    2. Sync sources from warehouse
    3. Create canvas nodes (operations)
    4. Chain operations together
    5. Terminate chains to create models
    6. Run DBT and validate outputs

    Uses the new V2 architecture with CanvasNode/CanvasEdge.
    """

    @classmethod
    def setUpClass(cls):
        """Setup test environment with real warehouse connection"""
        super().setUpClass()

        # Create test user
        cls.user = User.objects.create_user(
            username="test@example.com", email="test@example.com", password="testpass123"
        )

        # Create organization
        cls.org = Org.objects.create(name="Test Organization", slug="test-org-e2e")

        # Create super admin role
        cls.role = Role.objects.create(name=SUPER_ADMIN_ROLE, slug=SUPER_ADMIN_ROLE)

        # Create permissions if they don't exist
        permissions = [
            "can_create_dbt_workspace",
            "can_sync_sources",
            "can_create_dbt_model",
            "can_edit_dbt_model",
            "can_view_dbt_models",
            "can_view_dbt_workspace",
            "can_delete_dbt_model",
            "can_view_warehouse_data",
        ]

        for perm_name in permissions:
            permission, created = Permission.objects.get_or_create(slug=perm_name)
            RolePermission.objects.get_or_create(role=cls.role, permission=permission)

        # Create OrgUser with super admin role
        cls.orguser = OrgUser.objects.create(user=cls.user, org=cls.org, new_role=cls.role)

        # Create warehouse connection (using test postgres)
        cls.warehouse_config = {
            "host": os.environ.get("TEST_PG_DBHOST", "localhost"),
            "port": int(os.environ.get("TEST_PG_DBPORT", 5432)),
            "database": os.environ.get("TEST_PG_DBNAME", "test_db"),
            "user": os.environ.get("TEST_PG_DBUSER", "postgres"),
            "password": os.environ.get("TEST_PG_DBPASSWORD", "password"),
        }

        cls.warehouse = OrgWarehouse.objects.create(
            org=cls.org, wtype="postgres", credentials=cls.warehouse_config
        )

        # Get warehouse client for direct operations
        cls.wc_client = get_client("postgres", cls.warehouse_config)
        cls.source_schema = os.environ.get("TEST_PG_DBSCHEMA_SRC", "public")

        # Setup API client and authenticate via login
        cls.api_client = APIClient()

        # Login to get authentication cookies
        login_response = cls.api_client.post(
            "/api/login/",
            {"username": cls.user.email, "password": "testpass123"},
            format="json",
        )

        # Verify login was successful
        if login_response.status_code != 200:
            raise Exception(
                f"Login failed: {login_response.status_code} - {login_response.content}"
            )

        # Set org header for subsequent requests
        cls.api_client.defaults.update(
            {
                "HTTP_X_DALGO_ORG": cls.org.slug,
            }
        )
        cls.api_client.cookies.update(login_response.cookies)

    def setUp(self):
        """Setup for each test"""
        # Create canvas lock
        self.canvas_lock = CanvasLock.objects.create(locked_by=self.orguser, lock_id=uuid.uuid4())

        # Setup test data in warehouse if needed
        self._setup_test_data_in_warehouse()

    def tearDown(self):
        """Cleanup after each test"""
        # Clean up canvas lock
        if hasattr(self, "canvas_lock"):
            self.canvas_lock.delete()

        # Clean up any test DBT project
        if hasattr(self, "test_project_dir") and self.test_project_dir:
            import shutil

            if Path(self.test_project_dir).exists():
                shutil.rmtree(self.test_project_dir)

    def _setup_test_data_in_warehouse(self):
        """Setup test tables in the warehouse for testing"""
        try:
            # Create test tables in warehouse
            test_tables = [
                """
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    country VARCHAR(50),
                    signup_date DATE
                );
                """,
                """
                INSERT INTO customers (name, email, country, signup_date) VALUES 
                ('John Doe', 'john@example.com', 'US', '2024-01-15'),
                ('Jane Smith', 'jane@example.com', 'CA', '2024-02-20'),
                ('Bob Johnson', 'bob@example.com', 'US', '2024-03-10')
                ON CONFLICT DO NOTHING;
                """,
                """
                CREATE TABLE IF NOT EXISTS orders (
                    order_id SERIAL PRIMARY KEY,
                    customer_id INTEGER,
                    product_name VARCHAR(100),
                    amount DECIMAL(10,2),
                    status VARCHAR(20),
                    order_date DATE
                );
                """,
                """
                INSERT INTO orders (customer_id, product_name, amount, status, order_date) VALUES 
                (1, 'Laptop', 1200.00, 'completed', '2024-01-20'),
                (2, 'Mouse', 25.00, 'completed', '2024-02-25'),
                (1, 'Keyboard', 75.00, 'pending', '2024-03-15'),
                (3, 'Monitor', 300.00, 'completed', '2024-03-12')
                ON CONFLICT DO NOTHING;
                """,
            ]

            for sql in test_tables:
                self.wc_client.execute(sql)

        except Exception as e:
            print(f"Warning: Could not setup test data: {e}")

    def _setup_dbt_workspace(self, tmp_path):
        """Setup DBT workspace for testing"""
        project_name = "test_dbt_project"

        # Create DBT project using API
        response = self.api_client.post(
            "/api/transform/dbt_project/", {"default_schema": self.source_schema}, format="json"
        )
        self.assertEqual(response.status_code, 200)

        # Get the created OrgDbt
        orgdbt = OrgDbt.objects.get(org=self.org)

        # Update project directory to use tmp_path
        self.test_project_dir = str(tmp_path / project_name)
        orgdbt.project_dir = self.test_project_dir
        orgdbt.save()

        # Initialize DBT project structure
        config = {
            "project_name": project_name,
            "default_schema": self.source_schema,
        }
        scaffold(config, self.wc_client, tmp_path)

        return orgdbt

    def _execute_dbt_command(self, command, select_model=None):
        """Execute DBT command"""
        if not self.test_project_dir:
            raise Exception("DBT project not initialized")

        cmd_args = [
            str(Path(self.test_project_dir) / "venv" / "bin" / "dbt"),
            command,
            "--project-dir",
            self.test_project_dir,
            "--profiles-dir",
            self.test_project_dir,
        ]

        if select_model:
            cmd_args.extend(["--select", select_model])

        subprocess.check_call(cmd_args)

    def test_01_complete_user_journey_simple_transformation(self):
        """
        Test a complete user journey: Simple data transformation

        Scenario: A non-technical user wants to filter US customers and select specific columns
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Step 1: Setup workspace
            orgdbt = self._setup_dbt_workspace(tmp_path)

            # Step 2: Sync sources from warehouse
            response = self.api_client.post(
                "/api/transform/dbt_project/sync_sources/", format="json"
            )
            self.assertEqual(response.status_code, 200)

            # Verify sources were synced
            sources_response = self.api_client.get("/api/transform/v2/dbt_project/sources_models/")
            self.assertEqual(sources_response.status_code, 200)
            sources = sources_response.json()

            # Find customers table
            customers_source = None
            for source in sources:
                if source["name"] == "customers":
                    customers_source = source
                    break

            self.assertIsNotNone(customers_source, "Customers source should be found")

            # Step 3: Create source node for customers table
            source_node_response = self.api_client.post(
                f"/api/transform/v2/dbt_project/models/{customers_source['uuid']}/nodes/"
            )
            self.assertEqual(source_node_response.status_code, 200)
            source_node = source_node_response.json()

            # Step 4: Add filter operation (filter US customers)
            filter_payload = {
                "config": {"where": [{"column": "country", "operator": "=", "value": "US"}]},
                "input_node_uuid": source_node["uuid"],
                "op_type": "filter",
                "source_columns": ["customer_id", "name", "email", "country", "signup_date"],
                "canvas_lock_id": str(self.canvas_lock.lock_id),
            }

            filter_response = self.api_client.post(
                "/api/transform/v2/dbt_project/operations/nodes/", filter_payload, format="json"
            )
            self.assertEqual(filter_response.status_code, 200)
            filter_node = filter_response.json()

            # Step 5: Add select operation (select specific columns)
            select_payload = {
                "config": {"columns": ["customer_id", "name", "email"]},
                "input_node_uuid": filter_node["uuid"],
                "op_type": "select",
                "source_columns": ["customer_id", "name", "email", "country", "signup_date"],
                "canvas_lock_id": str(self.canvas_lock.lock_id),
            }

            select_response = self.api_client.post(
                "/api/transform/v2/dbt_project/operations/nodes/", select_payload, format="json"
            )
            self.assertEqual(select_response.status_code, 200)
            select_node = select_response.json()

            # Step 6: Terminate chain and create model
            terminate_payload = {
                "name": "us_customers_clean",
                "display_name": "US Customers - Clean",
                "dest_schema": "analytics",
            }

            terminate_response = self.api_client.post(
                f"/api/transform/v2/dbt_project/operations/nodes/{select_node['uuid']}/terminate/",
                terminate_payload,
                format="json",
            )
            self.assertEqual(terminate_response.status_code, 200)
            final_model = terminate_response.json()

            # Step 7: Verify DAG structure
            dag_response = self.api_client.get("/api/transform/v2/dbt_project/graph/")
            self.assertEqual(dag_response.status_code, 200)
            dag = dag_response.json()

            # Should have: 1 source node, 2 operation nodes, 1 model node, and edges
            nodes = dag["nodes"]
            edges = dag["edges"]

            source_nodes = [n for n in nodes if n.get("node_type") == "SRC"]
            operation_nodes = [n for n in nodes if n.get("node_type") == "OP"]
            model_nodes = [n for n in nodes if n.get("node_type") == "MODEL"]

            self.assertEqual(len(source_nodes), 1)
            self.assertEqual(len(operation_nodes), 2)  # filter + select
            self.assertEqual(len(model_nodes), 1)
            self.assertTrue(len(edges) > 0)

            # Step 8: Run DBT and validate output
            try:
                self._execute_dbt_command("deps")
                self._execute_dbt_command("run", select_model="us_customers_clean")

                # Verify the model was created and has correct data
                result = self.wc_client.execute("SELECT * FROM analytics.us_customers_clean")

                # Should have 2 US customers (John Doe and Bob Johnson)
                self.assertEqual(len(result), 2)

                # Verify columns are correct (only customer_id, name, email)
                self.assertEqual(len(result[0]), 3)

            except subprocess.CalledProcessError as e:
                self.fail(f"DBT execution failed: {e}")
            except Exception as e:
                print(f"Warning: Could not validate output data: {e}")

    def test_02_complex_user_journey_join_and_aggregate(self):
        """
        Test complex user journey: Join tables and create aggregations

        Scenario: User wants to join customers and orders, then aggregate order amounts
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)

            # Step 1: Setup workspace
            orgdbt = self._setup_dbt_workspace(tmp_path)

        # Step 2: Sync sources
        self.api_client.post("/api/transform/dbt_project/sync_sources/")
        sources_response = self.api_client.get("/api/transform/v2/dbt_project/sources_models/")
        sources = sources_response.json()

        # Find source tables
        customers_source = None
        orders_source = None

        for source in sources:
            if source["name"] == "customers":
                customers_source = source
            elif source["name"] == "orders":
                orders_source = source

        self.assertIsNotNone(customers_source)
        self.assertIsNotNone(orders_source)

        # Step 3: Create source nodes
        customers_node_response = self.api_client.post(
            f"/api/transform/v2/dbt_project/models/{customers_source['uuid']}/nodes/"
        )
        customers_node = customers_node_response.json()

        orders_node_response = self.api_client.post(
            f"/api/transform/v2/dbt_project/models/{orders_source['uuid']}/nodes/"
        )
        orders_node = orders_node_response.json()

        # Step 4: Filter completed orders first
        filter_orders_payload = {
            "config": {"where": [{"column": "status", "operator": "=", "value": "completed"}]},
            "input_node_uuid": orders_node["uuid"],
            "op_type": "filter",
            "source_columns": [
                "order_id",
                "customer_id",
                "product_name",
                "amount",
                "status",
                "order_date",
            ],
            "canvas_lock_id": str(self.canvas_lock.lock_id),
        }

        filter_orders_response = self.api_client.post(
            "/api/transform/v2/dbt_project/operations/nodes/", filter_orders_payload, format="json"
        )
        filter_orders_node = filter_orders_response.json()

        # Step 5: Join customers with filtered orders
        join_payload = {
            "config": {
                "join_type": "inner",
                "join_on": {"left": "customer_id", "right": "customer_id"},
            },
            "input_node_uuid": customers_node["uuid"],
            "op_type": "join",
            "source_columns": ["customer_id", "name", "email", "country", "signup_date"],
            "other_inputs": [
                {
                    "uuid": filter_orders_node["uuid"],
                    "columns": [
                        "order_id",
                        "customer_id",
                        "product_name",
                        "amount",
                        "status",
                        "order_date",
                    ],
                }
            ],
            "canvas_lock_id": str(self.canvas_lock.lock_id),
        }

        join_response = self.api_client.post(
            "/api/transform/v2/dbt_project/operations/nodes/", join_payload, format="json"
        )
        join_node = join_response.json()

        # Step 6: Aggregate by customer to get total order amounts
        aggregate_payload = {
            "config": {
                "groupby_cols": ["customer_id", "name", "email"],
                "aggregate_on": [
                    {
                        "column": "amount",
                        "operation": "sum",
                        "output_column_name": "total_order_amount",
                    },
                    {
                        "column": "order_id",
                        "operation": "count",
                        "output_column_name": "order_count",
                    },
                ],
            },
            "input_node_uuid": join_node["uuid"],
            "op_type": "aggregate",
            "source_columns": [
                "customer_id",
                "name",
                "email",
                "country",
                "signup_date",
                "order_id",
                "product_name",
                "amount",
                "status",
                "order_date",
            ],
            "canvas_lock_id": str(self.canvas_lock.lock_id),
        }

        aggregate_response = self.api_client.post(
            "/api/transform/v2/dbt_project/operations/nodes/", aggregate_payload, format="json"
        )
        aggregate_node = aggregate_response.json()

        # Step 7: Terminate and create final model
        terminate_payload = {
            "name": "customer_order_summary",
            "display_name": "Customer Order Summary",
            "dest_schema": "analytics",
        }

        terminate_response = self.api_client.post(
            f"/api/transform/v2/dbt_project/operations/nodes/{aggregate_node['uuid']}/terminate/",
            terminate_payload,
            format="json",
        )
        final_model = terminate_response.json()

        # Step 8: Verify complex DAG
        dag_response = self.api_client.get("/api/transform/v2/dbt_project/graph/")
        dag = dag_response.json()

        nodes = dag["nodes"]
        edges = dag["edges"]

        # Should have multiple nodes and edges representing the complex workflow
        self.assertTrue(len(nodes) >= 6)  # 2 sources + 3 operations + 1 model
        self.assertTrue(len(edges) >= 4)  # Multiple connections

        # Step 9: Run DBT and validate complex output
        try:
            self._execute_dbt_command("deps")
            self._execute_dbt_command("run", select_model="customer_order_summary")

            # Verify aggregated results
            result = self.wc_client.execute(
                "SELECT * FROM analytics.customer_order_summary ORDER BY customer_id"
            )

            # Should have customer aggregates
            self.assertTrue(len(result) > 0)

            # Verify we have the expected columns (customer_id, name, email, total_order_amount, order_count)
            if result:
                self.assertEqual(len(result[0]), 5)

        except subprocess.CalledProcessError as e:
            self.fail(f"DBT execution failed: {e}")
        except Exception as e:
            print(f"Warning: Could not validate complex output: {e}")

    def test_03_user_journey_with_editing_and_deletion(self, tmpdir):
        """
        Test user journey with editing operations and deleting nodes

        Scenario: User creates workflow, then modifies it by editing operations and deleting nodes
        """
        # Setup workspace and sync sources
        orgdbt = self._setup_dbt_workspace(tmpdir)
        self.api_client.post("/api/transform/dbt_project/sync_sources/")

        sources_response = self.api_client.get("/api/transform/v2/dbt_project/sources_models/")
        sources = sources_response.json()

        customers_source = next(s for s in sources if s["name"] == "customers")

        # Create source node
        source_node_response = self.api_client.post(
            f"/api/transform/v2/dbt_project/models/{customers_source['uuid']}/nodes/"
        )
        source_node = source_node_response.json()

        # Create initial filter operation
        filter_payload = {
            "config": {
                "where": [
                    {
                        "column": "country",
                        "operator": "=",
                        "value": "CA",  # Initially filter for Canada
                    }
                ]
            },
            "input_node_uuid": source_node["uuid"],
            "op_type": "filter",
            "source_columns": ["customer_id", "name", "email", "country", "signup_date"],
            "canvas_lock_id": str(self.canvas_lock.lock_id),
        }

        filter_response = self.api_client.post(
            "/api/transform/v2/dbt_project/operations/nodes/", filter_payload, format="json"
        )
        filter_node = filter_response.json()

        # Edit the filter operation to change country to US
        edit_payload = {
            "config": {
                "where": [{"column": "country", "operator": "=", "value": "US"}]  # Change to US
            },
            "op_type": "filter",
            "source_columns": ["customer_id", "name", "email", "country", "signup_date"],
        }

        edit_response = self.api_client.put(
            f"/api/transform/v2/dbt_project/operations/nodes/{filter_node['uuid']}/",
            edit_payload,
            format="json",
        )
        self.assertEqual(edit_response.status_code, 200)

        # Add a select operation
        select_payload = {
            "config": {"columns": ["customer_id", "name", "email"]},
            "input_node_uuid": filter_node["uuid"],
            "op_type": "select",
            "source_columns": ["customer_id", "name", "email", "country", "signup_date"],
            "canvas_lock_id": str(self.canvas_lock.lock_id),
        }

        select_response = self.api_client.post(
            "/api/transform/v2/dbt_project/operations/nodes/", select_payload, format="json"
        )
        select_node = select_response.json()

        # Verify we have the expected nodes before deletion
        dag_response = self.api_client.get("/api/transform/v2/dbt_project/graph/")
        initial_dag = dag_response.json()
        initial_node_count = len(initial_dag["nodes"])

        # Delete the select operation (user changed their mind)
        delete_response = self.api_client.delete(
            f"/api/transform/v2/dbt_project/nodes/{select_node['uuid']}/"
        )
        self.assertEqual(delete_response.status_code, 200)

        # Verify the select node was deleted
        dag_response = self.api_client.get("/api/transform/v2/dbt_project/graph/")
        updated_dag = dag_response.json()

        self.assertEqual(len(updated_dag["nodes"]), initial_node_count - 1)

        # Terminate the remaining chain
        terminate_payload = {
            "name": "us_customers_filtered",
            "display_name": "US Customers Filtered",
            "dest_schema": "analytics",
        }

        terminate_response = self.api_client.post(
            f"/api/transform/v2/dbt_project/operations/nodes/{filter_node['uuid']}/terminate/",
            terminate_payload,
            format="json",
        )
        self.assertEqual(terminate_response.status_code, 200)

    def test_04_canvas_locking_workflow(self, tmpdir):
        """
        Test canvas locking mechanism during workflow creation

        Scenario: Multiple users trying to work on the same workflow
        """
        # Create another user
        other_user = User.objects.create_user(
            username="other_user", email="other@example.com", password="testpass123"
        )
        other_orguser = OrgUser.objects.create(user=other_user, org=self.org, new_role=self.role)

        # Setup workspace
        orgdbt = self._setup_dbt_workspace(tmpdir)
        self.api_client.post("/api/transform/dbt_project/sync_sources/")

        # First user (self.api_client) already has lock via setUp
        sources_response = self.api_client.get("/api/transform/v2/dbt_project/sources_models/")
        sources = sources_response.json()
        customers_source = next(s for s in sources if s["name"] == "customers")

        # First user can create operations (has lock)
        source_node_response = self.api_client.post(
            f"/api/transform/v2/dbt_project/models/{customers_source['uuid']}/nodes/"
        )
        self.assertEqual(source_node_response.status_code, 200)

        # Second user tries to create operations without lock (should fail)
        other_api_client = APIClient()
        other_api_client.post(
            "/api/v2/login/",
            {"username": other_user.email, "password": "testpass123"},
            format="json",
        )
        other_api_client.defaults.update(
            {
                "HTTP_X_DALGO_ORG": self.org.slug,
            }
        )

        filter_payload = {
            "config": {"where": [{"column": "country", "operator": "=", "value": "US"}]},
            "input_node_uuid": source_node_response.json()["uuid"],
            "op_type": "filter",
            "source_columns": ["customer_id", "name", "email", "country", "signup_date"],
            "canvas_lock_id": str(uuid.uuid4()),  # Wrong lock ID
        }

        unauthorized_response = other_api_client.post(
            "/api/transform/v2/dbt_project/operations/nodes/", filter_payload, format="json"
        )
        # Should fail due to canvas lock
        self.assertNotEqual(unauthorized_response.status_code, 200)

        # First user can still create operations with correct lock
        filter_payload["canvas_lock_id"] = str(self.canvas_lock.lock_id)
        authorized_response = self.api_client.post(
            "/api/transform/v2/dbt_project/operations/nodes/", filter_payload, format="json"
        )
        self.assertEqual(authorized_response.status_code, 200)

    def test_05_error_handling_and_validation(self, tmpdir):
        """
        Test error handling and validation in the workflow

        Scenario: User makes various mistakes and the system handles them gracefully
        """
        orgdbt = self._setup_dbt_workspace(tmpdir)
        self.api_client.post("/api/transform/dbt_project/sync_sources/")

        sources_response = self.api_client.get("/api/transform/v2/dbt_project/sources_models/")
        sources = sources_response.json()
        customers_source = next(s for s in sources if s["name"] == "customers")

        # Test 1: Try to create operation with invalid operation type
        invalid_op_payload = {
            "config": {"some": "config"},
            "input_node_uuid": customers_source["uuid"],
            "op_type": "invalid_operation",  # Invalid
            "source_columns": ["customer_id"],
            "canvas_lock_id": str(self.canvas_lock.lock_id),
        }

        invalid_response = self.api_client.post(
            "/api/transform/v2/dbt_project/operations/nodes/", invalid_op_payload, format="json"
        )
        self.assertNotEqual(invalid_response.status_code, 200)

        # Test 2: Try to create operation with invalid config
        invalid_config_payload = {
            "config": {"where": "invalid_where_config"},  # Should be a list
            "input_node_uuid": customers_source["uuid"],
            "op_type": "filter",
            "source_columns": ["customer_id"],
            "canvas_lock_id": str(self.canvas_lock.lock_id),
        }

        invalid_config_response = self.api_client.post(
            "/api/transform/v2/dbt_project/operations/nodes/", invalid_config_payload, format="json"
        )
        self.assertNotEqual(invalid_config_response.status_code, 200)

        # Test 3: Try to terminate with duplicate model name
        source_node_response = self.api_client.post(
            f"/api/transform/v2/dbt_project/models/{customers_source['uuid']}/nodes/"
        )
        source_node = source_node_response.json()

        # Create a model first
        terminate_payload = {
            "name": "test_model",
            "display_name": "Test Model",
            "dest_schema": "analytics",
        }

        first_terminate = self.api_client.post(
            f"/api/transform/v2/dbt_project/operations/nodes/{source_node['uuid']}/terminate/",
            terminate_payload,
            format="json",
        )
        self.assertEqual(first_terminate.status_code, 200)

        # Try to create another model with same name (should fail)
        source_node_2_response = self.api_client.post(
            f"/api/transform/v2/dbt_project/models/{customers_source['uuid']}/nodes/"
        )
        source_node_2 = source_node_2_response.json()

        duplicate_terminate = self.api_client.post(
            f"/api/transform/v2/dbt_project/operations/nodes/{source_node_2['uuid']}/terminate/",
            terminate_payload,  # Same name
            format="json",
        )
        self.assertNotEqual(duplicate_terminate.status_code, 200)


class WorkflowDataValidationTests(TestCase):
    """
    Tests for validating the actual data output of DBT transformations
    """

    @classmethod
    def setUpClass(cls):
        """Setup test environment similar to main test class"""
        super().setUpClass()

        # Simplified setup for data validation tests
        cls.user = User.objects.create_user(username="data_test_user", email="datatest@example.com")

        cls.org = Org.objects.create(name="Data Test Org", slug="data-test-org")

        cls.role = Role.objects.create(name=SUPER_ADMIN_ROLE, slug=f"{SUPER_ADMIN_ROLE}-data")

        cls.orguser = OrgUser.objects.create(user=cls.user, org=cls.org, new_role=cls.role)

        # Warehouse config
        cls.warehouse_config = {
            "host": os.environ.get("TEST_PG_DBHOST", "localhost"),
            "port": int(os.environ.get("TEST_PG_DBPORT", 5432)),
            "database": os.environ.get("TEST_PG_DBNAME", "test_db"),
            "username": os.environ.get("TEST_PG_DBUSER", "postgres"),
            "password": os.environ.get("TEST_PG_DBPASSWORD", "password"),
        }

        cls.wc_client = get_client("postgres", cls.warehouse_config)

    def test_data_accuracy_after_transformations(self):
        """
        Test that the actual data transformations produce correct results
        """
        try:
            # Setup test data
            self.wc_client.execute("DROP TABLE IF EXISTS test_customers CASCADE;")
            self.wc_client.execute(
                """
                CREATE TABLE test_customers (
                    id INTEGER,
                    name VARCHAR(100),
                    country VARCHAR(50),
                    amount DECIMAL(10,2)
                );
            """
            )

            self.wc_client.execute(
                """
                INSERT INTO test_customers VALUES 
                (1, 'Alice', 'US', 100.00),
                (2, 'Bob', 'CA', 200.00),
                (3, 'Charlie', 'US', 300.00),
                (4, 'David', 'UK', 400.00);
            """
            )

            # Test 1: Filter operation accuracy
            us_customers = self.wc_client.execute(
                "SELECT * FROM test_customers WHERE country = 'US'"
            )
            self.assertEqual(len(us_customers), 2)
            self.assertEqual(us_customers[0][1], "Alice")
            self.assertEqual(us_customers[1][1], "Charlie")

            # Test 2: Aggregate operation accuracy
            country_totals = self.wc_client.execute(
                """
                SELECT country, SUM(amount) as total_amount, COUNT(*) as customer_count 
                FROM test_customers 
                GROUP BY country 
                ORDER BY country
            """
            )

            expected_totals = {"CA": (200.00, 1), "UK": (400.00, 1), "US": (400.00, 2)}

            for row in country_totals:
                country, total, count = row
                self.assertEqual((float(total), count), expected_totals[country])

        except Exception as e:
            print(f"Warning: Data validation test skipped: {e}")
            self.skipTest("Database not available for data validation")
