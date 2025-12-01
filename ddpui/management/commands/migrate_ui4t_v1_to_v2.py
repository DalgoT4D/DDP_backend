"""
Django management command to migrate UI4T v1 architecture to v2.
Migrates only connected nodes (with edges) from OrgDbtModel/OrgDbtOperation to CanvasNode/CanvasEdge.
"""

import uuid
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q
from ddpui.models.dbt_workflow import DbtEdge, OrgDbtModel, OrgDbtOperation
from ddpui.models.canvas_models import CanvasNode, CanvasEdge, CanvasNodeType
from ddpui.models.org import Org, OrgDbt, OrgWarehouse
from ddpui.schemas.dbt_workflow_schema import SequencedNode
from ddpui.core import dbtautomation_service


class Command(BaseCommand):
    help = "Migrate UI4T v1 data (OrgDbtModel, OrgDbtOperation, DbtEdge) to v2 (CanvasNode, CanvasEdge)"

    def add_arguments(self, parser):
        parser.add_argument("--org", type=str, required=True, help="Organization slug to migrate")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Run migration in dry-run mode (no database changes)",
        )

    def handle(self, *args, **options):
        org_slug = options["org"]
        dry_run = options["dry_run"]

        # Get organization
        try:
            org = Org.objects.get(slug=org_slug)
        except Org.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Organization with slug '{org_slug}' not found"))
            return

        # Get warehouse
        try:
            warehouse = OrgWarehouse.objects.get(org=org)
        except OrgWarehouse.DoesNotExist:
            self.stdout.write(
                self.style.ERROR(f"No warehouse configured for organization '{org_slug}'")
            )
            return

        # Get OrgDbt
        try:
            orgdbt = org.dbt
        except OrgDbt.DoesNotExist:
            self.stdout.write(
                self.style.ERROR(f"No local DBT workspace found for organization '{org_slug}'")
            )
            return

        self.stdout.write(f"Starting migration for organization: {org.name} ({org_slug})")
        if dry_run:
            self.stdout.write(
                self.style.WARNING("Running in DRY-RUN mode - no changes will be made")
            )

        # Check for existing v2 data
        existing_canvas_nodes = CanvasNode.objects.filter(orgdbt=orgdbt).count()
        existing_canvas_edges = CanvasEdge.objects.filter(from_node__orgdbt=orgdbt).count()

        if existing_canvas_nodes > 0 or existing_canvas_edges > 0:
            self.stdout.write(
                self.style.WARNING(
                    f"Found existing v2 data: {existing_canvas_nodes} CanvasNodes, "
                    f"{existing_canvas_edges} CanvasEdges"
                )
            )
            response = input("Continue with migration? This may create duplicates. (y/N): ")
            if response.lower() != "y":
                self.stdout.write("Migration cancelled")
                return

        try:
            with transaction.atomic():
                # population outcols for orgdbtmodels if not already present
                self._sync_output_cols_for_orgdbtmodels(warehouse, orgdbt)

                # Perform migration
                self._migrate_data(orgdbt)

                # handle the operations whose schema has been changed
                self._handle_breaking_operations(orgdbt)

                if dry_run:
                    # Rollback transaction in dry-run mode
                    transaction.set_rollback(True)
                    self.stdout.write(
                        self.style.WARNING("Dry-run complete - no changes were saved")
                    )

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Migration failed: {str(e)}"))
            raise

    def _sync_output_cols_for_orgdbtmodels(self, warehouse, orgdbt):
        """Populate output_cols for OrgDbtModels if not already set."""

        self.stdout.write("Syncing output_cols for OrgDbtModels...")
        for model in OrgDbtModel.objects.filter(orgdbt=orgdbt):
            if model.output_cols and len(model.output_cols) > 0:
                continue  # already populated

            # Logic to populate output_cols can be customized as needed
            try:
                model.output_cols = dbtautomation_service.update_output_cols_of_dbt_model(
                    warehouse, model
                )
                model.save()
                self.stdout.write(f"Populated output_cols for model: {model.name}")
            except Exception as e:
                self.stderr.write(
                    f"Failed to populate output_cols for model {model.name}: {str(e)}"
                )
                continue
            self.stdout.write(f"Populated output_cols for model: {model.name}")

    def _migrate_data(self, orgdbt):
        """Perform the actual migration of data from v1 to v2 using edge-first approach."""
        # Step 1: Get all edges for this orgdbt
        self.stdout.write("Step 1: Processing edges and building graph...")
        old_edges = DbtEdge.objects.filter(
            Q(from_node__orgdbt=orgdbt) | Q(to_node__orgdbt=orgdbt)
        ).select_related("from_node", "to_node")

        # Step 2: Process each edge
        processed_old_operation_nodes = set()
        for edge_num, edge in enumerate(old_edges, 1):
            self.stdout.write(
                f"\nProcessing edge {edge_num}/{len(old_edges)}: {edge.from_node.name} -> {edge.to_node.name}"
            )

            if edge.from_node.under_construction:
                self.stdout.write(
                    "  Source node is under construction; skipping edges/chain from it"
                )
                continue  # skip edges from under construction nodes

            source_canvas_node = self._create_canvas_node_for_model(edge.from_node, orgdbt)

            # Step 2c: Get all operations for the target model
            target_operations: list[OrgDbtOperation] = OrgDbtOperation.objects.filter(
                dbtmodel=edge.to_node
            ).order_by("seq")

            if target_operations:
                # There are operations between source and target
                self.stdout.write(
                    f"  Found {target_operations.count()} operations for target model"
                )

                # Create CanvasNodes for operations if they don't exist
                prev_op_canvas_node: CanvasNode = source_canvas_node
                for idx, op in enumerate(target_operations):
                    if op.uuid in processed_old_operation_nodes:
                        self.stdout.write(f"OrgDbtOperation {op.uuid} has already been processed")
                        continue

                    op_type = op.config.get("type", "unknown")
                    other_inputs = op.config.get("config", {}).get("other_inputs", [])

                    other_input_model_nodes: list[SequencedNode] = []
                    input_models = op.config.get("input_models", [])
                    for other_input, input_model in zip(other_inputs, input_models[1:]):
                        seq = other_input.get("seq")
                        model_uuid = input_model.get("uuid", "")
                        try:
                            dbtmodel = OrgDbtModel.objects.get(uuid=model_uuid, orgdbt=orgdbt)
                        except OrgDbtModel.DoesNotExist:
                            self.stderr.write(
                                f"Other input model with UUID {model_uuid} not found for operation {op.uuid}"
                            )
                            continue
                        model_canvas_node = self._create_canvas_node_for_model(dbtmodel, orgdbt)
                        other_input_model_nodes.append(
                            SequencedNode(seq=seq, node=model_canvas_node)
                        )

                    # curr op node
                    op_canvas_node = self._create_canvas_node_for_operation(op, orgdbt)
                    self.stdout.write(f"Created operation node: {op_type} (seq: {op.seq})")

                    # create canvas edges
                    # main edge
                    self._create_canvas_edge(prev_op_canvas_node, op_canvas_node, seq=1)

                    # edges for other inputs
                    for other_input_model_node in other_input_model_nodes:
                        self._create_canvas_edge(
                            other_input_model_node.node,
                            op_canvas_node,
                            seq=other_input_model_node.seq,
                        )

                    prev_op_canvas_node = op_canvas_node

                    # create the last edge to target model
                    if idx == len(target_operations) - 1:
                        if not edge.to_node.under_construction:
                            target_canvas_node = self._create_canvas_node_for_model(
                                edge.to_node, orgdbt
                            )

                            self._create_canvas_edge(prev_op_canvas_node, target_canvas_node, seq=1)

                    processed_old_operation_nodes.add(op.uuid)

    def _handle_breaking_operations(self, orgdbt):
        for op_node in CanvasNode.objects.filter(orgdbt=orgdbt):
            if op_node.node_type != CanvasNodeType.OPERATION:
                self.stdout.write("Skipping this canvas node since its not an operation node")
                continue

            operation_config = op_node.operation_config.copy()

            op_type = operation_config.get("type", "unknown")

            op_config = operation_config.get("config", None)

            if not op_config:
                self.stdout.write(f"Config for canvas node {op_node.name}|{op_node.uuid} not found")
                continue

            # update the source columns from the previous node's output
            incoming_edge = CanvasEdge.objects.filter(to_node=op_node, seq=1).first()

            if op_type == "groupby":
                # move source_columns to dimension_columns
                op_config["dimension_columns"] = op_config["source_columns"]

                if incoming_edge:
                    self.stdout.write(
                        "Found the incoming edge; going use its output cols and update source_columns"
                    )
                    op_config["source_columns"] = incoming_edge.from_node.output_cols

            if op_type == "pivot":
                # move source_columns to groupby_columns
                op_config["groupby_columns"] = op_config["source_columns"]

                if incoming_edge:
                    self.stdout.write(
                        "Found the incoming edge; going use its output cols and update source_columns"
                    )
                    op_config["source_columns"] = incoming_edge.from_node.output_cols

            op_node.operation_config = operation_config
            op_node.save()

    def _create_canvas_node_for_model(self, model: OrgDbtModel, orgdbt) -> CanvasNode:
        """Create a CanvasNode for an OrgDbtModel."""
        node = CanvasNode.objects.filter(orgdbt=orgdbt, dbtmodel=model).first()
        if not node:
            node_type = self._determine_node_type(model)
            node = CanvasNode.objects.create(
                orgdbt=orgdbt,
                uuid=uuid.uuid4(),
                node_type=node_type,
                name=model.name,
                output_cols=model.output_cols or [],
                dbtmodel=model,
            )

        return node

    def _create_canvas_node_for_operation(self, operation: OrgDbtOperation, orgdbt):
        """Create a CanvasNode for an OrgDbtOperation."""
        op_type = operation.config.get("type", "unknown")
        op_name = f"op_{operation.uuid}"
        op_config = operation.config.get("config", {})

        return CanvasNode.objects.create(
            orgdbt=orgdbt,
            uuid=uuid.uuid4(),
            node_type=CanvasNodeType.OPERATION,
            name=op_name,
            operation_config={
                "type": op_type,
                "config": op_config,
            },
            output_cols=operation.output_cols or [],
        )

    def _create_canvas_edge(self, from_node: CanvasNode, to_node: CanvasNode, seq=1):
        """Create a CanvasEdge if it doesn't already exist."""
        # Check if edge already exists
        existing_edge = CanvasEdge.objects.filter(from_node=from_node, to_node=to_node).first()

        if not existing_edge:
            return CanvasEdge.objects.create(from_node=from_node, to_node=to_node, seq=seq)
        return existing_edge

    def _determine_node_type(self, model: OrgDbtModel):
        """Determine the CanvasNodeType based on OrgDbtModel type."""
        if model.type == "source":
            return CanvasNodeType.SOURCE
        else:
            return CanvasNodeType.MODEL
