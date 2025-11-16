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
from ddpui.models.org import Org, OrgDbt


class Command(BaseCommand):
    help = "Migrate UI4T v1 data (OrgDbtModel, OrgDbtOperation, DbtEdge) to v2 (CanvasNode, CanvasEdge)"

    def add_arguments(self, parser):
        parser.add_argument("--org", type=str, required=True, help="Organization slug to migrate")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Run migration in dry-run mode (no database changes)",
        )
        parser.add_argument(
            "--verbose", action="store_true", help="Show detailed migration progress"
        )

    def handle(self, *args, **options):
        org_slug = options["org"]
        dry_run = options["dry_run"]
        verbose = options["verbose"]

        # Get organization
        try:
            org = Org.objects.get(slug=org_slug)
        except Org.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Organization with slug '{org_slug}' not found"))
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
                # Perform migration
                stats = self._migrate_data(orgdbt, verbose)

                # Display results
                self._display_results(stats, dry_run)

                if dry_run:
                    # Rollback transaction in dry-run mode
                    transaction.set_rollback(True)
                    self.stdout.write(
                        self.style.WARNING("Dry-run complete - no changes were saved")
                    )

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Migration failed: {str(e)}"))
            raise

    def _migrate_data(self, orgdbt, verbose):
        """Perform the actual migration of data from v1 to v2 using edge-first approach."""
        stats = {
            "total_edges": 0,
            "migrated_models": 0,
            "migrated_operations": 0,
            "created_canvas_nodes": 0,
            "created_canvas_edges": 0,
            "skipped_edges": 0,
        }

        # Mapping dictionaries to track old to new
        model_to_canvas_node_map = {}  # OrgDbtModel.id -> CanvasNode
        operation_to_canvas_node_map = {}  # OrgDbtOperation.id -> CanvasNode

        # Step 1: Get all edges for this orgdbt
        self.stdout.write("Step 1: Processing edges and building graph...")
        edges = DbtEdge.objects.filter(
            Q(from_node__orgdbt=orgdbt) | Q(to_node__orgdbt=orgdbt)
        ).select_related("from_node", "to_node")

        stats["total_edges"] = edges.count()

        if verbose:
            self.stdout.write(f"  Found {stats['total_edges']} edges to process")

        # Step 2: Process each edge
        for edge_num, edge in enumerate(edges, 1):
            if verbose:
                self.stdout.write(
                    f"\nProcessing edge {edge_num}/{stats['total_edges']}: {edge.from_node.name} -> {edge.to_node.name}"
                )

            # Step 2a: Create CanvasNode for source if doesn't exist
            if edge.from_node.id not in model_to_canvas_node_map:
                source_canvas_node = self._create_canvas_node_for_model(edge.from_node, orgdbt)
                model_to_canvas_node_map[edge.from_node.id] = source_canvas_node
                stats["migrated_models"] += 1
                stats["created_canvas_nodes"] += 1
                if verbose:
                    self.stdout.write(f"  Created source node: {edge.from_node.name}")
            else:
                source_canvas_node = model_to_canvas_node_map[edge.from_node.id]

            # Step 2b: Create CanvasNode for target if doesn't exist
            if edge.to_node.id not in model_to_canvas_node_map:
                target_canvas_node = self._create_canvas_node_for_model(edge.to_node, orgdbt)
                model_to_canvas_node_map[edge.to_node.id] = target_canvas_node
                stats["migrated_models"] += 1
                stats["created_canvas_nodes"] += 1
                if verbose:
                    self.stdout.write(f"  Created target node: {edge.to_node.name}")
            else:
                target_canvas_node = model_to_canvas_node_map[edge.to_node.id]

            # Step 2c: Get all operations for the target model
            target_operations = OrgDbtOperation.objects.filter(dbtmodel=edge.to_node).order_by(
                "seq"
            )

            if target_operations.exists():
                # There are operations between source and target
                if verbose:
                    self.stdout.write(
                        f"  Found {target_operations.count()} operations for target model"
                    )

                # Create CanvasNodes for operations if they don't exist
                operation_nodes = []
                for op in target_operations:
                    if op.id not in operation_to_canvas_node_map:
                        op_canvas_node = self._create_canvas_node_for_operation(op, orgdbt)
                        operation_to_canvas_node_map[op.id] = op_canvas_node
                        stats["migrated_operations"] += 1
                        stats["created_canvas_nodes"] += 1
                        if verbose:
                            op_type = op.config.get("type", "unknown")
                            self.stdout.write(
                                f"    Created operation node: {op_type} (seq: {op.seq})"
                            )
                    else:
                        op_canvas_node = operation_to_canvas_node_map[op.id]
                    operation_nodes.append((op.seq, op_canvas_node))

                # Sort operations by sequence
                operation_nodes.sort(key=lambda x: x[0])

                # Step 2d: Create edges maintaining operation sequence
                # First edge: source -> first operation
                first_op_node = operation_nodes[0][1]
                self._create_canvas_edge(source_canvas_node, first_op_node, seq=1)
                stats["created_canvas_edges"] += 1
                if verbose:
                    self.stdout.write(f"    Created edge: source -> operation 1")

                # Intermediate edges: operation -> operation
                for i in range(len(operation_nodes) - 1):
                    from_op = operation_nodes[i][1]
                    to_op = operation_nodes[i + 1][1]
                    seq = operation_nodes[i + 1][0]  # Use the seq from the next operation
                    self._create_canvas_edge(from_op, to_op, seq=seq)
                    stats["created_canvas_edges"] += 1
                    if verbose:
                        self.stdout.write(f"    Created edge: operation {i+1} -> operation {i+2}")

                # Last edge: last operation -> target model
                last_op_node = operation_nodes[-1][1]
                self._create_canvas_edge(
                    last_op_node, target_canvas_node, seq=len(operation_nodes) + 1
                )
                stats["created_canvas_edges"] += 1
                if verbose:
                    self.stdout.write(
                        f"    Created edge: operation {len(operation_nodes)} -> target"
                    )
            else:
                # No operations, direct edge from source to target
                self._create_canvas_edge(source_canvas_node, target_canvas_node, seq=edge.seq or 1)
                stats["created_canvas_edges"] += 1
                if verbose:
                    self.stdout.write(f"  Created direct edge: source -> target")

        # Step 3: Handle operations that might have multiple input models (join, union)
        self.stdout.write("\nStep 3: Processing multi-input operations...")
        multi_input_operations = OrgDbtOperation.objects.filter(
            id__in=operation_to_canvas_node_map.keys()
        ).filter(config__type__in=["join", "unionall"])

        for op in multi_input_operations:
            op_canvas_node = operation_to_canvas_node_map[op.id]
            input_models_data = op.config.get("input_models", [])

            if verbose and len(input_models_data) > 1:
                self.stdout.write(f"  Processing multi-input operation: {op.config.get('type')}")

            # Create edges from all input models to this operation
            for idx, input_data in enumerate(input_models_data):
                input_uuid = input_data.get("uuid")
                if input_uuid:
                    try:
                        input_model = OrgDbtModel.objects.get(uuid=input_uuid)
                        if input_model.id in model_to_canvas_node_map:
                            input_canvas_node = model_to_canvas_node_map[input_model.id]
                            # Check if edge already exists
                            if not CanvasEdge.objects.filter(
                                from_node=input_canvas_node, to_node=op_canvas_node
                            ).exists():
                                self._create_canvas_edge(
                                    input_canvas_node, op_canvas_node, seq=idx + 1
                                )
                                stats["created_canvas_edges"] += 1
                                if verbose:
                                    self.stdout.write(
                                        f"    Connected additional input: {input_model.name}"
                                    )
                    except OrgDbtModel.DoesNotExist:
                        if verbose:
                            self.stdout.write(
                                self.style.WARNING(f"    Input model {input_uuid} not found")
                            )

        return stats

    def _create_canvas_node_for_model(self, model: OrgDbtModel, orgdbt):
        """Create a CanvasNode for an OrgDbtModel."""
        node_type = self._determine_node_type(model)
        return CanvasNode.objects.create(
            orgdbt=orgdbt,
            uuid=uuid.uuid4(),
            node_type=node_type,
            name=model.name,
            output_cols=model.output_cols or [],
            dbtmodel=model,
        )

    def _create_canvas_node_for_operation(self, operation: OrgDbtOperation, orgdbt):
        """Create a CanvasNode for an OrgDbtOperation."""
        op_type = operation.config.get("type", "unknown")
        op_name = f"op_{operation.uuid}"

        return CanvasNode.objects.create(
            orgdbt=orgdbt,
            uuid=uuid.uuid4(),
            node_type=CanvasNodeType.OPERATION,
            name=op_name,
            operation_config={
                "operation_type": op_type,
                "config": operation.config.get("config", {}),
                "input_models": operation.config.get("input_models", []),
                "seq": operation.seq,
                "original_uuid": str(operation.uuid),
                "original_model_id": (str(operation.dbtmodel.uuid) if operation.dbtmodel else None),
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

    def _determine_node_type(self, model):
        """Determine the CanvasNodeType based on OrgDbtModel type."""
        if model.type == "source":
            return CanvasNodeType.SOURCE
        else:
            return CanvasNodeType.MODEL

    def _display_results(self, stats, dry_run):
        """Display migration results."""
        self.stdout.write("\n" + "=" * 50)
        self.stdout.write("Migration Summary:")
        self.stdout.write("=" * 50)
        self.stdout.write(f"Total DbtEdges processed: {stats['total_edges']}")
        self.stdout.write(f"Migrated models: {stats['migrated_models']}")
        self.stdout.write(f"Migrated operations: {stats['migrated_operations']}")
        self.stdout.write(f"Created CanvasNodes: {stats['created_canvas_nodes']}")
        self.stdout.write(f"Created CanvasEdges: {stats['created_canvas_edges']}")

        if not dry_run and stats["created_canvas_nodes"] > 0:
            self.stdout.write(self.style.SUCCESS("\n✓ Migration completed successfully!"))
        elif dry_run:
            self.stdout.write(self.style.WARNING("\nDry-run completed - review the summary above"))
        else:
            self.stdout.write(self.style.WARNING("\nNo data was migrated"))
