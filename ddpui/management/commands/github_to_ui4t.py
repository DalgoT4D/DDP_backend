# management/commands/github_to_ui4t.py --org {org} --file_path {file_path}

from django.core.management.base import BaseCommand
import json
import uuid
from ddpui.models.dbt_workflow import DbtEdge, OrgDbtModel, OrgDbtOperation
from ddpui.models.org import Org, OrgDbt


class Command(BaseCommand):
    help = 'Load DBT manifest data into the database'

    def add_arguments(self, parser):
        parser.add_argument("--org", type=str, help="Org slug")
        parser.add_argument("--file_path", type=str, help='Path to the DBT manifest JSON file')

    def read_manifest_json(self, file_path):
        """
        Reads a JSON file containing a DBT manifest and returns its contents as a dictionary.
        """

        with open(file_path) as f:
            manifest = json.load(f)
        return manifest

    def extract_metadata(self, manifest):
        """
        Extracts metadata from a DBT manifest and returns it as two separate dictionaries.
        """

        models_metadata = {}
        sources_metadata = {}
        column_names = []
        
        for node_key, node_value in manifest['nodes'].items():
            if node_key.startswith('model.'):
                model_metadata = {
                    "name": node_value.get('name'),
                    "schema": node_value.get('schema'),
                    "original_file_path": node_value.get('original_file_path'),
                    "resource_type": node_value.get('resource_type'),
                    "source_name": node_value.get('package_name'),
                    "depends_on": node_value.get('depends_on', {}).get('nodes', []),
                    "columns": [column_name for column_name in node_value.get('columns', {}).keys()]
                }
                models_metadata[node_key] = model_metadata
                column_names.extend(model_metadata['columns'])

        for source_key, source_value in manifest['sources'].items():
            source_metadata = {
                "name": source_value.get('name'),
                "identifier": source_value.get('identifier'),
                "schema": source_value.get('schema'),
                "database": source_value.get('database'),
                "resource_type": source_value.get('resource_type'),
                "package_name": source_value.get('package_name'),
                "source_name": source_value.get('source_name'),
                "path": source_value.get('path'),
            }
            sources_metadata[source_key] = source_metadata
        
        return models_metadata, sources_metadata


    def create_orgdbtmodel_instances(self, org, models_metadata, sources_metadata):
        """
        Creates instances of the OrgDbtModel model based on metadata from a DBT manifest.
        """

        orgdbt = OrgDbt.objects.get(org=org)
        for model_metadata in models_metadata.values():
            if 'name' in model_metadata and 'original_file_path' in model_metadata and 'schema' in model_metadata and 'resource_type' in model_metadata:
                orgdbt_model = OrgDbtModel(
                    uuid=uuid.uuid4(),
                    orgdbt=orgdbt,
                    name=model_metadata['name'],
                    sql_path=model_metadata['original_file_path'],
                    schema=model_metadata['schema'],
                    type=model_metadata['resource_type'],
                    source_name=model_metadata['schema'],
                    output_cols=model_metadata['columns']
                )
                orgdbt_model.save()
        
        for source_metadata in sources_metadata.values():
            if 'name' in source_metadata and 'schema' in source_metadata and 'database' in source_metadata and 'resource_type' in source_metadata:
                orgdbt_model = OrgDbtModel(
                    uuid=uuid.uuid4(),
                    orgdbt=orgdbt,
                    name=source_metadata['identifier'],
                    display_name=source_metadata['name'],
                    schema=source_metadata['schema'],
                    type=source_metadata['resource_type'],
                    source_name=source_metadata['source_name'],
                    sql_path=source_metadata['path'],
                )
                orgdbt_model.save()

    def create_dbtedge_instances(self, metadata):
        """
        Creates instances of the DbtEdge model based on metadata from a DBT manifest.
        """

        for model_metadata in metadata.values():
            orgdbt_model = OrgDbtModel.objects.get(name=model_metadata['name'], schema=model_metadata['schema'])
            for dependency in model_metadata.get('depends_on', []):
                if dependency.startswith('source.'):
                    _, dep_source, _, dep_name = dependency.split('.', 3)
                    parent_model = OrgDbtModel.objects.get(display_name=dep_name, type='source')
                elif dependency.startswith('model.'):
                    _, dep_source, dep_name = dependency.split('.', 2)
                    parent_model = OrgDbtModel.objects.get(name=dep_name, type='model')
                else:
                    continue
                dbt_edge = DbtEdge(
                    from_node=parent_model,
                    to_node=orgdbt_model
                )
                dbt_edge.save()

    def create_orgdbtoperation_instances(self, metadata):
        """
        Creates instances of the OrgDbtOperation model based on metadata from a DBT manifest.
        """

        for model_metadata in metadata.values():
            orgdbt_model = OrgDbtModel.objects.get(name=model_metadata['name'], schema=model_metadata['schema'])
            
            parent_model = None
            for dependency in model_metadata.get('depends_on', []):
                if dependency.startswith('source.'):
                    _, dep_source, _, dep_name = dependency.split('.', 3)
                    parent_model = OrgDbtModel.objects.get(display_name=dep_name, type='source')
                elif dependency.startswith('model.'):
                    _, dep_source, dep_name = dependency.split('.', 2)
                    parent_model = OrgDbtModel.objects.get(name=dep_name, type='model')
                else:
                    continue

            orgdbt_operation = OrgDbtOperation.objects.create(
                dbtmodel=orgdbt_model,
                uuid=uuid.uuid4(),
                seq=1,
                output_cols=model_metadata.get('columns', []),
                config={
                    'config': {
                        'op_config': 'op_type',
                        'input': None,
                        "other_inputs": {
                            "source_columns": model_metadata.get('columns', []),
                        }
                    },
                    'type': 'new_op_type',
                    'input_models': [{
                        'uuid': str(parent_model.uuid),
                        "name": parent_model.name,
                        "source_name": parent_model.source_name,
                        "schema": parent_model.schema,
                        "type": parent_model.type
                    }],
                },
            )
            orgdbt_operation.save()

    def handle(self, *args, **options):
        """
        Loads DBT manifest data into the database for a specified organization.
        """

        file_path = options['file_path']
        manifest_data = self.read_manifest_json(file_path)
        models_metadata, sources_metadata = self.extract_metadata(manifest_data)
        org = Org.objects.filter(slug=options["org"]).first()
        if org is None:
            print(f"Org with slug {options['org']} does not exist")
            return
        
        self.create_orgdbtmodel_instances(org, models_metadata, sources_metadata)
        self.create_dbtedge_instances(models_metadata)
        self.create_orgdbtoperation_instances(models_metadata)
        self.stdout.write(self.style.SUCCESS('DBT manifest data loaded successfully'))