import os
from dotenv import load_dotenv
from django.core.management.base import BaseCommand

from ddpui.models.org import Org
from ddpui.models.org import OrgWarehouse, OrgWarehouseRagTraining
from ddpui.utils import secretsmanager
from ddpui.utils.custom_logger import CustomLogger
from ddpui.core.warehousefunctions import (
    save_pgvector_creds,
    scaffold_rag_training,
    train_rag_on_warehouse,
)
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.datainsights.warehouse.warehouse_interface import WarehouseType
from ddpui.schemas.warehouse_api_schemas import WarehouseRagTrainConfig, PgVectorCreds
from sqlalchemy.sql.expression import text
import psycopg2

logger = CustomLogger("ddpui")
load_dotenv()


class Command(BaseCommand):
    """
    This script lets us scaffold various stuff for orgs to use the rag on warehouse for sql generation
    """

    help = "Estimate time for queued runs"

    def add_arguments(self, parser):
        def list_of_strings(arg: str):
            """converts a comma separated string into a list of strings"""
            return arg.split(",")

        parser.add_argument("org", type=str, help="Org slug")
        parser.add_argument(
            "--exclude_columns",
            type=list_of_strings,
            help="Columns to exclude while training",
            default=[],
        )
        parser.add_argument(
            "--exclude_tables",
            type=list_of_strings,
            help="Tables to exclude while training",
            default=[],
        )
        parser.add_argument(
            "--exclude_schemas",
            type=list_of_strings,
            help="Schemas to exclude while training",
            default=[],
        )

    def create_vector_db_for_org(self, org: Org):
        """
        Creates postgres db (with pgvector extension) for the org and stores the credentials in secrets manager
        Update the reference in Org for the secret name
        """
        load_dotenv()

        logger.info("Creating vector db for the org")
        save_pgvector_creds(org, overwrite=True)
        org_pgvector_creds_dict = secretsmanager.retrieve_pgvector_credentials(org)
        org_pgvector_creds = PgVectorCreds(**org_pgvector_creds_dict)

        master_pgvector_creds = {
            "user": os.getenv("PGVECTOR_MASTER_USER"),
            "password": os.getenv("PGVECTOR_MASTER_PASSWORD"),
            "host": os.getenv("PGVECTOR_HOST"),
            "port": os.getenv("PGVECTOR_PORT"),
            "database": "postgres",
        }

        create_db_stmt = f"""
            CREATE DATABASE "{org_pgvector_creds.database}"
            """

        create_user_stmt = f"""
            CREATE USER "{org_pgvector_creds.username}" with encrypted password '{org_pgvector_creds.password}' 
            """

        grant_priveleges = f"""
            GRANT ALL PRIVILEGES ON database "{org_pgvector_creds.database}" to "{org_pgvector_creds.username}"
            """

        conn = None
        try:
            conn = psycopg2.connect(**master_pgvector_creds)
            conn.set_session(autocommit=True)
            with conn.cursor() as curs:
                curs.execute(create_db_stmt)
                curs.execute(create_user_stmt)
                curs.execute(grant_priveleges)

            if conn:
                conn.close()
            logger.info("Created the db and role with privelges")
        except Exception as err:
            conn.close()
            logger.error("Failed to create or role or priveleges : " + str(err))

        create_vector_ext_statement = f"""
            CREATE EXTENSION vector
            """
        grant_public_schema_usage = f"""
            GRANT ALL ON schema public TO "{org_pgvector_creds.username}";
        """
        try:
            master_pgvector_creds = {
                "user": os.getenv("PGVECTOR_MASTER_USER"),
                "password": os.getenv("PGVECTOR_MASTER_PASSWORD"),
                "host": os.getenv("PGVECTOR_HOST"),
                "port": os.getenv("PGVECTOR_PORT"),
                "database": org_pgvector_creds.database,
            }
            conn = psycopg2.connect(**master_pgvector_creds)
            conn.set_session(autocommit=True)
            with conn.cursor() as curs:
                curs.execute(create_vector_ext_statement)
                curs.execute(grant_public_schema_usage)

            conn.close()
            logger.info(f"Created the vector extions in db {org_pgvector_creds.database}")
        except Exception as err:
            if conn:
                conn.close()
            logger.error("Failed to add extension to db : " + str(err))

    def setup_training_config_for_org(
        self,
        warehouse: OrgWarehouse,
        exclude_tables: list[str] = [],
        exclude_schemas: list[str] = [],
        exclude_columns: list[str] = [],
    ):
        """
        Creates the OrgWarehouseRagTraining for the org
        """
        scaffold_rag_training(
            warehouse,
            config=WarehouseRagTrainConfig(
                exclude_schemas=exclude_schemas,
                exclude_columns=exclude_columns,
                exclude_tables=exclude_tables,
            ),
        )

    def train_the_warehouse(self, warehouse: OrgWarehouse):
        """
        Use llm service and train the warehouse to generate embeddings for the warehouse training plan
        """
        return train_rag_on_warehouse(warehouse)

    def handle(self, *args, **options):
        orgs = Org.objects.all()
        if options["org"] != "all":
            orgs = orgs.filter(slug=options["org"])

        # create training plan of the warehouse
        for org in orgs:
            warehouse = OrgWarehouse.objects.filter(org=org).first()

            if not warehouse:
                logger.error("Looks like warehouse is not setup for the org; skipping")
                continue

            self.create_vector_db_for_org(org)

            logger.info(
                f"Setting up training plan with & excluding schemas: {options['exclude_schemas']} , tables: {options['exclude_tables']}, columns: {options['exclude_columns']}"
            )

            self.setup_training_config_for_org(
                warehouse,
                exclude_columns=options["exclude_columns"],
                exclude_schemas=options["exclude_schemas"],
                exclude_tables=options["exclude_tables"],
            )

            logger.info(f"Finished setting up training config and traning sql for org {org.slug}")

            logger.info("Starting training according to the plan created above")

            result = self.train_the_warehouse(warehouse=warehouse)

            logger.info("Finished training")
            logger.info(result)
