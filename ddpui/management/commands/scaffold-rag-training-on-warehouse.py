import os
from dotenv import load_dotenv
from django.core.management.base import BaseCommand
import psycopg2

from ddpui.models.org import Org
from ddpui.models.org import OrgWarehouse
from ddpui.utils import secretsmanager
from ddpui.utils.custom_logger import CustomLogger
from ddpui.core.warehousefunctions import (
    save_pgvector_creds,
    scaffold_rag_training,
    train_rag_on_warehouse,
)
from ddpui.schemas.warehouse_api_schemas import WarehouseRagTrainConfig, PgVectorCreds

logger = CustomLogger("ddpui")
load_dotenv()


def run_postgres_commands(commands: list, database: str):
    """Runs a list of postgres commands on the given database"""
    try:
        conn = psycopg2.connect(
            user=os.getenv("PGVECTOR_MASTER_USER"),
            password=os.getenv("PGVECTOR_MASTER_PASSWORD"),
            host=os.getenv("PGVECTOR_HOST"),
            port=os.getenv("PGVECTOR_PORT"),
            database=database,
        )
        conn.set_session(autocommit=True)
        with conn.cursor() as curs:
            for command in commands:
                curs.execute(command)
        conn.close()
    except Exception as err:
        conn.close()
        logger.error(f"Error while running the command: {str(err)}")
        raise err


def get_pgvector_creds(org: Org):
    """Returns the pgvector creds for the org"""
    save_pgvector_creds(org, overwrite=True)
    org_pgvector_creds_dict = secretsmanager.retrieve_pgvector_credentials(org)
    return PgVectorCreds(**org_pgvector_creds_dict)


class Command(BaseCommand):
    """This script lets us scaffold various stuff for orgs to use the rag on warehouse for sql generation"""

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
        parser.add_argument(
            "--reset", action="store_true", help="Drops the pgvector database and deletes the user"
        )

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

            # ========== step 1 ==========
            logger.info("***************** fetching pgvector creds for the org *****************")
            org_pgvector_creds = get_pgvector_creds(org)

            if options["reset"]:
                logger.info(
                    "***************** Dropping the vector db and deleting the user *****************"
                )
                try:
                    run_postgres_commands(
                        [
                            f"""
                            REVOKE ALL PRIVILEGES ON SCHEMA public FROM "{org_pgvector_creds.username}"
                            """,
                            f"""
                            REVOKE ALL PRIVILEGES ON DATABASE "{org_pgvector_creds.database}" FROM "{org_pgvector_creds.username}"
                            """,
                            f"""
                            DROP DATABASE IF EXISTS "{org_pgvector_creds.database}"
                            """,
                            f"""
                            DROP USER IF EXISTS "{org_pgvector_creds.username}"
                            """,
                        ],
                        database="postgres",
                    )
                except Exception:
                    pass

                continue

            # ========== step 2 ==========
            logger.info(
                "***************** Creating vector db with user for the org *****************"
            )
            try:
                run_postgres_commands(
                    [
                        f"""
                        CREATE DATABASE "{org_pgvector_creds.database}"
                        """,
                        f"""
                        CREATE USER "{org_pgvector_creds.username}" 
                        WITH ENCRYPTED PASSWORD '{org_pgvector_creds.password}' 
                        """,
                        f"""
                        GRANT ALL PRIVILEGES 
                        ON DATABASE "{org_pgvector_creds.database}" 
                        TO "{org_pgvector_creds.username}"
                        """,
                    ],
                    database="postgres",
                )
            except Exception:
                return

            # ========== step 3 ==========
            logger.info(
                "***************** installing the VECTOR extension on the db *****************"
            )
            try:
                run_postgres_commands(
                    [
                        """
                        CREATE EXTENSION vector
                        """,
                        f"""
                        GRANT ALL ON SCHEMA public TO "{org_pgvector_creds.username}";
                        """,
                    ],
                    database=org_pgvector_creds.database,
                )
            except Exception:
                return

            # ========== step 4 ==========
            logger.info(
                f"""***************** Setting up training plan with & excluding *****************
                    ***************** schemas: {options['exclude_schemas']}     ***************** 
                    ***************** tables: {options['exclude_tables']}       *****************
                    ***************** columns: {options['exclude_columns']}     *****************
                """
            )
            scaffold_rag_training(
                warehouse,
                config=WarehouseRagTrainConfig(
                    exclude_schemas=options["exclude_schemas"],
                    exclude_tables=options["exclude_tables"],
                    exclude_columns=options["exclude_columns"],
                ),
            )
            logger.info(
                f"***************** Finished setting up training config and traning sql for org {org.slug} *****************"
            )

            # ========== step 5 ==========
            logger.info(
                "***************** Starting training according to the plan created above *****************"
            )
            result = train_rag_on_warehouse(warehouse)

            logger.info("***************** Finished training *****************")
            logger.info(result)
