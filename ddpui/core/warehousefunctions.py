import json
import os
from datetime import datetime
from ninja.errors import HttpError


from ddpui.core import dbtautomation_service
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.helpers import convert_to_standard_types
from ddpui.models.org import OrgWarehouse, OrgWarehouseRagTraining, Org
from ddpui.utils.redis_client import RedisClient
from ddpui.utils import secretsmanager
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.schemas.warehouse_api_schemas import WarehouseRagTrainConfig, PgVectorCreds
from ddpui.utils.helpers import generate_hash_id
from ddpui.core import llm_service
import sqlparse
from sqlparse.tokens import Keyword

logger = CustomLogger("ddpui")


def get_warehouse_data(request, data_type: str, **kwargs):
    """
    Fetches data from a warehouse based on the data type
    and optional parameters
    """
    try:
        org_warehouse = kwargs.get("org_warehouse", None)
        if not org_warehouse:
            org_user = request.orguser
            org_warehouse = OrgWarehouse.objects.filter(org=org_user.org).first()

        data = []
        client = dbtautomation_service._get_wclient(org_warehouse)
        if data_type == "tables":
            data = client.get_tables(kwargs["schema_name"])
        elif data_type == "schemas":
            data = client.get_schemas()
        elif data_type == "table_columns":
            data = client.get_table_columns(kwargs["schema_name"], kwargs["table_name"])
        elif data_type == "table_data":
            data = client.get_table_data(
                schema=kwargs["schema_name"],
                table=kwargs["table_name"],
                limit=kwargs["limit"],
                page=kwargs["page"],
                order_by=kwargs["order_by"],
                order=kwargs["order"],
            )
    except Exception as error:
        logger.exception(f"Exception occurred in get_{data_type}: {error}")
        raise HttpError(500, f"Failed to get {data_type}")

    return convert_to_standard_types(data)


def fetch_warehouse_tables(request, org_warehouse, cache_key=None):
    """
    Fetch all the tables from the warehouse
    Cache the results
    """
    res = []
    schemas = get_warehouse_data(request, "schemas", org_warehouse=org_warehouse)
    logger.info(f"Inside helper function for fetching tables : {cache_key}")
    for schema in schemas:
        for table in get_warehouse_data(
            request, "tables", schema_name=schema, org_warehouse=org_warehouse
        ):
            res.append(
                {
                    "schema": schema,
                    "input_name": table,
                    "type": "src_model_node",
                    "id": schema + "-" + table,
                }
            )

    if cache_key:
        RedisClient.get_instance().set(cache_key, json.dumps(res))

    return res


def save_pgvector_creds(org: Org, overwrite=False):
    creds = PgVectorCreds(
        username=org.slug + "_vector",
        port=os.getenv("PGVECTOR_PORT"),
        host=os.getenv("PGVECTOR_HOST"),
        password=generate_hash_id(10),
        database=org.slug + "_vector",
    )

    if org.pgvector_creds:
        logger.info("Pg vector creds already found")
        if overwrite:
            logger.info("Generating new creds and overwriting at the current secret name")
            secretsmanager.update_pgvector_credentials(org, credentials=creds.dict())
    else:
        logger.info("Didn't find any pgvector creds for the org, creating fresh ones")
        secret_name = secretsmanager.save_pgvector_credentials(org, credentials=creds.dict())
        org.pgvector_creds = secret_name
        org.save()

    return org.pgvector_creds


def train_rag_on_warehouse(warehouse: OrgWarehouse) -> dict:
    """Trains the rag for the current warehouse by calling llm service"""
    org: Org = warehouse.org

    # fetch the warehouse creds for the org
    credentials = secretsmanager.retrieve_warehouse_credentials(warehouse)

    # fetch the (pg)vector db creds for the org
    if not org.pgvector_creds:
        raise Exception("Couldn't find the pgvector creds for the org")

    pgvector_creds_dict = secretsmanager.retrieve_pgvector_credentials(org)

    if not pgvector_creds_dict:
        raise Exception("Pg vector creds for the org not created/generated")

    try:
        PgVectorCreds(**pgvector_creds_dict)
    except Exception as err:
        raise Exception("Pg vector creds are not of the right schema" + str(err))

    # fetch the training sql
    rag_training_config = OrgWarehouseRagTraining.objects.filter(warehouse=warehouse).first()
    if not rag_training_config or not rag_training_config.training_sql:
        raise Exception("Rag training sql not found. Please generate to train your warehouse")

    # call the llm service's api to train
    # poll on the llm service
    result = llm_service.train_vanna_on_warehouse(
        training_sql=rag_training_config.training_sql,
        warehouse_creds=credentials,
        pg_vector_creds=pgvector_creds_dict,
        warehouse_type=warehouse.wtype,
    )

    rag_training_config.last_trained_at = datetime.now()
    rag_training_config.last_log = result
    rag_training_config.save()

    return result


def generate_training_sql(warehouse: OrgWarehouse, config: WarehouseRagTrainConfig):
    """Generate or build the training sql (str) that will be used by the rag to generate embeddings"""

    credentials = secretsmanager.retrieve_warehouse_credentials(warehouse)

    wclient = WarehouseFactory.connect(credentials, wtype=warehouse.wtype)

    # look at the exclude json and build a select query of information schema excluding all schemas, tables & columns
    sql = wclient.build_rag_training_sql(
        exclude_schemas=config.exclude_schemas,
        exclude_tables=config.exclude_tables,
        exclude_columns=config.exclude_columns,
    )

    # return the sql
    logger.info(f"Training sql that will be used: {sql}")

    return sql


def scaffold_rag_training(warehouse: OrgWarehouse, config: WarehouseRagTrainConfig):
    """Create/Update the OrgWarehouseRagTraining for the corresponding warehouse"""
    rag_training = OrgWarehouseRagTraining.objects.filter(warehouse=warehouse).first()
    if not rag_training:
        rag_training = OrgWarehouseRagTraining.objects.create(warehouse=warehouse)

    exclude = {}
    if config.exclude_schemas:
        exclude["schemas"] = config.exclude_schemas
    if config.exclude_tables:
        exclude["tables"] = config.exclude_tables
    if config.exclude_columns:
        exclude["columns"] = config.exclude_columns

    rag_training.exclude = exclude
    rag_training.training_sql = generate_training_sql(warehouse, config)

    rag_training.save()


def generate_sql_from_warehouse_rag(warehouse: OrgWarehouse, user_prompt: str) -> dict:
    """Generates the sql from warehouse trained rag using llm service"""
    org: Org = warehouse.org

    # fetch the warehouse creds for the org
    credentials = secretsmanager.retrieve_warehouse_credentials(warehouse)

    # fetch the (pg)vector db creds for the org
    if not org.pgvector_creds:
        raise Exception("Couldn't find the pgvector creds for the org")

    pgvector_creds_dict = secretsmanager.retrieve_pgvector_credentials(org)

    if not pgvector_creds_dict:
        raise Exception("Pg vector creds for the org not created/generated")

    try:
        PgVectorCreds(**pgvector_creds_dict)
    except Exception as err:
        raise Exception("Pg vector creds are not of the right schema" + str(err))

    # check if the rag is trained for the warehouse
    if not llm_service.check_if_rag_is_trained(
        pg_vector_creds=pgvector_creds_dict,
        warehouse_creds=credentials,
        warehouse_type=warehouse.wtype,
    ):
        raise Exception(
            f"Looks like the vector db for the org {warehouse.org} has no embeddings generated. Please perform the training"
        )

    # ask the rag
    sql = llm_service.ask_vanna_for_sql(
        user_prompt=user_prompt,
        pg_vector_creds=pgvector_creds_dict,
        warehouse_creds=credentials,
        warehouse_type=warehouse.wtype,
    )

    return sql


def parse_sql_query_with_limit_offset(sql_query, limit, offset):
    """
    Parse the SQL query and add LIMIT and OFFSET clauses.
    """
    parsed = sqlparse.parse(sql_query)
    statement = parsed[0]

    if not statement.get_type() == "SELECT":
        raise Exception("Only SELECT queries are allowed")

    # Check if LIMIT or OFFSET already exists
    has_limit = any(
        token.ttype is Keyword and token.value.upper() == "LIMIT" for token in statement.tokens
    )
    has_offset = any(
        token.ttype is Keyword and token.value.upper() == "OFFSET" for token in statement.tokens
    )

    if not has_limit:
        statement.tokens.append(sqlparse.sql.Token(Keyword, f" LIMIT {limit}"))
    if not has_offset:
        statement.tokens.append(sqlparse.sql.Token(Keyword, f" OFFSET {offset}"))

    return str(statement)
