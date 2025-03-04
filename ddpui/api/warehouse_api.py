import threading
import json
import csv
from io import StringIO
import uuid
import sqlparse
from sqlparse.tokens import Keyword, Token
import sqlalchemy
import sqlalchemy.exc
from sqlalchemy import text
from ninja import Router
from ninja.errors import HttpError

from django.http import StreamingHttpResponse
from ddpui import auth
from ddpui.core import dbtautomation_service
from ddpui.core.warehousefunctions import (
    get_warehouse_data,
    fetch_warehouse_tables,
    train_rag_on_warehouse,
    parse_sql_query_with_limit_offset,
    run_sql_and_fetch_results_from_warehouse,
)
from ddpui.models.org import OrgWarehouse
from ddpui.models.org_user import OrgUser
from ddpui.models.tasks import TaskProgressHashPrefix
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils.taskprogress import TaskProgress
from ddpui.utils.singletaskprogress import SingleTaskProgress
from ddpui.auth import has_permission

from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.datainsights.generate_result import GenerateResult, poll_for_column_insights
from ddpui.celeryworkers.tasks import (
    summarize_warehouse_results,
    generate_sql_from_prompt_asked_on_warehouse,
)

from ddpui.schemas.warehouse_api_schemas import (
    RequestorColumnSchema,
    AskWarehouseRequest,
    SaveLlmSessionRequest,
    LlmSessionFeedbackRequest,
    AskWarehouseRequestv1,
    FetchSqlqueryResults,
    SaveLlmSessionRequestv1,
)
from ddpui.models.llm import (
    LlmSession,
    LlmSessionStatus,
    LlmAssistantType,
)
from ddpui.utils import secretsmanager
from ddpui.utils.constants import LIMIT_ROWS_TO_SEND_TO_LLM
from ddpui.utils.redis_client import RedisClient

warehouse_router = Router()
logger = CustomLogger("ddpui")


@warehouse_router.get("/tables/{schema_name}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def get_table(request, schema_name: str):
    """Fetches table names from a warehouse"""
    return get_warehouse_data(request, "tables", schema_name=schema_name)


@warehouse_router.get("/schemas", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def get_schema(request):
    """Fetches schema names from a warehouse"""
    return get_warehouse_data(request, "schemas")


@warehouse_router.get("/table_columns/{schema_name}/{table_name}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def get_table_columns(request, schema_name: str, table_name: str):
    """Fetches column names for a specific table from a warehouse"""
    return get_warehouse_data(
        request, "table_columns", schema_name=schema_name, table_name=table_name
    )


@warehouse_router.get("/table_data/{schema_name}/{table_name}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def get_table_data(
    request,
    schema_name: str,
    table_name: str,
    page: int = 1,
    limit: int = 10,
    order_by: str = None,
    order: int = 1,
):
    """Fetches data from a specific table in a warehouse"""
    return get_warehouse_data(
        request,
        "table_data",
        schema_name=schema_name,
        table_name=table_name,
        page=page,
        limit=limit,
        order_by=order_by,
        order=order,
    )


@warehouse_router.get("/table_count/{schema_name}/{table_name}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def get_table_count(request, schema_name: str, table_name: str):
    """Fetches the total number of rows for a specified table."""
    try:
        org_user = request.orguser
        org_warehouse = OrgWarehouse.objects.filter(org=org_user.org).first()

        client = dbtautomation_service.get_wclient(org_warehouse)
        total_rows = client.get_total_rows(schema_name, table_name)
        return {"total_rows": total_rows}
    except Exception as e:
        logger.error(f"Failed to fetch total rows for {schema_name}.{table_name}: {e}")
        raise HttpError(500, f"Failed to fetch total rows for {schema_name}.{table_name}")


@warehouse_router.get("/dbt_project/json_columnspec/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def get_json_column_spec(request, source_schema: str, input_name: str, json_column: str):
    """Get the json column spec of a table in a warehouse"""
    orguser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    if not all([source_schema, input_name, json_column]):
        raise HttpError(400, "Missing required parameters")

    json_columnspec = dbtautomation_service.json_columnspec(
        org_warehouse, source_schema, input_name, json_column
    )
    return json_columnspec


@warehouse_router.get("/v1/table_data/{schema_name}/{table_name}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def get_warehouse_table_columns_spec(request, schema_name: str, table_name: str):
    """
    Get the json column(s) spec of a table in a warehouse
    This fetches table data using the sqlalchemy engine client
    """
    orguser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)

    try:
        wclient = WarehouseFactory.connect(credentials, wtype=org_warehouse.wtype)

        cols = wclient.get_table_columns(schema_name, table_name)
        return cols
    except sqlalchemy.exc.NoSuchTableError:
        raise HttpError(404, "Table not found")
    except Exception as err:
        logger.error(err)
        raise HttpError(500, str(err))


@warehouse_router.post("/insights/metrics/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def post_data_insights(request, payload: RequestorColumnSchema):
    """
    Run all the require queries to fetch insights for a column
    Will also save to redis results as queries are processed
    """
    orguser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    try:
        task_id = str(uuid.uuid4())

        taskprogress = TaskProgress(task_id, TaskProgressHashPrefix.DATAINSIGHTS)
        taskprogress.add(
            {
                "message": "Fetching insights",
                "status": GenerateResult.RESULT_STATUS_FETCHING,
                "results": [],
            }
        )

        poll_for_column_insights.delay(org_warehouse.id, payload.dict(), task_id)

        return {"task_id": task_id}
    except Exception as err:
        logger.error(err)
        raise HttpError(500, str(err))


@warehouse_router.get("/download/{schema_name}/{table_name}", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def get_download_warehouse_data(request, schema_name: str, table_name: str):
    """Stream and download data from a table in the warehouse"""

    orguser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    def stream_warehouse_data(
        request, schema_name, table_name, page_size=10, order_by=None, order=1
    ):
        page = 1
        header_written = False
        output = StringIO()
        data: list[dict] = get_warehouse_data(
            request,
            "table_data",
            schema_name=schema_name,
            table_name=table_name,
            page=page,
            limit=page_size,
            order_by=order_by,
            order=order,
        )
        writer = csv.DictWriter(output, fieldnames=data[0].keys() if data else [])
        while len(data) > 0:
            logger.info("Length of data: %s", len(data))
            for i, row in enumerate(data):
                if i == 0 and not header_written:
                    writer.writeheader()
                    header_written = True
                else:
                    writer.writerow(row)

            yield output.getvalue()
            output.truncate(0)
            output.seek(0)
            page += 1
            logger.info(f"Streaming page {page} of {schema_name}.{table_name}")
            data = get_warehouse_data(
                request,
                "table_data",
                schema_name=schema_name,
                table_name=table_name,
                page=page,
                limit=page_size,
                order_by=order_by,
                order=order,
            )
        output.close()

    response = StreamingHttpResponse(
        stream_warehouse_data(request, schema_name, table_name, page_size=30000),
        content_type="application/octet-stream",
    )
    response["Content-Disposition"] = f"attachment; filename={schema_name}__{table_name}.csv"

    return response


@warehouse_router.post("/ask/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def post_warehouse_prompt(request, payload: AskWarehouseRequest):
    """
    Ask the warehouse a question/prompt on a result set and get a response from llm service
    Be default a new session will be saved
    """
    stmts = sqlparse.parse(payload.sql)

    if len(stmts) > 1:
        raise HttpError(400, "Only one query is allowed")

    if len(stmts) == 0:
        raise HttpError(400, "No query provided")

    if not stmts[0].get_type() == "SELECT":
        raise HttpError(400, "Only SELECT queries are allowed")

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    # limit the records going to llm
    limit = float("inf")
    limit_found = False
    for stmt in stmts:
        for token in stmt.tokens:
            if not limit_found and token.ttype is Keyword and token.value.upper() == "LIMIT":
                limit_found = True
            if limit_found and token.ttype is Token.Literal.Number.Integer:
                limit = int(token.value)
                break

    if limit_found and limit > LIMIT_ROWS_TO_SEND_TO_LLM:
        raise HttpError(
            400,
            f"Please make sure the limit in query is less than {LIMIT_ROWS_TO_SEND_TO_LLM}",
        )

    if not limit_found:
        logger.info(f"Setting LIMIT {LIMIT_ROWS_TO_SEND_TO_LLM} to the query")
        payload.sql = f"{payload.sql} LIMIT {LIMIT_ROWS_TO_SEND_TO_LLM}"

    try:
        task = summarize_warehouse_results.apply_async(
            kwargs={
                "orguser_id": orguser.id,
                "org_warehouse_id": org_warehouse.id,
                "sql": payload.sql,
                "user_prompt": payload.user_prompt,
            },
        )

        # set progress in redis to poll on
        SingleTaskProgress(task.id, 60 * 10)

        return {"request_uuid": task.id}
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to summarize warehouse results") from error


@warehouse_router.post("v1/ask/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def post_warehouse_generate_sql(request, payload: AskWarehouseRequestv1):
    """
    Ask the warehouse a question/prompt
    Returns a llm generated "sql" query
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    try:
        task = generate_sql_from_prompt_asked_on_warehouse.apply_async(
            kwargs={
                "orguser_id": orguser.id,
                "org_warehouse_id": org_warehouse.id,
                "user_prompt": payload.user_prompt,
            },
        )

        # set progress in redis to poll on
        SingleTaskProgress(task.id, 60 * 10)

        return {"request_uuid": task.id}
    except Exception as error:
        logger.exception(error)
        raise HttpError(
            400, "failed to enqueue celery task generate_sql_from_prompt_asked_on_warehouse"
        ) from error


@warehouse_router.post("v1/ask/{session_id}/summarize/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def post_summarize_results_from_sql_and_prompt(
    request, session_id: str, payload: AskWarehouseRequest
):
    """
    Use the llm generated (edited) sql and the user prompt to get a summarized answer using file search
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    session = LlmSession.objects.filter(
        session_id=session_id,
        org=org,
        session_type=LlmAssistantType.LONG_TEXT_SUMMARIZATION,
    ).first()

    if not session:
        raise HttpError(404, "Session not found")

    if session.session_status == LlmSessionStatus.RUNNING:
        raise HttpError(422, "Summarization still in progress")

    try:
        task = summarize_warehouse_results.apply_async(
            kwargs={
                "orguser_id": orguser.id,
                "org_warehouse_id": org_warehouse.id,
                "sql": payload.sql,
                "user_prompt": payload.user_prompt,
                "llmsession_pk": session.id,
            },
        )

        # set progress in redis to poll on
        SingleTaskProgress(task.id, 60 * 10)

        return {"request_uuid": task.id}
    except Exception as error:
        logger.exception(error)
        raise HttpError(400, "failed to summarize warehouse results") from error


@warehouse_router.post("/ask/{new_session_id}/save/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def post_save_warehouse_prompt_session(
    request, new_session_id: str, payload: SaveLlmSessionRequest
):
    """Saving the llm session generated from warehouse prompt. Saving here means attaching it to a name"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    new_session = LlmSession.objects.filter(
        session_id=new_session_id,
        org=org,
        session_type=LlmAssistantType.LONG_TEXT_SUMMARIZATION,
    ).first()

    if not new_session:
        raise HttpError(404, "Session not found")

    if payload.overwrite and not payload.old_session_id:
        raise HttpError(400, "session to overwrite is required")

    # delete the old session if overwrite is true
    if payload.overwrite:
        old_session = LlmSession.objects.filter(
            session_id=payload.old_session_id,
            org=org,
            session_type=LlmAssistantType.LONG_TEXT_SUMMARIZATION,
        ).first()
        # since its overwriting the old session, we need to keep/persist the orguser (created_by)
        new_session.orguser = old_session.orguser

        if old_session:
            old_session.delete()
            logger.info(f"Deleted the old session llm analysis {payload.old_session_id}")

    if new_session.session_status == LlmSessionStatus.RUNNING:
        raise HttpError(400, "Session is still in progress")

    new_session.session_name = payload.session_name
    new_session.updated_by = orguser
    new_session.save()

    return {"success": 1}


@warehouse_router.post("/v1/ask/{session_id}/save/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def post_save_warehouse_prompt_session_v1(
    request, session_id: str, payload: SaveLlmSessionRequestv1
):
    """
    Saving the llm session generated from warehouse prompt.
    Saving here means attaching it to a name
    You can also update the sql while saving/overwriting
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    curr_session = LlmSession.objects.filter(
        session_id=session_id,
        org=org,
        session_type=LlmAssistantType.LONG_TEXT_SUMMARIZATION,
    ).first()

    if not curr_session:
        raise HttpError(404, "Session not found")

    if payload.overwrite and not payload.old_session_id:
        raise HttpError(400, "session to overwrite is required")

    if curr_session.session_status == LlmSessionStatus.RUNNING:
        raise HttpError(400, "Session is still in progress")

    # delete the old session if overwrite is true
    if payload.overwrite:
        old_session = LlmSession.objects.filter(
            session_id=payload.old_session_id,
            org=org,
            session_type=LlmAssistantType.LONG_TEXT_SUMMARIZATION,
        ).first()

        if old_session:
            # since its overwriting the old session, we need to keep/persist the orguser (created_by)
            curr_session.orguser = old_session.orguser

            old_session.delete()
            logger.info(f"Deleted the old session llm analysis {payload.old_session_id}")
        else:
            # update the sql or any other meta information
            if payload.sql:
                curr_session.request_meta = {"sql": payload.sql}

    curr_session.session_name = payload.session_name
    curr_session.updated_by = orguser
    curr_session.save()

    return {"success": 1}


@warehouse_router.post("/ask/{session_id}/feedback/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def post_feedback_llm_session(request, session_id: str, payload: LlmSessionFeedbackRequest):
    """Feedback"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    session = LlmSession.objects.filter(
        session_id=session_id,
        org=org,
        session_type=LlmAssistantType.LONG_TEXT_SUMMARIZATION,
    ).first()

    if not session:
        raise HttpError(404, "Session not found")

    session.feedback = payload.feedback
    session.save()

    return {"success": 1}


@warehouse_router.get("/ask/sessions", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def get_warehouse_llm_analysis_sessions(
    request, limit: int = 10, offset: int = 0, version: str = "v0"
):
    """Get all saved sessions with a session_name for the user"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    sessions = (
        LlmSession.objects.filter(
            org=org,
            session_name__isnull=False,  # fetch only saved sessions
            session_type=LlmAssistantType.LONG_TEXT_SUMMARIZATION,
            version=version,
        )
        .select_related("orguser__user")
        .order_by("-updated_at")[offset : offset + limit]
    )

    # can be optimized
    total_count = LlmSession.objects.filter(
        org=org,
        session_name__isnull=False,  # fetch only saved sessions
        session_type=LlmAssistantType.LONG_TEXT_SUMMARIZATION,
    ).count()

    return {
        "limit": limit,
        "offset": offset,
        "total_rows": total_count,
        "rows": [
            {
                "session_id": session.session_id,
                "session_name": session.session_name,
                "session_status": session.session_status,
                "request_uuid": session.request_uuid,
                "request_meta": session.request_meta,
                "assistant_prompt": session.assistant_prompt,
                "response": session.response,
                "created_at": session.created_at,
                "updated_at": session.updated_at,
                "created_by": {
                    "email": session.orguser.user.email,
                },
                "updated_by": (
                    {
                        "email": session.updated_by.user.email,
                    }
                    if session.updated_by
                    else None
                ),
            }
            for session in sessions
        ],
    }


@warehouse_router.get(
    "/sync_tables",
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_view_warehouse_data"])
def get_warehouse_schemas_and_tables(
    request,
):
    """
    Get all tables under all schemas in the warehouse
    Read from warehouse directly if no cache is found
    """
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    res = []
    cache_key = f"{org.slug}_warehouse_tables"
    redis_client = RedisClient.get_instance()
    try:
        # fetch & set response in redis asynchronously
        if redis_client.exists(cache_key):
            logger.info("Fetching warehouse tables from cache")
            res = json.loads(redis_client.get(cache_key))
            threading.Thread(
                target=fetch_warehouse_tables, args=(request, org_warehouse, cache_key)
            ).start()
        else:
            logger.info("Fetching warehouse tables directly")
            res = fetch_warehouse_tables(request, org_warehouse, cache_key)

    except Exception as err:
        logger.error("Failed to fetch data from the warehouse - %s", err)
        raise HttpError(500, "Failed to fetch data from the warehouse") from err

    return res


@warehouse_router.post(
    "/rag/train/",
    auth=auth.CustomAuthMiddleware(),
)
@has_permission(["can_view_warehouse_data"])
def post_train_rag_on_warehouse(request):
    """
    Train the rag on warehouse schema
    This will always refresh the training
    """

    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    try:
        train_rag_on_warehouse(warehouse=org_warehouse)
    except Exception as err:
        logger.error(err)
        raise HttpError(500, str(err))


@warehouse_router.post("/table_data/run_sql/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def post_warehouse_run_sql_query(request, payload: FetchSqlqueryResults):
    """Runs a SQL query against the warehouse and returns the results"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    try:
        # Parse the SQL query and add LIMIT and OFFSET
        sql_query_with_limit_offset = parse_sql_query_with_limit_offset(
            payload.sql, payload.limit, payload.offset
        )

        results = run_sql_and_fetch_results_from_warehouse(
            warehouse=org_warehouse, sql=text(sql_query_with_limit_offset)
        )
        columns = []
        if len(results) > 0:
            columns = list(results[0].keys())

        return {"rows": results, "columns": columns}
    except Exception as err:
        logger.error(err)
        raise HttpError(500, str(err))


@warehouse_router.post("/row_count/sql/", auth=auth.CustomAuthMiddleware())
@has_permission(["can_view_warehouse_data"])
def post_row_count_sql(request, payload: FetchSqlqueryResults):
    """Returns the row count of the result of the given SQL query"""
    orguser: OrgUser = request.orguser
    org = orguser.org

    org_warehouse = OrgWarehouse.objects.filter(org=org).first()
    if not org_warehouse:
        raise HttpError(404, "Please set up your warehouse first")

    try:
        subquery = f"({payload.sql}) AS subquery"
        count_query = text(f"SELECT COUNT(1) AS c FROM {subquery}")
        results = run_sql_and_fetch_results_from_warehouse(warehouse=org_warehouse, sql=count_query)
        return {"row_count": results[0]["c"]}
    except Exception as err:
        logger.error(err)
        raise HttpError(500, str(err))
