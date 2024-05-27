import time
import json
import threading
from datetime import datetime
from sqlalchemy.sql.selectable import Select
from channels.generic.websocket import WebsocketConsumer

from ddpui.celery import app
from ddpui.datainsights.insights.insight_interface import ColInsight
from ddpui.datainsights.insights.insight_factory import InsightsFactory
from ddpui.datainsights.insights.common.base_insights import BaseInsights
from ddpui.datainsights.warehouse.warehouse_interface import Warehouse
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.datainsights.insights.insight_interface import DataTypeColInsights
from ddpui.models.org import Org, OrgWarehouse
from ddpui.utils.taskprogress import TaskProgress
from ddpui.schemas.warehouse_api_schemas import RequestorColumnSchema
from ddpui.models.tasks import TaskProgressHashPrefix
from ddpui.redis_client import RedisClient
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager


logger = CustomLogger("ddpui")


class DataInsightsConsumer(WebsocketConsumer):

    def connect(self):
        logger.info("Trying to establish connection")
        self.accept()
        self.scope["user"] = "ishan"

    def websocket_receive(self, message):
        logger.info("Recieved the message from client")
        # payload = message["text"]
        # self.send(payload)
        payload = json.loads(message["text"])
        task_id = payload.get("task_id")

        if task_id:
            logger.info("Starting to poll for the celery task")
            result = TaskProgress.fetch(
                task_id=task_id, hashkey=TaskProgressHashPrefix.DATAINSIGHTS
            )
            if result:
                result = result[-1]
                while (
                    result
                    and "status" in result
                    and result["status"]
                    not in [
                        GenerateResult.RESULT_STATUS_COMPLETED,
                        GenerateResult.RESULT_STATUS_ERROR,
                    ]
                ):
                    time.sleep(5)
                    result = TaskProgress.fetch(
                        task_id=task_id, hashkey=TaskProgressHashPrefix.DATAINSIGHTS
                    )[-1]
                self.send(
                    json.dumps(
                        {
                            "status": "success",
                            "message": "success",
                            "body": result["results"],
                        }
                    )
                )
            else:
                self.send(
                    json.dumps(
                        {
                            "status": "failed",
                            "message": "task id not found",
                            "body": {},
                        }
                    )
                )

        else:
            self.send(
                json.dumps(
                    {"status": "failed", "message": "task id not provided", "body": {}}
                )
            )


@app.task(bind=True)
def poll_for_column_insights(
    self,
    org_warehouse_id: str,
    requestor_col: dict,
):
    """
    This will help frontend fetch insights for the column provided
    Poll for queries to be run
    Wait for results
    Return results if all queries are completed; else return a status of error
    """
    requestor_col: RequestorColumnSchema = RequestorColumnSchema.parse_obj(
        requestor_col
    )

    taskprogress = TaskProgress(
        self.request.id, TaskProgressHashPrefix.DATAINSIGHTS, 10 * 60
    )
    taskprogress.add(
        {
            "message": "Fetching insights",
            "status": GenerateResult.RESULT_STATUS_FETCHING,
            "results": [],
        }
    )

    org_warehouse = OrgWarehouse.objects.filter(id=org_warehouse_id).first()

    if not org_warehouse:
        logger.error("Warehouse not found")
        taskprogress.add(
            {
                "message": "Warehouse not found",
                "status": GenerateResult.RESULT_STATUS_ERROR,
                "results": [],
            }
        )
        return

    # if the lock is not acquire, then acquire a lock and run the queries for this column
    credentials = secretsmanager.retrieve_warehouse_credentials(org_warehouse)

    wclient = WarehouseFactory.connect(credentials, wtype=org_warehouse.wtype)

    insight_objs = GenerateResult.prepare_insight_objects(wclient, requestor_col)

    GenerateResult.execute_insight_queries(
        org_warehouse.org, wclient, insight_objs, requestor_col
    )

    final_result = GenerateResult.fetch_results(
        org_warehouse.org,
        requestor_col.db_schema,
        requestor_col.db_table,
        requestor_col.column_name,
    )

    if not GenerateResult.validate_results(insight_objs, final_result):
        taskprogress.add(
            {
                "message": "Partial data fetched",
                "status": GenerateResult.RESULT_STATUS_ERROR,
                "results": [],
            }
        )
    else:
        # return the saved results
        taskprogress.add(
            {
                "message": "Fetched results",
                "status": GenerateResult.RESULT_STATUS_COMPLETED,
                "results": final_result,
            }
        )


class GenerateResult:
    """
    Class that generates result by executing the insight(s) queries
    """

    RESULT_STATUS_FETCHING = "fetching"
    RESULT_STATUS_COMPLETED = "completed"
    RESULT_STATUS_ERROR = "completed"

    org_locks: dict[str, threading.Lock] = {}

    @classmethod
    def get_org_lock(cls, org: Org) -> threading.Lock:
        if org.slug not in cls.org_locks:
            cls.org_locks[org.slug] = threading.Lock()
        return cls.org_locks[org.slug]

    @classmethod
    def build_queries_locking_hash(cls, org: Org, query: ColInsight) -> str:
        return f"{org.slug}-{query.db_schema}-{query.db_table}-queries"

    @classmethod
    def execute_insight_queries(
        cls,
        org: Org,
        wclient: Warehouse,
        insights: list[DataTypeColInsights],
        requestor_col: RequestorColumnSchema,
    ) -> dict:
        """
        1. Collates all queries to run
        2. Gets locks on each query before run
        3. If lock is present; skips the query since its already running
        """
        # clear insights based on the refresh flag
        if requestor_col.refresh:
            GenerateResult.clear_insights(
                org,
                requestor_col.db_schema,
                requestor_col.db_table,
                requestor_col.column_name,
            )

        insight_queries: list[ColInsight] = []
        for insight in insights:
            insight_queries += insight.insights

        logger.info(f"Total number of queries to executed {len(insight_queries)}")

        # acquire & run the queries lock for all queries
        for query in insight_queries:
            if cls.acquire_query_lock(org, query):
                # run the queries and save results
                try:
                    stmt = query.generate_sql()
                    stmt = stmt.compile(
                        bind=wclient.engine, compile_kwargs={"literal_binds": True}
                    )
                    results = wclient.execute(stmt)

                    # parse result of this query
                    results = query.parse_results(results)

                    # save result to redis
                    cls.save_results(org, query, results)

                    # release the lock for this query
                    cls.release_query_lock(org, query)
                except Exception as err:
                    logger.error(
                        "Something went wrong while executing the query or saving the results; clearing the lock"
                    )
                    logger.error(err)
                    cls.release_query_lock(org, query)

    @classmethod
    def acquire_query_lock(cls, org: Org, query: ColInsight) -> bool:
        """
        Acquire lock to run the query: return True
        Poll till you get the lock
        """
        hash = GenerateResult.build_queries_locking_hash(org, query)
        key = query.query_id()

        logger.info(f"Acquiring lock for query hash: {key}")

        payload = {
            "status": GenerateResult.RESULT_STATUS_FETCHING,
            "created_on": str(
                datetime.now().isoformat()
            ),  # will help the scheduler release locks
        }

        redis = RedisClient.get_instance()

        is_locked = False
        current_lock = redis.hget(hash, key)
        if current_lock is not None:
            is_locked = True

        if is_locked is False:
            logger.info("creating the hash for queries")
            # set if key does not exist
            if redis.hsetnx(hash, key, json.dumps(payload)):
                pass
            else:
                is_locked = True

        if is_locked:
            # poll for the lock till it releases basically till the query finishes running
            current_lock = redis.hget(hash, key)
            while current_lock is not None:
                time.sleep(5)
                current_lock = redis.hget(hash, key)

        run_query = not is_locked

        return run_query

    @classmethod
    def release_query_lock(cls, org: Org, query: ColInsight) -> None:
        """
        Release the lock for the query; if lock is not there then do nothing
        """
        hash = GenerateResult.build_queries_locking_hash(org, query)
        key = query.query_id()

        redis = RedisClient.get_instance()

        logger.info("clearing the query lock")
        redis.hdel(hash, key)

        return None

    @classmethod
    def save_results(cls, org: Org, query: ColInsight, parsed_results: dict) -> None:
        """Save results to redis"""
        # place to the save the results in redis should be created
        redis = RedisClient.get_instance()

        lock = GenerateResult.get_org_lock(org)

        if lock.acquire(timeout=15):
            current_results = GenerateResult.fetch_results(
                org, query.db_schema, query.db_table
            )

            logger.info(current_results)

            merged_results = {
                key: {**current_results.get(key, {}), **parsed_results.get(key, {})}
                for key in set(current_results) | set(parsed_results)
            }

            # save results to redis
            hash = f"{org.slug}-insights"
            key = f"{query.db_schema}-{query.db_table}"

            redis.hset(hash, key, json.dumps(merged_results))

            lock.release()

    @classmethod
    def clear_insights(
        cls, org: Org, db_schema: str, db_table: str, column_name: str = None
    ) -> None:
        """
        Clear the results from redis
        If column is None; clear insights for all columns
        If column is specified: clear insights for that column
        """
        hash = f"{org.slug}-insights"
        key = f"{db_schema}-{db_table}"

        redis = RedisClient.get_instance()

        lock = GenerateResult.get_org_lock(org)

        if lock.acquire(timeout=15):
            results = redis.hget(hash, key)
            results: dict = json.loads(results) if results else None

            if column_name:
                if results and column_name in results:
                    results.pop(column_name, None)
                    redis.hset(hash, key, json.dumps(results))
            else:
                # clear everything for the org
                redis.hdel(hash, key)

            lock.release()

    @classmethod
    def fetch_results(
        cls, org: Org, db_schema: str, db_table: str, column_name: str = None
    ) -> dict:
        """
        Fetch the results from redis
        If column is None; fetch insights for all columns
        If column is specified: fetch insights for that column
        """
        hash = f"{org.slug}-insights"
        key = f"{db_schema}-{db_table}"

        redis = RedisClient.get_instance()

        results = redis.hget(hash, key)
        results = json.loads(results) if results else None

        if column_name:
            return results[f"{column_name}"] if column_name in results else None

        return results

    @classmethod
    def validate_results(
        cls, insights: list[DataTypeColInsights], parsed_result: dict
    ) -> bool:
        """
        Validate the results
        """
        for insight in insights:
            for query in insight.insights:
                if not query.validate_query_results(parsed_result):
                    return False

        return True

    @classmethod
    def prepare_insight_objects(
        cls,
        wclient: Warehouse,
        requestor_col: RequestorColumnSchema,
    ) -> list[DataTypeColInsights]:
        db_schema = requestor_col.db_schema
        db_table = requestor_col.db_table
        column_name = requestor_col.column_name

        insight_objs: list[DataTypeColInsights] = []

        insight_objs.append(
            BaseInsights(
                wclient.get_table_columns(db_schema, db_table),
                db_table,
                db_schema,
                requestor_col.filter,
                wclient.get_wtype(),
            )
        )
        column_configs = [
            col
            for col in wclient.get_table_columns(db_schema, db_table)
            if col["name"] == column_name
        ]
        if len(column_configs) == 0:
            raise ValueError("Couldnt find the column in the table")

        insight_objs.append(
            InsightsFactory.initiate_insight(
                column_configs,
                db_table,
                db_schema,
                column_configs[0]["translated_type"],
                requestor_col.filter,
                wclient.get_wtype(),
            )
        )

        return insight_objs
