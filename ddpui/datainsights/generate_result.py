import time
import json
from datetime import datetime
from redis.lock import Lock
from channels.generic.websocket import WebsocketConsumer

from ddpui.celery import app
from ddpui.datainsights.insights.insight_interface import ColInsight
from ddpui.datainsights.insights.insight_factory import InsightsFactory
from ddpui.datainsights.insights.common.base_insights import BaseInsights
from ddpui.datainsights.warehouse.warehouse_interface import Warehouse
from ddpui.datainsights.warehouse.warehouse_factory import WarehouseFactory
from ddpui.models.org import Org, OrgWarehouse
from ddpui.utils.taskprogress import TaskProgress
from ddpui.schemas.warehouse_api_schemas import RequestorColumnSchema
from ddpui.models.tasks import TaskProgressHashPrefix
from ddpui.utils.redis_client import RedisClient
from ddpui.utils.custom_logger import CustomLogger
from ddpui.utils import secretsmanager
from ddpui.utils.helpers import convert_to_standard_types


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
    task_id: str,
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

    taskprogress = TaskProgress(task_id, TaskProgressHashPrefix.DATAINSIGHTS, 10 * 60)
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

    execute_queries = GenerateResult.queries_to_execute(
        org_warehouse.org, wclient, requestor_col
    )

    # if filter is present; run query & return result directly without saving to org results
    if requestor_col.filter:
        query_results = GenerateResult.execute_insight_queries(
            org_warehouse.org, wclient, execute_queries, requestor_col
        )
        current = GenerateResult.fetch_results(
            org_warehouse.org,
            requestor_col.db_schema,
            requestor_col.db_table,
        )
        final = {}
        # there should be only one query result
        if len(query_results) == 1:
            final = GenerateResult.merge_results(current, query_results[0])[
                requestor_col.column_name
            ]

        # return the saved results
        if GenerateResult.validate_results(execute_queries, final):
            taskprogress.add(
                {
                    "message": "Fetched results",
                    "status": GenerateResult.RESULT_STATUS_COMPLETED,
                    "results": convert_to_standard_types(final),
                }
            )
            return
        else:
            taskprogress.add(
                {
                    "message": "Partial data fetched",
                    "status": GenerateResult.RESULT_STATUS_ERROR,
                    "results": [],
                }
            )
        return

    # fetch the results from redis and see if they can be returned
    final_result = GenerateResult.fetch_results(
        org_warehouse.org,
        requestor_col.db_schema,
        requestor_col.db_table,
        requestor_col.column_name,
    )

    # if results from redis are partial; re-compute them
    if requestor_col.refresh or not GenerateResult.validate_results(
        execute_queries, final_result
    ):
        GenerateResult.execute_insight_queries(
            org_warehouse.org, wclient, execute_queries, requestor_col
        )
        final_result = convert_to_standard_types(
            GenerateResult.fetch_results(
                org_warehouse.org,
                requestor_col.db_schema,
                requestor_col.db_table,
                requestor_col.column_name,
            )
        )

    # if the results are still invalid/partial; return an error state
    if not GenerateResult.validate_results(execute_queries, final_result):
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
    RESULT_STATUS_ERROR = "error"
    ORG_INSIGHTS_EXPIRY = 60 * 30  # 30 minutes = 1800 seconds
    QUERY_LOCK_TIME = 60 * 5  # 5 minutes = 300 seconds

    org_locks: dict[str, Lock] = {}

    @classmethod
    def get_org_lock(cls, org: Org) -> Lock:
        """Return a persistent redis lock to access shared results"""
        if org.slug not in cls.org_locks:
            cls.org_locks[org.slug] = Lock(
                RedisClient.get_instance(), org.slug, timeout=15
            )
        return cls.org_locks[org.slug]

    @classmethod
    def build_queries_locking_hash(cls, org: Org, query: ColInsight) -> str:
        return f"{org.slug}-{query.db_schema}-{query.db_table}-queries"

    @classmethod
    def build_insights_hash_to_store_results(
        cls, org: Org, db_schema: str, db_table: str
    ) -> str:
        return f"{org.slug}-{db_schema}-{db_table}-insights"

    @classmethod
    def execute_insight_queries(
        cls,
        org: Org,
        wclient: Warehouse,
        execute_queries: list[ColInsight],
        requestor_col: RequestorColumnSchema,
    ) -> dict:
        """
        1. Collates all queries to run
        2. Gets locks on each query before run
        3. If lock is present; skips the query since its already running
        """

        logger.info(f"Total number of queries to executed {len(execute_queries)}")

        output_of_all_queries = []
        # acquire & run the queries lock for all queries
        for query in execute_queries:
            is_filter = True if requestor_col.filter else False
            if cls.acquire_query_lock(org, query, force_acquire=is_filter):
                # run the queries and save results
                logger.info(
                    f"Lock acquired & running the query: {query.query_id()} for col: {requestor_col.column_name}"
                )
                try:
                    stmt = query.generate_sql()
                    stmt = stmt.compile(
                        bind=wclient.engine, compile_kwargs={"literal_binds": True}
                    )
                    logger.info(stmt)
                    results = wclient.execute(stmt)

                    # parse result of this query
                    results = query.parse_results(results)
                    output_of_all_queries.append(results)

                    # when you are just filtering on an insight or chart; we dont update the saved org results
                    # instead just return result below
                    if not is_filter:
                        # save result to redis
                        cls.save_results(org, query, results)

                    # release the lock for this query
                    cls.release_query_lock(org, query)
                except Exception as err:
                    logger.error(
                        "Something went wrong while executing the query or saving the results; clearing the lock"
                    )
                    logger.error(err)
                    cls.release_query_lock(org, query, force_expire=True)

        return convert_to_standard_types(output_of_all_queries)

    @classmethod
    def is_query_locked(cls, query_payload: str, check_expiry: bool = False) -> bool:
        """
        Input is the query string picked from redis, can be none if query key is not present in redis
        payload: {"status": "fetching", "expire_at": "2021-09-01}
        """
        if query_payload is None:
            return False

        try:
            is_locked = False
            payload = json.loads(query_payload)
            if "status" in payload:
                if payload["status"] != GenerateResult.RESULT_STATUS_COMPLETED:
                    is_locked = True
                else:
                    if check_expiry:
                        # check for expiry, query remains locked until it expires
                        expire_at = payload["expire_at"]
                        if expire_at:
                            query_expiry_at = datetime.fromisoformat(expire_at)
                            diff = datetime.now() - query_expiry_at
                            if diff.total_seconds() < cls.QUERY_LOCK_TIME:
                                is_locked = True

            return is_locked

        except Exception as e:
            logger.error(f"Could not convert query payload {query_payload} to json")
            return False

    @classmethod
    def acquire_query_lock(
        cls, org: Org, query: ColInsight, force_acquire: bool = False
    ) -> bool:
        """
        Acquire lock to run the query: return True
        Poll till you get the lock
        """
        hash = GenerateResult.build_queries_locking_hash(org, query)
        key = query.query_id()

        redis = RedisClient.get_instance()

        if force_acquire:
            cls.release_query_lock(org, query, force_expire=True)

        run_query = False

        current_lock = redis.hget(hash, key)
        if cls.is_query_locked(current_lock, check_expiry=True):
            # poll for the lock till it releases
            # expiry is longer so we dont wait till then
            while cls.is_query_locked(current_lock):
                logger.info(f"Polling query id : {key}")
                time.sleep(1)
                current_lock = redis.hget(hash, key)
        else:
            # create a new lock
            logger.info("creating a new lock the query")
            payload = {
                "status": GenerateResult.RESULT_STATUS_FETCHING,
                "expire_at": str(datetime.now().isoformat()),
            }
            redis.hset(hash, key, json.dumps(payload))

            run_query = True

        return run_query

    @classmethod
    def release_query_lock(
        cls, org: Org, query: ColInsight, force_expire: bool = False
    ) -> None:
        """
        Release the lock for the query; means updating the status in the query lock to RESULT_STATUS_COMPLETED
        remove the hash if query has reached its expiry or if force_expire = True
        """
        hash = GenerateResult.build_queries_locking_hash(org, query)
        key = query.query_id()

        redis = RedisClient.get_instance()

        if force_expire:
            redis.hdel(hash, key)
            return None

        # for updating query results
        query_lock: Lock = redis.lock(f"{hash}-{key}", timeout=15)

        if query_lock.acquire():
            query_status = redis.hget(hash, key)
            if query_status:
                query_status = json.loads(query_status)
                query_status["status"] = GenerateResult.RESULT_STATUS_COMPLETED

                logger.info("updating the status of query to completed")
                redis.hset(hash, key, json.dumps(query_status))

                query_status = redis.hget(hash, key)
                if not cls.is_query_locked(query_status, check_expiry=True):
                    logger.info("clearing the query hash since it has expired")
                    redis.hdel(hash, key)

            query_lock.release()
        return None

    @classmethod
    def merge_results(cls, current: dict, parsed: dict) -> dict:
        current_results = current if current else {}
        parsed_results = parsed if parsed else {}

        merged_results = {
            key: {**current_results.get(key, {}), **parsed_results.get(key, {})}
            for key in set(current_results) | set(parsed_results)
        }

        return merged_results

    @classmethod
    def save_results(cls, org: Org, query: ColInsight, parsed_results: dict) -> None:
        """Save results to redis"""
        # place to the save the results in redis should be created
        redis = RedisClient.get_instance()

        lock = cls.get_org_lock(org)

        if lock.acquire():
            current_results = cls.fetch_results(org, query.db_schema, query.db_table)

            final_result = cls.merge_results(current_results, parsed_results)

            # save results to redis
            hash = cls.build_insights_hash_to_store_results(
                org, query.db_schema, query.db_table
            )
            key = f"{query.db_schema}-{query.db_table}"

            redis.hset(hash, key, json.dumps(final_result))
            if redis.ttl(hash) < 0:
                redis.expire(hash, cls.ORG_INSIGHTS_EXPIRY)

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
        hash = cls.build_insights_hash_to_store_results(org, db_schema, db_table)
        key = f"{db_schema}-{db_table}"

        redis = RedisClient.get_instance()

        results = redis.hget(hash, key)
        results = json.loads(results) if results else None

        if results and column_name:
            return results[f"{column_name}"] if column_name in results else None

        return results

    @classmethod
    def validate_results(cls, queries: list[ColInsight], parsed_result: dict) -> bool:
        """
        Validate the results
        """
        for query in queries:
            if not query.validate_query_results(parsed_result):
                return False

        return True

    @classmethod
    def queries_to_execute(
        cls,
        org: Org,
        wclient: Warehouse,
        requestor_col: RequestorColumnSchema,
    ) -> list[ColInsight]:
        db_schema = requestor_col.db_schema
        db_table = requestor_col.db_table
        column_name = requestor_col.column_name

        execute_queries: list[ColInsight] = []
        if not requestor_col.filter:
            base_insights = BaseInsights(
                wclient.get_table_columns(db_schema, db_table),
                db_table,
                db_schema,
                requestor_col.filter,
                wclient.get_wtype(),
            )
            for common_query in base_insights.insights:

                execute_queries.append(common_query)

        column_configs = [
            col
            for col in wclient.get_table_columns(db_schema, db_table)
            if col["name"] == column_name
        ]
        if len(column_configs) == 0:
            raise ValueError("Couldnt find the column in the table")

        for specific_query in InsightsFactory.initiate_insight(
            column_configs,
            db_table,
            db_schema,
            column_configs[0]["translated_type"],
            requestor_col.filter,
            wclient.get_wtype(),
        ).insights:
            execute_queries.append(specific_query)

        return execute_queries
