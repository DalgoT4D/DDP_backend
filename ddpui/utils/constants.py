import os

# Master task slugs; this should match the task slugs in the seed/tasks.json file
TASK_DBTRUN = "dbt-run"
TASK_DBTTEST = "dbt-test"
TASK_DBTCLEAN = "dbt-clean"
TASK_DBTDEPS = "dbt-deps"
TASK_GITPULL = "git-pull"
TASK_DBTCLOUD_JOB = "dbt-cloud-job"  # this is task slug so it should match the seed data.
TASK_DOCSGENERATE = "dbt-docs-generate"
TASK_AIRBYTESYNC = "airbyte-sync"
TASK_AIRBYTERESET = "airbyte-reset"
TASK_AIRBYTECLEAR = "airbyte-clear"
TASK_DBTSEED = "dbt-seed"
TASK_GENERATE_EDR = "generate-edr"
UPDATE_SCHEMA = "update-schema"

# Dbt transformation sequence task slugs; we can always take this from the user/frontend also
TRANSFORM_TASKS_SEQ = {
    TASK_GITPULL: 1,
    TASK_DBTCLEAN: 2,
    TASK_DBTDEPS: 3,
    TASK_DBTSEED: 4,
    TASK_DBTRUN: 5,
    TASK_DBTTEST: 6,
    TASK_DOCSGENERATE: 7,
    TASK_GENERATE_EDR: 8,
    TASK_DBTCLOUD_JOB: 20,
}
# when a new pipeline is created; these are the transform tasks being pushed by default
DEFAULT_TRANSFORM_TASKS_IN_PIPELINE = [
    TASK_GITPULL,
    TASK_DBTCLEAN,
    TASK_DBTDEPS,
    TASK_DBTRUN,
    TASK_DBTTEST,
    TASK_DBTCLOUD_JOB,
]

# These are tasks to be run via deployment
# Adding a new task here will work for any new orgtask created
# But for the current ones a script would need to be run to set them with a deployment
LONG_RUNNING_TASKS = [TASK_DBTRUN, TASK_DBTSEED, TASK_DBTTEST, TASK_DBTCLOUD_JOB]

# airbyte sync timeout in deployment params
PREFECT_AIRBYTE_TASKS_TIMEOUT = int(
    os.getenv("PREFECT_AIRBYTE_TASKS_TIMEOUT_SECS", 15)
)  # default to 1 hour


# system user email
SYSTEM_USER_EMAIL = "System User"

# prefect flow run states

# offset limit for fetching logs
FLOW_RUN_LOGS_OFFSET_LIMIT = 200


# LLM data analysis ; row limit fetched and sent to llm service
LIMIT_ROWS_TO_SEND_TO_LLM = 500

# Org plans

DALGO_WITH_SUPERSET = {
    "pipeline": ["Ingest", "Transform", "Orchestrate"],
    "aiFeatures": ["AI data analysis"],
    "dataQuality": ["Data quality dashboards"],
    "superset": ["Superset dashboards", "Superset Usage dashboards"],
}

DALGO = {
    "pipeline": ["Ingest", "Transform", "Orchestrate"],
    "aiFeatures": ["AI data analysis"],
    "dataQuality": ["Data quality dashboards"],
}

FREE_TRIAL = {
    "pipeline": ["Ingest", "Transform", "Orchestrate"],
    "aiFeatures": ["AI data analysis"],
    "dataQuality": ["Data quality dashboards"],
    "superset": ["Superset dashboards", "Superset Usage dashboards"],
}
