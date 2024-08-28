# Master task slugs; this should match the task slugs in the seed/tasks.json file
TASK_DBTRUN = "dbt-run"
TASK_DBTTEST = "dbt-test"
TASK_DBTCLEAN = "dbt-clean"
TASK_DBTDEPS = "dbt-deps"
TASK_GITPULL = "git-pull"
TASK_DOCSGENERATE = "dbt-docs-generate"
TASK_AIRBYTESYNC = "airbyte-sync"
TASK_AIRBYTERESET = "airbyte-reset"
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
}
# when a new pipeline is created; these are the transform tasks being pushed by default
DEFAULT_TRANSFORM_TASKS_IN_PIPELINE = [
    TASK_GITPULL,
    TASK_DBTCLEAN,
    TASK_DBTDEPS,
    TASK_DBTRUN,
    TASK_DBTTEST,
]

# These are tasks to be run via deployment
# Adding a new task here will work for any new orgtask created
# But for the current ones a script would need to be run to set them with a deployment
LONG_RUNNING_TASKS = [TASK_DBTRUN, TASK_DBTSEED, TASK_DBTTEST]

# airbyte sync timeout in deployment params
AIRBYTE_SYNC_TIMEOUT = 15


# system user email
SYSTEM_USER_EMAIL = "System User"

# prefect flow run states
