# Master task slugs; this should match the task slugs in the seed/tasks.json file
TASK_DBTRUN = "dbt-run"
TASK_DBTTEST = "dbt-test"
TASK_DBTCLEAN = "dbt-clean"
TASK_DBTDEPS = "dbt-deps"
TASK_GITPULL = "git-pull"
TASK_DOCSGENERATE = "dbt-docs-generate"
TASK_AIRBYTESYNC = "airbyte-sync"


# Dbt transformation sequence task slugs; we can always take this from the user/frontend also
TRANSFORM_TASKS_SEQ = {
    TASK_GITPULL: 1,
    TASK_DBTDEPS: 2,
    TASK_DBTCLEAN: 3,
    TASK_DBTRUN: 4,
    TASK_DBTTEST: 5,
    TASK_DOCSGENERATE: 6,
}

# airbyte sync timeout in deployment params
AIRBYTE_SYNC_TIMEOUT = 15
