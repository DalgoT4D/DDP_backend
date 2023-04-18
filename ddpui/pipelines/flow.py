from prefect import flow
from prefect_airbyte import AirbyteConnection
from prefect_airbyte.flows import run_connection_sync
from prefect_dbt.cli.commands import DbtCoreOperation


@flow
def manual_airbyte_connection_flow(blockname):
    """Prefect flow to run airbyte connection"""
    airbyte_connection = AirbyteConnection.load(blockname)
    return run_connection_sync(airbyte_connection)


@flow
def manual_dbt_core_flow(blockname):
    """Prefect flow to run dbt"""
    dbt_op = DbtCoreOperation.load(blockname)
    dbt_op.run()


@flow
def schedule_deployment_flow(flow: Flow):
    """A general flow function that will help us create deployments"""
    for airbyte in flow.airbytes:
        airbyte.sync()

    flow.dbt.pull_dbt_repo()
    flow.dbt.dbt_deps()
    flow.dbt.dbt_source_snapshot_freshness()
    flow.dbt.dbt_run()
    flow.dbt.dbt_test()
