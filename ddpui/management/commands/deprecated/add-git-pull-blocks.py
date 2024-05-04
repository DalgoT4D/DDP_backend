from django.core.management.base import BaseCommand
from django.utils.text import slugify

from ddpui.models.org import Org, OrgPrefectBlock
from ddpui.ddpprefect import SHELLOPERATION, DBTCORE, prefect_service
from ddpui.ddpprefect.schema import PrefectShellSetup


class Command(BaseCommand):
    """
    This script adds a "git-pull" shell block to an org's list of
    prefect blocks if there isn't one there already
    to be run 2023-07-27/28
    """

    help = "Adds git-pull blocks to deployments if required"

    @staticmethod
    def create_shell_block_for_migration(org: Org):
        """looks for a shell block, runs only if it doesn't find one"""
        if not OrgPrefectBlock.objects.filter(
            org=org, block_type=SHELLOPERATION
        ).exists():
            print(f"no shell blocks for {org.slug}, creating")

            command = "git pull"
            shell_env = {"secret-git-pull-url-block": ""}
            block_name = f"{org.slug}-{slugify(command)}"

            shell_cmd = PrefectShellSetup(
                blockname=block_name,
                commands=[command],
                workingDir=org.dbt.project_dir,
                env=shell_env,
            )
            block_response = prefect_service.create_shell_block(shell_cmd)
            # store prefect shell block in database
            shellprefectblock = OrgPrefectBlock(
                org=org,
                block_type=SHELLOPERATION,
                block_id=block_response["block_id"],
                block_name=block_response["block_name"],
                display_name=block_name,
                seq=0,
                command=slugify(command),
            )
            shellprefectblock.save()
            print(f"created shell block {block_name} having seq=0")

        # if there is a still a dbt-core block having seq=0, then incr all blocks' seq
        if OrgPrefectBlock.objects.filter(org=org, block_type=DBTCORE, seq=0).exists():
            for dbt_block in OrgPrefectBlock.objects.filter(
                org=org, block_type=DBTCORE
            ):
                dbt_block.seq += 1
                dbt_block.save()
                print(f"set seq for {dbt_block.block_name} to {dbt_block.seq}")
        else:
            print("seq numbers have already been upgraded for dbt blocks:")
            for dbt_block in OrgPrefectBlock.objects.filter(
                org=org, block_type=DBTCORE
            ):
                print(f"seq for {dbt_block.block_name} is {dbt_block.seq}")

    def handle(self, *args, **options):
        """runs for all orgs in the database"""
        for org in Org.objects.exclude(dbt__project_dir__isnull=True):
            Command.create_shell_block_for_migration(org)
