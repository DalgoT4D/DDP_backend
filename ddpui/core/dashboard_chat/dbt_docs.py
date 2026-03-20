"""dbt docs helpers for dashboard chat context builds."""

from dataclasses import dataclass
import json
from pathlib import Path
import tempfile

import yaml
from django.utils import timezone

from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.ddpprefect import prefect_service
from ddpui.models.org import Org, OrgDbt
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.dashboard_chat.dbt_docs")


class DashboardChatDbtDocsError(Exception):
    """Raised when dbt docs generation for dashboard chat fails."""


@dataclass(frozen=True)
class DashboardChatDbtDocsArtifacts:
    """dbt docs artifacts required to build dashboard chat context."""

    manifest_json: dict
    catalog_json: dict
    generated_at: timezone.datetime
    target_dir: Path


def _write_profiles_file(org: Org, orgdbt: OrgDbt, profiles_dir: Path) -> Path:
    """Write the dbt profiles.yml required for dbt CLI execution."""
    if orgdbt.cli_profile_block is None:
        raise DashboardChatDbtDocsError("dbt CLI profile block not found")

    try:
        dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, orgdbt)
        profile = prefect_service.get_dbt_cli_profile_block(orgdbt.cli_profile_block.block_name)[
            "profile"
        ]
    except Exception as error:
        raise DashboardChatDbtDocsError(
            f"Failed to load dbt CLI profile for dashboard chat: {error}"
        ) from error

    profiles_dir.mkdir(parents=True, exist_ok=True)
    profile_path = profiles_dir / "profiles.yml"
    with open(profile_path, "w", encoding="utf-8") as profile_file:
        yaml.safe_dump(profile, profile_file)
    return profile_path


def generate_dashboard_chat_dbt_docs_artifacts(
    org: Org,
    orgdbt: OrgDbt,
) -> DashboardChatDbtDocsArtifacts:
    """Run dbt docs generate and return the manifest/catalog payloads."""
    if orgdbt is None:
        raise DashboardChatDbtDocsError("dbt workspace not configured")

    with tempfile.TemporaryDirectory(prefix=f"dashboard-chat-dbt-{org.id}-") as profiles_dir:
        profile_path = _write_profiles_file(org, orgdbt, Path(profiles_dir))
        profiles_dir_arg = str(profile_path.parent)

        try:
            logger.info("running dbt deps for dashboard chat org=%s", org.id)
            DbtProjectManager.run_dbt_command(
                org,
                orgdbt,
                command=["deps"],
                keyword_args={"profiles-dir": profiles_dir_arg},
            )
            logger.info("running dbt docs generate for dashboard chat org=%s", org.id)
            DbtProjectManager.run_dbt_command(
                org,
                orgdbt,
                command=["docs", "generate"],
                keyword_args={"profiles-dir": profiles_dir_arg},
            )
        except Exception as error:
            raise DashboardChatDbtDocsError(
                f"dbt docs generate failed for dashboard chat: {error}"
            ) from error

    target_dir = Path(DbtProjectManager.get_dbt_project_dir(orgdbt)) / "target"
    manifest_path = target_dir / "manifest.json"
    catalog_path = target_dir / "catalog.json"

    if not manifest_path.exists():
        raise DashboardChatDbtDocsError("dbt docs generate did not produce manifest.json")
    if not catalog_path.exists():
        raise DashboardChatDbtDocsError("dbt docs generate did not produce catalog.json")

    with open(manifest_path, "r", encoding="utf-8") as manifest_file:
        manifest_json = json.load(manifest_file)
    with open(catalog_path, "r", encoding="utf-8") as catalog_file:
        catalog_json = json.load(catalog_file)

    generated_at = timezone.now()
    orgdbt.docs_generated_at = generated_at
    orgdbt.save(update_fields=["docs_generated_at", "updated_at"])

    return DashboardChatDbtDocsArtifacts(
        manifest_json=manifest_json,
        catalog_json=catalog_json,
        generated_at=generated_at,
        target_dir=target_dir,
    )
