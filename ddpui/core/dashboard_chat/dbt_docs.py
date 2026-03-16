"""dbt docs helpers for dashboard chat ingestion."""

from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path

import yaml
from django.utils import timezone

from ddpui.core.orgdbt_manager import DbtProjectManager
from ddpui.ddpprefect import prefect_service
from ddpui.models.org import Org, OrgDbt
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.dashboard_chat.dbt_docs")


class DashboardChatDbtDocsError(Exception):
    """Raised when dashboard chat dbt docs generation fails."""


@dataclass(frozen=True)
class DashboardChatDbtDocsArtifacts:
    """dbt docs artifacts required by the dashboard chat ingestion pipeline."""

    manifest_json: dict
    catalog_json: dict
    manifest_sha256: str
    catalog_sha256: str
    generated_at: timezone.datetime
    target_dir: Path


def compute_dashboard_chat_json_sha256(payload: dict) -> str:
    """Compute a deterministic hash for a JSON payload."""
    canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return sha256(canonical_json.encode("utf-8")).hexdigest()


def _write_profiles_file(org: Org, orgdbt: OrgDbt) -> Path:
    """Write the dbt profiles.yml file required for command execution."""
    if orgdbt.cli_profile_block is None:
        raise DashboardChatDbtDocsError("DBT CLI profile block not found for dashboard chat ingest")

    try:
        dbt_project_params = DbtProjectManager.gather_dbt_project_params(org, orgdbt)
        profile = prefect_service.get_dbt_cli_profile_block(orgdbt.cli_profile_block.block_name)[
            "profile"
        ]
        profiles_dir = Path(dbt_project_params.project_dir) / "profiles"
        profiles_dir.mkdir(parents=True, exist_ok=True)
        profile_path = profiles_dir / "profiles.yml"
        with open(profile_path, "w", encoding="utf-8") as profile_file:
            yaml.safe_dump(profile, profile_file)
        return profile_path
    except Exception as error:
        raise DashboardChatDbtDocsError(
            f"Failed to prepare dbt profiles.yml for dashboard chat ingest: {error}"
        ) from error


def generate_dashboard_chat_dbt_docs_artifacts(
    org: Org,
    orgdbt: OrgDbt,
) -> DashboardChatDbtDocsArtifacts:
    """Run dbt docs generate and return manifest/catalog payloads plus stable hashes."""
    if orgdbt is None:
        raise DashboardChatDbtDocsError("dbt workspace not configured for dashboard chat ingest")

    _write_profiles_file(org, orgdbt)

    try:
        logger.info("running dbt deps for dashboard chat docs generate org=%s", org.id)
        DbtProjectManager.run_dbt_command(
            org,
            orgdbt,
            command=["deps"],
            keyword_args={"profiles-dir": "profiles"},
        )
        logger.info("running dbt docs generate for dashboard chat org=%s", org.id)
        DbtProjectManager.run_dbt_command(
            org,
            orgdbt,
            command=["docs", "generate"],
            keyword_args={"profiles-dir": "profiles"},
        )
    except Exception as error:
        raise DashboardChatDbtDocsError(
            f"dbt docs generate failed for dashboard chat ingest: {error}"
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
    manifest_sha256 = compute_dashboard_chat_json_sha256(manifest_json)
    catalog_sha256 = compute_dashboard_chat_json_sha256(catalog_json)

    orgdbt.ai_manifest_sha256 = manifest_sha256
    orgdbt.ai_catalog_sha256 = catalog_sha256
    orgdbt.ai_docs_generated_at = generated_at
    orgdbt.save(
        update_fields=[
            "ai_manifest_sha256",
            "ai_catalog_sha256",
            "ai_docs_generated_at",
            "updated_at",
        ]
    )

    return DashboardChatDbtDocsArtifacts(
        manifest_json=manifest_json,
        catalog_json=catalog_json,
        manifest_sha256=manifest_sha256,
        catalog_sha256=catalog_sha256,
        generated_at=generated_at,
        target_dir=target_dir,
    )
