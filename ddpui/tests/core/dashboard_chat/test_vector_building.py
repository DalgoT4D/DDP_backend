from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch
import json

import pytest

from django.contrib.auth.models import User
from django.utils import timezone

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.dashboard_chat.context.dbt_docs import (
    DashboardChatDbtDocsArtifacts,
    generate_dashboard_chat_dbt_docs_artifacts,
)
from ddpui.core.dashboard_chat.config import DashboardChatSourceConfig
from ddpui.core.dashboard_chat.vector.building import DashboardChatVectorBuildService
from ddpui.core.dashboard_chat.vector.documents import (
    DashboardChatSourceType,
    build_dashboard_chat_collection_name,
)
from ddpui.core.dashboard_chat.vector.store import DashboardChatStoredDocument
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.ddpprefect import DBTCLIPROFILE
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import DashboardAIContext, OrgAIContext
from ddpui.models.org import Org, OrgDbt, OrgPrefectBlockv1, TransformType
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.visualization import Chart
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


class FakeDashboardChatVectorStore:
    """In-memory vector store used to exercise vector build diffing logic."""

    def __init__(self):
        self.documents_by_collection = {}
        self.upsert_calls = []
        self.delete_calls = []

    def collection_name(self, org_id, *, version=None):
        return build_dashboard_chat_collection_name(org_id, version=version)

    def load_collection(self, org_id, *, collection_name=None, allow_legacy_fallback=True):
        resolved_collection_name = collection_name or self.collection_name(org_id)
        if resolved_collection_name in self.documents_by_collection:
            return {"name": resolved_collection_name}
        if collection_name and allow_legacy_fallback:
            legacy_collection_name = self.collection_name(org_id)
            if legacy_collection_name in self.documents_by_collection:
                return {"name": legacy_collection_name}
        return None

    def delete_collection(self, org_id, *, collection_name=None):
        resolved_collection_name = collection_name or self.collection_name(org_id)
        existed = resolved_collection_name in self.documents_by_collection
        self.documents_by_collection.pop(resolved_collection_name, None)
        return existed

    def list_org_collection_names(self, org_id):
        base_name = self.collection_name(org_id)
        return [
            collection_name
            for collection_name in self.documents_by_collection
            if collection_name == base_name or collection_name.startswith(f"{base_name}__")
        ]

    def get_documents(
        self,
        org_id,
        source_types=None,
        dashboard_id=None,
        include_documents=False,
        collection_name=None,
    ):
        resolved_collection_name = collection_name or self.collection_name(org_id)
        rows = list(self.documents_by_collection.get(resolved_collection_name, {}).values())
        if source_types:
            allowed = {
                source_type.value if hasattr(source_type, "value") else source_type
                for source_type in source_types
            }
            rows = [row for row in rows if row.metadata["source_type"] in allowed]
        if dashboard_id is not None:
            rows = [row for row in rows if row.metadata.get("dashboard_id") == dashboard_id]
        return [
            DashboardChatStoredDocument(
                document_id=row.document_id,
                metadata=row.metadata,
                content=row.content if include_documents else None,
            )
            for row in rows
        ]

    def upsert_documents(self, org_id, documents, collection_name=None):
        self.upsert_calls.append([document.document_id for document in documents])
        resolved_collection_name = collection_name or self.collection_name(org_id)
        org_documents = self.documents_by_collection.setdefault(resolved_collection_name, {})
        for document in documents:
            org_documents[document.document_id] = DashboardChatStoredDocument(
                document_id=document.document_id,
                metadata=document.metadata(),
                content=document.content,
            )
        return [document.document_id for document in documents]

    def delete_documents(
        self,
        org_id,
        ids=None,
        source_types=None,
        dashboard_id=None,
        collection_name=None,
    ):
        self.delete_calls.append(
            {
                "org_id": org_id,
                "ids": list(ids) if ids is not None else None,
                "source_types": source_types,
                "dashboard_id": dashboard_id,
                "collection_name": collection_name,
            }
        )
        resolved_collection_name = collection_name or self.collection_name(org_id)
        org_documents = self.documents_by_collection.setdefault(resolved_collection_name, {})
        if ids is None:
            return 0
        for document_id in ids:
            org_documents.pop(document_id, None)
        return len(ids)


@dataclass(frozen=True)
class StoredArtifacts:
    """Factory payload for deterministic dbt docs test fixtures."""

    manifest_json: dict
    catalog_json: dict
    generated_at: datetime

    def to_artifacts(self):
        return DashboardChatDbtDocsArtifacts(
            manifest_json=self.manifest_json,
            catalog_json=self.catalog_json,
            generated_at=self.generated_at,
            target_dir=Path("/tmp"),
        )


@pytest.fixture
def org():
    organization = Org.objects.create(
        name="Dashboard Chat Org",
        slug="dashchat",
        airbyte_workspace_id="ws-1",
    )
    yield organization
    organization.delete()


@pytest.fixture
def orguser(org, seed_db):
    user = User.objects.create(
        username="dashchat-user",
        email="dashchat-user@test.com",
        password="testpassword",
    )
    org_user = OrgUser.objects.create(
        user=user,
        org=org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    yield org_user
    org_user.delete()
    user.delete()


@pytest.fixture
def orgdbt(org):
    cli_block = OrgPrefectBlockv1.objects.create(
        org=org,
        block_type=DBTCLIPROFILE,
        block_name="dashboard-chat-profile",
        block_id="profile-block-id",
    )
    dbt = OrgDbt.objects.create(
        project_dir="dashchat/dbtrepo",
        dbt_venv="dbt-1.8.7",
        target_type="postgres",
        default_schema="analytics",
        cli_profile_block=cli_block,
    )
    org.dbt = dbt
    org.save(update_fields=["dbt"])
    yield dbt
    cli_block.delete()
    dbt.delete()


@pytest.fixture
def chart(org, orguser):
    instance = Chart.objects.create(
        title="Program Reach",
        description="Program reach over time",
        chart_type="line",
        schema_name="analytics",
        table_name="program_reach",
        extra_config={"metric": "beneficiaries"},
        created_by=orguser,
        last_modified_by=orguser,
        org=org,
    )
    yield instance
    instance.delete()


@pytest.fixture
def dashboard(org, orguser, chart):
    instance = Dashboard.objects.create(
        title="Impact Overview",
        description="Program KPI dashboard",
        dashboard_type="native",
        components={
            "chart-1": {
                "id": "chart-1",
                "type": "chart",
                "config": {"chartId": chart.id},
            }
        },
        created_by=orguser,
        last_modified_by=orguser,
        org=org,
    )
    yield instance
    instance.delete()


def test_generate_dashboard_chat_dbt_docs_artifacts_updates_timestamp(org, orgdbt, tmp_path):
    """dbt docs generation should write profiles, load artifacts, and persist the timestamp."""
    project_dir = tmp_path / "dashchat" / "dbtrepo"
    target_dir = project_dir / "target"
    target_dir.mkdir(parents=True)
    manifest_json = {"metadata": {"project_name": "dashchat"}, "nodes": {}, "sources": {}}
    catalog_json = {"nodes": {}, "sources": {}}
    (target_dir / "manifest.json").write_text(json.dumps(manifest_json), encoding="utf-8")
    (target_dir / "catalog.json").write_text(json.dumps(catalog_json), encoding="utf-8")

    with patch(
        "ddpui.core.dashboard_chat.context.dbt_docs.DbtProjectManager.gather_dbt_project_params",
        return_value=DbtProjectParams(
            dbt_binary="/mock/dbt",
            dbt_env_dir="/mock/env",
            venv_binary="/mock/bin",
            target="analytics",
            project_dir=str(project_dir),
            org_project_dir=str(project_dir.parent),
        ),
    ), patch(
        "ddpui.core.dashboard_chat.context.dbt_docs.prefect_service.get_dbt_cli_profile_block",
        return_value={"profile": {"dashchat": {"outputs": {"dev": {"type": "postgres"}}}}},
    ), patch(
        "ddpui.core.dashboard_chat.context.dbt_docs.DbtProjectManager.run_dbt_command",
        return_value=Mock(stdout="ok", returncode=0),
    ) as mock_run_dbt, patch(
        "ddpui.core.dashboard_chat.context.dbt_docs.DbtProjectManager.get_dbt_project_dir",
        return_value=str(project_dir),
    ):
        artifacts = generate_dashboard_chat_dbt_docs_artifacts(org, orgdbt)

    orgdbt.refresh_from_db()
    assert mock_run_dbt.call_count == 2
    assert mock_run_dbt.call_args_list[0].kwargs["command"] == ["deps"]
    assert mock_run_dbt.call_args_list[1].kwargs["command"] == ["docs", "generate"]
    first_profiles_dir = Path(mock_run_dbt.call_args_list[0].kwargs["keyword_args"]["profiles-dir"])
    second_profiles_dir = Path(
        mock_run_dbt.call_args_list[1].kwargs["keyword_args"]["profiles-dir"]
    )
    assert first_profiles_dir == second_profiles_dir
    assert not (project_dir / "profiles" / "profiles.yml").exists()
    assert not first_profiles_dir.exists()
    assert artifacts.manifest_json == manifest_json
    assert artifacts.catalog_json == catalog_json
    assert orgdbt.docs_generated_at is not None


def test_generate_dashboard_chat_dbt_docs_artifacts_pulls_git_repo_before_generating(
    org,
    orgdbt,
    tmp_path,
):
    """Git-backed dbt projects should refresh the local checkout before docs generation."""
    project_dir = tmp_path / "dashchat" / "dbtrepo"
    target_dir = project_dir / "target"
    target_dir.mkdir(parents=True)
    (target_dir / "manifest.json").write_text(
        json.dumps({"metadata": {"project_name": "dashchat"}, "nodes": {}, "sources": {}}),
        encoding="utf-8",
    )
    (target_dir / "catalog.json").write_text(
        json.dumps({"nodes": {}, "sources": {}}),
        encoding="utf-8",
    )
    orgdbt.transform_type = TransformType.GIT
    orgdbt.gitrepo_access_token_secret = "pat-secret"
    orgdbt.save(update_fields=["transform_type", "gitrepo_access_token_secret"])

    mock_git_manager = Mock()

    with patch(
        "ddpui.core.dashboard_chat.context.dbt_docs.DbtProjectManager.gather_dbt_project_params",
        return_value=DbtProjectParams(
            dbt_binary="/mock/dbt",
            dbt_env_dir="/mock/env",
            venv_binary="/mock/bin",
            target="analytics",
            project_dir=str(project_dir),
            org_project_dir=str(project_dir.parent),
        ),
    ), patch(
        "ddpui.core.dashboard_chat.context.dbt_docs.prefect_service.get_dbt_cli_profile_block",
        return_value={"profile": {"dashchat": {"outputs": {"dev": {"type": "postgres"}}}}},
    ), patch(
        "ddpui.core.dashboard_chat.context.dbt_docs.DbtProjectManager.run_dbt_command",
        return_value=Mock(stdout="ok", returncode=0),
    ), patch(
        "ddpui.core.dashboard_chat.context.dbt_docs.DbtProjectManager.get_dbt_project_dir",
        return_value=str(project_dir),
    ), patch(
        "ddpui.core.dashboard_chat.context.dbt_docs.secretsmanager.retrieve_github_pat",
        return_value="actual-pat",
    ) as mock_retrieve_pat, patch(
        "ddpui.core.dashboard_chat.context.dbt_docs.GitManager",
        return_value=mock_git_manager,
    ) as mock_git_manager_class:
        generate_dashboard_chat_dbt_docs_artifacts(org, orgdbt)

    mock_retrieve_pat.assert_called_once_with("pat-secret")
    mock_git_manager_class.assert_called_once_with(
        repo_local_path=str(project_dir),
        pat="actual-pat",
        validate_git=True,
    )
    mock_git_manager.pull_changes.assert_called_once_with()


def test_build_org_vector_context_is_idempotent_and_removes_stale_docs(org, orgdbt, orguser, dashboard):
    """A repeated identical build should skip writes, and a removed source should be deleted."""
    OrgAIContext.objects.create(
        org=org,
        markdown="# Org context\n\nImportant org notes.",
        updated_by=orguser,
        updated_at=timezone.now(),
    )
    dashboard_context = DashboardAIContext.objects.create(
        dashboard=dashboard,
        markdown="## Dashboard context\n\nThis dashboard tracks monthly reach.",
        updated_by=orguser,
        updated_at=timezone.now(),
    )
    vector_store = FakeDashboardChatVectorStore()
    artifacts = StoredArtifacts(
        manifest_json={
            "metadata": {"project_name": "dashchat"},
            "sources": {
                "source.dashchat.raw.program_reach": {
                    "source_name": "raw",
                    "schema": "raw",
                    "name": "program_reach",
                    "columns": {
                        "id": {"name": "id", "data_type": "integer"},
                        "period": {"name": "period", "data_type": "date"},
                    },
                }
            },
            "nodes": {
                "model.dashchat.fact_program_reach": {
                    "resource_type": "model",
                    "schema": "analytics",
                    "name": "fact_program_reach",
                    "description": "Monthly reach facts",
                    "depends_on": {"nodes": ["source.dashchat.raw.program_reach"]},
                    "columns": {"beneficiaries": {"name": "beneficiaries", "data_type": "integer"}},
                }
            },
        },
        catalog_json={
            "sources": {
                "source.dashchat.raw.program_reach": {
                    "metadata": {
                        "database": "warehouse",
                        "schema": "raw",
                        "name": "program_reach",
                        "type": "table",
                    },
                    "columns": {"id": {"type": "integer"}},
                }
            },
            "nodes": {
                "model.dashchat.fact_program_reach": {
                    "metadata": {
                        "database": "warehouse",
                        "schema": "analytics",
                        "name": "fact_program_reach",
                        "type": "table",
                    },
                    "columns": {"beneficiaries": {"type": "integer"}},
                }
            },
        },
        generated_at=timezone.now(),
    )
    service = DashboardChatVectorBuildService(
        vector_store=vector_store,
        dbt_docs_generator=lambda org_instance, orgdbt_instance: artifacts.to_artifacts(),
    )

    first_result = service.build_org_vector_context(org)
    upsert_count_after_first_vector_build = len(vector_store.upsert_calls)
    second_result = service.build_org_vector_context(org)

    dashboard_context.markdown = ""
    dashboard_context.updated_at = timezone.now()
    dashboard_context.save(update_fields=["markdown", "updated_at"])
    third_result = service.build_org_vector_context(org)

    active_collection_name = build_dashboard_chat_collection_name(
        org.id,
        version=org.dbt.vector_last_ingested_at,
    )
    stored_source_types = {
        document.metadata["source_type"]
        for document in vector_store.get_documents(
            org.id,
            include_documents=False,
            collection_name=active_collection_name,
        )
    }

    assert first_result.source_document_counts["dashboard_context"] == 1
    assert second_result.upserted_document_ids
    assert second_result.deleted_document_ids == []
    assert len(vector_store.upsert_calls) == upsert_count_after_first_vector_build + 2
    assert third_result.source_document_counts["dashboard_context"] == 0
    assert third_result.deleted_document_ids == []
    assert "dashboard_context" not in stored_source_types


def test_build_org_vector_context_keeps_collections_isolated_per_org(org, orgdbt, orguser, dashboard, seed_db):
    """The context build should never mix documents between org collections."""
    other_org = Org.objects.create(
        name="Dashboard Chat Org 2",
        slug="dashch2",
        airbyte_workspace_id="ws-2",
    )
    other_user = User.objects.create(
        username="dashchat-user-2",
        email="dashchat-user-2@test.com",
        password="testpassword",
    )
    other_orguser = OrgUser.objects.create(
        user=other_user,
        org=other_org,
        new_role=Role.objects.filter(slug=ACCOUNT_MANAGER_ROLE).first(),
    )
    other_orgdbt = OrgDbt.objects.create(
        project_dir="dashch2/dbtrepo",
        dbt_venv="dbt-1.8.7",
        target_type="postgres",
        default_schema="analytics",
    )
    other_org.dbt = other_orgdbt
    other_org.save(update_fields=["dbt"])
    Dashboard.objects.create(
        title="Other Dashboard",
        dashboard_type="native",
        created_by=other_orguser,
        last_modified_by=other_orguser,
        org=other_org,
    )
    OrgAIContext.objects.create(org=org, markdown="Org one")
    OrgAIContext.objects.create(org=other_org, markdown="Org two")

    artifacts = StoredArtifacts(
        manifest_json={"metadata": {"project_name": "dashchat"}, "sources": {}, "nodes": {}},
        catalog_json={"sources": {}, "nodes": {}},
        generated_at=timezone.now(),
    )
    vector_store = FakeDashboardChatVectorStore()
    service = DashboardChatVectorBuildService(
        vector_store=vector_store,
        dbt_docs_generator=lambda org_instance, orgdbt_instance: artifacts.to_artifacts(),
    )

    service.build_org_vector_context(org)
    service.build_org_vector_context(other_org)

    org_collection_names = vector_store.list_org_collection_names(org.id)
    other_collection_names = vector_store.list_org_collection_names(other_org.id)
    assert len(org_collection_names) == 1
    assert len(other_collection_names) == 1
    assert vector_store.documents_by_collection[org_collection_names[0]]
    assert vector_store.documents_by_collection[other_collection_names[0]]

    other_orguser.delete()
    other_user.delete()
    other_orgdbt.delete()
    other_org.delete()


def test_build_org_vector_context_keeps_last_good_context_when_upsert_fails(org, orgdbt, orguser, dashboard):
    """A failed rebuild should not delete the previously indexed documents."""
    OrgAIContext.objects.create(
        org=org,
        markdown="# Org context\n\nOriginal context.",
        updated_by=orguser,
        updated_at=timezone.now(),
    )
    vector_store = FakeDashboardChatVectorStore()
    artifacts = StoredArtifacts(
        manifest_json={"metadata": {"project_name": "dashchat"}, "sources": {}, "nodes": {}},
        catalog_json={"sources": {}, "nodes": {}},
        generated_at=timezone.now(),
    )
    service = DashboardChatVectorBuildService(
        vector_store=vector_store,
        dbt_docs_generator=lambda org_instance, orgdbt_instance: artifacts.to_artifacts(),
    )

    first_result = service.build_org_vector_context(org)
    original_ids = set(first_result.upserted_document_ids)
    assert original_ids

    org.ai_context.markdown = "# Org context\n\nUpdated context."
    org.ai_context.updated_at = timezone.now()
    org.ai_context.save(update_fields=["markdown", "updated_at"])

    def _raise_on_upsert(org_id, documents, collection_name=None):
        raise RuntimeError("upsert failed")

    vector_store.upsert_documents = _raise_on_upsert

    with pytest.raises(RuntimeError, match="upsert failed"):
        service.build_org_vector_context(org)

    remaining_ids = {
        document.document_id
        for documents in vector_store.documents_by_collection.values()
        for document in documents.values()
    }
    assert remaining_ids == original_ids
    assert vector_store.delete_calls == []


def test_build_org_vector_context_deletes_disabled_source_documents(org, orgdbt, orguser, dashboard):
    """Disabled source types should be omitted from the target document set."""
    OrgAIContext.objects.create(
        org=org,
        markdown="# Org context\n\nImportant org notes.",
        updated_by=orguser,
        updated_at=timezone.now(),
    )
    DashboardAIContext.objects.create(
        dashboard=dashboard,
        markdown="## Dashboard context\n\nThis dashboard tracks monthly reach.",
        updated_by=orguser,
        updated_at=timezone.now(),
    )
    vector_store = FakeDashboardChatVectorStore()
    artifacts = StoredArtifacts(
        manifest_json={"metadata": {"project_name": "dashchat"}, "sources": {}, "nodes": {}},
        catalog_json={"sources": {}, "nodes": {}},
        generated_at=timezone.now(),
    )
    initial_service = DashboardChatVectorBuildService(
        vector_store=vector_store,
        dbt_docs_generator=lambda org_instance, orgdbt_instance: artifacts.to_artifacts(),
    )
    initial_service.build_org_vector_context(org)

    disabled_source_service = DashboardChatVectorBuildService(
        vector_store=vector_store,
        dbt_docs_generator=lambda org_instance, orgdbt_instance: artifacts.to_artifacts(),
        source_config=DashboardChatSourceConfig(
            enabled_source_types=(
                DashboardChatSourceType.DASHBOARD_CONTEXT,
                DashboardChatSourceType.DASHBOARD_EXPORT,
                DashboardChatSourceType.DBT_MANIFEST,
            )
        ),
    )

    result = disabled_source_service.build_org_vector_context(org)
    stored_source_types = {
        document.metadata["source_type"]
        for document in vector_store.get_documents(org.id, include_documents=False)
    }

    assert result.source_document_counts["org_context"] == 0
    assert result.source_document_counts["dbt_catalog"] == 0
    assert "org_context" not in stored_source_types
    assert "dbt_catalog" not in stored_source_types


def test_build_org_vector_context_skips_dbt_docs_when_dbt_sources_are_disabled(org, orgdbt, dashboard):
    """Disabling both dbt sources should skip dbt docs generation entirely."""
    vector_store = FakeDashboardChatVectorStore()
    dbt_docs_generator = Mock(side_effect=AssertionError("dbt docs should not run"))
    service = DashboardChatVectorBuildService(
        vector_store=vector_store,
        dbt_docs_generator=dbt_docs_generator,
        source_config=DashboardChatSourceConfig(
            enabled_source_types=(
                DashboardChatSourceType.ORG_CONTEXT,
                DashboardChatSourceType.DASHBOARD_CONTEXT,
                DashboardChatSourceType.DASHBOARD_EXPORT,
            )
        ),
    )

    result = service.build_org_vector_context(org)

    dbt_docs_generator.assert_not_called()
    assert result.docs_generated_at is None
    assert result.source_document_counts["dbt_manifest"] == 0
    assert result.source_document_counts["dbt_catalog"] == 0
