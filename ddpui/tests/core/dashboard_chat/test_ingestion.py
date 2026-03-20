import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch
import json

import django
import pytest

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

from django.contrib.auth.models import User
from django.utils import timezone

from ddpui.auth import ACCOUNT_MANAGER_ROLE
from ddpui.core.dashboard_chat.dbt_docs import (
    DashboardChatDbtDocsArtifacts,
    generate_dashboard_chat_dbt_docs_artifacts,
)
from ddpui.core.dashboard_chat.config import DashboardChatSourceConfig
from ddpui.core.dashboard_chat.ingestion import DashboardChatIngestionService
from ddpui.core.dashboard_chat.vector_store import DashboardChatStoredDocument
from ddpui.ddpdbt.schema import DbtProjectParams
from ddpui.ddpprefect import DBTCLIPROFILE
from ddpui.models.dashboard import Dashboard
from ddpui.models.dashboard_chat import DashboardAIContext, OrgAIContext
from ddpui.models.org import Org, OrgDbt, OrgPrefectBlockv1
from ddpui.models.org_user import OrgUser
from ddpui.models.role_based_access import Role
from ddpui.models.visualization import Chart
from ddpui.tests.api_tests.test_user_org_api import seed_db

pytestmark = pytest.mark.django_db


class FakeDashboardChatVectorStore:
    """In-memory vector store used to exercise ingest diffing logic."""

    def __init__(self):
        self.documents_by_org = {}
        self.upsert_calls = []
        self.delete_calls = []

    def get_documents(
        self,
        org_id,
        source_types=None,
        dashboard_id=None,
        include_documents=False,
    ):
        rows = list(self.documents_by_org.get(org_id, {}).values())
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

    def upsert_documents(self, org_id, documents):
        self.upsert_calls.append([document.document_id for document in documents])
        org_documents = self.documents_by_org.setdefault(org_id, {})
        for document in documents:
            org_documents[document.document_id] = DashboardChatStoredDocument(
                document_id=document.document_id,
                metadata=document.metadata(),
                content=document.content,
            )
        return [document.document_id for document in documents]

    def delete_documents(self, org_id, ids=None, source_types=None, dashboard_id=None):
        self.delete_calls.append(
            {
                "org_id": org_id,
                "ids": list(ids) if ids is not None else None,
                "source_types": source_types,
                "dashboard_id": dashboard_id,
            }
        )
        org_documents = self.documents_by_org.setdefault(org_id, {})
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
        "ddpui.core.dashboard_chat.dbt_docs.DbtProjectManager.gather_dbt_project_params",
        return_value=DbtProjectParams(
            dbt_binary="/mock/dbt",
            dbt_env_dir="/mock/env",
            venv_binary="/mock/bin",
            target="analytics",
            project_dir=str(project_dir),
            org_project_dir=str(project_dir.parent),
        ),
    ), patch(
        "ddpui.core.dashboard_chat.dbt_docs.prefect_service.get_dbt_cli_profile_block",
        return_value={"profile": {"dashchat": {"outputs": {"dev": {"type": "postgres"}}}}},
    ), patch(
        "ddpui.core.dashboard_chat.dbt_docs.DbtProjectManager.run_dbt_command",
        return_value=Mock(stdout="ok", returncode=0),
    ) as mock_run_dbt, patch(
        "ddpui.core.dashboard_chat.dbt_docs.DbtProjectManager.get_dbt_project_dir",
        return_value=str(project_dir),
    ):
        artifacts = generate_dashboard_chat_dbt_docs_artifacts(org, orgdbt)

    orgdbt.refresh_from_db()
    assert (project_dir / "profiles" / "profiles.yml").exists()
    assert mock_run_dbt.call_count == 2
    assert mock_run_dbt.call_args_list[0].kwargs["command"] == ["deps"]
    assert mock_run_dbt.call_args_list[1].kwargs["command"] == ["docs", "generate"]
    assert artifacts.manifest_json == manifest_json
    assert artifacts.catalog_json == catalog_json
    assert orgdbt.docs_generated_at is not None


def test_ingest_org_is_idempotent_and_removes_stale_docs(org, orgdbt, orguser, dashboard):
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
    service = DashboardChatIngestionService(
        vector_store=vector_store,
        dbt_docs_generator=lambda org_instance, orgdbt_instance: artifacts.to_artifacts(),
    )

    first_result = service.ingest_org(org)
    upsert_count_after_first_ingest = len(vector_store.upsert_calls)
    second_result = service.ingest_org(org)

    dashboard_context.markdown = ""
    dashboard_context.updated_at = timezone.now()
    dashboard_context.save(update_fields=["markdown", "updated_at"])
    third_result = service.ingest_org(org)

    stored_source_types = {
        document.metadata["source_type"]
        for document in vector_store.get_documents(org.id, include_documents=False)
    }

    assert first_result.source_document_counts["dashboard_context"] == 1
    assert second_result.upserted_document_ids == []
    assert second_result.deleted_document_ids == []
    assert len(vector_store.upsert_calls) == upsert_count_after_first_ingest
    assert third_result.source_document_counts["dashboard_context"] == 0
    assert third_result.deleted_document_ids
    assert "dashboard_context" not in stored_source_types


def test_ingest_org_keeps_collections_isolated_per_org(org, orgdbt, orguser, dashboard, seed_db):
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
    service = DashboardChatIngestionService(
        vector_store=vector_store,
        dbt_docs_generator=lambda org_instance, orgdbt_instance: artifacts.to_artifacts(),
    )

    service.ingest_org(org)
    service.ingest_org(other_org)

    assert set(vector_store.documents_by_org.keys()) == {org.id, other_org.id}
    assert vector_store.documents_by_org[org.id]
    assert vector_store.documents_by_org[other_org.id]

    other_orguser.delete()
    other_user.delete()
    other_orgdbt.delete()
    other_org.delete()


def test_ingest_org_keeps_last_good_context_when_upsert_fails(org, orgdbt, orguser, dashboard):
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
    service = DashboardChatIngestionService(
        vector_store=vector_store,
        dbt_docs_generator=lambda org_instance, orgdbt_instance: artifacts.to_artifacts(),
    )

    first_result = service.ingest_org(org)
    original_ids = set(first_result.upserted_document_ids)
    assert original_ids

    org.ai_context.markdown = "# Org context\n\nUpdated context."
    org.ai_context.updated_at = timezone.now()
    org.ai_context.save(update_fields=["markdown", "updated_at"])

    def _raise_on_upsert(org_id, documents):
        raise RuntimeError("upsert failed")

    vector_store.upsert_documents = _raise_on_upsert

    with pytest.raises(RuntimeError, match="upsert failed"):
        service.ingest_org(org)

    remaining_ids = {
        document.document_id for document in vector_store.get_documents(org.id, include_documents=False)
    }
    assert remaining_ids == original_ids
    assert vector_store.delete_calls == []


def test_ingest_org_deletes_disabled_source_documents(org, orgdbt, orguser, dashboard):
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
    initial_service = DashboardChatIngestionService(
        vector_store=vector_store,
        dbt_docs_generator=lambda org_instance, orgdbt_instance: artifacts.to_artifacts(),
    )
    initial_service.ingest_org(org)

    disabled_source_service = DashboardChatIngestionService(
        vector_store=vector_store,
        dbt_docs_generator=lambda org_instance, orgdbt_instance: artifacts.to_artifacts(),
        source_config=DashboardChatSourceConfig(
            enabled_source_types=("dashboard_context", "dashboard_export", "dbt_manifest")
        ),
    )

    result = disabled_source_service.ingest_org(org)
    stored_source_types = {
        document.metadata["source_type"]
        for document in vector_store.get_documents(org.id, include_documents=False)
    }

    assert result.source_document_counts["org_context"] == 0
    assert result.source_document_counts["dbt_catalog"] == 0
    assert "org_context" not in stored_source_types
    assert "dbt_catalog" not in stored_source_types
