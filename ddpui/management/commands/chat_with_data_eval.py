"""Run the Chat with Data golden-set evals (evals-plan.md Phase 1).

    uv run python manage.py chat_with_data_eval --org test-ngo
    uv run python manage.py chat_with_data_eval --org test-ngo --seed
    uv run python manage.py chat_with_data_eval --org test-ngo --no-judge --run-name pre-merge-fix

The golden items live in git (ddpui/core/ai/evals/golden_v1.jsonl) — the source
of truth. --seed mirrors them into a Langfuse dataset so runs show side-by-side
in the dataset UI; the run itself always reads the file, and links to the
Langfuse items when the dataset exists. Real model calls: ~$1-2 per full run.
"""

import asyncio
import hashlib
import json
import uuid
from datetime import date
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from ddpui.core.ai.agent.context_builder import ChatWithDataNotReady, build_run_context
from ddpui.core.ai.evals.runner import run_items
from ddpui.core.ai.tracing import get_langfuse
from ddpui.models.org_user import OrgUser

DEFAULT_DATASET_FILE = Path(__file__).parent.parent.parent / "core/ai/evals/golden_v1.jsonl"
DATASET_NAME = "golden-v1"


def load_items(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


class Command(BaseCommand):
    """Golden-set eval runner for Chat with Data."""

    help = "Run (or seed) the Chat with Data golden-set evals"

    def add_arguments(self, parser):
        parser.add_argument("--org", required=True, help="org slug whose warehouse to eval against")
        parser.add_argument("--file", default=str(DEFAULT_DATASET_FILE), help="golden JSONL file")
        parser.add_argument(
            "--dataset-name",
            default=DATASET_NAME,
            help="Langfuse dataset to seed/link (one per golden file, e.g. golden-work-orders)",
        )
        parser.add_argument("--seed", action="store_true", help="mirror items into Langfuse")
        parser.add_argument("--run-name", default=None, help="dataset run name")
        parser.add_argument(
            "--no-judge", action="store_true", help="skip the LLM faithfulness judge"
        )
        parser.add_argument("--tag", default=None, help="only items with this tag (e.g. canary)")
        parser.add_argument(
            "--schemas",
            default="test_ngo",
            help="comma-separated schema allowlist for the run — pins the agent to the "
            "schemas the gold SQL targets, so runs are reproducible (dev warehouses "
            "carry lookalike schemas the agent could otherwise wander into)",
        )

    def handle(self, *args, **options):
        items = load_items(Path(options["file"]))
        if options["tag"]:
            items = [i for i in items if options["tag"] in (i.get("tags") or [])]
        if not items:
            raise CommandError("no golden items matched")

        client = get_langfuse()

        if options["seed"]:
            self._seed(client, items, options["dataset_name"])
            return

        orguser = OrgUser.objects.filter(org__slug=options["org"]).first()
        if orguser is None:
            raise CommandError(f"No orguser found for org slug '{options['org']}'")
        try:
            context = build_run_context(orguser)
        except ChatWithDataNotReady as err:
            raise CommandError(str(err)) from err
        if options["schemas"]:
            context.allowed_schemas = [s.strip() for s in options["schemas"].split(",")]

        run_name = options["run_name"] or f"{date.today().isoformat()}-{uuid.uuid4().hex[:6]}"
        dataset = None
        if client is not None:
            try:
                dataset = client.get_dataset(options["dataset_name"])
            except Exception:  # pylint: disable=broad-except
                self.stdout.write("Langfuse dataset not found — run --seed first to link runs")

        summary = asyncio.run(
            run_items(
                items,
                context=context,
                run_name=run_name,
                judge=not options["no_judge"],
                langfuse_client=client,
                dataset=dataset,
            )
        )
        self.stdout.write(summary.render())
        if summary.passed < len(summary.results):
            self.stdout.write(self.style.WARNING("hard metrics regressed items — see above"))
        else:
            self.stdout.write(self.style.SUCCESS("all hard metrics passed"))

    def _seed(self, client, items, dataset_name):
        if client is None:
            raise CommandError("Langfuse is not configured (LANGFUSE_* keys) — cannot seed")
        client.create_dataset(name=dataset_name)
        for item in items:
            client.create_dataset_item(
                dataset_name=dataset_name,
                # deterministic id → re-seeding upserts instead of duplicating
                id=hashlib.sha256(item["question"].encode()).hexdigest()[:24],
                input={"question": item["question"]},
                expected_output={
                    "expected_intent": item.get("expected_intent"),
                    "gold_sql": item.get("gold_sql"),
                    "expected_value": item.get("expected_value"),
                    "answer_expectations": item.get("answer_expectations"),
                },
                metadata={
                    "tags": item.get("tags", []),
                    "expected_tables": item.get("expected_tables", []),
                },
            )
        self.stdout.write(self.style.SUCCESS(f"seeded {len(items)} items into '{dataset_name}'"))
