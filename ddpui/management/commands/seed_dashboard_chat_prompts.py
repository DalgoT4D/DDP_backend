from django.core.management.base import BaseCommand

from ddpui.core.dashboard_chat.agents.prompt_template_store import DEFAULT_DASHBOARD_CHAT_PROMPTS
from ddpui.models.dashboard_chat import DashboardChatPromptTemplate


class Command(BaseCommand):
    """Seeds DashboardChatPromptTemplate with the default prompts from prompt_template_store.py."""

    help = "Seed dashboard chat prompt templates with default prompts"

    def handle(self, *args, **options):
        created_count = 0
        updated_count = 0

        for key, prompt in DEFAULT_DASHBOARD_CHAT_PROMPTS.items():
            _, created = DashboardChatPromptTemplate.objects.update_or_create(
                key=key,
                defaults={"prompt": prompt},
            )
            if created:
                created_count += 1
                self.stdout.write(f"  created: {key}")
            else:
                updated_count += 1
                self.stdout.write(f"  updated: {key}")

        self.stdout.write(
            self.style.SUCCESS(f"Done. {created_count} created, {updated_count} updated.")
        )
