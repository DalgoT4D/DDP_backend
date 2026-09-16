"""Create the LangGraph checkpointer tables for Chat with Data.

Run once per environment (deploy step), after migrations:

    uv run python manage.py chat_with_data_setup

The tables (checkpoints, checkpoint_blobs, checkpoint_writes,
checkpoint_migrations) belong to langgraph-checkpoint-postgres, not to a Django
migration — the library owns their schema and its own migration history.
"""

import asyncio

from django.core.management.base import BaseCommand

from ddpui.core.ai.agent.checkpointer import setup_tables


class Command(BaseCommand):
    """Set up Chat with Data conversation-memory tables."""

    help = "Create the LangGraph checkpointer tables used by Chat with Data"

    def handle(self, *args, **options):
        asyncio.run(setup_tables())
        self.stdout.write(self.style.SUCCESS("Chat with Data checkpointer tables ready"))
