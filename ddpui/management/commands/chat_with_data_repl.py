"""Terminal REPL for the Chat with Data agent — the developer harness.

Drives the exact production agent (real warehouse, real guard, real middleware)
without the webapp. With LANGSMITH_TRACING=true in .env, every turn appears in
LangSmith with each tool call and its SQL.

    uv run python manage.py chat_with_data_repl --org <org-slug>
    uv run python manage.py chat_with_data_repl --org <org-slug> --model claude-haiku-4-5

Ctrl-D or /quit exits. Conversation memory persists for the session (in-memory
thread; use the webapp for durable sessions).
"""

import asyncio
import uuid

from django.core.management.base import BaseCommand, CommandError
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.checkpoint.memory import InMemorySaver

from ddpui.core.chat_with_data.agent import RECURSION_LIMIT, build_agent, get_chat_model
from ddpui.core.chat_with_data.context import ChatWithDataNotReady, build_run_context
from ddpui.models.org_user import OrgUser


class Command(BaseCommand):
    """Interactive Chat with Data session against a real org warehouse."""

    help = "Chat with an org's data from the terminal (dev harness)"

    def add_arguments(self, parser):
        parser.add_argument("--org", required=True, help="org slug")
        parser.add_argument("--model", default=None, help="override CHAT_WITH_DATA_MODEL")

    def handle(self, *args, **options):
        orguser = OrgUser.objects.filter(org__slug=options["org"]).first()
        if orguser is None:
            raise CommandError(f"No orguser found for org slug '{options['org']}'")

        try:
            context = build_run_context(orguser)
        except ChatWithDataNotReady as err:
            raise CommandError(str(err)) from err

        self.stdout.write(
            f"Chatting with {options['org']} ({context.dialect}); "
            f"schemas: {', '.join(context.allowed_schemas)}. Ctrl-D to exit."
        )

        model = None
        if options["model"]:
            model = get_chat_model().__class__(model=options["model"], max_tokens=4096)

        asyncio.run(self._chat_loop(context, model))

    async def _chat_loop(self, context, model):
        agent = build_agent(checkpointer=InMemorySaver(), model=model)
        config = {
            "configurable": {"thread_id": str(uuid.uuid4())},
            "recursion_limit": RECURSION_LIMIT,
        }

        while True:
            try:
                question = await asyncio.to_thread(input, "\nyou> ")
            except (EOFError, KeyboardInterrupt):
                self.stdout.write("\nbye")
                return
            if question.strip() in ("", "/quit", "/exit"):
                if question.strip():
                    return
                continue

            await self._run_turn(agent, config, context, question)

    async def _run_turn(self, agent, config, context, question):
        """Stream one turn; mirrors the WS event mapping (tokens + tool events)."""
        async for mode, chunk in agent.astream(
            {"messages": [HumanMessage(question)]},
            config=config,
            context=context,
            stream_mode=["messages", "updates"],
        ):
            if mode == "messages":
                message_chunk, _meta = chunk
                if isinstance(message_chunk, AIMessage) and message_chunk.content:
                    self.stdout.write(message_chunk.content, ending="")
                    self.stdout.flush()
            elif mode == "updates":
                for update in chunk.values():
                    for message in (update or {}).get("messages", []):
                        if isinstance(message, AIMessage) and message.tool_calls:
                            for tool_call in message.tool_calls:
                                args = {
                                    k: (str(v)[:120] + "…" if len(str(v)) > 120 else v)
                                    for k, v in tool_call["args"].items()
                                }
                                self.stdout.write(
                                    self.style.HTTP_INFO(f"\n⚙ {tool_call['name']} {args}")
                                )
                        elif isinstance(message, ToolMessage):
                            status = str(message.content).split("\n", maxsplit=1)[0][:100]
                            self.stdout.write(self.style.HTTP_NOT_MODIFIED(f"  ↳ {status}"))
        self.stdout.write("")
