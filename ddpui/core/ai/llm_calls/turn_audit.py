"""Post-execution result validator — one cheap adversarial check per turn.

Runs AFTER message_complete (off the critical path): given the question, the
SQL that ran, the result, and the answer, a small model hunts for the four
silent text-to-SQL failures — wrong grain, missing filter, false zero, and
numbers that don't match the result. Its verdict becomes a UI caveat, an audit
column, and a Langfuse score; it never blocks or changes the answer.

Non-fatal everywhere: any failure returns None and the turn proceeds unmarked.
"""

from langchain_core.language_models.chat_models import BaseChatModel
from pydantic import ValidationError

from ddpui.core.ai.agent.base import build_model
from ddpui.core.ai.constants import FAST_MODEL, VALIDATOR_MAX_TOKENS, VALIDATOR_MODEL_ENV_VAR
from ddpui.core.ai.llm_calls.parsing import parse_json_reply
from ddpui.core.ai.messages.content import extract_text
from ddpui.core.ai.prompts import TURN_AUDIT_PROMPT
from ddpui.schemas.chat_with_data_schemas import TurnAuditReply
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")


# keep the judge's inputs bounded
MAX_ANSWER_CHARS = 2000
MAX_RESULT_ROWS = 10


def get_validator_model() -> BaseChatModel:
    return build_model(VALIDATOR_MODEL_ENV_VAR, FAST_MODEL, VALIDATOR_MAX_TOKENS)


def _render_sql(sql_queries: list[dict]) -> str:
    lines = []
    for entry in sql_queries:
        status = entry.get("status", "?")
        lines.append(f"[{status}] {entry.get('sql')}")
        if entry.get("row_count") is not None:
            lines[-1] += f"  -- {entry['row_count']} rows"
    return "\n".join(lines)


def _render_result(result_table: dict | None) -> str:
    if not result_table or not result_table.get("columns"):
        return "(no result table)"
    lines = [" | ".join(result_table["columns"])]
    for row in result_table.get("rows", [])[:MAX_RESULT_ROWS]:
        lines.append(" | ".join(str(cell) for cell in row))
    return "\n".join(lines)


async def audit_turn(
    *,
    question: str,
    sql_queries: list[dict],
    result_table: dict | None,
    answer: str,
    model: BaseChatModel | None = None,
) -> dict | None:
    """{verdict, assumptions, caveat} — or None when there is nothing to
    validate or validation itself failed."""
    if not sql_queries:
        return None
    try:
        model = model or get_validator_model()
        prompt = TURN_AUDIT_PROMPT.format(
            question=question[:1000],
            sql_block=_render_sql(sql_queries),
            result_block=_render_result(result_table),
            answer=answer[:MAX_ANSWER_CHARS],
        )
        response = await model.ainvoke(prompt)
        reply = TurnAuditReply.model_validate(parse_json_reply(extract_text(response.content)))
        return reply.model_dump()
    except ValidationError as err:
        logger.warning(f"chat_with_data: turn audit reply rejected (non-fatal): {err}")
        return None
    except Exception:  # pylint: disable=broad-except
        logger.exception("chat_with_data: result validation failed (non-fatal)")
        return None
