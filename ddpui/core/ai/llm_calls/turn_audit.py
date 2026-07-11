"""Post-execution result validator — one cheap adversarial check per turn.

Runs AFTER message_complete (off the critical path): given the question, the
SQL that ran, the result, and the answer, a small model hunts for the four
silent text-to-SQL failures — wrong grain, missing filter, false zero, and
numbers that don't match the result. Its verdict becomes a UI caveat, an audit
column, and a Langfuse score; it never blocks or changes the answer.

Non-fatal everywhere: any failure returns None and the turn proceeds unmarked.
"""

from langchain_core.language_models.chat_models import BaseChatModel

from ddpui.core.ai.agent.base import build_model
from ddpui.core.ai.llm_calls.parsing import parse_json_reply
from ddpui.core.ai.messages.content import extract_text
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

DEFAULT_VALIDATOR_MODEL = "claude-haiku-4-5"
VALIDATOR_MAX_TOKENS = 400
VERDICTS = {"ok", "warn"}

# keep the judge's inputs bounded
MAX_ANSWER_CHARS = 2000
MAX_RESULT_ROWS = 10

_PROMPT = """You are auditing a data answer for a non-technical user. Find \
problems; do not be polite. If unsure whether something is a problem, it is not.

Question: {question}

SQL executed (in order):
{sql_block}

Result (first rows):
{result_block}

Answer given to the user:
{answer}

Check exactly these:
1. GRAIN — if the question asks "how many <entities>", does the SQL count that
   entity (COUNT(DISTINCT ...) or one-row-per-entity table), or is it counting
   other rows (visits, events)?
2. FILTERS — is every condition in the question (place, program, time range)
   present in the SQL? A missing filter means a wrong answer.
3. FALSE ZERO — if the result is 0 rows or 0, could the filter VALUE be wrong
   (e.g. 'Maharashtra' vs 'MH') rather than the data truly empty?
4. NUMBERS — do the figures stated in the answer match the result table?

Return ONLY JSON:
{{"verdict": "ok" | "warn",
 "assumptions": [short strings — what the SQL assumed],
 "caveat": one plain-language sentence for the user, or null if verdict is ok}}"""


def get_validator_model() -> BaseChatModel:
    return build_model(
        "CHAT_WITH_DATA_VALIDATOR_MODEL", DEFAULT_VALIDATOR_MODEL, VALIDATOR_MAX_TOKENS
    )


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


async def validate_turn(
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
        prompt = _PROMPT.format(
            question=question[:1000],
            sql_block=_render_sql(sql_queries),
            result_block=_render_result(result_table),
            answer=answer[:MAX_ANSWER_CHARS],
        )
        response = await model.ainvoke(prompt)
        data = parse_json_reply(extract_text(response.content))

        verdict = data.get("verdict")
        if verdict not in VERDICTS:
            return None
        return {
            "verdict": verdict,
            "assumptions": [str(a) for a in data.get("assumptions") or []],
            "caveat": str(data["caveat"]) if data.get("caveat") else None,
        }
    except Exception:  # pylint: disable=broad-except
        logger.exception("chat_with_data: result validation failed (non-fatal)")
        return None
