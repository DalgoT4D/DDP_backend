"""The eval runner — golden items through the real TurnGraph, scored.

Per item (evals-plan.md §4 score vocabulary):
    eval_routing      hard   route_node intent == expected_intent
    eval_sql_correct  hard   gold SQL executed vs the agent's result set
                             (or expected_value substring fallback)
    eval_faithful     judge  autoevals ClosedQA on a DIFFERENT model family
                             (OpenAI) than the agent — fail-open, informs only

When a Langfuse client is available, each item becomes a trace tagged "eval",
linked to its dataset item under the run name, with the scores attached.
Hard metrics gate; the judge informs (plan §4 rule).
"""

import uuid
from dataclasses import dataclass, field

from langgraph.checkpoint.memory import InMemorySaver

from ddpui.core.ai.agent.chat_data_agent import RECURSION_LIMIT, build_agent
from ddpui.core.ai.chat.turn_graph import build_turn_graph
from ddpui.core.ai.evals.sql_compare import answer_contains_value, gold_satisfied
from ddpui.core.ai.llm_calls.router import casual_reply, route_question
from ddpui.core.ai.messages.artifacts import extract_turn_results
from ddpui.core.ai.messages.conversation import turn_segment
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui")

FAITHFULNESS_CRITERIA = (
    "The submission's numbers and named entities are all supported by this "
    "query result table (a claim not derivable from the table is a failure):\n{table}"
)


@dataclass
class ItemResult:
    question: str
    intent: str = ""
    routing_ok: bool | None = None
    sql_ok: bool | None = None
    faithful: float | None = None
    answer: str = ""
    error: str | None = None
    # failure diagnostics — what ran vs what gold expected
    agent_sql: str = ""
    agent_rows: list = field(default_factory=list)
    gold_rows: list = field(default_factory=list)

    @property
    def hard_pass(self) -> bool:
        """Both hard metrics pass (None = not applicable = pass)."""
        return self.error is None and self.routing_ok is not False and self.sql_ok is not False


@dataclass
class RunSummary:
    run_name: str
    results: list[ItemResult] = field(default_factory=list)

    @property
    def passed(self) -> int:
        return sum(1 for r in self.results if r.hard_pass)

    def render(self) -> str:
        lines = [f"run {self.run_name}: {self.passed}/{len(self.results)} hard-metric pass"]
        for r in self.results:
            flags = []
            if r.error:
                flags.append(f"ERROR {r.error[:80]}")
            if r.routing_ok is False:
                flags.append(f"routing({r.intent})")
            if r.sql_ok is False:
                flags.append("sql")
            status = "PASS" if r.hard_pass else "FAIL " + ", ".join(flags)
            judge = f"  faithful={r.faithful:.2f}" if r.faithful is not None else ""
            lines.append(f"  [{status}]{judge} {r.question[:70]}")
            if r.sql_ok is False:
                lines.append(f"      agent sql : {r.agent_sql[:160]}")
                lines.append(f"      agent rows: {str(r.agent_rows)[:160]}")
                lines.append(f"      gold rows : {str(r.gold_rows)[:160]}")
        return "\n".join(lines)


def judge_faithfulness(question: str, answer: str, result_table: dict | None) -> float | None:
    """autoevals ClosedQA judge (OpenAI family — deliberately not the agent's
    family, plan §9 blind-spot risk). Fail-open: any error returns None.

    An explicit OpenAI client bypasses autoevals' default Braintrust gateway —
    judge traffic goes straight to OpenAI with our own key, nowhere else."""
    if not answer or not result_table:
        return None
    try:
        import openai
        from autoevals import ClosedQA

        rows = [result_table.get("columns", [])] + list(result_table.get("rows", []))
        table = "\n".join(" | ".join(str(c) for c in row) for row in rows)
        result = ClosedQA(client=openai.OpenAI())(
            input=question,
            output=answer,
            criteria=FAITHFULNESS_CRITERIA.format(table=table[:4000]),
        )
        return result.score
    except Exception:  # pylint: disable=broad-except
        logger.exception("eval faithfulness judge failed (fail-open)")
        return None


async def run_item(item: dict, *, context, model=None, judge=True) -> ItemResult:
    """One golden item through a fresh TurnGraph (in-memory checkpointer)."""
    result = ItemResult(question=item["question"])
    saver = InMemorySaver()
    graph = build_turn_graph(
        build_agent(checkpointer=saver, model=model),
        route_fn=route_question,
        casual_reply_fn=casual_reply,
        validate_fn=None,  # the runner scores; the product validator stays out
        checkpointer=saver,
    )
    try:
        state = await graph.ainvoke(
            {"messages": [("user", item["question"])], "question": item["question"]},
            config={
                "configurable": {"thread_id": str(uuid.uuid4())},
                "recursion_limit": RECURSION_LIMIT,
            },
            context=context,
        )
    except Exception as err:  # pylint: disable=broad-except
        result.error = str(err)
        return result

    result.intent = (state.get("route") or {}).get("intent", "")
    if item.get("expected_intent"):
        result.routing_ok = result.intent == item["expected_intent"]

    sql_queries, result_table, answer = extract_turn_results(turn_segment(state["messages"]))
    result.answer = answer

    if item.get("gold_sql"):
        try:
            gold_rows = context.warehouse.execute(item["gold_sql"])
            result.gold_rows = list(gold_rows)
            result.agent_sql = (sql_queries[-1].get("sql") or "") if sql_queries else ""
            result.agent_rows = result_table.get("rows", []) if result_table else []
            result.sql_ok = result_table is not None and gold_satisfied(
                gold_rows, result_table.get("rows", []), answer
            )
        except Exception as err:  # pylint: disable=broad-except
            result.error = f"gold SQL failed: {err}"
    elif item.get("expected_value"):
        result.sql_ok = answer_contains_value(answer, item["expected_value"])

    if judge and result.routing_ok is not False:
        result.faithful = judge_faithfulness(item["question"], answer, result_table)
    return result


async def run_items(
    items: list[dict],
    *,
    context,
    run_name: str,
    model=None,
    judge=True,
    langfuse_client=None,
    dataset=None,
) -> RunSummary:
    """Run every item; push scores + dataset links to Langfuse when given.

    `dataset` is a Langfuse dataset whose items' input matches by question —
    linking is best-effort (an unmatched question still runs and scores)."""
    dataset_items = {}
    if dataset is not None:
        dataset_items = {(i.input or {}).get("question"): i for i in getattr(dataset, "items", [])}

    summary = RunSummary(run_name=run_name)
    for item in items:
        result = await run_item(item, context=context, model=model, judge=judge)
        summary.results.append(result)
        logger.info(f"eval item done hard_pass={result.hard_pass} q={item['question'][:60]}")
        if langfuse_client is not None:
            _record(langfuse_client, dataset_items, item, result, run_name)
    if langfuse_client is not None:
        try:
            langfuse_client.flush()
        except Exception:  # pylint: disable=broad-except
            logger.exception("eval langfuse flush failed")
    return summary


def _record(client, dataset_items, item, result: ItemResult, run_name: str) -> None:
    """One trace per item, scores attached, linked to the dataset item."""
    try:
        trace = client.trace(
            name="chat_eval_turn",
            input=item["question"],
            output=result.answer[:4000],
            tags=["eval"],
            metadata={"run_name": run_name, "error": result.error},
        )
        if result.routing_ok is not None:
            trace.score(name="eval_routing", value=1 if result.routing_ok else 0)
        if result.sql_ok is not None:
            trace.score(name="eval_sql_correct", value=1 if result.sql_ok else 0)
        if result.faithful is not None:
            trace.score(name="eval_faithful", value=result.faithful)
        dataset_item = dataset_items.get(item["question"])
        if dataset_item is not None:
            dataset_item.link(trace, run_name)
    except Exception:  # pylint: disable=broad-except
        logger.exception("eval langfuse record failed (item still counted)")
