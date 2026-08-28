# Chat with Data — Evals

Golden questions run through the real chat agent and get scored automatically.
You bring a file of questions with known-correct answers; the runner replays
them, compares what the agent did against what it should have done, and pushes
every score to Langfuse where runs sit side by side.

Use it to answer one question: **"did my change make the agent better or worse?"**
— before users find out.

## Quickstart: run an existing dataset

```bash
# full run (~$1-2, ~10 min, real model calls against the dev warehouse)
uv run python manage.py chat_with_data_eval --org test-ngo \
  --file ddpui/core/ai/evals/golden_work_orders.jsonl \
  --dataset-name golden-work-orders --run-name my-change-name

# quick check: only items tagged "canary" (~5 items, ~30¢)
... same command ... --tag canary

# skip the LLM judges (hard metrics only — cheaper, faster)
... same command ... --no-judge
```

Results print in the console and appear in **Langfuse → Datasets →
\<dataset-name\> → Runs**. Name runs after the change you're testing
(`pr-142-prompt-tweak`), so the Runs tab reads like a changelog.

## Add your own dataset (the 4-step loop)

### 1. Write the questions as JSONL

One JSON object per line. Full example item:

```json
{"question": "How many enrollments are currently active?",
 "expected_intent": "data_question",
 "gold_sql": "SELECT COUNT(*) AS n FROM test_ngo.enrollments WHERE status = 'Active'",
 "answer_expectations": "89 active enrollments.",
 "expected_tables": ["test_ngo.enrollments"],
 "tags": ["false-zero-bait", "canary"]}
```

| Field | Required? | What it does |
|---|---|---|
| `question` | yes | Asked exactly as a user would type it |
| `expected_intent` | no | `data_question` \| `small_talk` \| `needs_clarification` — scores the router |
| `gold_sql` | no | Hand-written correct SQL. **Executed against the warehouse** and its result compared to the agent's — the strongest score. Must be verified (step 2) |
| `expected_value` | no | Lighter alternative to gold_sql: this value must appear in the answer text (e.g. `"14909222"`) |
| `answer_expectations` | no | One sentence describing a correct answer — scored by an LLM judge. The only way to score questions with no SQL truth (absence questions, "what does this program do?") |
| `expected_tables` | no | Metadata for now (future table-selection score) |
| `tags` | no | Free labels for `--tag` filtering; tag ~5 items `canary` for the cheap quick-check subset |

**Writing questions that score reliably** (each rule exists because a run broke without it):

- **Unambiguous.** "How many beneficiaries are enrolled?" defensibly means 200
  *or* 171 depending on the table — the agent will flip between them and your
  item will be flaky. Ask "how many individual beneficiaries have enrolled in
  at least one program?"
- **Gold SQL for absence questions doesn't work.** "How much silt in
  Maharashtra?" where Maharashtra has no rows: a correct agent says "there's no
  Maharashtra data" — which a `0.00` gold can't score. Use
  `answer_expectations` instead.
- **Let the data's dirt be the test.** `status = 'Active'` vs a user saying
  "active", a placeholder `'Unknown'` NGO name, TEXT date columns — real traps
  beat invented ones.

### 2. Verify every gold SQL by executing it

Never trust a gold you haven't run. This catches typos AND records the expected
values:

```bash
uv run python manage.py shell -c "
from ddpui.models.org import OrgWarehouse
from ddpui.utils.warehouse.client.warehouse_factory import WarehouseFactory
wh = WarehouseFactory.get_warehouse_client(OrgWarehouse.objects.filter(org__slug='test-ngo').first())
print(wh.execute(\"<your gold sql>\"))"
```

### 3. Seed it into Langfuse

```bash
uv run python manage.py chat_with_data_eval --org test-ngo \
  --file path/to/your_dataset.jsonl --dataset-name golden-yourname --seed
```

Seeding is idempotent (items are keyed by question hash) — edit the file and
re-seed freely. The JSONL file in git is the source of truth; Langfuse is the
scoreboard.

### 4. Run it, read it, fix it

Run the command from Quickstart. Expect your **first run to fail on item
wording, not agent bugs** — ours did (8/14 → 13/14 purely from fixing
ambiguous items and gold mistakes). Failed items print full diagnostics:

```
[FAIL sql] How many NGOs are working on GDGS work orders?
    agent sql : SELECT COUNT(DISTINCT ngo_name) ... AND ngo_name <> 'Unknown'
    agent rows: [['214']]
    gold rows : [{'n': 215}]      ← the agent was right; the gold forgot 'Unknown'
```

## The scores

| Score | Type | Meaning |
|---|---|---|
| `eval_routing` | **gate** | Router picked the expected intent (string equality) |
| `eval_sql_correct` | **gate** | Gold SQL and agent SQL, both **executed**, return the same answer |
| `eval_faithful` | inform | LLM judge: answer's claims supported by the query result |
| `eval_expectations` | inform | LLM judge: answer satisfies `answer_expectations` |
| `eval_sql_judge` | inform | LLM judge: agent SQL ≈ gold SQL, judged from text |

**Gates block; judges inform. This is measured, not dogma:** on our first
judged run, `eval_sql_judge` disagreed with execution on 8 of 11 items — all
8 were the judge false-failing queries that provably return identical results
(agents write structurally different SQL; a judge can't run it, only squint at
it). Never let a judge veto a merge. The judges run on OpenAI — a different
model family than the agent — so they don't share the agent's blind spots.

`eval_sql_correct` is deliberately forgiving of *presentation* (extra context
columns, full rankings for LIMIT-1 golds, invented labels when the numbers
identify rows, "7 states" answered by a 7-row breakdown) and strict on
*substance* (wrong numbers, wrong rows, empty results, a wrong claim in the
answer). The rules live in `sql_compare.py`, each with a regression test and a
real failure behind it.

## Gotchas

- **`--schemas` pins the agent's world** (default `test_ngo`). Dev warehouses
  carry lookalike schemas (`demo.beneficiaries` vs `test_ngo.beneficiaries`);
  unpinned, the agent wanders between them and your results aren't reproducible.
- **Compare runs only across the same metric version.** A pass-rate jump after
  touching `sql_compare.py` means the scoring changed, not the agent.
- **A flaky item is a finding, not noise.** Our Q3-vs-Q4 question fails a
  different way every few runs (empty result set, router diversion) — that's
  the agent's real weakness on date-bucketing, caught deterministically.
  Promote such items to `canary`.
- **Costs:** ~10¢/item with judges (agent turn + 3 judge calls); `--no-judge`
  roughly halves it; `--tag canary` for the cheap loop.
- Eval traces are tagged `eval` — filter them **out** when reading production
  dashboards, **in** when studying runs.

## What runs where

```
your JSONL (git, source of truth)
   │  --seed (idempotent)
   ▼
Langfuse dataset  ◄── item.link() ──  runner (manage.py chat_with_data_eval)
   │                                     │ real TurnGraph, dev warehouse,
   ▼                                     │ scores computed client-side
Runs tab: run vs run comparison  ◄───────┘
```

The runner is `runner.py` (orchestration) + `sql_compare.py` (hard metric) +
autoevals `ClosedQA`/`Sql` (judges). Offline unit tests (`test_eval_runner.py`,
`test_sql_compare.py`) cover all of it with a scripted model — zero API cost.
