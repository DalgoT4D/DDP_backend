# Trial Lifecycle Emails Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Automatically send five free-trial emails — three new progress-driven ones (day-3 not-started, day-3 in-progress, completion) plus the two already-written-but-unwired lifecycle ones (midpoint, pre-end) — from a single hourly Celery sweep that never double-sends.

**Architecture:** One new module `ddpui/core/trial/lifecycle_emails.py` holds a pure decision ladder plus a sweep that queries live free-trial `OrgPlans` rows, computes elapsed days and walkthrough-completion count, picks at most one email per trial per run, sends it, and stamps a flag in a new `UserPreferences.trial_emails_sent` JSON field. Templates and senders extend the existing `email_templates.py` / `awsses.py` pair. The existing `check_org_plan_expiry_notify_people` task is deleted because the new emails supersede it.

**Tech Stack:** Django 4 + Django ORM, Celery with RedBeat scheduler, AWS SES via boto3, pytest + `pytest.mark.django_db`, `unittest.mock.patch`.

## Global Constraints

- Only `insights` and `automate_pipeline` count as walkthroughs. `product_tour` is never counted and never rendered as a checklist row.
- A flow counts as done only when its entry is `completed is True`. `skipped: true` does **not** count.
- Trial identification is `OrgPlans.base_plan == OrgPlanType.FREE_TRIAL.value`. `Org` has **no** `type` column — the `OrgType` enum is legacy.
- `day_number = (now - start_date).days` — "day 3" means 72 hours elapsed.
- `total_days = (end_date - start_date).days`, falling back to `TRIAL_DURATION_DAYS` (14) **only when `total_days <= 0`**. Never `max(...)` — that would round a deliberately shorter admin-set trial up to 14.
- At most one email per trial per sweep run. First matching rule wins.
- The send-state flag is written **after** a successful send, inside `transaction.atomic()` with `select_for_update()` on the `UserPreferences` row.
- Flag keys, exactly: `day3`, `completion`, `midpoint`, `pre_end`. Values are ISO-8601 timestamp strings.
- Renderers return a `(plain_text, html)` tuple, matching every existing `render_*` function.
- Never modify the existing `render_trial_midpoint_email` / `render_trial_pre_end_email` renderers — they are already correct. Only add senders for them.
- Run `pre-commit run --all-files` before each commit; the repo uses black formatting.

---

## File Structure

| File | Responsibility |
|---|---|
| `ddpui/models/userpreferences.py` | Add `trial_emails_sent` JSONField + expose in `to_json()` |
| `ddpui/migrations/0175_userpreferences_trial_emails_sent.py` | The migration |
| `ddpui/utils/email_templates.py` | 3 new render helpers + 3 new renderers (A/B/C) |
| `ddpui/utils/awsses.py` | 5 new sender functions |
| `ddpui/core/trial/lifecycle_emails.py` | Constants, window maths, completion counting, decision ladder, dispatcher, sweep |
| `ddpui/celeryworkers/tasks.py` | New Celery task + beat entry; delete `check_org_plan_expiry_notify_people` |
| `ddpui/settings.py` | `TRIAL_UPGRADE_URL`, `TRIAL_SCHEDULE_CALL_URL` |
| `ddpui/tests/core/trial/test_lifecycle_emails.py` | Tests for the new module |
| `ddpui/tests/utils/test_email_templates.py` | Tests for the new renderers |
| `ddpui/tests/utils/test_awsses.py` | Tests for the new senders |

The module splits into pure functions (window maths, counting, the ladder — all trivially testable with no DB) and one impure sweep that does the querying and sending. Tasks 5–7 build the pure half before Task 8 wires the impure half on top.

---

### Task 1: `trial_emails_sent` field and migration

**Files:**
- Modify: `ddpui/models/userpreferences.py:24-35`
- Create: `ddpui/migrations/0175_userpreferences_trial_emails_sent.py` (generated)
- Test: `ddpui/tests/api_tests/test_user_preferences_api.py`

**Interfaces:**
- Consumes: nothing
- Produces: `UserPreferences.trial_emails_sent` — a `dict` field, default `{}`, keys `day3 | completion | midpoint | pre_end`, values ISO-8601 strings. Also appears in `UserPreferences.to_json()` under the same key.

- [ ] **Step 1: Write the failing test**

Append to `ddpui/tests/api_tests/test_user_preferences_api.py`:

```python
def test_trial_emails_sent_defaults_to_empty_dict(orguser):
    """a fresh UserPreferences row has no emails recorded as sent"""
    prefs = UserPreferences.objects.create(orguser=orguser)
    assert prefs.trial_emails_sent == {}


def test_trial_emails_sent_round_trips_and_appears_in_to_json(orguser):
    """flags written to the field survive a reload and are exposed via to_json"""
    prefs = UserPreferences.objects.create(orguser=orguser)
    prefs.trial_emails_sent = {"day3": "2026-08-09T10:00:00+00:00"}
    prefs.save()

    prefs.refresh_from_db()
    assert prefs.trial_emails_sent == {"day3": "2026-08-09T10:00:00+00:00"}
    assert prefs.to_json()["trial_emails_sent"] == {"day3": "2026-08-09T10:00:00+00:00"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest ddpui/tests/api_tests/test_user_preferences_api.py -k trial_emails_sent -v`
Expected: FAIL with `AttributeError: 'UserPreferences' object has no attribute 'trial_emails_sent'`

- [ ] **Step 3: Add the model field**

In `ddpui/models/userpreferences.py`, directly after the `trial_walkthrough` field:

```python
    # Which automated trial emails have already gone out, keyed by email kind
    # ("day3" | "completion" | "midpoint" | "pre_end") with an ISO-8601 send timestamp as the
    # value. A sibling of trial_walkthrough rather than a key inside it: that field is keyed by
    # flow name and is iterated by the frontend's flow-gate logic, which must not trip over
    # send-flags. Written only by the trial lifecycle-email sweep.
    trial_emails_sent = models.JSONField(default=dict, blank=True)
```

And in `to_json()`, after the `trial_walkthrough` entry:

```python
            "trial_emails_sent": self.trial_emails_sent,
```

- [ ] **Step 4: Generate the migration**

Run: `python manage.py makemigrations ddpui --name userpreferences_trial_emails_sent`
Expected: creates `ddpui/migrations/0175_userpreferences_trial_emails_sent.py` adding one field. Open it and confirm it contains exactly one `AddField` for `trial_emails_sent` and nothing else — if it picked up unrelated model drift, delete it, resolve the drift separately, and regenerate.

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest ddpui/tests/api_tests/test_user_preferences_api.py -k trial_emails_sent -v`
Expected: PASS (2 tests)

- [ ] **Step 6: Commit**

```bash
pre-commit run --all-files
git add ddpui/models/userpreferences.py ddpui/migrations/0175_userpreferences_trial_emails_sent.py ddpui/tests/api_tests/test_user_preferences_api.py
git commit -m "feat: add UserPreferences.trial_emails_sent for trial email dedupe"
```

---

### Task 2: Shared render helpers for the new templates

**Files:**
- Modify: `ddpui/utils/email_templates.py` (add after `_render_trial_progress_bar`, around line 463)
- Test: `ddpui/tests/utils/test_email_templates.py`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `_render_trial_checklist(items: list[tuple[bool, str, str]]) -> str` — items are `(done, title, subtitle)`
  - `_render_trial_testimonial() -> str` — takes no arguments
  - `_render_trial_text_link(label: str, url: str) -> str`

- [ ] **Step 1: Write the failing test**

Append to `ddpui/tests/utils/test_email_templates.py`:

```python
from ddpui.utils.email_templates import (
    _render_trial_checklist,
    _render_trial_testimonial,
    _render_trial_text_link,
)


def test_checklist_marks_done_and_pending_rows_differently():
    """a completed row shows the tick glyph, a pending row does not"""
    html_out = _render_trial_checklist(
        [
            (True, "Build your first insight", "Build out your first dashboard and share it"),
            (False, "Setup an automated data pipeline", "Setup your data to be updated"),
        ]
    )
    assert "Build your first insight" in html_out
    assert "Setup an automated data pipeline" in html_out
    # the green tick is drawn once — only for the completed row
    assert html_out.count("&#10003;") == 1


def test_checklist_escapes_titles_and_subtitles():
    """caller-supplied copy is escaped, never injected raw"""
    html_out = _render_trial_checklist([(False, "<script>x</script>", "a & b")])
    assert "<script>" not in html_out
    assert "&lt;script&gt;" in html_out
    assert "a &amp; b" in html_out


def test_testimonial_contains_quote_and_attribution():
    """the SEE WHAT'S POSSIBLE block is fixed copy, identical in all three emails"""
    html_out = _render_trial_testimonial()
    assert "SEE WHAT'S POSSIBLE" in html_out
    assert "Anindita" in html_out
    assert "SNEHA" in html_out


def test_text_link_renders_label_and_href_with_arrow():
    """the footer link carries its label, the url, and the trailing arrow glyph"""
    html_out = _render_trial_text_link("Schedule a call with us", "https://cal.example/x")
    assert 'href="https://cal.example/x"' in html_out
    assert "Schedule a call with us" in html_out
    assert "&#8599;" in html_out


def test_text_link_escapes_url():
    """a url containing a quote cannot break out of the href attribute"""
    html_out = _render_trial_text_link("Call", 'https://x/"onmouseover="alert(1)')
    assert 'onmouseover="alert(1)' not in html_out
    assert "&quot;" in html_out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest ddpui/tests/utils/test_email_templates.py -k "checklist or testimonial or text_link" -v`
Expected: FAIL at import with `ImportError: cannot import name '_render_trial_checklist'`

- [ ] **Step 3: Implement the three helpers**

Add to `ddpui/utils/email_templates.py`, immediately after `_render_trial_progress_bar`:

```python
def _render_trial_checklist(items: list) -> str:
    """The circle/tick checklist used by the day-3 and completion emails.

    Each item is ``(done, title, subtitle)``. A done row gets a filled green disc with a tick;
    a pending row gets an empty grey-outlined circle. Distinct from
    ``_render_trial_action_list``, which draws emoji icons inside a bordered box — this list has
    no border and encodes progress, not suggestions.
    """
    rows = []
    for done, title, subtitle in items:
        marker = (
            '<div style="width:26px; height:26px; border-radius:13px; background-color:#16a34a;'
            ' color:#ffffff; text-align:center; line-height:26px; font-size:14px;">&#10003;</div>'
            if done
            else '<div style="width:26px; height:26px; border-radius:13px;'
            ' border:2px solid #d1d5db; box-sizing:border-box;"></div>'
        )
        rows.append(
            f"""
                <tr>
                  <td width="26" valign="top" style="padding:0 14px 20px 0;">{marker}</td>
                  <td valign="top" style="padding:0 0 20px 0;">
                    <div style="font-size:15px; color:#111827; font-weight:700; line-height:1.4;">{html.escape(title)}</div>
                    <div style="font-size:14px; color:#4b5563; line-height:1.5; margin-top:2px;">{html.escape(subtitle)}</div>
                  </td>
                </tr>"""
        )
    return f"""
              <table width="100%" cellpadding="0" cellspacing="0" style="margin:0 0 8px;">{"".join(rows)}
              </table>"""


def _render_trial_testimonial() -> str:
    """The grey "SEE WHAT'S POSSIBLE" quote block. Fixed copy — identical in all three
    progress emails, so it takes no arguments."""
    return """
              <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f8fafc; border-radius:8px; margin:16px 0 24px;">
                <tr>
                  <td style="padding:20px 22px;">
                    <div style="font-size:12px; color:#00897B; font-weight:700; letter-spacing:0.08em; margin-bottom:10px;">SEE WHAT'S POSSIBLE</div>
                    <div style="font-size:15px; color:#111827; font-weight:700; line-height:1.55;">&ldquo;After each day of reporting, we are able to see the reports reflecting the changes automatically. That visibility has brought a lot of meaningful impact to the team&rdquo;</div>
                    <div style="font-size:13px; color:#6b7280; margin-top:10px;">&mdash; Anindita, SNEHA</div>
                  </td>
                </tr>
              </table>"""


def _render_trial_text_link(label: str, url: str) -> str:
    """The teal text link with a trailing up-right arrow, used as the email footer."""
    return (
        f'<a href="{html.escape(url, quote=True)}" style="color:#00897B; font-size:15px;'
        f' font-weight:700; text-decoration:none;">{html.escape(label)} &#8599;</a>'
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest ddpui/tests/utils/test_email_templates.py -k "checklist or testimonial or text_link" -v`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
pre-commit run --all-files
git add ddpui/utils/email_templates.py ddpui/tests/utils/test_email_templates.py
git commit -m "feat: add checklist, testimonial and text-link email helpers"
```

---

### Task 3: The three new renderers (A, B, C)

**Files:**
- Modify: `ddpui/utils/email_templates.py` (add after `render_trial_post_deletion_email`)
- Test: `ddpui/tests/utils/test_email_templates.py`

**Interfaces:**
- Consumes: `_render_trial_checklist`, `_render_trial_testimonial`, `_render_trial_text_link`, `_render_trial_email_shell`, `_render_trial_cta_button` (Task 2 + existing)

> **Note on test style.** The spec suggested the golden-HTML fixture pattern used by the report and
> mention emails. These tests assert on content instead. A golden fixture protects existing output
> from accidental drift during refactors — valuable for templates that already shipped, but for
> brand-new ones it would only freeze whatever this task happens to produce, and any copy tweak
> becomes a fixture regeneration that nobody reads. Content assertions pin what actually matters:
> the tick counts, the row ordering, the CTA labels, the escaping.

- Produces, all returning `(plain_text: str, html_body: str)`:
  - `render_trial_day3_not_started_email(workspace_url: str, schedule_call_url: str) -> tuple`
  - `render_trial_day3_in_progress_email(completed_flow: str, workspace_url: str, schedule_call_url: str) -> tuple` — `completed_flow` is `"insights"` or `"automate_pipeline"`
  - `render_trial_completion_email(upgrade_url: str, workspace_url: str, schedule_call_url: str) -> tuple`
  - `TRIAL_FLOW_COPY: dict[str, tuple[str, str]]` — flow key → `(title, subtitle)`

- [ ] **Step 1: Write the failing test**

Append to `ddpui/tests/utils/test_email_templates.py`:

```python
from ddpui.utils.email_templates import (
    render_trial_day3_not_started_email,
    render_trial_day3_in_progress_email,
    render_trial_completion_email,
)


def test_day3_not_started_shows_both_rows_unticked():
    """email A nudges a user who has completed nothing — no ticks, one CTA"""
    plain, html_out = render_trial_day3_not_started_email(
        "https://app.example", "https://cal.example"
    )
    assert "Ready to see Dalgo in action?" in html_out
    assert "Build your first insight" in html_out
    assert "Setup an automated data pipeline" in html_out
    # product_tour is never tracked, so "Explore the platform" must not appear
    assert "Explore the platform" not in html_out
    assert html_out.count("&#10003;") == 0
    assert "OPEN WORKSPACE" in html_out
    assert "Ready to see Dalgo in action?" in plain


def test_day3_in_progress_ticks_completed_flow_and_lists_it_first():
    """email B ticks the finished flow and puts it at the top of the list"""
    plain, html_out = render_trial_day3_in_progress_email(
        "insights", "https://app.example", "https://cal.example"
    )
    assert "Pick up where you left off" in html_out
    assert html_out.count("&#10003;") == 1
    assert html_out.index("Build your first insight") < html_out.index(
        "Setup an automated data pipeline"
    )
    assert "CONTINUE WHERE I LEFT OFF" in html_out
    # the mockup's [first insight/automated pipeline] placeholders are resolved, not literal
    assert "[first insight" not in html_out
    assert "You've built your first insight" in plain
    assert "automated data pipeline" in plain


def test_day3_in_progress_reverses_order_for_the_other_flow():
    """completing the pipeline flow instead puts that row first, ticked"""
    _, html_out = render_trial_day3_in_progress_email(
        "automate_pipeline", "https://app.example", "https://cal.example"
    )
    assert html_out.index("Setup an automated data pipeline") < html_out.index(
        "Build your first insight"
    )
    assert html_out.count("&#10003;") == 1


def test_completion_email_ticks_both_and_offers_two_ctas():
    """email C congratulates and offers UPGRADE plus KEEP EXPLORING"""
    plain, html_out = render_trial_completion_email(
        "https://upgrade.example", "https://app.example", "https://cal.example"
    )
    assert "Congratulations" in html_out
    assert html_out.count("&#10003;") == 2
    assert "UPGRADE" in html_out
    assert "KEEP EXPLORING" in html_out
    assert "https://upgrade.example" in html_out
    assert "Congratulations" in plain


def test_all_three_carry_testimonial_and_call_link():
    """the testimonial block and footer link are common to A, B and C"""
    renders = [
        render_trial_day3_not_started_email("https://app.example", "https://cal.example"),
        render_trial_day3_in_progress_email(
            "insights", "https://app.example", "https://cal.example"
        ),
        render_trial_completion_email(
            "https://upgrade.example", "https://app.example", "https://cal.example"
        ),
    ]
    for _, html_out in renders:
        assert "SEE WHAT'S POSSIBLE" in html_out
        assert "Schedule a call with us" in html_out
        assert "https://cal.example" in html_out
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest ddpui/tests/utils/test_email_templates.py -k "day3 or completion_email or all_three" -v`
Expected: FAIL at import with `ImportError: cannot import name 'render_trial_day3_not_started_email'`

- [ ] **Step 3: Implement the three renderers**

Add to `ddpui/utils/email_templates.py`, after `render_trial_post_deletion_email`:

```python
# Checklist copy for the two tracked walkthrough flows, keyed by the flow name used in
# UserPreferences.trial_walkthrough. product_tour is deliberately absent — it is not tracked by
# any lifecycle email and must never appear as a checklist row.
TRIAL_FLOW_COPY = {
    "insights": (
        "Build your first insight",
        "Build out your first dashboard and share it",
    ),
    "automate_pipeline": (
        "Setup an automated data pipeline",
        "Setup your data to be updated, cleaned and computed regularly",
    ),
}

# Short forms used in email B's subhead, e.g. "You've built your first insight. Next, go
# ahead with your automated data pipeline."
_TRIAL_FLOW_SHORT = {
    "insights": "first insight",
    "automate_pipeline": "automated data pipeline",
}


def _trial_footer_html(schedule_call_url: str) -> str:
    """Testimonial block plus the schedule-a-call link — the shared tail of emails A, B and C."""
    return (
        _render_trial_testimonial()
        + _render_trial_text_link("Schedule a call with us", schedule_call_url)
    )


def render_trial_day3_not_started_email(workspace_url: str, schedule_call_url: str) -> tuple:
    """Template A — day 3, no walkthrough completed yet.

    Returns:
        (plain_text_body, html_body) tuple
    """
    safe_workspace_url = html.escape(workspace_url)
    items = [(False, *TRIAL_FLOW_COPY[flow]) for flow in ("insights", "automate_pipeline")]

    plain_text = (
        "Ready to see Dalgo in action?\n"
        "\n"
        "Your workspace is setup. Try out one of these guides to get started on Dalgo today\n"
        "\n" + "\n".join(f"- {title}: {subtitle}" for _, title, subtitle in items) + "\n\n"
        f"Open your workspace: {workspace_url}\n"
        f"Schedule a call with us: {schedule_call_url}\n"
    )

    body_html = f"""\
              <p style="margin:0 0 8px; font-size:22px; color:#111827; font-weight:800; line-height:1.3;">
                Ready to see Dalgo in action?
              </p>
              <p style="margin:0 0 24px; font-size:15px; color:#4b5563; line-height:1.6;">
                Your workspace is setup. Try out one of these guides to get started on Dalgo today
              </p>
              {_render_trial_checklist(items)}
              {_render_trial_cta_button("OPEN WORKSPACE", safe_workspace_url)}
              {_trial_footer_html(schedule_call_url)}"""

    return plain_text, _render_trial_email_shell(body_html)


def render_trial_day3_in_progress_email(
    completed_flow: str, workspace_url: str, schedule_call_url: str
) -> tuple:
    """Template B — day 3, exactly one walkthrough completed.

    Args:
        completed_flow: "insights" or "automate_pipeline" — the one already finished. It is
            ticked and listed first; the other is the one the copy points at next.

    Returns:
        (plain_text_body, html_body) tuple
    """
    remaining_flow = "automate_pipeline" if completed_flow == "insights" else "insights"
    safe_workspace_url = html.escape(workspace_url)
    items = [
        (True, *TRIAL_FLOW_COPY[completed_flow]),
        (False, *TRIAL_FLOW_COPY[remaining_flow]),
    ]
    subhead = (
        f"You've built your {_TRIAL_FLOW_SHORT[completed_flow]}."
        f" Next, go ahead with your {_TRIAL_FLOW_SHORT[remaining_flow]}"
    )

    plain_text = (
        "Pick up where you left off\n"
        "\n"
        f"{subhead}\n"
        "\n" + "\n".join(f"- {title}: {subtitle}" for _, title, subtitle in items) + "\n\n"
        f"Continue where you left off: {workspace_url}\n"
        f"Schedule a call with us: {schedule_call_url}\n"
    )

    body_html = f"""\
              <p style="margin:0 0 8px; font-size:22px; color:#111827; font-weight:800; line-height:1.3;">
                Pick up where you left off
              </p>
              <p style="margin:0 0 24px; font-size:15px; color:#4b5563; line-height:1.6;">
                {html.escape(subhead)}
              </p>
              {_render_trial_checklist(items)}
              {_render_trial_cta_button("CONTINUE WHERE I LEFT OFF", safe_workspace_url)}
              {_trial_footer_html(schedule_call_url)}"""

    return plain_text, _render_trial_email_shell(body_html)


def render_trial_completion_email(
    upgrade_url: str, workspace_url: str, schedule_call_url: str
) -> tuple:
    """Template C — both walkthroughs completed, on or after day 3.

    Returns:
        (plain_text_body, html_body) tuple
    """
    safe_upgrade_url = html.escape(upgrade_url)
    safe_workspace_url = html.escape(workspace_url)
    items = [(True, *TRIAL_FLOW_COPY[flow]) for flow in ("insights", "automate_pipeline")]

    plain_text = (
        "Congratulations you've completed your tour of Dalgo.\n"
        "\n"
        "Upgrade to a full account, talk to us or explore the platform further.\n"
        "\n" + "\n".join(f"- {title}: {subtitle}" for _, title, subtitle in items) + "\n\n"
        f"Upgrade: {upgrade_url}\n"
        f"Keep exploring: {workspace_url}\n"
        f"Schedule a call with us: {schedule_call_url}\n"
    )

    body_html = f"""\
              <p style="margin:0 0 8px; font-size:22px; color:#111827; font-weight:800; line-height:1.3;">
                Congratulations you've completed your tour of Dalgo.
              </p>
              <p style="margin:0 0 24px; font-size:15px; color:#4b5563; line-height:1.6;">
                Upgrade to a full account, talk to us or explore the platform further.
              </p>
              {_render_trial_checklist(items)}
              <table cellpadding="0" cellspacing="0"><tr>
                <td>{_render_trial_cta_button("UPGRADE", safe_upgrade_url)}</td>
                <td style="padding-left:12px;">{_render_trial_cta_button("KEEP EXPLORING", safe_workspace_url, primary=False)}</td>
              </tr></table>
              {_trial_footer_html(schedule_call_url)}"""

    return plain_text, _render_trial_email_shell(body_html)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest ddpui/tests/utils/test_email_templates.py -v`
Expected: PASS — all new tests plus the pre-existing golden-fixture tests still green (the golden fixtures cover report/mention emails, which this task does not touch).

- [ ] **Step 5: Commit**

```bash
pre-commit run --all-files
git add ddpui/utils/email_templates.py ddpui/tests/utils/test_email_templates.py
git commit -m "feat: add day-3 and completion trial email templates"
```

---

### Task 4: Senders in awsses.py

**Files:**
- Modify: `ddpui/utils/awsses.py:9-12` (imports) and after line 115 (new functions)
- Test: `ddpui/tests/utils/test_awsses.py`

**Interfaces:**
- Consumes: the three renderers from Task 3, plus the existing `render_trial_midpoint_email` and `render_trial_pre_end_email`
- Produces, all returning `None`:
  - `send_trial_day3_not_started_email(to_email, workspace_url, schedule_call_url)`
  - `send_trial_day3_in_progress_email(to_email, completed_flow, workspace_url, schedule_call_url)`
  - `send_trial_completion_email(to_email, upgrade_url, workspace_url, schedule_call_url)`
  - `send_trial_midpoint_email(to_email, day_number, total_days, upgrade_url, schedule_call_url)`
  - `send_trial_pre_end_email(to_email, day_number, total_days, end_date, upgrade_url, schedule_call_url)` — `end_date` is a pre-formatted display string such as `"15 Aug 2026"`, because `render_trial_pre_end_email` does no date maths

- [ ] **Step 1: Write the failing test**

Append to `ddpui/tests/utils/test_awsses.py`:

```python
from ddpui.utils.awsses import (
    send_trial_day3_not_started_email,
    send_trial_day3_in_progress_email,
    send_trial_completion_email,
    send_trial_midpoint_email,
    send_trial_pre_end_email,
)


def test_send_trial_day3_not_started_email():
    """sends the html+text pair with the day-3 not-started subject"""
    with patch("ddpui.utils.awsses.send_html_message") as mock_send:
        send_trial_day3_not_started_email("to@x.org", "https://app", "https://cal")
        assert mock_send.call_count == 1
        to_email, subject, text_body, html_body = mock_send.call_args[0]
        assert to_email == "to@x.org"
        assert subject == "Ready to see Dalgo in action?"
        assert "Ready to see Dalgo in action?" in text_body
        assert "OPEN WORKSPACE" in html_body


def test_send_trial_day3_in_progress_email_passes_completed_flow_through():
    """the completed flow reaches the renderer, so the right row is ticked"""
    with patch("ddpui.utils.awsses.send_html_message") as mock_send:
        send_trial_day3_in_progress_email(
            "to@x.org", "automate_pipeline", "https://app", "https://cal"
        )
        _, subject, _, html_body = mock_send.call_args[0]
        assert subject == "Pick up where you left off"
        assert html_body.index("Setup an automated data pipeline") < html_body.index(
            "Build your first insight"
        )


def test_send_trial_completion_email():
    """the completion email carries the upgrade url"""
    with patch("ddpui.utils.awsses.send_html_message") as mock_send:
        send_trial_completion_email("to@x.org", "https://upgrade", "https://app", "https://cal")
        _, subject, _, html_body = mock_send.call_args[0]
        assert subject == "You've completed your tour of Dalgo"
        assert "https://upgrade" in html_body


def test_send_trial_midpoint_email():
    """the midpoint email renders the day-of-total progress bar"""
    with patch("ddpui.utils.awsses.send_html_message") as mock_send:
        send_trial_midpoint_email("to@x.org", 7, 14, "https://upgrade", "https://cal")
        _, subject, _, html_body = mock_send.call_args[0]
        assert subject == "You're halfway through your Dalgo trial"
        assert "Day 7 of 14" in html_body


def test_send_trial_pre_end_email():
    """the pre-end email shows the remaining days and the formatted end date"""
    with patch("ddpui.utils.awsses.send_html_message") as mock_send:
        send_trial_pre_end_email("to@x.org", 12, 14, "15 Aug 2026", "https://upgrade", "https://cal")
        _, subject, _, html_body = mock_send.call_args[0]
        assert subject == "2 days left in your Dalgo trial"
        assert "15 Aug 2026" in html_body
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest ddpui/tests/utils/test_awsses.py -k trial -v`
Expected: FAIL at import with `ImportError: cannot import name 'send_trial_day3_not_started_email'`

- [ ] **Step 3: Implement the senders**

Extend the import block at `ddpui/utils/awsses.py:9`:

```python
from ddpui.utils.email_templates import (
    render_verify_email,
    render_trial_welcome_email,
    render_trial_day3_not_started_email,
    render_trial_day3_in_progress_email,
    render_trial_completion_email,
    render_trial_midpoint_email,
    render_trial_pre_end_email,
)
```

Add after `send_trial_welcome_email`:

```python
def send_trial_day3_not_started_email(
    to_email: str, workspace_url: str, schedule_call_url: str
) -> None:
    """day-3 nudge for a trial user who has completed no walkthrough yet"""
    subject = "Ready to see Dalgo in action?"
    text_body, html_body = render_trial_day3_not_started_email(workspace_url, schedule_call_url)
    send_html_message(to_email, subject, text_body, html_body)


def send_trial_day3_in_progress_email(
    to_email: str, completed_flow: str, workspace_url: str, schedule_call_url: str
) -> None:
    """day-3 nudge for a trial user who has completed exactly one walkthrough"""
    subject = "Pick up where you left off"
    text_body, html_body = render_trial_day3_in_progress_email(
        completed_flow, workspace_url, schedule_call_url
    )
    send_html_message(to_email, subject, text_body, html_body)


def send_trial_completion_email(
    to_email: str, upgrade_url: str, workspace_url: str, schedule_call_url: str
) -> None:
    """sent once both tracked walkthroughs are complete, on or after day 3"""
    subject = "You've completed your tour of Dalgo"
    text_body, html_body = render_trial_completion_email(
        upgrade_url, workspace_url, schedule_call_url
    )
    send_html_message(to_email, subject, text_body, html_body)


def send_trial_midpoint_email(
    to_email: str, day_number: int, total_days: int, upgrade_url: str, schedule_call_url: str
) -> None:
    """mid-trial nudge, e.g. day 7 of 14"""
    subject = "You're halfway through your Dalgo trial"
    text_body, html_body = render_trial_midpoint_email(
        day_number, total_days, upgrade_url, schedule_call_url
    )
    send_html_message(to_email, subject, text_body, html_body)


def send_trial_pre_end_email(
    to_email: str,
    day_number: int,
    total_days: int,
    end_date: str,
    upgrade_url: str,
    schedule_call_url: str,
) -> None:
    """expiry warning, sent two days before the trial ends.

    `end_date` is already formatted for display (e.g. "15 Aug 2026") — the renderer does no
    date maths of its own.
    """
    subject = f"{total_days - day_number} days left in your Dalgo trial"
    text_body, html_body = render_trial_pre_end_email(
        day_number, total_days, end_date, upgrade_url, schedule_call_url
    )
    send_html_message(to_email, subject, text_body, html_body)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest ddpui/tests/utils/test_awsses.py -v`
Expected: PASS (5 new tests plus the existing ones)

- [ ] **Step 5: Commit**

```bash
pre-commit run --all-files
git add ddpui/utils/awsses.py ddpui/tests/utils/test_awsses.py
git commit -m "feat: add senders for trial progress and lifecycle emails"
```

---

### Task 5: Settings for the CTA urls

**Files:**
- Modify: `ddpui/settings.py:334-335`
- Modify: `.env.example`

**Interfaces:**
- Consumes: nothing
- Produces: `settings.TRIAL_UPGRADE_URL` and `settings.TRIAL_SCHEDULE_CALL_URL`, both `str`, both defaulting to `""`

- [ ] **Step 1: Add the settings**

In `ddpui/settings.py`, directly after the `FRONTEND_URL_V2` line:

```python
# Destinations for the UPGRADE and SCHEDULE-A-CALL buttons in the trial lifecycle emails.
# Both default to empty: the buttons still render, but an empty href is a dead click, so these
# must be populated before the lifecycle emails are enabled in production. See
# docs/superpowers/specs/2026-08-09-trial-lifecycle-emails-design.md, "Open decisions".
TRIAL_UPGRADE_URL = os.getenv("TRIAL_UPGRADE_URL", "")
TRIAL_SCHEDULE_CALL_URL = os.getenv("TRIAL_SCHEDULE_CALL_URL", "")
```

- [ ] **Step 2: Document them in `.env.example`**

Append:

```
# Trial lifecycle email CTAs. Empty renders a dead button — populate before enabling the
# hourly trial-email sweep in production.
TRIAL_UPGRADE_URL=
TRIAL_SCHEDULE_CALL_URL=
```

- [ ] **Step 3: Verify the settings load**

Run: `python -c "import django, os; os.environ.setdefault('DJANGO_SETTINGS_MODULE','ddpui.settings'); django.setup(); from django.conf import settings; print(repr(settings.TRIAL_UPGRADE_URL), repr(settings.TRIAL_SCHEDULE_CALL_URL))"`
Expected: `'' ''`

- [ ] **Step 4: Commit**

```bash
pre-commit run --all-files
git add ddpui/settings.py .env.example
git commit -m "feat: add TRIAL_UPGRADE_URL and TRIAL_SCHEDULE_CALL_URL settings"
```

---

### Task 6: Window maths and walkthrough counting

**Files:**
- Create: `ddpui/core/trial/lifecycle_emails.py`
- Test: `ddpui/tests/core/trial/test_lifecycle_emails.py` (create)

**Interfaces:**
- Consumes: `TRIAL_DURATION_DAYS` from `ddpui.core.trial.clone_service`
- Produces:
  - `TRACKED_FLOWS: tuple[str, str]` = `("insights", "automate_pipeline")`
  - `EMAIL_DAY3`, `EMAIL_COMPLETION`, `EMAIL_MIDPOINT`, `EMAIL_PRE_END` — the four flag-key string constants
  - `DAY3_THRESHOLD_DAYS = 3`, `MIDPOINT_THRESHOLD_DAYS = 7`, `PRE_END_DAYS_BEFORE = 2`
  - `completed_flows(trial_walkthrough: dict) -> list[str]` — tracked flows marked complete, always in `TRACKED_FLOWS` order
  - `trial_window(start_date, end_date, now) -> tuple[int, int]` — `(day_number, total_days)`

- [ ] **Step 1: Write the failing test**

Create `ddpui/tests/core/trial/test_lifecycle_emails.py`:

```python
from datetime import datetime, timedelta

import pytz

from ddpui.core.trial.lifecycle_emails import completed_flows, trial_window


UTC = pytz.UTC
START = datetime(2026, 8, 1, 9, 0, tzinfo=UTC)


def test_completed_flows_counts_only_completed_entries():
    """a flow counts only when completed is True"""
    assert completed_flows({"insights": {"completed": True, "skipped": False}}) == ["insights"]


def test_completed_flows_ignores_skipped():
    """skipping a walkthrough is not completing it"""
    assert completed_flows({"insights": {"completed": False, "skipped": True}}) == []


def test_completed_flows_ignores_product_tour():
    """product_tour is untracked and never counts, even when completed"""
    walkthrough = {
        "product_tour": {"completed": True, "skipped": False},
        "insights": {"completed": True, "skipped": False},
    }
    assert completed_flows(walkthrough) == ["insights"]


def test_completed_flows_returns_stable_order():
    """order follows TRACKED_FLOWS, not dict insertion order"""
    walkthrough = {
        "automate_pipeline": {"completed": True},
        "insights": {"completed": True},
    }
    assert completed_flows(walkthrough) == ["insights", "automate_pipeline"]


def test_completed_flows_handles_empty_and_malformed():
    """an empty dict, or a non-dict value, counts as nothing completed"""
    assert completed_flows({}) == []
    assert completed_flows({"insights": None}) == []
    assert completed_flows({"insights": "yes"}) == []


def test_trial_window_computes_elapsed_and_total_days():
    """day 3 means 72 hours elapsed; total comes from the plan's own dates"""
    day_number, total_days = trial_window(START, START + timedelta(days=14), START + timedelta(days=3))
    assert day_number == 3
    assert total_days == 14


def test_trial_window_day_number_truncates():
    """71 hours in is still day 2 — .days floors"""
    day_number, _ = trial_window(
        START, START + timedelta(days=14), START + timedelta(hours=71)
    )
    assert day_number == 2


def test_trial_window_respects_a_shorter_admin_set_window():
    """a 7-day window renders as 7, never rounded up to the 14-day default"""
    _, total_days = trial_window(START, START + timedelta(days=7), START + timedelta(days=1))
    assert total_days == 7


def test_trial_window_falls_back_when_window_is_zero():
    """identical dates would divide by zero in the progress bar — fall back to 14"""
    _, total_days = trial_window(START, START, START)
    assert total_days == 14


def test_trial_window_falls_back_when_window_is_inverted():
    """an end before the start is nonsense — fall back rather than render a negative bar"""
    _, total_days = trial_window(START, START - timedelta(days=3), START)
    assert total_days == 14
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest ddpui/tests/core/trial/test_lifecycle_emails.py -v`
Expected: FAIL at import with `ModuleNotFoundError: No module named 'ddpui.core.trial.lifecycle_emails'`

- [ ] **Step 3: Create the module with constants and the two pure functions**

Create `ddpui/core/trial/lifecycle_emails.py`:

```python
"""Automated free-trial lifecycle emails — decision ladder and hourly sweep.

Five emails are driven from here: three progress-based (day-3 not-started, day-3 in-progress,
completion) and two date-based (midpoint, pre-end). An hourly Celery task calls
`run_trial_lifecycle_sweep`, which sends at most ONE email per trial per run and records what
was sent in `UserPreferences.trial_emails_sent` so nothing goes out twice.

Design: docs/superpowers/specs/2026-08-09-trial-lifecycle-emails-design.md
"""

from ddpui.core.trial.clone_service import TRIAL_DURATION_DAYS
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.lifecycle_emails")

# The walkthrough flows these emails track. `product_tour` is deliberately excluded — product
# decided it neither counts toward completion nor appears as a checklist row.
TRACKED_FLOWS = ("insights", "automate_pipeline")

# Keys written into UserPreferences.trial_emails_sent. The day-3 not-started and in-progress
# emails share ONE key because only one of them can ever fire for a given user.
EMAIL_DAY3 = "day3"
EMAIL_COMPLETION = "completion"
EMAIL_MIDPOINT = "midpoint"
EMAIL_PRE_END = "pre_end"

# Days elapsed since OrgPlans.start_date before each rule becomes eligible.
DAY3_THRESHOLD_DAYS = 3
MIDPOINT_THRESHOLD_DAYS = 7
# The pre-end warning goes out this many days before OrgPlans.end_date.
PRE_END_DAYS_BEFORE = 2


def completed_flows(trial_walkthrough: dict) -> list:
    """Tracked walkthrough flows this user has COMPLETED, in TRACKED_FLOWS order.

    `skipped: true` is not completion — a user who dismissed a walkthrough has not seen what it
    teaches, so they still deserve the nudge. Malformed entries (None, a bare string) count as
    not completed rather than raising, because this JSON is written by the frontend.
    """
    walkthrough = trial_walkthrough or {}
    done = []
    for flow in TRACKED_FLOWS:
        entry = walkthrough.get(flow)
        if isinstance(entry, dict) and entry.get("completed") is True:
            done.append(flow)
    return done


def trial_window(start_date, end_date, now) -> tuple:
    """Return `(day_number, total_days)` for a trial.

    `day_number` floors, so "day 3" means a full 72 hours have elapsed.

    `total_days` is derived from the plan's own dates rather than TRIAL_DURATION_DAYS, so that a
    trial an admin extended or shortened via `createorgplan` renders its real window. It falls
    back to TRIAL_DURATION_DAYS ONLY when the window is non-positive — `createorgplan` sets the
    two dates independently with no validation, and a zero-length window would raise
    ZeroDivisionError inside `_render_trial_progress_bar`. Deliberately not
    `max(total_days, TRIAL_DURATION_DAYS)`: that would silently render a legitimate 7-day trial
    as "Day 7 of 14" on the day it ends.
    """
    day_number = (now - start_date).days
    total_days = (end_date - start_date).days
    if total_days <= 0:
        logger.warning(
            "trial window is non-positive (start=%s end=%s); falling back to %s days",
            start_date,
            end_date,
            TRIAL_DURATION_DAYS,
        )
        total_days = TRIAL_DURATION_DAYS
    return day_number, total_days
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest ddpui/tests/core/trial/test_lifecycle_emails.py -v`
Expected: PASS (10 tests)

- [ ] **Step 5: Commit**

```bash
pre-commit run --all-files
git add ddpui/core/trial/lifecycle_emails.py ddpui/tests/core/trial/test_lifecycle_emails.py
git commit -m "feat: add trial window maths and walkthrough completion counting"
```

---

### Task 7: The decision ladder

**Files:**
- Modify: `ddpui/core/trial/lifecycle_emails.py`
- Test: `ddpui/tests/core/trial/test_lifecycle_emails.py`

**Interfaces:**
- Consumes: `completed_flows`, `trial_window`, and the constants from Task 6
- Produces:
  - `decide_email(day_number, completed_count, flags, now, end_date) -> str | None` — returns one of the four `EMAIL_*` constants, or `None` if nothing is due. `flags` is the `trial_emails_sent` dict.
  - `FLAGS_STAMPED_BY: dict[str, tuple[str, ...]]` — which flag keys each decision writes

- [ ] **Step 1: Write the failing test**

Add these imports to the **top** of `ddpui/tests/core/trial/test_lifecycle_emails.py`, beside the
existing ones (not mid-file — the linter rejects E402):

```python
import pytest

from ddpui.core.trial.lifecycle_emails import (
    decide_email,
    FLAGS_STAMPED_BY,
    EMAIL_DAY3,
    EMAIL_COMPLETION,
    EMAIL_MIDPOINT,
    EMAIL_PRE_END,
)
```

Then append the module-level constant and the tests:

```python
END = START + timedelta(days=14)


def _decide(day, completed, flags=None, now=None):
    """decide_email with the fixed 14-day window, so cases read as (day, completed, flags)"""
    at = now if now is not None else START + timedelta(days=day)
    return decide_email(day, completed, flags or {}, at, END)


@pytest.mark.parametrize(
    "day,completed,expected",
    [
        (0, 0, None),  # nothing before day 3
        (2, 0, None),
        (2, 2, None),  # C never fires before day 3, even when both are done
        (3, 0, EMAIL_DAY3),  # A
        (3, 1, EMAIL_DAY3),  # B
        (3, 2, EMAIL_COMPLETION),  # C outranks the day-3 email
        (5, 2, EMAIL_COMPLETION),  # C can fire later than day 3
    ],
)
def test_ladder_picks_the_right_email(day, completed, expected):
    assert _decide(day, completed) == expected


def test_completion_beats_day3_on_day_three():
    """with both walkthroughs done on day 3 the user gets C, never B"""
    assert _decide(3, 2) == EMAIL_COMPLETION


def test_day3_never_fires_after_completion():
    """once C has gone out, A and B are locked out forever"""
    flags = {EMAIL_COMPLETION: "2026-08-04T09:00:00+00:00"}
    assert _decide(3, 2, flags) is None
    assert _decide(4, 1, flags) is None


def test_day3_does_not_repeat():
    """the day-3 email is one-shot"""
    flags = {EMAIL_DAY3: "2026-08-04T09:00:00+00:00"}
    assert _decide(3, 0, flags) is None


def test_in_progress_email_is_day_three_only():
    """a user who finishes their first walkthrough on day 6 gets nothing then"""
    flags = {EMAIL_DAY3: "2026-08-04T09:00:00+00:00"}
    assert _decide(6, 1, flags) is None


def test_completion_still_fires_after_the_day3_email():
    """A on day 3 then C on day 9 is the expected two-email path"""
    flags = {EMAIL_DAY3: "2026-08-04T09:00:00+00:00"}
    assert _decide(9, 2, flags) == EMAIL_COMPLETION


def test_midpoint_fires_at_day_seven():
    flags = {EMAIL_DAY3: "x", EMAIL_COMPLETION: "y"}
    assert _decide(7, 2, flags) == EMAIL_MIDPOINT


def test_midpoint_fires_even_after_completion():
    """midpoint and pre-end are unconditional — C does not suppress them"""
    flags = {EMAIL_DAY3: "x", EMAIL_COMPLETION: "y"}
    assert _decide(7, 2, flags) == EMAIL_MIDPOINT
    flags[EMAIL_MIDPOINT] = "z"
    assert _decide(12, 2, flags) == EMAIL_PRE_END


def test_pre_end_fires_two_days_before_the_end():
    flags = {EMAIL_DAY3: "x", EMAIL_MIDPOINT: "y"}
    assert _decide(11, 0, flags) is None
    assert _decide(12, 0, flags) == EMAIL_PRE_END


def test_only_one_email_per_run_when_two_rules_match():
    """a day-7 trial with no day3 flag matches rules 2 and 3 — the earlier rule wins"""
    assert _decide(7, 0) == EMAIL_DAY3


def test_flags_stamped_by_completion_includes_day3():
    """C stamps day3 too, or the next run would fire B on top of the congratulations"""
    assert set(FLAGS_STAMPED_BY[EMAIL_COMPLETION]) == {EMAIL_COMPLETION, EMAIL_DAY3}


def test_flags_stamped_by_other_emails_are_self_only():
    assert FLAGS_STAMPED_BY[EMAIL_DAY3] == (EMAIL_DAY3,)
    assert FLAGS_STAMPED_BY[EMAIL_MIDPOINT] == (EMAIL_MIDPOINT,)
    assert FLAGS_STAMPED_BY[EMAIL_PRE_END] == (EMAIL_PRE_END,)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest ddpui/tests/core/trial/test_lifecycle_emails.py -k "ladder or day3 or midpoint or pre_end or completion or flags_stamped or one_email" -v`
Expected: FAIL at import with `ImportError: cannot import name 'decide_email'`

- [ ] **Step 3: Implement the ladder**

First add `from datetime import timedelta` to the imports at the **top** of
`ddpui/core/trial/lifecycle_emails.py`. Then append:

```python
# Which flag keys each decision writes once its email has been sent. The completion email
# stamps `day3` as well as its own key: without that, the next hourly run would see "no day3
# flag" and fire the in-progress email on top of the congratulations email.
FLAGS_STAMPED_BY = {
    EMAIL_DAY3: (EMAIL_DAY3,),
    EMAIL_COMPLETION: (EMAIL_COMPLETION, EMAIL_DAY3),
    EMAIL_MIDPOINT: (EMAIL_MIDPOINT,),
    EMAIL_PRE_END: (EMAIL_PRE_END,),
}


def decide_email(day_number: int, completed_count: int, flags: dict, now, end_date):
    """Pick the ONE email due for this trial right now, or None.

    Rules are checked in order and the first match wins — at most one email per trial per run.
    A rule that was also eligible fires on a later run an hour later, so two emails never land
    in the same inbox at the same moment.

    Args:
        day_number: full days elapsed since OrgPlans.start_date
        completed_count: how many of TRACKED_FLOWS are complete (0, 1 or 2)
        flags: the user's `trial_emails_sent` dict
        now / end_date: used only by the pre-end rule

    Returns:
        one of the EMAIL_* constants, or None
    """
    flags = flags or {}

    # 1. Both walkthroughs done, on or after day 3. Sits above the day-3 rule so a user who
    #    finished everything by day 3 gets the congratulations, not the "pick up where you
    #    left off" nudge.
    if (
        day_number >= DAY3_THRESHOLD_DAYS
        and completed_count == len(TRACKED_FLOWS)
        and EMAIL_COMPLETION not in flags
    ):
        return EMAIL_COMPLETION

    # 2. The day-3 nudge — which of the two templates is chosen by the caller from
    #    completed_count. Guarded on the completion flag as well as its own so it can never
    #    follow a congratulations email.
    if (
        day_number >= DAY3_THRESHOLD_DAYS
        and EMAIL_DAY3 not in flags
        and EMAIL_COMPLETION not in flags
    ):
        return EMAIL_DAY3

    # 3 & 4. Date-driven lifecycle nudges. Unconditional on progress — they still fire for a
    #        user who already received the completion email, because an upgrade prompt and an
    #        expiry warning serve a different purpose from the progress nudges.
    if day_number >= MIDPOINT_THRESHOLD_DAYS and EMAIL_MIDPOINT not in flags:
        return EMAIL_MIDPOINT

    if now >= end_date - timedelta(days=PRE_END_DAYS_BEFORE) and EMAIL_PRE_END not in flags:
        return EMAIL_PRE_END

    return None
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest ddpui/tests/core/trial/test_lifecycle_emails.py -v`
Expected: PASS (all Task 6 tests plus 19 new ones)

- [ ] **Step 5: Commit**

```bash
pre-commit run --all-files
git add ddpui/core/trial/lifecycle_emails.py ddpui/tests/core/trial/test_lifecycle_emails.py
git commit -m "feat: add trial lifecycle email decision ladder"
```

---

### Task 8: The sweep — query, send, stamp

**Files:**
- Modify: `ddpui/core/trial/lifecycle_emails.py`
- Test: `ddpui/tests/core/trial/test_lifecycle_emails.py`

**Interfaces:**
- Consumes: everything from Tasks 6–7, the senders from Task 4, the settings from Task 5
- Produces:
  - `send_decided_email(kind, to_email, completed, day_number, total_days, end_date) -> None` — dispatches to the right sender
  - `process_trial(org_plan, now) -> str | None` — handles one trial end-to-end, returns the email kind sent or `None`
  - `run_trial_lifecycle_sweep(now=None) -> int` — returns how many emails were sent

- [ ] **Step 1: Write the failing test**

Add these imports to the **top** of `ddpui/tests/core/trial/test_lifecycle_emails.py`, beside the
existing ones. No `django.setup()` call is needed — `pyproject.toml` sets
`DJANGO_SETTINGS_MODULE` under `[tool.pytest.ini_options]`, so pytest-django configures Django
before collection.

```python
from unittest.mock import patch

from django.contrib.auth.models import User
from django.utils import timezone

from ddpui.models.org import Org
from ddpui.models.org_plans import OrgPlans, OrgPlanType
from ddpui.models.org_user import OrgUser
from ddpui.models.userpreferences import UserPreferences
from ddpui.core.trial.lifecycle_emails import run_trial_lifecycle_sweep
```

Then append the DB marker and the tests. `OrgUser.new_role` is nullable, so the fixture does not
need to seed roles.

```python
pytestmark = pytest.mark.django_db


def _make_trial(slug, days_ago, completed=(), plan=OrgPlanType.FREE_TRIAL.value, duration=14):
    """a free-trial org whose plan started `days_ago` days ago, with the given flows completed"""
    now = timezone.now()
    org = Org.objects.create(slug=slug, name=slug, airbyte_workspace_id=None)
    OrgPlans.objects.create(
        org=org,
        base_plan=plan,
        start_date=now - timedelta(days=days_ago),
        end_date=now - timedelta(days=days_ago) + timedelta(days=duration),
    )
    user = User.objects.create(username=f"{slug}@x.org", email=f"{slug}@x.org")
    orguser = OrgUser.objects.create(user=user, org=org)
    UserPreferences.objects.create(
        orguser=orguser,
        trial_walkthrough={flow: {"completed": True} for flow in completed},
    )
    return org, orguser


def test_sweep_sends_not_started_email_on_day_three():
    """a day-3 trial with nothing completed gets email A and is flagged"""
    org, orguser = _make_trial("trial-a", days_ago=3)
    with patch(
        "ddpui.core.trial.lifecycle_emails.send_trial_day3_not_started_email"
    ) as mock_send:
        assert run_trial_lifecycle_sweep() == 1
        mock_send.assert_called_once()
        assert mock_send.call_args[0][0] == "trial-a@x.org"

    prefs = UserPreferences.objects.get(orguser=orguser)
    assert EMAIL_DAY3 in prefs.trial_emails_sent


def test_sweep_sends_in_progress_email_with_the_completed_flow():
    """one completed flow routes to email B, and the flow name is passed through"""
    _make_trial("trial-b", days_ago=3, completed=("insights",))
    with patch(
        "ddpui.core.trial.lifecycle_emails.send_trial_day3_in_progress_email"
    ) as mock_send:
        assert run_trial_lifecycle_sweep() == 1
        assert mock_send.call_args[0][1] == "insights"


def test_sweep_sends_completion_email_and_stamps_both_flags():
    """both flows complete sends C and locks out the day-3 email"""
    _, orguser = _make_trial("trial-c", days_ago=5, completed=("insights", "automate_pipeline"))
    with patch("ddpui.core.trial.lifecycle_emails.send_trial_completion_email") as mock_send:
        assert run_trial_lifecycle_sweep() == 1
        mock_send.assert_called_once()

    prefs = UserPreferences.objects.get(orguser=orguser)
    assert EMAIL_COMPLETION in prefs.trial_emails_sent
    assert EMAIL_DAY3 in prefs.trial_emails_sent


def test_sweep_is_idempotent():
    """a second sweep with unchanged state sends nothing"""
    _make_trial("trial-d", days_ago=3)
    with patch("ddpui.core.trial.lifecycle_emails.send_trial_day3_not_started_email"):
        assert run_trial_lifecycle_sweep() == 1
        assert run_trial_lifecycle_sweep() == 0


def test_sweep_sends_one_email_per_run():
    """a day-7 trial with no flags gets the day-3 email first, midpoint on the next run"""
    _make_trial("trial-e", days_ago=7)
    with patch(
        "ddpui.core.trial.lifecycle_emails.send_trial_day3_not_started_email"
    ) as mock_a, patch(
        "ddpui.core.trial.lifecycle_emails.send_trial_midpoint_email"
    ) as mock_mid:
        assert run_trial_lifecycle_sweep() == 1
        assert mock_a.call_count == 1
        assert mock_mid.call_count == 0
        assert run_trial_lifecycle_sweep() == 1
        assert mock_mid.call_count == 1


def test_sweep_skips_non_trial_plans():
    """only Free Trial plans are swept"""
    _make_trial("paid-org", days_ago=5, plan=OrgPlanType.DALGO.value)
    assert run_trial_lifecycle_sweep() == 0


def test_sweep_skips_expired_trials():
    """a trial past its end_date has dropped out of the query"""
    _make_trial("trial-old", days_ago=20)
    assert run_trial_lifecycle_sweep() == 0


def test_sweep_skips_plans_without_a_start_date():
    """a null start_date cannot produce a day number"""
    org = Org.objects.create(slug="trial-nostart", name="x", airbyte_workspace_id=None)
    OrgPlans.objects.create(
        org=org,
        base_plan=OrgPlanType.FREE_TRIAL.value,
        start_date=None,
        end_date=timezone.now() + timedelta(days=5),
    )
    assert run_trial_lifecycle_sweep() == 0


def test_sweep_leaves_flag_unset_when_the_send_fails():
    """an SES failure must not mark the email as sent — the next run retries"""
    _, orguser = _make_trial("trial-f", days_ago=3)
    with patch(
        "ddpui.core.trial.lifecycle_emails.send_trial_day3_not_started_email",
        side_effect=Exception("ses down"),
    ):
        assert run_trial_lifecycle_sweep() == 0

    prefs = UserPreferences.objects.get(orguser=orguser)
    assert prefs.trial_emails_sent == {}


def test_sweep_continues_after_one_trial_raises():
    """one bad row must not stop the run"""
    _make_trial("trial-g", days_ago=3)
    _make_trial("trial-h", days_ago=3)
    calls = {"n": 0}

    def _flaky(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise Exception("ses down")

    with patch(
        "ddpui.core.trial.lifecycle_emails.send_trial_day3_not_started_email",
        side_effect=_flaky,
    ):
        assert run_trial_lifecycle_sweep() == 1
    assert calls["n"] == 2


def test_sweep_skips_a_trial_with_no_orguser():
    """a half-reaped trial has no recipient"""
    org = Org.objects.create(slug="trial-orphan", name="x", airbyte_workspace_id=None)
    OrgPlans.objects.create(
        org=org,
        base_plan=OrgPlanType.FREE_TRIAL.value,
        start_date=timezone.now() - timedelta(days=3),
        end_date=timezone.now() + timedelta(days=11),
    )
    assert run_trial_lifecycle_sweep() == 0


def test_sweep_creates_missing_preferences_rather_than_skipping():
    """a missing prefs row must not deny email A to the users it targets"""
    org = Org.objects.create(slug="trial-noprefs", name="x", airbyte_workspace_id=None)
    OrgPlans.objects.create(
        org=org,
        base_plan=OrgPlanType.FREE_TRIAL.value,
        start_date=timezone.now() - timedelta(days=3),
        end_date=timezone.now() + timedelta(days=11),
    )
    user = User.objects.create(username="noprefs@x.org", email="noprefs@x.org")
    OrgUser.objects.create(user=user, org=org)

    with patch("ddpui.core.trial.lifecycle_emails.send_trial_day3_not_started_email"):
        assert run_trial_lifecycle_sweep() == 1
    assert UserPreferences.objects.filter(orguser__org=org).exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest ddpui/tests/core/trial/test_lifecycle_emails.py -k sweep -v`
Expected: FAIL at import with `ImportError: cannot import name 'run_trial_lifecycle_sweep'`

- [ ] **Step 3: Implement the dispatcher and sweep**

Append to `ddpui/core/trial/lifecycle_emails.py`. Extend the imports at the top of the file first:

```python
from django.conf import settings
from django.db import transaction
from django.utils import timezone

from ddpui.models.org_plans import OrgPlans, OrgPlanType
from ddpui.models.org_user import OrgUser
from ddpui.models.userpreferences import UserPreferences
from ddpui.utils.awsses import (
    send_trial_day3_not_started_email,
    send_trial_day3_in_progress_email,
    send_trial_completion_email,
    send_trial_midpoint_email,
    send_trial_pre_end_email,
)
```

Then append:

```python
# How OrgPlans.end_date is formatted for the pre-end email, which takes a display string and
# does no date maths of its own.
END_DATE_DISPLAY_FORMAT = "%d %b %Y"


def send_decided_email(kind, to_email, completed, day_number, total_days, end_date) -> None:
    """Dispatch to the sender for `kind`.

    Which of the two day-3 templates goes out is decided here, from how many flows are
    complete — the ladder returns a single EMAIL_DAY3 decision because both templates share
    one dedupe flag.
    """
    workspace_url = settings.FRONTEND_URL_V2 or ""
    upgrade_url = settings.TRIAL_UPGRADE_URL
    call_url = settings.TRIAL_SCHEDULE_CALL_URL

    if kind == EMAIL_DAY3:
        if completed:
            send_trial_day3_in_progress_email(to_email, completed[0], workspace_url, call_url)
        else:
            send_trial_day3_not_started_email(to_email, workspace_url, call_url)
    elif kind == EMAIL_COMPLETION:
        send_trial_completion_email(to_email, upgrade_url, workspace_url, call_url)
    elif kind == EMAIL_MIDPOINT:
        send_trial_midpoint_email(to_email, day_number, total_days, upgrade_url, call_url)
    elif kind == EMAIL_PRE_END:
        send_trial_pre_end_email(
            to_email,
            day_number,
            total_days,
            end_date.strftime(END_DATE_DISPLAY_FORMAT),
            upgrade_url,
            call_url,
        )


def process_trial(org_plan: OrgPlans, now) -> str:
    """Decide, send and stamp for ONE trial. Returns the email kind sent, or None.

    The whole read-decide-send-stamp sequence runs under `select_for_update` on the
    UserPreferences row so two overlapping sweeps cannot both decide to send. The flag is
    written only AFTER the send returns, so an SES failure leaves it unset and the next run
    retries.
    """
    org = org_plan.org
    orguser = OrgUser.objects.filter(org=org).select_related("user").first()
    if orguser is None:
        logger.warning("trial org %s has no orguser; skipping lifecycle email", org.slug)
        return None

    with transaction.atomic():
        prefs, _ = UserPreferences.objects.select_for_update().get_or_create(orguser=orguser)
        day_number, total_days = trial_window(org_plan.start_date, org_plan.end_date, now)
        completed = completed_flows(prefs.trial_walkthrough)
        kind = decide_email(
            day_number, len(completed), prefs.trial_emails_sent, now, org_plan.end_date
        )
        if kind is None:
            return None

        send_decided_email(
            kind, orguser.user.email, completed, day_number, total_days, org_plan.end_date
        )

        stamped = dict(prefs.trial_emails_sent or {})
        for flag in FLAGS_STAMPED_BY[kind]:
            stamped[flag] = now.isoformat()
        prefs.trial_emails_sent = stamped
        prefs.save(update_fields=["trial_emails_sent"])

    logger.info("sent trial '%s' email to %s (org %s)", kind, orguser.user.email, org.slug)
    return kind


def run_trial_lifecycle_sweep(now=None) -> int:
    """Send any due trial lifecycle emails. Returns how many went out.

    Runs hourly. Postgres does the date filtering, so this only ever iterates live free trials —
    a handful of rows. Every trial past day 3 keeps matching the query on every run; the
    per-user flags, not the query, are what stop duplicates.
    """
    now = now or timezone.now()
    org_plans = OrgPlans.objects.filter(
        base_plan=OrgPlanType.FREE_TRIAL.value,
        start_date__isnull=False,
        end_date__gt=now,
    ).select_related("org")

    sent = 0
    for org_plan in org_plans:
        try:
            if process_trial(org_plan, now):
                sent += 1
        except Exception as err:  # skipcq PYL-W0703
            # one bad trial must not stop the sweep
            logger.error(
                "trial lifecycle email failed for org %s: %s", org_plan.org.slug, err
            )
    return sent
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest ddpui/tests/core/trial/test_lifecycle_emails.py -v`
Expected: PASS — all tests from Tasks 6, 7 and 8

- [ ] **Step 5: Commit**

```bash
pre-commit run --all-files
git add ddpui/core/trial/lifecycle_emails.py ddpui/tests/core/trial/test_lifecycle_emails.py
git commit -m "feat: add hourly trial lifecycle email sweep"
```

---

### Task 9: Celery task, beat entry, and retiring the superseded task

**Files:**
- Modify: `ddpui/celeryworkers/tasks.py:1130-1153` (delete `check_org_plan_expiry_notify_people`), around line 1308 (add the new task), `ddpui/celeryworkers/tasks.py:1349-1354` (swap the beat entry)
- Test: `ddpui/tests/core/trial/test_lifecycle_emails.py`

**Interfaces:**
- Consumes: `run_trial_lifecycle_sweep` from Task 8
- Produces: Celery task `ddpui.celeryworkers.tasks.send_trial_lifecycle_emails`, registered hourly under the beat name `"trial lifecycle emails"`

- [ ] **Step 1: Write the failing test**

Append to `ddpui/tests/core/trial/test_lifecycle_emails.py`:

```python
def test_celery_task_delegates_to_the_sweep():
    """the task is a thin wrapper — all logic lives in the sweep"""
    from ddpui.celeryworkers.tasks import send_trial_lifecycle_emails

    with patch(
        "ddpui.celeryworkers.tasks.run_trial_lifecycle_sweep", return_value=3
    ) as mock_sweep:
        assert send_trial_lifecycle_emails() == 3
        mock_sweep.assert_called_once_with()


def test_superseded_expiry_task_is_gone():
    """check_org_plan_expiry_notify_people duplicated the midpoint and pre-end emails"""
    import ddpui.celeryworkers.tasks as tasks_module

    assert not hasattr(tasks_module, "check_org_plan_expiry_notify_people")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest ddpui/tests/core/trial/test_lifecycle_emails.py -k "celery_task or superseded" -v`
Expected: FAIL — `ImportError: cannot import name 'send_trial_lifecycle_emails'` for the first, and an `AssertionError` for the second (the old task still exists).

- [ ] **Step 3: Delete the superseded task**

In `ddpui/celeryworkers/tasks.py`, delete the whole `check_org_plan_expiry_notify_people` function (from its `@app.task()` decorator through the closing `logger.error(err)`, currently lines 1129–1153).

It emails free-trial orgs at `end_date − 7d` and `end_date − 2d` — day 7 and day 12 of a 14-day trial, the exact days the midpoint and pre-end emails now cover. Left in place, every trial user would receive two emails on both days.

Then delete its beat registration (currently lines 1349–1354):

```python
    # check org plan expiry & notify users; daily at midnight
    sender.add_periodic_task(
        crontab(minute=0, hour=0),
        check_org_plan_expiry_notify_people.s(),
        name="check org plan expiry and notify the right people",
    )
```

After deleting, check whether `ACCOUNT_MANAGER_ROLE`, `pytz` or `OrgPlanType` are now unused in the module and remove any import that no longer has a consumer. Run `pre-commit run --all-files` — the linter will flag leftovers.

- [ ] **Step 4: Add the new task**

In `ddpui/celeryworkers/tasks.py`, add to the imports:

```python
from ddpui.core.trial.lifecycle_emails import run_trial_lifecycle_sweep
```

And add the task immediately before `setup_periodic_tasks`:

```python
@app.task()
def send_trial_lifecycle_emails():
    """send any due free-trial lifecycle emails; runs hourly

    Thin wrapper — the decision ladder and all the sending live in
    ddpui/core/trial/lifecycle_emails.py.
    """
    return run_trial_lifecycle_sweep()
```

- [ ] **Step 5: Register the hourly schedule**

Inside `setup_periodic_tasks`, where the deleted expiry entry used to be:

```python
    # free-trial lifecycle emails (day-3 nudges, completion, midpoint, pre-end); every hour.
    # Hourly rather than daily so the completion email lands within an hour of the user
    # finishing their second walkthrough. Supersedes the old check_org_plan_expiry task.
    sender.add_periodic_task(
        3600.0,
        send_trial_lifecycle_emails.s(),
        name="trial lifecycle emails",
    )
```

- [ ] **Step 6: Run test to verify it passes**

Run: `pytest ddpui/tests/core/trial/test_lifecycle_emails.py -v`
Expected: PASS (all tests)

- [ ] **Step 7: Verify the beat schedule registers cleanly**

Run: `python -c "import os; os.environ.setdefault('DJANGO_SETTINGS_MODULE','ddpui.settings'); import django; django.setup(); from ddpui.celery import app; import ddpui.celeryworkers.tasks; print('trial lifecycle task:', 'ddpui.celeryworkers.tasks.send_trial_lifecycle_emails' in app.tasks)"`
Expected: `trial lifecycle task: True`

- [ ] **Step 8: Run the full trial and email test suites**

Run: `pytest ddpui/tests/core/trial/ ddpui/tests/utils/test_email_templates.py ddpui/tests/utils/test_awsses.py ddpui/tests/api_tests/test_user_preferences_api.py -v`
Expected: PASS, no regressions

- [ ] **Step 9: Commit**

```bash
pre-commit run --all-files
git add ddpui/celeryworkers/tasks.py ddpui/tests/core/trial/test_lifecycle_emails.py
git commit -m "feat: schedule hourly trial lifecycle emails, retire plan-expiry task"
```

---

## Before shipping

`TRIAL_UPGRADE_URL` and `TRIAL_SCHEDULE_CALL_URL` are still empty, and Task 9 removes the old
expiry email that currently tells trial users to contact `support@dalgo.org` to renew. Until those
two settings are populated, a trial user reaching day 12 gets an expiry warning whose buttons go
nowhere. Populate them — or decide the upgrade destination per the spec's Open decisions — before
enabling this in production.
