"""Result-set comparison tests — the eval_sql_correct hard metric."""

from decimal import Decimal

from ddpui.core.ai.evals.sql_compare import answer_contains_value, result_sets_match


def test_row_order_does_not_matter():
    gold = [{"region": "East", "n": 40}, {"region": "West", "n": 48}]
    agent = [["West", "48"], ["East", "40"]]  # UI result-table shape, reordered
    assert result_sets_match(gold, agent)


def test_column_aliases_do_not_matter():
    gold = [{"total": Decimal("14909222.00")}]
    agent = [{"sum_amount": 14909222.0}]
    assert result_sets_match(gold, agent)


def test_numeric_tolerance_and_string_numbers_agree():
    assert result_sets_match([{"avg": 34.9}], [["34.900000001"]])


def test_wrong_value_fails():
    assert not result_sets_match([{"n": 171}], [["300"]])  # the grain trap


def test_missing_and_extra_rows_fail():
    gold = [{"region": "East", "n": 40}, {"region": "West", "n": 48}]
    assert not result_sets_match(gold, [["East", "40"]])
    assert not result_sets_match([["East", "40"]], gold)


def test_duplicate_rows_are_counted_not_set_matched():
    assert not result_sets_match([["a"], ["a"]], [["a"], ["b"]])


def test_enriched_scalar_result_passes_when_answer_asserts_it():
    """The agent returned context columns alongside the right value AND said
    the right number — the real first-run failure this rule fixes."""
    from ddpui.core.ai.evals.sql_compare import gold_satisfied

    gold = [{"n": 171}]
    agent = [["200", "171", "300", "89"]]
    assert gold_satisfied(gold, agent, "**171** beneficiaries are enrolled.")


def test_enriched_result_fails_when_answer_asserts_the_wrong_number():
    """171 sits in a context column, but the agent CLAIMED 300 — must fail."""
    from ddpui.core.ai.evals.sql_compare import gold_satisfied

    gold = [{"n": 171}]
    agent = [["200", "171", "300", "89"]]
    assert not gold_satisfied(gold, agent, "**300** beneficiaries are enrolled.")


def test_breakdown_with_extra_columns_passes_by_projection():
    from ddpui.core.ai.evals.sql_compare import gold_satisfied

    gold = [{"region": "East", "n": 40}, {"region": "West", "n": 48}]
    agent = [["West", "48", "0.55"], ["East", "40", "0.45"]]  # extra share column
    assert gold_satisfied(gold, agent, "East has 40 and West has 48.")


def test_breakdown_with_wrong_or_extra_rows_fails():
    from ddpui.core.ai.evals.sql_compare import gold_satisfied

    gold = [{"region": "East", "n": 40}, {"region": "West", "n": 48}]
    assert not gold_satisfied(gold, [["East", "40"]], "East has 40.")
    assert not gold_satisfied(gold, [["East", "41", "x"], ["West", "48", "y"]], "…")


def test_scalar_count_satisfied_by_row_count():
    """'7 states' answered with a 7-row breakdown — the row count IS the value."""
    from ddpui.core.ai.evals.sql_compare import gold_satisfied

    gold = [{"states_impacted": 7}]
    agent = [["Bihar", "3"], ["MP", "8"], ["Rajasthan", "3"], ["Telangana", "3"],
             ["Jharkhand", "2"], ["Chattisgarh", "1"], ["Uttarakhand", "1"]]  # fmt: skip
    assert gold_satisfied(gold, agent, "**7** states were impacted.")
    assert not gold_satisfied(gold, agent, "**8** states were impacted.")


def test_single_row_gold_found_in_full_ranking():
    """LIMIT-1 golds pass when the agent shows the whole ranking and narrates
    the right winner — and fail when it narrates the wrong one."""
    from ddpui.core.ai.evals.sql_compare import gold_satisfied

    gold = [{"state": "Uttarakhand", "total": 0.0}]
    agent = [["Uttarakhand", "0.0"], ["Rajasthan", "4012.5"], ["Bihar", "54973.4"]]
    assert gold_satisfied(gold, agent, "Uttarakhand achieved the least (0).")
    assert not gold_satisfied(gold, agent, "Rajasthan achieved the least.")


def test_label_formatting_tolerated_in_projection():
    from ddpui.core.ai.evals.sql_compare import gold_satisfied

    gold = [{"period": "Q3", "total": 155718.60}, {"period": "Q4", "total": 4012.50}]
    agent = [["t", "Q3 (Oct-Dec 2025)", "155718.6"], ["t", "Q4 (Jan-Mar 2026)", "4012.5"]]
    assert gold_satisfied(gold, agent, "Q3 saw 155,718.6 vs Q4's 4,012.5.")


def test_answer_contains_value_ignores_thousands_separators_and_case():
    assert answer_contains_value("You received **₹1,49,09,222** in total.", "14909222")
    assert answer_contains_value("Most are in PUNE district.", "Pune")
    assert not answer_contains_value("You received 12,000.", "14909222")
