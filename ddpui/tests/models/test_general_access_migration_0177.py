"""Tests for the 0177 data migration mapping functions (D1: per-role
general access -- ``analyst_level``/``member_level`` replacing the old
``audience``/``level`` pair, at both the per-resource layer and the
OrgPreferences org-default layer).

There is no django-test-migrations-style harness in this repo (see
``test_general_access_backfill.py`` for the 0169 precedent), and unlike
0169 this migration REMOVES the old columns in the same migration it adds
the new ones -- by the time the migration finishes, the current-state
models no longer have ``general_audience``/``general_level`` fields at
all, so there is no way to round-trip the RunPython functions against real
ORM model instances the way 0169's test does.

Instead this tests the pure (audience, level) <-> (analyst_level,
member_level) mapping functions directly -- they take/return plain
strings, no DB access, and are exactly what the migration's RunPython
functions delegate to for every row.
"""

import importlib
import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ddpui.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
django.setup()

migration = importlib.import_module("ddpui.migrations.0177_per_role_general_access_levels")


class TestForwardMapping:
    """old (audience, level) -> new (analyst_level, member_level)."""

    def test_private_collapses_to_none_none_regardless_of_level(self):
        assert migration._forward_pair("private", "view") == ("none", "none")
        assert migration._forward_pair("private", "edit") == ("none", "none")

    def test_admins_collapses_to_none_none_regardless_of_level(self):
        """Admins are never stored -- an old `admins` audience row maps to
        (none, none) same as `private`, since Admin access is always the
        org-wide resolver override, never general access."""
        assert migration._forward_pair("admins", "view") == ("none", "none")
        assert migration._forward_pair("admins", "edit") == ("none", "none")

    def test_analysts_plus_gives_analyst_the_level_member_none(self):
        assert migration._forward_pair("analysts_plus", "view") == ("view", "none")
        assert migration._forward_pair("analysts_plus", "edit") == ("edit", "none")

    def test_all_users_gives_both_roles_the_same_level(self):
        assert migration._forward_pair("all_users", "view") == ("view", "view")
        assert migration._forward_pair("all_users", "edit") == ("edit", "edit")

    def test_unrecognized_audience_default_denies_both_roles(self):
        """Defensive: an unexpected/legacy audience value never raises --
        mirrors the resolver's 'unknown -> None' philosophy."""
        assert migration._forward_pair("some-legacy-value", "view") == ("none", "none")


class TestReverseMapping:
    """new (analyst_level, member_level) -> old (audience, level),
    best-effort and documented-lossy for mixed combinations."""

    def test_none_none_reverses_to_private(self):
        audience, _level = migration._reverse_pair("none", "none")
        assert audience == "private"

    def test_analyst_only_reverses_to_analysts_plus_at_that_level(self):
        assert migration._reverse_pair("view", "none") == ("analysts_plus", "view")
        assert migration._reverse_pair("edit", "none") == ("analysts_plus", "edit")

    def test_matching_levels_reverse_to_all_users_at_that_level(self):
        assert migration._reverse_pair("view", "view") == ("all_users", "view")
        assert migration._reverse_pair("edit", "edit") == ("all_users", "edit")

    def test_mixed_edit_view_reverses_to_analysts_plus_edit_lossy(self):
        """Documented lossy case from the D1 brief: (edit, view) has no
        exact old-model equivalent (the old model can't give Analyst more
        than Member independently) -- collapses to analysts_plus/edit."""
        assert migration._reverse_pair("edit", "view") == ("analysts_plus", "edit")

    def test_other_mixed_combinations_also_collapse_to_analysts_plus_edit(self):
        """Every other inexpressible combination (view/edit reversed, or a
        member level with no matching analyst level) uses the same lossy
        fallback -- the old model has no way to represent 'Member sees
        something Analyst doesn't' at all."""
        assert migration._reverse_pair("view", "edit") == ("analysts_plus", "edit")
        assert migration._reverse_pair("none", "view") == ("analysts_plus", "edit")
        assert migration._reverse_pair("none", "edit") == ("analysts_plus", "edit")

    def test_reverse_is_defined_for_every_reachable_combination(self):
        """The 3x3 (analyst_level, member_level) space is fully covered --
        no combination falls through to the defensive default silently."""
        levels = ("none", "view", "edit")
        for analyst_level in levels:
            for member_level in levels:
                assert (analyst_level, member_level) in migration.REVERSE_PAIR


class TestRoundTrip:
    """Forward then reverse doesn't have to recover the original for the
    lossy cases, but MUST be idempotent/stable and never raise."""

    def test_forward_then_reverse_is_lossless_for_the_documented_exact_cases(self):
        exact_cases = [
            ("private", "view"),
            ("analysts_plus", "view"),
            ("analysts_plus", "edit"),
            ("all_users", "view"),
            ("all_users", "edit"),
        ]
        for audience, level in exact_cases:
            analyst_level, member_level = migration._forward_pair(audience, level)
            recovered_audience, recovered_level = migration._reverse_pair(
                analyst_level, member_level
            )
            assert (recovered_audience, recovered_level) == (audience, level)

    def test_admins_forward_then_reverse_lands_on_private_not_admins(self):
        """`admins` is NOT recoverable -- it collapses to the same
        (none, none) as `private` going forward, and the reverse mapping's
        documented choice for (none, none) is `private` (per the D1 brief),
        so an old `admins` row becomes `private` after a forward+reverse
        round trip. This is intentional lossiness, not a bug."""
        analyst_level, member_level = migration._forward_pair("admins", "edit")
        assert migration._reverse_pair(analyst_level, member_level) == ("private", "view")
