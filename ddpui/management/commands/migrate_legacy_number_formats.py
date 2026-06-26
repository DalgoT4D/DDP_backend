"""Migrate legacy ``numberFormat`` values on Chart + ReportSnapshot rows.

Before this enhancement:
- ``percentage`` simply appended ``%`` (no multiplication)
- ``currency`` prepended ``$`` with en_US grouping

After this enhancement:
- ``percentage`` multiplies the value by 100 and appends ``%`` (real percent)
- ``currency`` is removed; users use ``numberPrefix='$'`` instead

This command rewrites every Chart row + every frozen chart config inside
``ReportSnapshot.frozen_chart_configs`` holding a legacy value into a safe
equivalent. ``FrozenKpiConfig`` entries (KPI snapshots) have no
``extra_config`` field, so they are skipped automatically.

Usage:
    python manage.py migrate_legacy_number_formats --dry-run
    python manage.py migrate_legacy_number_formats
    python manage.py migrate_legacy_number_formats --charts-only
    python manage.py migrate_legacy_number_formats --snapshots-only
"""

from django.core.management.base import BaseCommand

from ddpui.models.report import ReportSnapshot
from ddpui.models.visualization import Chart


# ──────────────────────────────────────────────────────────────────────────
# Transformation helpers (pure; no DB access)
# ──────────────────────────────────────────────────────────────────────────
#
# Number chart has ``numberPrefix`` / ``numberSuffix`` companion fields, so we
# preserve the OLD render exactly:
#
#     percentage  → numberFormat="default"  + numberSuffix = (old) + "%"
#     currency    → numberFormat="default"  + numberPrefix = "$" + (old)
#
# Pie ``numberFormat``, Bar/Line ``xAxisNumberFormat`` + ``yAxisNumberFormat``,
# and Table ``columnFormatting[col].numberFormat`` have no prefix/suffix slots.
# For those:
#   - ``percentage`` is LEFT IN PLACE — the new multiply-by-100 semantic kicks
#     in at render time. Values will change (e.g. an axis label of "50%" now
#     renders as "5000%" if the underlying number was 50, or as "50%" if it
#     was 0.5). Per product call: numbers may change; the format itself stays
#     correct going forward.
#   - ``currency`` must be migrated because the new NumberFormat Literal
#     rejects it. It's demoted to ``default`` (loses ``$`` + grouping). No
#     prefix slot exists to preserve the symbol on these surfaces.


def _migrate_number_chart(cust: dict, log: list) -> None:
    """Number chart — preserve old behavior via prefix/suffix companion fields."""
    nf = cust.get("numberFormat")
    if nf == "percentage":
        cust["numberFormat"] = "default"
        cust["numberSuffix"] = (cust.get("numberSuffix") or "") + "%"
        log.append("numberFormat: percentage -> default + suffix '%'")
    elif nf == "currency":
        cust["numberFormat"] = "default"
        cust["numberPrefix"] = "$" + (cust.get("numberPrefix") or "")
        log.append("numberFormat: currency -> default + prefix '$'")


def _migrate_currency_only(cust: dict, key: str, label_prefix: str, log: list) -> None:
    """Migrate legacy ``currency`` at ``cust[key]`` to ``default``.

    ``percentage`` is intentionally left in place on these axis / column slots
    so the new multiply-by-100 semantic kicks in at render. No prefix slot
    exists here to preserve the ``$`` for currency.
    """
    val = cust.get(key)
    if val == "currency":
        cust[key] = "default"
        log.append(f"{label_prefix}{key}: currency -> default")


def normalize_customizations(cust, chart_type: str) -> list:
    """Mutate ``cust`` in place. Returns log entries describing what changed.

    Idempotent: a re-run finds no remaining ``currency`` value and produces
    no entries. ``percentage`` is intentionally left on non-Number paths.
    """
    if not isinstance(cust, dict):
        return []
    log: list = []

    if chart_type == "number":
        _migrate_number_chart(cust, log)
    elif chart_type == "pie":
        _migrate_currency_only(cust, "numberFormat", "", log)
    elif chart_type in ("bar", "line"):
        _migrate_currency_only(cust, "xAxisNumberFormat", "", log)
        _migrate_currency_only(cust, "yAxisNumberFormat", "", log)
    elif chart_type == "table":
        col_fmt = cust.get("columnFormatting")
        if isinstance(col_fmt, dict):
            for col, col_cfg in col_fmt.items():
                if isinstance(col_cfg, dict):
                    _migrate_currency_only(
                        col_cfg, "numberFormat", f"columnFormatting[{col}].", log
                    )
    # else: 'map' or unknown — no numberFormat to migrate

    return log


def walk_extra_config(extra_config, chart_type: str) -> list:
    """Drill into ``extra_config.customizations`` and normalize. Returns log.

    Returns empty list (and does nothing) when ``extra_config`` is not a dict
    or ``customizations`` is missing / not a dict.
    """
    if not isinstance(extra_config, dict):
        return []
    cust = extra_config.get("customizations")
    if not isinstance(cust, dict):
        return []
    log = normalize_customizations(cust, chart_type)
    if log:
        # Re-assign so the parent dict's reference picks up the mutation
        extra_config["customizations"] = cust
    return log


# ──────────────────────────────────────────────────────────────────────────
# Management command
# ──────────────────────────────────────────────────────────────────────────


class Command(BaseCommand):
    help = (
        "Normalize legacy 'percentage'/'currency' numberFormat values on Chart "
        "rows and inside ReportSnapshot.frozen_chart_configs."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print what would change without writing anything",
        )
        scope = parser.add_mutually_exclusive_group()
        scope.add_argument(
            "--charts-only",
            action="store_true",
            help="Only process Chart rows (skip ReportSnapshot)",
        )
        scope.add_argument(
            "--snapshots-only",
            action="store_true",
            help="Only process ReportSnapshot rows (skip Chart)",
        )

    def handle(self, *args, **options):
        dry_run: bool = options["dry_run"]
        charts_only: bool = options["charts_only"]
        snapshots_only: bool = options["snapshots_only"]

        header = ("DRY RUN — " if dry_run else "") + (
            "Normalizing legacy 'percentage'/'currency' numberFormat values"
        )
        self.stdout.write("")
        self.stdout.write("=" * 72)
        self.stdout.write(header)
        self.stdout.write("=" * 72)

        if not snapshots_only:
            self._migrate_charts(dry_run=dry_run)
        if not charts_only:
            self._migrate_snapshots(dry_run=dry_run)

        self.stdout.write("=" * 72)

    # ── Charts ─────────────────────────────────────────────────────────

    def _migrate_charts(self, *, dry_run: bool) -> None:
        self.stdout.write("")
        self.stdout.write("--- Charts ---")

        per_type: dict = {"number": 0, "pie": 0, "bar": 0, "line": 0, "table": 0}
        total = 0
        to_save: list = []

        for chart in Chart.objects.only("id", "title", "chart_type", "extra_config").iterator(
            chunk_size=500
        ):
            cfg = chart.extra_config or {}
            log = walk_extra_config(cfg, chart.chart_type or "")
            if not log:
                continue

            total += 1
            if chart.chart_type in per_type:
                per_type[chart.chart_type] += 1

            self.stdout.write(f"  Chart #{chart.id} ({chart.chart_type}) {chart.title!r}:")
            for entry in log:
                self.stdout.write(f"      - {entry}")

            if not dry_run:
                chart.extra_config = cfg
                to_save.append(chart)

        if not dry_run and to_save:
            Chart.objects.bulk_update(to_save, ["extra_config"], batch_size=500)

        verb = "would update" if dry_run else "updated"
        self.stdout.write("")
        if total:
            self.stdout.write(self.style.SUCCESS(f"Charts {verb}: {total}"))
            for ct, n in per_type.items():
                if n:
                    self.stdout.write(f"  - {ct}: {n}")
        else:
            self.stdout.write(self.style.SUCCESS(f"Charts {verb}: 0 (nothing to migrate)"))

    # ── Report snapshots ───────────────────────────────────────────────

    def _migrate_snapshots(self, *, dry_run: bool) -> None:
        self.stdout.write("")
        self.stdout.write("--- Report snapshots ---")

        snap_total = 0
        entity_total = 0
        to_save: list = []

        for snap in ReportSnapshot.objects.only("id", "title", "frozen_chart_configs").iterator(
            chunk_size=500
        ):
            frozen = snap.frozen_chart_configs or {}
            if not isinstance(frozen, dict):
                continue

            snap_log: list = []
            for entity_id, frozen_cfg in frozen.items():
                if not isinstance(frozen_cfg, dict):
                    continue
                ct = frozen_cfg.get("chart_type", "")
                log = walk_extra_config(frozen_cfg.get("extra_config"), ct)
                if log:
                    frozen[entity_id] = frozen_cfg
                    snap_log.append((entity_id, ct, log))

            if not snap_log:
                continue

            snap_total += 1
            entity_total += len(snap_log)

            self.stdout.write(f"  Snapshot #{snap.id} {snap.title!r}:")
            for entity_id, ct, log in snap_log:
                self.stdout.write(f"      entity {entity_id} ({ct or 'unknown'}):")
                for entry in log:
                    self.stdout.write(f"          - {entry}")

            if not dry_run:
                snap.frozen_chart_configs = frozen
                to_save.append(snap)

        if not dry_run and to_save:
            ReportSnapshot.objects.bulk_update(to_save, ["frozen_chart_configs"], batch_size=500)

        verb = "would update" if dry_run else "updated"
        self.stdout.write("")
        if snap_total:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Snapshots {verb}: {snap_total} " f"({entity_total} frozen chart configs)"
                )
            )
        else:
            self.stdout.write(self.style.SUCCESS(f"Snapshots {verb}: 0 (nothing to migrate)"))
