# DDP Backend - Code Review Memory

## Project Structure
- Django + Ninja API framework
- Public API endpoints (no auth) at `ddpui/api/public_api.py`
- Models: `ddpui/models/dashboard.py`, `ddpui/models/report.py`, `ddpui/models/org.py`
- Tests: `ddpui/tests/api_tests/`
- Architecture: API -> Core -> Models (one-way dependency per CLAUDE.md)

## Key Patterns
- Public endpoints use token-based access (not auth): `Dashboard.public_share_token`, `ReportSnapshot.public_share_token`
- Both Dashboard and ReportSnapshot share the same public sharing field pattern (is_public, public_share_token, etc.)
- `_get_public_report_snapshot()` is a reusable helper for report endpoints with RENDER_SECRET bypass
- Dashboard endpoints do NOT have a similar reusable helper
- Test fixtures use `seed_db` for loading role/permission fixtures

## Known Issues (as of 2026-04-01)
- `public_api.py` has duplicate route registrations for `/regions/`, `/regions/{id}/children/`, `/regions/{id}/geojsons/`
- Dead code in `get_public_filter_preview`: unused `get_filter_preview` import, `MockRequest` class, `mock_request` variable
- Dashboard-only endpoints (`get_public_chart_metadata`, `get_public_chart_data`, etc.) only look up Dashboard, not ReportSnapshot
- `get_public_filter_preview` is the ONLY shared endpoint that falls back to ReportSnapshot
- `OrgWarehouse` is imported both at module level (line 22) and locally inside `get_public_filter_preview` (line 348)

## Testing Patterns
- Use `uv run pytest` for tests
- `seed_db` fixture loads roles/permissions from JSON fixtures
- Mock paths: For module-level imports use `ddpui.api.public_api.X`; for local imports either path works since they reference the same class object
