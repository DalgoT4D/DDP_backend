import json
from django.conf import settings
from ddpui.utils.custom_logger import CustomLogger

logger = CustomLogger("ddpui.core.trial.source_config")


def _load_file() -> dict:
    path = getattr(settings, "TEMPLATE_SOURCE_CREDS_FILE", None)
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        logger.error(f"template source creds file not found: {path}")
        return {}


def load_template_source_config(source_name: str) -> dict | None:
    """real (unmasked) Airbyte source config for a template source, keyed by source name."""
    return _load_file().get(source_name)


def validate_template_source_configs(source_names: list[str]) -> list[str]:
    """return the template source names that have NO entry in the creds file."""
    have = _load_file()
    return [name for name in source_names if name not in have]
