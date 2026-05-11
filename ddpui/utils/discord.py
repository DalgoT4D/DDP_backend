import time
import requests


def send_discord_notification(webhook_url: str, message: str):
    """sends a plain text message to a discord webhook"""
    data = {"content": message}
    response = requests.post(webhook_url, json=data, timeout=10)
    response.raise_for_status()


def send_discord_embed(
    webhook_url: str,
    title: str,
    description: str,
    color: int,
    fields: list[dict] | None = None,
    footer: str | None = None,
):
    """
    sends a rich embed message to a discord webhook.

    color: integer RGB value (e.g. 0xFF0000 for red, 0x00C853 for green)
    fields: list of {"name": str, "value": str, "inline": bool}
    """
    embed = {
        "title": title,
        "description": description,
        "color": color,
        "timestamp": _unix_to_iso(time.time()),
        "fields": fields or [],
    }
    if footer:
        embed["footer"] = {"text": footer}

    response = requests.post(webhook_url, json={"embeds": [embed]}, timeout=10)
    response.raise_for_status()


def _unix_to_iso(ts: float) -> str:
    """converts a unix timestamp to ISO 8601 format expected by Discord"""
    import datetime

    return datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%dT%H:%M:%S.000Z")
