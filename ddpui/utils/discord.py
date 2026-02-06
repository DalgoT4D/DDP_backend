import requests


def send_discord_notification(webhook_url, message):
    """sends a message to a discord webhook"""
    data = {"content": message}

    response = requests.post(webhook_url, json=data, timeout=10)

    response.raise_for_status()
