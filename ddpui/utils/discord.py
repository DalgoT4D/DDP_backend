import requests


def send_discord_notification(webhook_url, message):
    """sends a message to a discord webhook"""
    data = {"content": message}

    response = requests.post(webhook_url, json=data, timeout=10)

    if response.status_code != 204:
        raise Exception(
            f"Failed to send notification. Status code: {response.status_code}, Response: {response.text}"
        )
