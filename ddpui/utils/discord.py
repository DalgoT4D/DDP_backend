import requests


def send_discord_notification(webhook_url, message):
    data = {"content": message}

    response = requests.post(webhook_url, json=data)

    if response.status_code != 204:
        raise Exception(
            f"Failed to send notification. Status code: {response.status_code}, Response: {response.text}"
        )
