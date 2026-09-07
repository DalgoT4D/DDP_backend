"""Notification triggers — one module per user-facing event.

Each helper resolves recipients, writes in-app rows via ``create_notification``,
and sends specialized emails via the templates in ``../templates``. Callers
(resource business logic) invoke a single trigger function and forget about
the fan-out; new channels or template changes land here, not in the callers.
"""
