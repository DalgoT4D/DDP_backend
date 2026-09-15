"""HTML email templates for Dalgo notifications.

All outbound Dalgo emails live under this package, split by visual family:

- **Notification shell** (teal header, ``shell._render_email_shell``) —
  alert, mention, report share, generic in-app notification.
- **Trial shell** (white header + navy wordmark, ``trial_shell._render_trial_email_shell``) —
  verify / welcome / midpoint / pre-end / post-deletion / day-3 nudges.
- **Biz-dev plain-text** (``biz_dev``) — internal team notifications, no shell.

Each shell is defined exactly once; every ``render_*`` function delegates
to the appropriate shell. ``test_shell_is_single_source_of_truth`` /
``test_trial_shell_is_single_source_of_truth`` enforce the invariant.
"""

from ddpui.core.notifications.templates.alert import render_alert_email
from ddpui.core.notifications.templates.biz_dev import (
    WORK_DOMAIN_LABELS,
    build_new_org_signup_email,
    build_subscription_request_email,
)
from ddpui.core.notifications.templates.generic import render_notification_email
from ddpui.core.notifications.templates.mention import render_mention_email
from ddpui.core.notifications.templates.report_share import render_share_report_email
from ddpui.core.notifications.templates.shell import _render_email_shell
from ddpui.core.notifications.templates.trial import (
    TRIAL_FLOW_COPY,
    render_trial_completion_email,
    render_trial_day3_in_progress_email,
    render_trial_day3_not_started_email,
    render_trial_midpoint_email,
    render_trial_post_deletion_email,
    render_trial_pre_end_email,
    render_trial_welcome_email,
    render_verify_email,
)
from ddpui.core.notifications.templates.trial_shell import _render_trial_email_shell

__all__ = [
    "TRIAL_FLOW_COPY",
    "WORK_DOMAIN_LABELS",
    "_render_email_shell",
    "_render_trial_email_shell",
    "build_new_org_signup_email",
    "build_subscription_request_email",
    "render_alert_email",
    "render_mention_email",
    "render_notification_email",
    "render_share_report_email",
    "render_trial_completion_email",
    "render_trial_day3_in_progress_email",
    "render_trial_day3_not_started_email",
    "render_trial_midpoint_email",
    "render_trial_post_deletion_email",
    "render_trial_pre_end_email",
    "render_trial_welcome_email",
    "render_verify_email",
]
