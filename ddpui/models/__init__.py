from ddpui.models.tasks import Task
from ddpui.models.llm import AssistantPrompt
from ddpui.models.airbyte import SyncStats, AirbyteJob
from ddpui.models.org_preferences import OrgPreferences
from ddpui.models.org_supersets import OrgSupersets
from ddpui.models.org_wren import OrgWren
from ddpui.models.visualization import Chart
from ddpui.models.dashboard import Dashboard, DashboardFilter, DashboardLock
from ddpui.models.admin_user import AdminUser
from ddpui.models.georegion import GeoRegion
from ddpui.models.geojson import GeoJSON
from ddpui.models.canvas_models import CanvasNode, CanvasEdge
from ddpui.models.comment import Comment, CommentReadStatus
from ddpui.models.metric import Metric, KPI
from ddpui.models.alert import Alert, AlertLog, AlertType
from ddpui.models.trial_signup import TrialSignup

# Not re-exported for API use — imported so these models are ALWAYS registered
# with the app registry. Their modules were previously only imported by feature
# code, so any process that skipped that code didn't know their tables existed;
# Django's test-database flush (TRUNCATE) then failed on their FKs.
# MapLayer is deliberately NOT imported: it has no migration (its table is
# loaded out-of-band from geojson dumps), so registering it breaks test-database
# creation, and no other model has a foreign key to it.
from ddpui.models.canvaslock import CanvasLock
from ddpui.models.flow_runs import PrefectFlowRun
from ddpui.models.notifications import Notification, NotificationRecipient
from ddpui.models.org_plans import OrgPlans
from ddpui.models.orgtnc import OrgTnC
from ddpui.models.userpreferences import UserPreferences
from ddpui.models.chat_with_data import (
    ChatWithDataOrgConfig,
    ChatWithDataSession,
    ChatWithDataTurnAudit,
)
