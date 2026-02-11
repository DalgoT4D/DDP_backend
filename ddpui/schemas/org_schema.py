from typing import Optional
from pydantic import HttpUrl

from ninja import Schema


class OrgSchema(Schema):
    """Docstring"""

    name: str
    slug: str = None
    airbyte_workspace_id: str = None
    viz_url: HttpUrl = None
    viz_login_type: str = None
    tnc_accepted: bool = None
    is_demo: bool = False


class CreateOrgSchema(Schema):
    """payload for org creation"""

    name: str
    slug: str = None
    airbyte_workspace_id: str = None
    viz_url: HttpUrl = None
    viz_login_type: str = None
    tnc_accepted: bool = None
    is_demo: bool = False
    base_plan: str
    can_upgrade_plan: bool
    subscription_duration: str
    superset_included: bool
    start_date: Optional[str]
    end_date: Optional[str]
    website: Optional[HttpUrl] = None


class CreateFreeTrialOrgSchema(CreateOrgSchema):
    """payload for org creation specifically for free trial account"""

    email: str
    superset_ec2_machine_id: str
    superset_port: int
