from ddpui.models import AuditLog
from django.contrib.auth.models import User
from ddpui.models.org import Org

class AuditService:
    @staticmethod
    def log_action(user: User, action: str, resource_type: str, resource_id: str = None, org: Org = None, payload: dict = None, request = None):
        """
        Creates an audit log entry.
        """
        ip_address = None
        user_agent = None
        
        if request:
            # Extract IP address and User-Agent from request if provided
            x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
            if x_forwarded_for:
                ip_address = x_forwarded_for.split(',')[0]
            else:
                ip_address = request.META.get('REMOTE_ADDR')
            
            user_agent = request.META.get('HTTP_USER_AGENT')

        AuditLog.objects.create(
            user=user,
            org=org,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            payload=payload,
            ip_address=ip_address,
            user_agent=user_agent
        )
