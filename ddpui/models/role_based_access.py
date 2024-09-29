from django.db import models
import uuid


class Role(models.Model):
    """Roles for an orguser"""

    uuid = models.UUIDField(editable=False, unique=True, default=uuid.uuid4)
    slug = models.CharField(max_length=255, unique=True)
    name = models.CharField(max_length=255)
    level = models.SmallIntegerField(default=1)  # keep the lowest role as default

    def __str__(self):
        return f"{self.name} | {self.slug} | {self.level}"


class Permission(models.Model):
    """List of permissions to be assigned to roles"""

    uuid = models.UUIDField(editable=False, unique=True, default=uuid.uuid4)
    slug = models.CharField(max_length=255, unique=True)
    name = models.CharField(max_length=255)

    def __str__(self):
        return f"{self.name} | {self.slug}"


class RolePermission(models.Model):
    """Mapping of roles to permissions"""

    role = models.ForeignKey(Role, on_delete=models.CASCADE, related_name="rolepermissions")
    permission = models.ForeignKey(
        Permission,
        on_delete=models.CASCADE,
    )

    def __str__(self):
        return f"{self.role.slug} | {self.permission.slug}"
