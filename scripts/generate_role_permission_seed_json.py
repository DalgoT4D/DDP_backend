import json
import pandas as pd


roles = pd.read_excel("scripts/RBAC.xlsx", sheet_name="role")

permissions = pd.read_excel("scripts/RBAC.xlsx", sheet_name="permissions")

roles.rename(
    columns={"Name": "name", "Slug": "slug", "ID": "id", "Level": "level"}, inplace=True
)
roles_dict = []
for role in roles.to_dict(orient="records"):
    id = role["id"]
    del role["id"]
    temp = {
        "model": "ddpui.Role",
        "pk": id,
        "fields": {"name": role["name"], "slug": role["slug"], "level": role["level"]},
    }
    roles_dict.append(temp)

with open("seed/001_roles.json", "w") as f:
    json.dump(roles_dict, f)

permissions.rename(columns={"Name": "name", "Slug": "slug", "ID": "id"}, inplace=True)
permission_dict = []
role_permissions_map = []
for permission in permissions.to_dict(orient="records"):
    id = permission["id"]
    del permission["id"]
    temp = {
        "model": "ddpui.Permission",
        "pk": id,
        "fields": {"name": permission["name"], "slug": permission["slug"]},
    }
    permission_dict.append(temp)

    for role in roles_dict:
        role_slug = role["fields"]["slug"]
        if permission[role_slug]:
            role_permissions_map.append(
                {
                    "model": "ddpui.RolePermission",
                    "pk": len(role_permissions_map) + 1,
                    "fields": {"role": role["pk"], "permission": id},
                }
            )

with open("seed/002_permissions.json", "w") as f:
    json.dump(permission_dict, f)


with open("seed/003_role_permissions.json", "w") as f:
    json.dump(role_permissions_map, f)
