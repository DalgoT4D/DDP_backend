# User Management

## Overview
Multi-tenant user system with role-based access control.

## Key Components
- **Models**: `ddpui/models/org_user.py`, `ddpui/models/role_based_access.py`
- **API**: `ddpui/api/user_org_api.py`
- **Auth**: `ddpui/auth.py`

## Features
- User registration/login
- Organization management
- Role-based permissions
- Email verification

## API Endpoints
- `POST /api/users/signup/` - User registration
- `POST /api/users/login/` - User login
- `GET /api/users/org/users/` - List organization users

## Management Commands
```bash
python manage.py createorganduser "Org Name" "email@example.com" --role super-admin
```

## Related
- Authentication
- Organizations
- Permissions