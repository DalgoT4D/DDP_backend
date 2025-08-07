# Notifications v2 Enhancement Documentation

## Overview

This document describes the enhancements made to the Dalgo notification system to support categorized notifications, category-based subscriptions, and urgent notification management.

## New Features

### 1. Notification Categories

Notifications are now categorized into the following types:

- **incident**: Platform incidents, downtime notifications (sent by platform admins)
- **schema_change**: Database schema changes (sent by platform)
- **job_failure**: Job execution failures (sent by platform)
- **late_runs**: Pipeline runs that are running late (sent by platform)
- **dbt_test_failure**: DBT test failures (sent by platform)

### 2. Category-based Subscriptions

Users can now subscribe/unsubscribe from specific notification categories:

- Each user has individual subscription preferences for each category
- By default, users are subscribed to all categories
- Notifications are only sent to users who are subscribed to that category
- Subscription preferences are stored in the UserPreferences model

### 3. Urgent Notification Bar

Urgent notifications now support dismissal functionality:

- Urgent notifications can be dismissed by individual users
- Dismissed notifications won't appear in the urgent notification bar
- The system tracks which users have dismissed each urgent notification

## Database Changes

### Notification Model Updates

```python
class Notification(models.Model):
    # ... existing fields ...
    category = models.CharField(
        max_length=20,
        choices=NotificationCategory.choices,
        default=NotificationCategory.INCIDENT,
        help_text="Category of the notification"
    )
    dismissed_by = models.ManyToManyField(
        OrgUser,
        blank=True,
        related_name="dismissed_notifications",
        help_text="Users who have dismissed this urgent notification"
    )
```

### UserPreferences Model Updates

```python
class UserPreferences(models.Model):
    # ... existing fields ...
    subscribe_incident_notifications = models.BooleanField(default=True)
    subscribe_schema_change_notifications = models.BooleanField(default=True)
    subscribe_job_failure_notifications = models.BooleanField(default=True)
    subscribe_late_runs_notifications = models.BooleanField(default=True)
    subscribe_dbt_test_failure_notifications = models.BooleanField(default=True)
```

## API Endpoints

### Enhanced Existing Endpoints

#### 1. Create Notification
**POST** `/notifications/`

Now accepts a `category` field:

```json
{
    "author": "admin@example.com",
    "message": "System maintenance scheduled",
    "sent_to": "all_users",
    "urgent": true,
    "category": "incident"
}
```

#### 2. Get Notification History
**GET** `/notifications/history`

Now supports category filtering:

```
GET /notifications/history?category=incident&page=1&limit=10
```

#### 3. Get User Notifications
**GET** `/notifications/v1`

Now supports category filtering:

```
GET /notifications/v1?category=job_failure&read_status=0
```

### New Endpoints

#### 1. Get Urgent Notifications
**GET** `/notifications/urgent`

Returns urgent notifications that haven't been dismissed by the user:

```json
{
    "success": true,
    "res": [
        {
            "id": 123,
            "author": "admin@example.com",
            "message": "System maintenance in progress",
            "timestamp": "2024-01-15T10:00:00Z",
            "category": "incident",
            "read_status": false
        }
    ]
}
```

#### 2. Dismiss Urgent Notification
**POST** `/notifications/urgent/dismiss`

Dismisses an urgent notification for the current user:

```json
{
    "notification_id": 123
}
```

#### 3. Get Notifications by Category
**GET** `/notifications/categories/{category}`

Returns notifications for a specific category:

```
GET /notifications/categories/job_failure?page=1&limit=10
```

#### 4. Update Category Subscriptions
**PUT** `/notifications/category-subscriptions`

Updates user's category subscription preferences:

```json
{
    "subscribe_incident_notifications": true,
    "subscribe_job_failure_notifications": false,
    "subscribe_late_runs_notifications": true
}
```

### User Preferences API Updates

#### Get User Preferences
**GET** `/user-preferences/`

Now returns category subscription preferences:

```json
{
    "success": true,
    "res": {
        "enable_email_notifications": true,
        "disclaimer_shown": true,
        "subscribe_incident_notifications": true,
        "subscribe_schema_change_notifications": true,
        "subscribe_job_failure_notifications": false,
        "subscribe_late_runs_notifications": true,
        "subscribe_dbt_test_failure_notifications": true
    }
}
```

#### Update User Preferences
**PUT** `/user-preferences/`

Now accepts category subscription fields:

```json
{
    "enable_email_notifications": true,
    "subscribe_incident_notifications": false,
    "subscribe_job_failure_notifications": true
}
```

## Business Logic Changes

### Notification Delivery

The notification delivery logic now includes category subscription checking:

1. When a notification is created, the system checks each recipient's subscription preferences
2. Notifications are only sent to users who are subscribed to that category
3. Users who are not subscribed to a category will not receive notifications of that type

### Urgent Notification Management

1. Urgent notifications are tracked separately for dismissal
2. Each user can dismiss urgent notifications independently
3. Dismissed urgent notifications won't appear in the urgent notification bar
4. The dismissal state is persistent across sessions

## Frontend Integration Points

### Urgent Notification Bar

The frontend should:

1. Call `GET /notifications/urgent` to get urgent notifications
2. Display them in a prominent horizontal bar at the top of the page
3. Allow users to dismiss notifications via `POST /notifications/urgent/dismiss`
4. Remove dismissed notifications from the bar

### Category Management

The frontend should:

1. Provide a settings page for category subscriptions
2. Use `GET /user-preferences/` to get current subscription settings
3. Use `PUT /user-preferences/` or `PUT /notifications/category-subscriptions` to update settings
4. Allow filtering notifications by category in the notification list

### Notification Display

The frontend should:

1. Display the category for each notification
2. Allow filtering by category using the enhanced API endpoints
3. Group notifications by category if desired

## Migration Notes

**Important**: Database migrations need to be created and run to add the new fields:

1. Add `category` field to Notification model
2. Add `dismissed_by` ManyToMany field to Notification model
3. Add category subscription fields to UserPreferences model

## Testing

A test script (`test_notifications_v2.py`) has been created to verify the implementation. Run it with:

```bash
source .venv/bin/activate
python test_notifications_v2.py
```

## Backward Compatibility

- All existing API endpoints continue to work as before
- New fields have sensible defaults
- Existing notifications will have the default category "incident"
- Existing users will be subscribed to all categories by default

## Usage Examples

### Creating Category-specific Notifications

```python
# Job failure notification
notification_data = {
    "author": "system@dalgo.com",
    "message": "Pipeline 'daily_etl' failed",
    "category": "job_failure",
    "urgent": False,
    "recipients": [user_id]
}

# Incident notification
notification_data = {
    "author": "admin@dalgo.com", 
    "message": "System maintenance starting in 30 minutes",
    "category": "incident",
    "urgent": True,
    "recipients": all_user_ids
}
```

### Managing User Subscriptions

```python
# Unsubscribe from job failure notifications
user_prefs.subscribe_job_failure_notifications = False
user_prefs.save()

# Check if user is subscribed to a category
if user_prefs.is_subscribed_to_category("incident"):
    # Send notification
    pass
```

This enhancement provides a much more flexible and user-friendly notification system that allows users to control what types of notifications they receive while ensuring important urgent messages are prominently displayed.