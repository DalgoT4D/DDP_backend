# Notification System Enhancements

## Overview

This document outlines the enhancements made to the Dalgo notification system to support categorized notifications, user subscription preferences, and an urgent notifications bar.

## New Features Implemented

### 1. Notification Categories

Added support for 6 notification categories:

- **Incident Notifications** (`incident`): System downtime, platform issues
- **Schema Changes** (`schema_change`): Database schema modifications
- **Job Failures** (`job_failure`): Data pipeline failures
- **Late Runs** (`late_run`): Pipelines running behind schedule
- **dbt Test Failures** (`dbt_test_failure`): Failed dbt tests
- **System Notifications** (`system`): General system messages

### 2. User Subscription Management

Users can now:
- Subscribe/unsubscribe from specific notification categories
- Control email notification delivery per category
- Set preferences through a dedicated API endpoint

### 3. Urgent Notifications Bar

- Urgent notifications are now displayed prominently in a notification bar
- Only shows unread urgent notifications
- Separate API endpoint for retrieving urgent notifications

### 4. Automatic Categorized Notifications

#### Job Failure Notifications
- Automatically created when Prefect flow runs fail or crash
- Categorized as "job_failure"
- Includes flow run ID and error details
- Only sent to users subscribed to job failure notifications

#### LLM Summaries for Prefect Failures
- Enhanced the existing webhook system to trigger LLM summaries
- Controlled by `AUTO_GENERATE_LLM_FAILURE_SUMMARIES` environment variable
- Integrates with the existing LLM summarization system

## Database Changes

### New Models

1. **NotificationCategory** (TextChoices enum)
   - Defines available notification categories
   - Used for validation and frontend display

2. **UserNotificationPreferences**
   - Stores user subscription preferences for each category
   - Tracks email notification preferences
   - One-to-one relationship with OrgUser

### Modified Models

1. **Notification**
   - Added `category` field with default value "system"
   - Foreign key to NotificationCategory choices

## API Changes

### New Endpoints

1. **GET /api/notifications/categories**
   - Returns list of available notification categories
   - Used for frontend category selection

2. **GET /api/notifications/preferences**
   - Returns user's notification preferences
   - Shows subscription status for each category

3. **PUT /api/notifications/preferences**
   - Updates user's notification preferences
   - Allows bulk update of category subscriptions

4. **GET /api/notifications/filtered**
   - Retrieves notifications filtered by category and other criteria
   - Supports pagination and multiple filter options

5. **GET /api/notifications/urgent-bar**
   - Returns urgent unread notifications for the notification bar
   - Optimized for UI display

### Modified Endpoints

1. **POST /api/notifications/**
   - Now accepts `category` parameter
   - Defaults to "system" category if not specified

2. **GET /api/notifications/v1**
   - Enhanced to work with category-based filtering
   - Respects user subscription preferences

## Service Layer Changes

### New Functions

1. **create_job_failure_notification()**
   - Creates categorized job failure notifications
   - Automatically triggered by webhook failures

2. **create_dbt_test_failure_notification()**
   - Creates notifications for dbt test failures
   - Ready for integration with dbt testing system

3. **create_late_run_notification()**
   - Creates notifications for delayed pipeline runs
   - Ready for scheduling system integration

4. **get_user_notification_preferences()**
   - Retrieves user's category subscription preferences

5. **update_user_notification_preferences()**
   - Updates user's notification preferences

6. **get_filtered_notifications()**
   - Advanced filtering for notifications

7. **get_urgent_notifications_for_user()**
   - Retrieves urgent notifications for notification bar

### Enhanced Functions

1. **handle_recipient()**
   - Now checks user subscription preferences before sending
   - Filters notifications based on category subscriptions

2. **create_notification()**
   - Enhanced to handle category field
   - Validates category against available options

## Webhook Integration

### Enhanced Prefect Webhook Handler

1. **Job Failure Detection**
   - Automatically creates job failure notifications
   - Integrates with existing failure email system
   - Triggers LLM summary generation

2. **LLM Summary Generation**
   - Added `generate_llm_failure_summary()` function
   - Controlled by environment variable
   - Uses system user for LLM operations

## Environment Variables

### New Variables

1. **AUTO_GENERATE_LLM_FAILURE_SUMMARIES**
   - Controls automatic LLM summary generation
   - Values: "true", "1", "yes" to enable

## Usage Examples

### Creating a Job Failure Notification

```python
from ddpui.core.notifications_service import create_job_failure_notification

create_job_failure_notification(
    flow_run_id="flow-123",
    org=organization,
    error_message="Pipeline failed due to connection timeout"
)
```

### Creating a dbt Test Failure Notification

```python
from ddpui.core.notifications_service import create_dbt_test_failure_notification

create_dbt_test_failure_notification(
    test_name="test_customer_data_quality",
    org=organization,
    error_details="Expected 0 null values, found 5"
)
```

### Updating User Preferences

```python
from ddpui.core.notifications_service import update_user_notification_preferences

update_user_notification_preferences(
    orguser=user,
    email_notifications_enabled=True,
    job_failure_notifications=False,
    incident_notifications=True
)
```

## Frontend Integration

### Notification Bar Component

Create a notification bar component that:
1. Calls `/api/notifications/urgent-bar` endpoint
2. Displays urgent notifications prominently
3. Allows users to dismiss notifications
4. Updates in real-time

### Category Filtering

Add category filters to notification views:
1. Use `/api/notifications/categories` for filter options
2. Call `/api/notifications/filtered` with category parameter
3. Display category badges on notifications

### Preferences Management

Create a preferences page that:
1. Fetches current preferences with `/api/notifications/preferences`
2. Updates preferences with `/api/notifications/preferences`
3. Shows category descriptions and subscription status

## Migration Notes

### For Existing Notifications

- All existing notifications will have category="system"
- Users will be subscribed to all categories by default
- No data loss during migration

### For Existing Preferences

- Existing UserPreferences model remains unchanged
- New UserNotificationPreferences model works alongside
- Email preferences are now managed through new model

## Testing

### Unit Tests

Add tests for:
1. Category-based notification creation
2. User preference management
3. Webhook integration with categorized notifications
4. API endpoints for new functionality

### Integration Tests

Test:
1. End-to-end notification flow with categories
2. Webhook triggering of categorized notifications
3. User subscription filtering
4. Email delivery with category preferences

## Future Enhancements

### Planned Features

1. **Late Run Detection**
   - Integrate with scheduling system
   - Monitor pipeline execution times
   - Automatically create late run notifications

2. **Schema Change Detection**
   - Monitor database schema changes
   - Create notifications for significant changes
   - Integration with migration system

3. **Advanced Filtering**
   - Date range filtering
   - Multiple category selection
   - Custom notification rules

4. **Mobile Notifications**
   - Push notification support
   - Mobile app integration
   - Real-time delivery

### Technical Improvements

1. **Performance Optimization**
   - Implement notification caching
   - Optimize database queries
   - Add pagination improvements

2. **Monitoring and Analytics**
   - Track notification delivery rates
   - Monitor user engagement
   - Category usage analytics

## Deployment

### Environment Setup

1. Set `AUTO_GENERATE_LLM_FAILURE_SUMMARIES=true` to enable LLM summaries
2. Run database migrations to add new tables and fields
3. Update frontend to use new API endpoints

### Migration Steps

1. Deploy backend changes
2. Run database migrations
3. Update frontend components
4. Test notification flows
5. Enable auto-generation features

This enhancement provides a robust foundation for categorized notifications while maintaining backward compatibility with the existing system.
