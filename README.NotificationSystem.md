# Notification System for Dalgo

## Project Details

### Understanding of the Project
- Interface for admin to post notifications
- Interface for users to see notifications
- Python backend for managing notifications

## Admin UI Framework

### Notification History

- **Lists notifications**: 
  - Display notifications with details such as sender, recipients, message content, timestamp, send status, and urgency level.
- **Actions**: 
  - View notification details(show recipients, time for which notification is scheduled, etc).
  - Delete notifications(only the future notifications).

### Send Notification

- **Notification can be sent to**
  - all users
  - all users in an org
  - all users in an org having role >= manager
  - a single-user

The send Notification form will have:
- **Message Content**: 
  - Text area for entering the notification message.
- **Urgency Level**: 
  - Can be a toggle switch to set urgent to `True` or `False`.
- **Send Now or Schedule**: 
  - Option to send the notification immediately or schedule it for a later time.


### User Authentication and Permissions

- **Security**: 
  - Ensure authentication and permission-based access to the notification system.

## Interface for users to see notifications

### Notification Icon/Button

- **Display icon/button in the header**: 
  - Indicates notifications.
  - The badge indicates unread notifications.

### Notifications Dropdown/Modal

- **Clicking the icon/button reveals the notifications list**: 
  - Cards show sender, message, timestamp, and urgency.

### Notification Details

- **Click the notification to view full details**: 

### Notification Preferences

- **Access preferences/settings**: 
  - Manage channels and notification preferences.

### Urgent Notifications

- **Highlight urgent notifications with a different color**.

### Real-Time Updates

- **Interface updates in real-time for new notifications**.

### Authentication and Permissions

- **Authenticate users to access notifications**.
- **Apply permissions based on user roles**.

## Python Backend for Managing Notifications

### Notification Model
Creation of a Django model named `Notification` to store notification details such as:
- Author(sender)
- Message content
- Timestamp
- Urgency level
- Scheduled Time
- Sent Time

### NotificationRecipients Model
Creation of a Django model named `Notification Recipients` to store n details such as:
- Notification Id (Foreign Key)
- Recipient
- Read status

### User Preference Model
Creation of a Django model named `User_Preferences` to store user's preference details such as:
- Enable discord notifications
- Enable email notifications
- Discord Webhook (to receive discord notification)
- Email Id (to be taken from the user object)

### API Endpoints(for user panel)
Development of Django REST Framework API endpoints for:
- Fetching notifications for a single user(through user id).
- Mark notifications as read or unread
- Managing user notification preferences.

### API Endpoints(for admin panel)
Development of Django REST Framework API endpoints for:
- Creating new notifications.
- Fetching notification history.
- Deleting notifications(only the future ones).

### Email and Discord Handlers
- Creation of handler functions to process notifications and send them via configured email and Discord channels.
- Use `SendGrid API` for emails and the `Discord Webhooks` for Discord messages.

### Database Storage
Ensure that notification history is stored in the database for auditing and reference purposes.

## Timeline

### Week 1 (Project Setup and learning migrations)

Week 1 is dedicated to Requirement Gathering and Planning. Here, the project scope and objectives are defined, and requirements for the admin and user panels are gathered separately.

### Week 2 (Backend Development)

Week 3 is for the Backend Development process. Here, database models for notifications and users are implemented, and RESTful APIs for core functionalities are developed.

### Week 3 (Functionalities for User Panel Frontend)

UI components for the user notification interface are designed, and features for viewing notifications, managing preferences, and marking notifications as read are developed.

### Week 4 (Writing JEST Tests)

JEST tests are written for the User Panel Frontend application.


### Week 5 (Functionalities for Admin Panel Frontend)

UI components for the admin dashboard are created, and admin-specific features such as notification creation and preference management are implemented. Integration with backend APIs for data retrieval and manipulation is also carried out.

### Week 6 (Buffer Week)

Any remaining features or enhancements for both admin and user panels are implemented. Support for responsive design and accessibility is also added during this phase.

