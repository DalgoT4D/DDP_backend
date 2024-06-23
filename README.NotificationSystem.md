# Notification System for Dalgo

## Project Overview

The Dalgo Notification System is designed to streamline communication within the Dalgo platform, facilitating administrators to disseminate important information to users effectively. With a focus on versatility and user engagement, this system offers administrators an intuitive interface within the Dalgo admin tool to compose and post notifications.
<br>
Additionally, the system maintains a comprehensive notification history, stored securely in the backend database. This enables both administrators and users to access past notifications for reference and auditing purposes, ensuring transparency and accountability within the platform.
<br>
In summary, the Dalgo Notification System serves as a pivotal component in enhancing communication and user engagement within the Dalgo ecosystem. By offering administrators a streamlined process for broadcasting notifications and empowering users with customizable preferences, the system contributes to a more efficient and informed collaborative environment.

## Project Details

### Understanding of the Project
The Dalgo Notification System aims to facilitate effective communication between administrators and users by providing a versatile platform for sending notifications via multiple channels. It will include
- Interface for admin to post notifications
- Interface for users to see notifications
- Python backend for managing notifications

## Admin UI Framework
The admin UI for Dalgo Notifications empowers administrators to compose and broadcast messages effortlessly. It offers intuitive controls for defining recipients, composing messages, setting urgency levels, and selecting communication channels.

### Dashboard Overview

- **Displays recent notifications**: Show the most recent notifications received by the user.
- **Unread count**: Display the number of unread notifications.
- **Urgent notifications**: Highlight urgent notifications prominently.

### Create Notification Form

- **Recipient(s)**: 
  - Dropdown or input field for selecting users or groups to receive the notification.
- **Message Content**: 
  - Text area with support for rich text formatting.
- **Urgency Level**: 
  - Dropdown for selecting the urgency of the notification.
- **Notification Channels**: 
  - Checkboxes or dropdowns to select the channels (e.g., Email, Discord) through which the notification will be sent.
- **Send Now or Schedule**: 
  - Option to send the notification immediately or schedule it for a later time.

### Notification History

- **Lists past notifications**: 
  - Display notifications with details such as sender, recipients, message content, timestamp, and urgency level.
- **Actions**: 
  - View notification details.
  - Mark notifications as read.
  - Delete notifications.

### Notification Preferences Management

- **Admins manage user/group notification preferences**: 
  - Options for enabling or disabling notification channels.
  - Adjust settings for each channel.

### Notification Alerts

- **Real-time alerts**: 
  - Provide real-time notifications for new messages and urgent notifications.

### Pub-Sub System Integration

- **Integration**: 
  - Seamlessly integrate with the Pub-sub system for publishing notifications.

### User Authentication and Permissions

- **Security**: 
  - Ensure authentication and permission-based access to the notification system.

## Interface for users to see notifications
For the interface allowing users to see notifications in the Dalgo Notification System, consider a user-friendly web interface integrated into the Dalgo frontend application.

### Notification Icon/Button

- **Display icon/button in the header**: 
  - Indicates notifications.
  - The badge indicates unread notifications.

### Notifications Dropdown/Modal

- **Clicking the icon/button reveals the notifications list**: 
  - Cards show sender, message, timestamp, and urgency.

### Notification Details

- **Click the notification to view full details**: 
  - Options to mark as read or delete.

### Notification Preferences

- **Access preferences/settings**: 
  - Manage channels and frequency.

### Urgent Notifications Bar

- **Separate section for urgent notifications**.

### Real-Time Updates

- **Interface updates in real-time for new notifications**.

### Authentication and Permissions

- **Authenticate users to access notifications**.
- **Apply permissions based on user roles**.

## Python Backend for Managing Notifications

The backend will provide a robust infrastructure for managing notifications, including creation, storage, retrieval, and delivery through various channels like email and Discord. It will offer a RESTful API for interaction with the front end and incorporate a pub-sub model for efficient notification distribution.

### Notification Model
Creation of a Django model named `Notification` to store notification details such as:
- Sender
- Recipients
- Message content
- Timestamp
- Urgency level
- Read status

### API Endpoints
Development of Django REST Framework API endpoints for:
- Creating new notifications.
- Fetching unread notifications for a user.
- Fetching notification history.
- Managing user notification preferences.

### Pub-Sub Model
Implement a pub-sub architecture using Django signals or Django Channels, allowing handlers for various notification channels to subscribe and receive new notifications for sending.

### Email and Discord Handlers
- Creation of handler functions to process notifications and send them via configured email and Discord channels.
- Utilization of Django's built-in `EmailMessage` for emails and the `discord.py` library for Discord messages.

### Database Storage
Ensuring that notification history is stored in the database for auditing and reference purposes.

## Timeline

### Week 1 (Requirement Gathering and Planning)

Week 1 is dedicated to Requirement Gathering and Planning. Here, the project scope and objectives are defined, and requirements for the admin and user panels are gathered separately.

### Week 2 (Backend Framework Source-Code Architecture)

Week 2 is the first part of Backend Development. During this phase, the Django project structure is established, and initial setup tasks are completed.

### Week 3 (Backend Development)

Week 3 continues the Backend Development process. Here, database models for notifications and users are implemented, and RESTful APIs for core functionalities are developed.

### Week 4 (Admin Panel Frontend Development)

UI components for the admin dashboard are created, and admin-specific features such as notification creation and preference management are implemented. Integration with backend APIs for data retrieval and manipulation is also carried out.

### Week 5 (User Panel Frontend Development)

UI components for the user notification interface are designed, and features for viewing notifications, managing preferences, and marking notifications as read are developed.

### Week 6 (Integration and Testing)

In Week 6, integration of admin and user frontend components takes place, followed by thorough integration testing to ensure seamless communication with the backend.

### Week 7 (Additional Features Development)

Any remaining features or enhancements for both admin and user panels are implemented. Support for responsive design and accessibility is also added during this phase.

### Week 8 (Refinement and Optimization)

Frontend code is fine-tuned for performance and efficiency, and usability testing is conducted to address any usability issues. Additionally, Final Testing and Quality Assurance are performed, involving thorough testing of admin and user panels, and any bugs or issues identified during testing are fixed promptly.

