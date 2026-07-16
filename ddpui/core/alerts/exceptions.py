"""Alerts service exceptions."""


class AlertServiceError(Exception):
    def __init__(self, message: str, error_code: str = "ALERT_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class AlertNotFoundError(AlertServiceError):
    def __init__(self, alert_id: int):
        super().__init__(f"Alert with id {alert_id} not found", "ALERT_NOT_FOUND")
        self.alert_id = alert_id


class AlertValidationError(AlertServiceError):
    def __init__(self, message: str):
        super().__init__(message, "VALIDATION_ERROR")


class AlertPermissionError(AlertServiceError):
    def __init__(self, message: str):
        super().__init__(message, "PERMISSION_DENIED")
