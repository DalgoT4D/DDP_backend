"""Alert exceptions"""


class AlertError(Exception):
    """Base exception for alert errors"""

    def __init__(self, message: str, error_code: str = "ALERT_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class AlertNotFoundError(AlertError):
    """Raised when alert is not found"""

    def __init__(self, alert_id: int):
        super().__init__(f"Alert with id {alert_id} not found", "ALERT_NOT_FOUND")
        self.alert_id = alert_id


class AlertValidationError(AlertError):
    """Raised when alert validation fails"""

    def __init__(self, message: str):
        super().__init__(message, "ALERT_VALIDATION_ERROR")


class AlertWarehouseError(AlertError):
    """Raised when warehouse query fails"""

    def __init__(self, message: str):
        super().__init__(message, "ALERT_WAREHOUSE_ERROR")
