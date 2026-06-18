class MetricServiceError(Exception):
    def __init__(self, message: str, error_code: str = "METRIC_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class MetricNotFoundError(MetricServiceError):
    def __init__(self, metric_id: int):
        super().__init__(f"Metric with id {metric_id} not found", "METRIC_NOT_FOUND")
        self.metric_id = metric_id


class MetricValidationError(MetricServiceError):
    def __init__(self, message: str):
        super().__init__(message, "VALIDATION_ERROR")


class MetricDeleteBlockedError(MetricServiceError):
    def __init__(self, message: str, consumers: dict):
        super().__init__(message, "DELETE_BLOCKED")
        self.consumers = consumers
