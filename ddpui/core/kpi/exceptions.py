class KPIServiceError(Exception):
    def __init__(self, message: str, error_code: str = "KPI_ERROR"):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class KPINotFoundError(KPIServiceError):
    def __init__(self, kpi_id: int):
        super().__init__(f"KPI with id {kpi_id} not found", "KPI_NOT_FOUND")
        self.kpi_id = kpi_id


class KPIValidationError(KPIServiceError):
    def __init__(self, message: str):
        super().__init__(message, "VALIDATION_ERROR")


class KPIPermissionError(KPIServiceError):
    def __init__(self, message: str):
        super().__init__(message, "PERMISSION_DENIED")
