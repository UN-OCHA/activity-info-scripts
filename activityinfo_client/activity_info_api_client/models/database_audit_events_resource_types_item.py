from enum import Enum


class DatabaseAuditEventsResourceTypesItem(str, Enum):
    AUTOMATION = "AUTOMATION"
    DATABASE = "DATABASE"
    FOLDER = "FOLDER"
    FORM = "FORM"
    LOCK = "LOCK"
    RECORD = "RECORD"
    REPORT = "REPORT"
    ROLE = "ROLE"
    USER_PERMISSION = "USER_PERMISSION"

    def __str__(self) -> str:
        return str(self.value)
