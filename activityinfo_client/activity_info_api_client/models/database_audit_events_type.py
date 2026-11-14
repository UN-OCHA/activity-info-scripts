from enum import Enum


class DatabaseAuditEventsType(str, Enum):
    AUTOMATION = "AUTOMATION"
    DATABASE_TREE = "DATABASE_TREE"
    FORM = "FORM"
    RECORD = "RECORD"
    USER = "USER"

    def __str__(self) -> str:
        return str(self.value)
