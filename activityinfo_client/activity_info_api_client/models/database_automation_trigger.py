from enum import Enum


class DatabaseAutomationTrigger(str, Enum):
    RECORD_ADDED = "RECORD_ADDED"
    RECORD_DELETED = "RECORD_DELETED"
    RECORD_EDITED = "RECORD_EDITED"

    def __str__(self) -> str:
        return str(self.value)
