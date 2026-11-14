from enum import Enum


class DatabaseResourceType(str, Enum):
    DATABASE = "DATABASE"
    FOLDER = "FOLDER"
    FORM = "FORM"
    REPORT = "REPORT"
    SUB_FORM = "SUB_FORM"

    def __str__(self) -> str:
        return str(self.value)
