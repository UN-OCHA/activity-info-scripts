from enum import Enum


class FieldConditionCriteria(str, Enum):
    ALL = "ALL"
    ANY = "ANY"

    def __str__(self) -> str:
        return str(self.value)
