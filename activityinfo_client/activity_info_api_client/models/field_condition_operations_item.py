from enum import Enum


class FieldConditionOperationsItem(str, Enum):
    EDIT_FIELD = "EDIT_FIELD"
    VIEW_FIELD = "VIEW_FIELD"

    def __str__(self) -> str:
        return str(self.value)
