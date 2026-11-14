from enum import Enum


class FormSchemaElementParameterCardinality(str, Enum):
    MULTIPLE = "multiple"
    SINGLE = "single"

    def __str__(self) -> str:
        return str(self.value)
