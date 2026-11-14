from enum import Enum


class FormSchemaElementParameterPresentation(str, Enum):
    AUTOMATIC = "automatic"
    DROPDOWN = "dropdown"

    def __str__(self) -> str:
        return str(self.value)
