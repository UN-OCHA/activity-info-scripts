from enum import Enum


class FormSchemaElementParameterCaptureMethodsItem(str, Enum):
    CAMERA = "CAMERA"
    FILE = "FILE"
    SIGNATURE = "SIGNATURE"

    def __str__(self) -> str:
        return str(self.value)
