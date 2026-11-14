from enum import Enum


class DatabaseResourceVisibility(str, Enum):
    PRIVATE = "PRIVATE"
    PUBLIC = "PUBLIC"
    REFERENCE = "REFERENCE"

    def __str__(self) -> str:
        return str(self.value)
