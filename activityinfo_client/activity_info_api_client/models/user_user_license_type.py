from enum import Enum


class UserUserLicenseType(str, Enum):
    BASIC = "BASIC"
    FULL = "FULL"
    NONE = "NONE"

    def __str__(self) -> str:
        return str(self.value)
