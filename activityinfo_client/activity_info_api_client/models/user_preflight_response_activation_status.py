from enum import Enum


class UserPreflightResponseActivationStatus(str, Enum):
    ACTIVE = "ACTIVE"
    CLOSED = "CLOSED"
    INACTIVE = "INACTIVE"
    NO_ACCOUNT = "NO_ACCOUNT"
    PENDING = "PENDING"

    def __str__(self) -> str:
        return str(self.value)
