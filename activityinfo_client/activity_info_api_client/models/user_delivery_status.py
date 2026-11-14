from enum import Enum


class UserDeliveryStatus(str, Enum):
    HARD_BOUNCE = "HARD_BOUNCE"
    MANUAL_SUPPRESSION = "MANUAL_SUPPRESSION"
    OK = "OK"
    SPAM_COMPLAINT = "SPAM_COMPLAINT"
    UNKNOWN = "UNKNOWN"

    def __str__(self) -> str:
        return str(self.value)
