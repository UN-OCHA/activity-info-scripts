from enum import Enum


class JobStatusState(str, Enum):
    COMPLETED = "completed"
    FAILED = "failed"
    STARTED = "started"

    def __str__(self) -> str:
        return str(self.value)
