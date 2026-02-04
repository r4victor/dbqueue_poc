import uuid
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class RunStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PROVISIONING = "provisioning"
    RUNNING = "running"
    TERMINATING = "terminating"
    TERMINATED = "terminated"
    FAILED = "failed"
    DONE = "done"

    @classmethod
    def finished_statuses(cls) -> list["RunStatus"]:
        return [cls.TERMINATED, cls.FAILED, cls.DONE]

    def is_finished(self):
        return self in self.finished_statuses()


class JobStatus(str, Enum):
    SUBMITTED = "submitted"
    PROVISIONING = "provisioning"
    PULLING = "pulling"
    RUNNING = "running"
    TERMINATING = "terminating"
    TERMINATED = "terminated"
    ABORTED = "aborted"
    FAILED = "failed"
    DONE = "done"

    @classmethod
    def finished_statuses(cls) -> list["JobStatus"]:
        return [cls.TERMINATED, cls.ABORTED, cls.FAILED, cls.DONE]

    def is_finished(self):
        return self in self.finished_statuses()


class PlacementGroupStatus(str, Enum):
    ACTIVE = "ACTIVE"
    TERMINATING = "terminating"
    TERMINATED = "terminated"

    @classmethod
    def finished_statuses(cls) -> list["PlacementGroupStatus"]:
        return [cls.TERMINATED]

    def is_finished(self):
        return self in self.finished_statuses()


class CreateRunRequest(BaseModel):
    name: Optional[str] = None


class CreateJobRequest(BaseModel):
    run_id: uuid.UUID
    name: Optional[str] = None


class CreatePlacementGroupRequest(BaseModel):
    name: Optional[str] = None
