import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol


class PipelineItem(Protocol):
    id: uuid.UUID
    lock_expires_at: datetime
    lock_token: uuid.UUID


@dataclass
class ProcessingResult:
    requeue: bool
