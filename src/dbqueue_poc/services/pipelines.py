from typing import Protocol

from dbqueue_poc.utils.logging import get_logger

logger = get_logger(__name__)


class PipelineHinterProtocol(Protocol):
    def hint_fetch(self, model_name: str) -> None:
        pass
