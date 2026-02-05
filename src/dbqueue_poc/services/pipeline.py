from abc import abstractmethod
from typing import Protocol

from dbqueue_poc.utils.logging import get_logger

logger = get_logger(__name__)


class Pipeline(Protocol):
    def start(self):
        pass

    def shutdown(self):
        pass

    def hint_fetch(self):
        pass

    @property
    @abstractmethod
    def hint_fetch_model_name(self) -> str:
        pass


class PipelineHinter:
    def __init__(self, pipelines: list[Pipeline]) -> None:
        self._pipelines = pipelines
        self._hint_fetch_map = {p.hint_fetch_model_name: p for p in self._pipelines}

    def hint_fetch(self, model_name: str):
        pipeline = self._hint_fetch_map.get(model_name)
        if pipeline is None:
            logger.warning("Model %s not registered for fetch hints", model_name)
            return
        pipeline.hint_fetch()
