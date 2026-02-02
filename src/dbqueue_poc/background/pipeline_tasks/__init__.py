from dbqueue_poc.background.pipeline_tasks.process_runs import RunPipeline


class PipelineManager:
    def __init__(self) -> None:
        self._pipelines = [RunPipeline()]

    def start(self):
        for pipeline in self._pipelines:
            pipeline.start()

    def shutdown(self):
        pass


def start_pipeline_tasks() -> PipelineManager:
    """
    Start tasks processed by fetch-workers pipelines based on db + in-memory queues.
    Suitable for tasks that run frequently and need to lock rows for a long time.
    """
    pipeline_manager = PipelineManager()
    pipeline_manager.start()
    return pipeline_manager
