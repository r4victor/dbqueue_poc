import asyncio
import uuid
from datetime import timedelta
from typing import Sequence, cast

from sqlalchemy import or_, select, update
from sqlalchemy.orm import load_only

from dbqueue_poc.background.pipeline_tasks.base import (
    Fetcher,
    Heartbeater,
    Pipeline,
    PipelineItem,
    Worker,
)
from dbqueue_poc.db import get_db, get_session_ctx
from dbqueue_poc.models import JobModel, RunModel
from dbqueue_poc.schemas import JobStatus
from dbqueue_poc.services.locking import get_locker
from dbqueue_poc.utils.common import get_current_datetime
from dbqueue_poc.utils.logging import get_logger

logger = get_logger(__name__)


class JobPipeline(Pipeline):
    """
    An example of a pipeline that needs to lock and update one resource type (`JobModel`),
    but the resource type can also be locked by another pipeline (`RunPipeline`)
    when processing the parent resource type (`RunModel`).

    Highlights:
        * All features of the simplest pipeline (`PlacementGroupPipeline`), plus
        * It never picks up stale items locked by another pipeline (`RunPipeline`).
        * It may skip locking items to let "parent" pipeline lock all related items eventually,
          e.g. running jobs not processed while the run is locked waiting for all active jobs to unlock.
    """

    def __init__(
        self,
        workers_num: int = 50,
        queue_lower_limit_factor: float = 0.5,
        queue_upper_limit_factor: float = 2.0,
        min_processing_interval: timedelta = timedelta(seconds=5),
        lock_timeout: timedelta = timedelta(seconds=20),
        heartbeat_trigger: timedelta = timedelta(seconds=10),
    ) -> None:
        super().__init__(
            workers_num=workers_num,
            queue_lower_limit_factor=queue_lower_limit_factor,
            queue_upper_limit_factor=queue_upper_limit_factor,
            min_processing_interval=min_processing_interval,
            lock_timeout=lock_timeout,
            heartbeat_trigger=heartbeat_trigger,
        )
        self.__heartbeater = Heartbeater[JobModel](
            model_type=JobModel,
            lock_timeout=self._lock_timeout,
            heartbeat_trigger=self._heartbeat_trigger,
        )
        self.__fetcher = JobFetcher(
            queue=self._queue,
            queue_desired_minsize=self._queue_desired_minsize,
            min_processing_interval=self._min_processing_interval,
            lock_timeout=self._lock_timeout,
            heartbeater=self._heartbeater,
        )
        self.__workers = [
            JobWorker(queue=self._queue, heartbeater=self._heartbeater)
            for _ in range(self._workers_num)
        ]

    @property
    def hint_fetch_model_name(self) -> str:
        return JobModel.__name__

    @property
    def _heartbeater(self) -> Heartbeater:
        return self.__heartbeater

    @property
    def _fetcher(self) -> Fetcher:
        return self.__fetcher

    @property
    def _workers(self) -> Sequence["JobWorker"]:
        return self.__workers


class JobFetcher(Fetcher):
    def __init__(
        self,
        queue: asyncio.Queue[PipelineItem],
        queue_desired_minsize: int,
        min_processing_interval: timedelta,
        lock_timeout: timedelta,
        heartbeater: Heartbeater[JobModel],
        queue_check_delay: float = 1.0,
    ) -> None:
        super().__init__(
            queue=queue,
            queue_desired_minsize=queue_desired_minsize,
            min_processing_interval=min_processing_interval,
            lock_timeout=lock_timeout,
            heartbeater=heartbeater,
            queue_check_delay=queue_check_delay,
        )

    async def fetch(self, limit: int) -> list[PipelineItem]:
        job_lock, _ = get_locker(get_db().dialect_name).get_lockset(JobModel.__tablename__)
        async with job_lock:
            async with get_session_ctx() as session:
                now = get_current_datetime()
                res = await session.execute(
                    select(JobModel)
                    .join(JobModel.run)
                    .where(
                        JobModel.status.not_in(JobStatus.finished_statuses()),
                        or_(
                            JobModel.last_processed_at <= now - self._min_processing_interval,
                            JobModel.last_processed_at == JobModel.submitted_at,
                        ),
                        or_(
                            JobModel.lock_expires_at.is_(None),
                            JobModel.lock_expires_at < now,
                        ),
                        or_(
                            JobModel.lock_owner.is_(None),
                            JobModel.lock_owner == JobPipeline.__name__,
                        ),
                        # Do not try to lock running jobs if the run is being locked so that
                        # the run pipeline is guaranteed to lock all the jobs eventually.
                        # Still lock non-running because because submitted, provisioning, etc
                        # take longer time and not indefinite unlike running.
                        or_(
                            JobModel.status != JobStatus.RUNNING,
                            RunModel.lock_owner.is_(None),
                        ),
                    )
                    .order_by(JobModel.priority.desc(), JobModel.last_processed_at.asc())
                    .limit(limit)
                    .with_for_update(of=JobModel, skip_locked=True, key_share=True)
                    .options(load_only(JobModel.id, JobModel.lock_token, JobModel.lock_expires_at))
                )
                job_models = list(res.scalars().all())
                lock_expires_at = get_current_datetime() + self._lock_timeout
                lock_token = uuid.uuid4()
                for job_model in job_models:
                    job_model.lock_expires_at = lock_expires_at
                    job_model.lock_token = lock_token
                    job_model.lock_owner = JobPipeline.__name__
                await session.commit()
        return [cast(PipelineItem, r) for r in job_models]


class JobWorker(Worker):
    def __init__(
        self,
        queue: asyncio.Queue[PipelineItem],
        heartbeater: Heartbeater[JobModel],
    ) -> None:
        super().__init__(
            queue=queue,
            heartbeater=heartbeater,
        )

    async def process(self, item: PipelineItem):
        logger.debug("Processing job %s", item.id)
        async with get_session_ctx() as session:
            res = await session.execute(
                select(JobModel).where(
                    JobModel.id == item.id,
                    JobModel.lock_token == item.lock_token,
                )
            )
            job_model = res.scalar_one_or_none()
            if job_model is None:
                logger.warning(
                    "Failed to process job: lock_token mismatch."
                    " The job is expected to be processed and updated on another fetch iteration."
                )
                return

        # Do some work ...
        await asyncio.sleep(30)

        async with get_session_ctx() as session:
            res = await session.execute(
                update(JobModel)
                .where(
                    JobModel.id == job_model.id,
                    JobModel.lock_token == job_model.lock_token,
                )
                .values(
                    lock_expires_at=None,
                    lock_token=None,
                    lock_owner=None,
                    last_processed_at=get_current_datetime(),
                    status=JobStatus.DONE,
                )
            )
            if res.rowcount == 0:  # pyright: ignore[reportAttributeAccessIssue]
                logger.warning(
                    "Failed to update the job after processing: lock_token changed."
                    " The job is expected to be processed and updated on another fetch iteration."
                )
                return
        logger.debug("Processed job %s", item.id)
