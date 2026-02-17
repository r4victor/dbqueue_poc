import asyncio
import uuid
from datetime import timedelta
from typing import Sequence, cast

from sqlalchemy import or_, select, update
from sqlalchemy.orm import load_only, selectinload

from dbqueue_poc.background.pipeline_tasks.base import (
    Fetcher,
    Heartbeater,
    Pipeline,
    PipelineItem,
    Worker,
)
from dbqueue_poc.db import get_db, get_session_ctx
from dbqueue_poc.models import JobModel, RunModel
from dbqueue_poc.schemas import JobStatus, RunStatus
from dbqueue_poc.services.locking import get_locker
from dbqueue_poc.utils.common import get_current_datetime
from dbqueue_poc.utils.logging import get_logger

logger = get_logger(__name__)


class RunPipeline(Pipeline):
    """
    An example of a pipeline that needs to lock and update the main resource type (`RunModel`)
    and related resource types (`JobModel`).

    Highlights:
        * All features of the simplest pipeline (`PlacementGroupPipeline`), plus
        * Workers lock all related items (`JobModel`). If not all items can be locked, the run is requeued:
          it's kept in the DB with `lock_owner`, `lock_expires_at` and `lock_token` reset, and not heartbeated.
          This allows signaling other pipelines that the item is locked and process it later via regular pipeline path.
        * Heartbeating related items is not needed. Stale locked related items can be
          processed only by the same pipeline (due to `lock_owner` check) so they'll be picked up
          when processing the main item again.
    """

    def __init__(
        self,
        workers_num: int = 25,
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
        self.__heartbeater = Heartbeater[RunModel](
            model_type=RunModel,
            lock_timeout=self._lock_timeout,
            heartbeat_trigger=self._heartbeat_trigger,
        )
        self.__fetcher = RunFetcher(
            queue=self._queue,
            queue_desired_minsize=self._queue_desired_minsize,
            min_processing_interval=self._min_processing_interval,
            lock_timeout=self._lock_timeout,
            heartbeater=self._heartbeater,
        )
        self.__workers = [
            RunWorker(
                queue=self._queue,
                heartbeater=self._heartbeater,
            )
            for _ in range(self._workers_num)
        ]

    @property
    def hint_fetch_model_name(self) -> str:
        return RunModel.__name__

    @property
    def _heartbeater(self) -> Heartbeater:
        return self.__heartbeater

    @property
    def _fetcher(self) -> Fetcher:
        return self.__fetcher

    @property
    def _workers(self) -> Sequence["RunWorker"]:
        return self.__workers


class RunFetcher(Fetcher):
    def __init__(
        self,
        queue: asyncio.Queue[PipelineItem],
        queue_desired_minsize: int,
        min_processing_interval: timedelta,
        lock_timeout: timedelta,
        heartbeater: Heartbeater[RunModel],
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
        run_lock, _ = get_locker(get_db().dialect_name).get_lockset(RunModel.__tablename__)
        async with run_lock:
            async with get_session_ctx() as session:
                now = get_current_datetime()
                res = await session.execute(
                    select(RunModel)
                    .where(
                        RunModel.status.not_in(RunStatus.finished_statuses()),
                        or_(
                            RunModel.last_processed_at <= now - self._min_processing_interval,
                            RunModel.last_processed_at == RunModel.submitted_at,
                        ),
                        or_(
                            RunModel.lock_expires_at.is_(None),
                            RunModel.lock_expires_at < now,
                        ),
                        or_(
                            RunModel.lock_owner.is_(None),
                            RunModel.lock_owner == RunPipeline.__name__,
                        ),
                    )
                    .order_by(RunModel.priority.desc(), RunModel.last_processed_at.asc())
                    .limit(limit)
                    .with_for_update(skip_locked=True, key_share=True)
                    .options(load_only(RunModel.id, RunModel.lock_token, RunModel.lock_expires_at))
                )
                run_models = list(res.scalars().all())
                lock_expires_at = get_current_datetime() + self._lock_timeout
                lock_token = uuid.uuid4()
                for run_model in run_models:
                    run_model.lock_expires_at = lock_expires_at
                    run_model.lock_token = lock_token
                    run_model.lock_owner = RunPipeline.__name__
                await session.commit()
        return [cast(PipelineItem, r) for r in run_models]


class RunWorker(Worker):
    def __init__(
        self,
        queue: asyncio.Queue[PipelineItem],
        heartbeater: Heartbeater[RunModel],
    ) -> None:
        super().__init__(
            queue=queue,
            heartbeater=heartbeater,
        )

    async def process(self, item: PipelineItem):
        job_lock, _ = get_locker(get_db().dialect_name).get_lockset(JobModel.__tablename__)
        async with job_lock:
            async with get_session_ctx() as session:
                # This is an example of how a pipeline can lock related resources.
                # The worker either successfully locks all the related resources or requeues the main resource.
                # While the main resource is locked, other pipelines may choose not to lock to related resources
                # so that the main resource can acquire all locks eventually.
                # Note: this is the worst case example of always pre-locking.
                # The optimal processing would lock related resource only when necessary.
                res = await session.execute(
                    select(RunModel)
                    .where(
                        RunModel.id == item.id,
                        RunModel.lock_token == item.lock_token,
                    )
                    .options(
                        selectinload(
                            RunModel.jobs.and_(
                                JobModel.status.not_in(JobStatus.finished_statuses())
                            )
                        )
                    )
                )
                run_model = res.scalar_one_or_none()
                if run_model is None:
                    logger.warning(
                        "Failed to process %s item %s: lock_token mismatch."
                        " The item is expected to be processed and updated on another fetch iteration.",
                        item.__tablename__,
                        item.id,
                    )
                    return

                res = await session.execute(
                    select(JobModel)
                    .where(
                        JobModel.run_id == item.id,
                        JobModel.status.not_in(JobStatus.finished_statuses()),
                        or_(
                            JobModel.lock_expires_at.is_(None),
                            JobModel.lock_expires_at < get_current_datetime(),
                        ),
                        or_(
                            JobModel.lock_owner.is_(None),
                            JobModel.lock_owner == RunPipeline.__name__,
                        ),
                    )
                    .with_for_update(skip_locked=True, key_share=True)
                )
                locked_job_models = res.scalars().all()
                if len(run_model.jobs) != len(locked_job_models):
                    logger.debug(
                        "Failed to lock run %s jobs. The run will be processed later.",
                        run_model.id,
                    )
                    now = get_current_datetime()
                    # Keep `lock_owner` so that `JobPipeline` sees that the run is being locked
                    # but reset `lock_expires_at` to process the item again ASAP (after `min_processing_interval`).
                    # Reset `lock_token` so that heartbeater can no longer update the item.
                    res = await session.execute(
                        update(RunModel)
                        .where(
                            RunModel.id == run_model.id,
                            RunModel.lock_token == run_model.lock_token,
                        )
                        .values(
                            lock_expires_at=now,
                            lock_token=None,
                            last_processed_at=now,
                        )
                    )
                    if res.rowcount == 0:  # pyright: ignore[reportAttributeAccessIssue]
                        logger.warning(
                            "Failed to reset lock_expires_at: lock_token changed."
                            " The run is expected to be processed and updated on another fetch iteration."
                        )
                        return
                    return

                for job_model in locked_job_models:
                    job_model.lock_expires_at = run_model.lock_expires_at
                    job_model.lock_token = run_model.lock_token
                    job_model.lock_owner = RunPipeline.__name__
                await session.commit()

        # Do some work ...
        await asyncio.sleep(3)

        async with get_session_ctx() as session:
            res = await session.execute(
                update(RunModel)
                .where(
                    RunModel.id == run_model.id,
                    RunModel.lock_token == run_model.lock_token,
                )
                .values(
                    lock_expires_at=None,
                    lock_token=None,
                    lock_owner=None,
                    last_processed_at=get_current_datetime(),
                    status=RunStatus.DONE,
                )
                .returning(RunModel.id)
            )
            updated_ids = list(res.scalars().all())
            if len(updated_ids) == 0:
                logger.warning(
                    "Failed to update %s item %s after processing: lock_token changed."
                    " The item is expected to be processed and updated on another fetch iteration.",
                    item.__tablename__,
                    item.id,
                )
            else:
                job_ids = [j.id for j in run_model.jobs]
                res = await session.execute(
                    update(JobModel)
                    .where(
                        JobModel.id.in_(job_ids),
                        JobModel.lock_token == run_model.lock_token,
                    )
                    .values(
                        lock_expires_at=None,
                        lock_token=None,
                        lock_owner=None,
                        last_processed_at=get_current_datetime(),
                    )
                )
                if len(job_ids) > 0 and res.rowcount == 0:  # pyright: ignore[reportAttributeAccessIssue]
                    logger.warning(
                        "Failed to update the jobs after processing: lock_token changed."
                        " The jobs are expected to be processed and updated on another fetch iteration."
                    )
                    return
