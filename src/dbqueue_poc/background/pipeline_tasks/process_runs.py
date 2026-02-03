import asyncio
import math
import random
import uuid
from datetime import timedelta
from typing import cast

from sqlalchemy import and_, or_, select, update
from sqlalchemy.orm import load_only, selectinload

from dbqueue_poc.background.pipeline_tasks.base import PipelineItem, ProcessingResult
from dbqueue_poc.db import get_db, get_session_ctx
from dbqueue_poc.models import JobModel, RunModel
from dbqueue_poc.schemas import JobStatus, RunStatus
from dbqueue_poc.services.locking import get_locker
from dbqueue_poc.utils.common import get_current_datetime
from dbqueue_poc.utils.logging import get_logger

logger = get_logger(__name__)


class RunPipeline:
    def __init__(
        self,
        workers_num: int = 25,
        queue_lower_limit_factor: float = 0.5,
        queue_upper_limit_factor: float = 2.0,
        min_processing_interval: timedelta = timedelta(seconds=5),
        lock_timeout: timedelta = timedelta(seconds=20),
        heartbeat_trigger: timedelta = timedelta(seconds=10),
    ) -> None:
        self._workers_num = workers_num
        self._queue_lower_limit_factor = queue_lower_limit_factor
        self._queue_upper_limit_factor = queue_upper_limit_factor
        self._queue_desired_minsize = math.ceil(workers_num * queue_lower_limit_factor)
        self._queue_maxsize = math.ceil(workers_num * queue_upper_limit_factor)
        self._min_processing_interval = min_processing_interval
        self._lock_timeout = lock_timeout
        self._heartbeat_trigger = heartbeat_trigger
        self._queue = asyncio.Queue[PipelineItem](maxsize=self._queue_maxsize)
        self._heartbeater = RunHeartbeater(
            lock_timeout=self._lock_timeout,
            heartbeat_trigger=self._heartbeat_trigger,
        )
        self._fetcher = RunFetcher(
            queue=self._queue,
            queue_desired_minsize=self._queue_desired_minsize,
            queue_maxsize=self._queue_maxsize,
            min_processing_interval=self._min_processing_interval,
            lock_timeout=self._lock_timeout,
            heartbeater=self._heartbeater,
        )
        self._workers = [
            RunWorker(queue=self._queue, heartbeater=self._heartbeater)
            for _ in range(self._workers_num)
        ]

    def start(self):
        asyncio.create_task(self._heartbeater.start())
        for worker in self._workers:
            asyncio.create_task(worker.start())
        asyncio.create_task(self._fetcher.start())

    def shutdown(self):
        self._fetcher.shutdown()
        self._heartbeater.shutdown()


class RunHeartbeater:
    def __init__(
        self,
        lock_timeout: timedelta,
        heartbeat_trigger: timedelta,
        heartbeat_delay: float = 1.0,
    ) -> None:
        self._lock_timeout = lock_timeout
        self._hearbeat_margin = heartbeat_trigger
        self._items: dict[uuid.UUID, PipelineItem] = {}
        self._untrack_lock = asyncio.Lock()
        self._heartbeat_delay = heartbeat_delay
        self._running = False

    async def start(self):
        self._running = True
        while self._running:
            try:
                await self.heartbeat()
            except Exception:
                logger.exception("Unexpected exception when running heartbeat")
            await asyncio.sleep(self._heartbeat_delay)

    def shutdown(self):
        self._running = False

    async def heartbeat(self):
        updated_items = []
        now = get_current_datetime()
        items = list(self._items.values())
        for item in items:
            if item.lock_expires_at < now:
                logger.warning(
                    "Failed to heartbeat run %s in time."
                    " The run is expected to be processed on another fetch iteration.",
                    item.id,
                )
                await self.untrack(item)
            elif item.lock_expires_at < now + self._hearbeat_margin:
                updated_items.append(item)
        if len(updated_items) == 0:
            return
        logger.debug("Updating lock_expires_at for runs: %s", [str(r.id) for r in updated_items])
        async with get_session_ctx() as session:
            per_item_filters = [
                and_(RunModel.id == item.id, RunModel.lock_token == item.lock_token)
                for item in updated_items
            ]
            res = await session.execute(
                update(RunModel)
                .where(or_(*per_item_filters))
                .values(lock_expires_at=now + self._lock_timeout)
            )
            if res.rowcount == 0:  # pyright: ignore[reportAttributeAccessIssue]
                logger.warning(
                    "Failed to update lock_expires_at: lock_token changed."
                    " The run is expected to be processed and updated on another fetch iteration."
                )
                return
        for item in updated_items:
            item.lock_expires_at = now + self._lock_timeout

    async def track(self, item: PipelineItem):
        self._items[item.id] = item

    async def untrack(self, item: PipelineItem):
        async with self._untrack_lock:
            tracked = self._items.get(item.id)
            # Prevent expired fetch iteration to unlock item processed by new iteration.
            if tracked is not None and tracked.lock_token == item.lock_token:
                del self._items[item.id]


class RunFetcher:
    _FETCH_DELAYS = [0.5, 1, 2, 5]

    def __init__(
        self,
        queue: asyncio.Queue[PipelineItem],
        queue_desired_minsize: int,
        queue_maxsize: int,
        min_processing_interval: timedelta,
        lock_timeout: timedelta,
        heartbeater: RunHeartbeater,
        queue_check_delay: float = 1.0,
    ) -> None:
        self._queue = queue
        self._queue_desired_minsize = queue_desired_minsize
        self._queue_maxsize = queue_maxsize
        self._min_processing_interval = min_processing_interval
        self._lock_timeout = lock_timeout
        self._heartbeater = heartbeater
        self._queue_check_delay = queue_check_delay
        self._running = False

    async def start(self):
        self._running = True
        empty_fetch_count = 0
        while self._running:
            if self._queue.qsize() >= self._queue_desired_minsize:
                await asyncio.sleep(self._queue_check_delay)
                continue
            fetch_limit = self._queue_maxsize - self._queue.qsize()
            try:
                items = await self.fetch(limit=fetch_limit)
            except Exception:
                logger.exception("Unexpected exception when fetching new items")
                items = []
            if len(items) == 0:
                await asyncio.sleep(self._next_fetch_delay(empty_fetch_count))
                empty_fetch_count += 1
                continue
            else:
                empty_fetch_count = 0
            for item in items:
                self._queue.put_nowait(item)  # should never raise
                await self._heartbeater.track(item)

    def shutdown(self):
        self._running = False

    async def fetch(self, limit: int) -> list[PipelineItem]:
        run_lock, _ = get_locker(get_db().dialect_name).get_lockset(RunModel.__tablename__)
        async with get_session_ctx() as session:
            async with run_lock:
                now = get_current_datetime()
                res = await session.execute(
                    select(RunModel)
                    .where(
                        RunModel.status.not_in(RunStatus.finished_statuses()),
                        RunModel.last_processed_at <= now - self._min_processing_interval,
                        or_(
                            RunModel.lock_expires_at.is_(None),
                            RunModel.lock_expires_at < now,
                        ),
                        or_(
                            RunModel.lock_owner.is_(None),
                            RunModel.lock_owner == self.__class__.__name__,
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
                    run_model.lock_owner = self.__class__.__name__
                await session.commit()
        return [cast(PipelineItem, r) for r in run_models]

    def _next_fetch_delay(self, empty_fetch_count: int) -> float:
        next_delay = self._FETCH_DELAYS[min(empty_fetch_count, len(self._FETCH_DELAYS) - 1)]
        jitter = random.random() * 0.4 - 0.2
        return next_delay * (1 + jitter)


class RunWorker:
    def __init__(
        self,
        queue: asyncio.Queue[PipelineItem],
        heartbeater: RunHeartbeater,
    ) -> None:
        self._queue = queue
        self._heartbeater = heartbeater

    async def start(self):
        while True:
            item = await self._queue.get()
            requeue = False
            try:
                processing_result = await self.process(item)
                requeue = processing_result.requeue
            except Exception:
                logger.exception("Unexpected exception when processing item")
            if requeue:
                # Requeue mechanism allows workers to process the item later while keeping it locked,
                # e.g. to wait for related resources to be unlocked.
                await self._queue.put(item)
            else:
                await self._heartbeater.untrack(item)

    async def process(self, item: PipelineItem) -> ProcessingResult:
        logger.debug("Processing run %s", item.id)
        async with get_session_ctx() as session:
            # This is an example of how a pipeline can lock related resources.
            # The worker either successfully locks all the related resources or requeues the main resource.
            # While the main resource is locked, related resources cannot be locked by their pipelines.
            # This guarantees that the pipeline processing the main resource can acquire all locks eventually.
            res = await session.execute(
                select(RunModel)
                .where(
                    RunModel.id == item.id,
                    RunModel.lock_token == item.lock_token,
                )
                .options(
                    selectinload(
                        RunModel.jobs.and_(JobModel.status.not_in(JobStatus.finished_statuses()))
                    )
                )
            )
            run_model = res.scalar_one_or_none()
            if run_model is None:
                logger.warning(
                    "Failed to process run: lock_token mismatch."
                    " The run is expected to be processed and updated on another fetch iteration."
                )
                return ProcessingResult(requeue=False)

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
                        JobModel.lock_owner == self.__class__.__name__,
                    ),
                )
                .with_for_update(skip_locked=True, key_share=True)
            )
            locked_job_models = res.scalars().all()
            if len(run_model.jobs) != len(locked_job_models):
                logger.debug(
                    "Failed to lock run %s jobs. The run will be requeued and processed later.",
                    run_model.id,
                )
                return ProcessingResult(requeue=True)

            for job_model in locked_job_models:
                job_model.lock_expires_at = run_model.lock_expires_at
                run_model.lock_token = run_model.lock_token
                run_model.lock_owner = self.__class__.__name__
            await session.commit()

        # Do some work ...
        await asyncio.sleep(30)

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
            )
            if res.rowcount == 0:  # pyright: ignore[reportAttributeAccessIssue]
                logger.warning(
                    "Failed to update the run after processing: lock_token changed."
                    " The run is expected to be processed and updated on another fetch iteration."
                )
                return ProcessingResult(requeue=False)
        logger.debug("Processed run %s", item.id)
        return ProcessingResult(requeue=False)
