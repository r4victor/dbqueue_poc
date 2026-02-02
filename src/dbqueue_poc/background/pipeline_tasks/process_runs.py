import asyncio
import math
import uuid
from datetime import timedelta

from sqlalchemy import and_, or_, select, update
from sqlalchemy.orm import load_only

from dbqueue_poc.db import get_db, get_session_ctx
from dbqueue_poc.models import RunModel
from dbqueue_poc.schemas import RunStatus
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
        lock_timeout: timedelta = timedelta(seconds=20),
        heartbeat_margin: timedelta = timedelta(seconds=10),
    ) -> None:
        self._workers_num = workers_num
        self._queue_lower_limit_factor = queue_lower_limit_factor
        self._queue_upper_limit_factor = queue_upper_limit_factor
        self._queue_desired_minsize = math.ceil(workers_num * queue_lower_limit_factor)
        self._queue_maxsize = math.ceil(workers_num * queue_upper_limit_factor)
        self._lock_timeout = lock_timeout
        self._heartbeat_margin = heartbeat_margin
        self._queue = asyncio.Queue(maxsize=self._queue_maxsize)
        self._heartbeater = RunHeartbeater(
            lock_timeout=self._lock_timeout,
            heartbeat_margin=self._heartbeat_margin,
        )
        self._fetcher = RunFetcher(
            queue=self._queue,
            queue_desired_minsize=self._queue_desired_minsize,
            queue_maxsize=self._queue_maxsize,
            lock_timeout=self._lock_timeout,
            heartbeater=self._heartbeater,
        )
        self._workers = [
            RunWorker(queue=self._queue, heartbeater=self._heartbeater) for _ in range(workers_num)
        ]

    def start(self):
        asyncio.create_task(self._heartbeater.start())
        for worker in self._workers:
            asyncio.create_task(worker.start())
        asyncio.create_task(self._fetcher.start())


class RunHeartbeater:
    def __init__(
        self,
        lock_timeout: timedelta,
        heartbeat_margin: timedelta,
    ) -> None:
        self._lock_timeout = lock_timeout
        self._hearbeat_margin = heartbeat_margin
        self._items: dict[uuid.UUID, RunModel] = {}
        self._untrack_lock = asyncio.Lock()

    async def start(self):
        while True:
            await self.heartbeat()
            await asyncio.sleep(1)

    async def heartbeat(self):
        models_to_update = []
        now = get_current_datetime()
        run_models = list(self._items.values())
        for run_model in run_models:
            assert run_model.lock_expires_at is not None
            if run_model.lock_expires_at < now + self._hearbeat_margin:
                run_model.lock_expires_at = now + self._lock_timeout
                models_to_update.append(run_model)
            if run_model.lock_expires_at < now:
                logger.warning(
                    "Failed to heartbeat run %s in time."
                    " The run is expected to be processed on another fetch iteration.",
                    run_model.id,
                )
                await self.untrack(run_model)
        if len(models_to_update) == 0:
            return
        logger.debug(
            "Updating lock expiration for runs: %s", [str(r.id) for r in models_to_update]
        )
        runs_to_update_filters = [
            and_(RunModel.id == run_model.id, RunModel.lock_token == run_model.lock_token)
            for run_model in models_to_update
        ]
        async with get_session_ctx() as session:
            res = await session.execute(
                update(RunModel)
                .where(or_(*runs_to_update_filters))
                .values(lock_expires_at=now + self._lock_timeout)
            )
            if res.rowcount == 0:  # pyright: ignore[reportAttributeAccessIssue]
                logger.warning(
                    "Failed to update lock expiration: lock_token changed."
                    " The run is expected to be processed and updated by another worker."
                )

    async def track(self, item: RunModel):
        self._items[item.id] = item

    async def untrack(self, item: RunModel):
        async with self._untrack_lock:
            tracked = self._items.get(item.id)
            # Prevent iteration with expired lock to unlock item processed by new iteration.
            if tracked is not None and tracked.lock_token == item.lock_token:
                del self._items[item.id]


class RunFetcher:
    def __init__(
        self,
        queue: asyncio.Queue,
        queue_desired_minsize: int,
        queue_maxsize: int,
        lock_timeout: timedelta,
        heartbeater: RunHeartbeater,
    ) -> None:
        self._queue = queue
        self._queue_desired_minsize = queue_desired_minsize
        self._queue_maxsize = queue_maxsize
        self._lock_timeout = lock_timeout
        self._heartbeater = heartbeater

    async def start(self):
        while True:
            if self._queue.qsize() >= self._queue_desired_minsize:
                await asyncio.sleep(1)
                continue
            fetch_limit = self._queue_maxsize - self._queue.qsize()
            items = await self.fetch(limit=fetch_limit)
            if len(items) == 0:
                await asyncio.sleep(1)
                continue
            for item in items:
                self._queue.put_nowait(item)  # should never raise
                await self._heartbeater.track(item)

    async def fetch(self, limit: int) -> list[RunModel]:
        run_lock, _ = get_locker(get_db().dialect_name).get_lockset(RunModel.__tablename__)
        async with get_session_ctx() as session:
            async with run_lock:
                res = await session.execute(
                    select(RunModel)
                    .where(
                        RunModel.status.not_in(RunStatus.finished_statuses()),
                        or_(
                            RunModel.lock_expires_at.is_(None),
                            RunModel.lock_expires_at < get_current_datetime(),
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
                await session.commit()
        return run_models


class RunWorker:
    def __init__(
        self,
        queue: asyncio.Queue,
        heartbeater: RunHeartbeater,
    ) -> None:
        self._queue = queue
        self._heartbeater = heartbeater

    async def start(self):
        while True:
            item = await self._queue.get()
            await self.process(item)
            await self._heartbeater.untrack(item)

    async def process(self, item: RunModel):
        logger.debug("Processing run %s", item.id)
        async with get_session_ctx() as session:
            res = await session.execute(
                select(RunModel).where(
                    RunModel.id == item.id,
                    RunModel.lock_token == item.lock_token,
                )
            )
            run_model = res.scalar_one_or_none()
            if run_model is None:
                logger.warning(
                    "Failed to process run: lock_token mismatch."
                    " The run is expected to be processed and updated by another worker."
                )
                return

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
                    last_processed_at=get_current_datetime(),
                    status=RunStatus.DONE,
                )
            )
            if res.rowcount == 0:  # pyright: ignore[reportAttributeAccessIssue]
                logger.warning(
                    "Failed to update the run after processing: lock_token changed."
                    " The run is expected to be processed and updated by another worker."
                )
                return
        logger.debug("Processed run %s", run_model.id)
