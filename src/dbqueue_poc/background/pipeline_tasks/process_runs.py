import asyncio
import math
import uuid
from datetime import timedelta

from sqlalchemy import or_, select, update

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
        workers_num: int = 10,
        queue_lower_limit_factor: float = 0.5,
        queue_upper_limit_factor: float = 2.0,
        processing_timeout: int = 20,
    ) -> None:
        self._workers_num = workers_num
        self._queue_lower_limit_factor = queue_lower_limit_factor
        self._queue_upper_limit_factor = queue_upper_limit_factor
        self._processing_timeout = processing_timeout
        self._queue_desired_minsize = math.ceil(workers_num * queue_lower_limit_factor)
        self._queue_maxsize = math.ceil(workers_num * queue_upper_limit_factor)
        self._lock_timeout = processing_timeout + self.queue_wait_timeout(
            processing_timeout=processing_timeout,
            workers_num=workers_num,
            queue_maxsize=self._queue_maxsize,
        )
        self._queue = asyncio.Queue(maxsize=self._queue_maxsize)
        self._fetcher = RunFetcher(
            queue=self._queue,
            queue_desired_minsize=self._queue_desired_minsize,
            queue_maxsize=self._queue_maxsize,
            lock_timeout=self._lock_timeout,
        )
        self._workers = [RunWorker(queue=self._queue) for _ in range(workers_num)]

    def start(self):
        for worker in self._workers:
            asyncio.create_task(worker.start())
        asyncio.create_task(self._fetcher.start())

    @staticmethod
    def queue_wait_timeout(
        processing_timeout: int,
        workers_num: int,
        queue_maxsize: int,
    ) -> int:
        return math.ceil(queue_maxsize * processing_timeout / workers_num)


class RunFetcher:
    def __init__(
        self,
        queue: asyncio.Queue,
        queue_desired_minsize: int,
        queue_maxsize: int,
        lock_timeout: int,
    ) -> None:
        self._queue = queue
        self._queue_desired_minsize = queue_desired_minsize
        self._queue_maxsize = queue_maxsize
        self._lock_timeout = timedelta(seconds=lock_timeout)

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

    async def fetch(self, limit: int) -> list[uuid.UUID]:
        run_lock, _ = get_locker(get_db().dialect_name).get_lockset(RunModel.__tablename__)
        async with get_session_ctx() as session:
            async with run_lock:
                res = await session.execute(
                    select(RunModel.id)
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
                )
                run_ids = list(res.scalars().all())
                await session.execute(
                    update(RunModel)
                    .where(RunModel.id.in_(run_ids))
                    .values(lock_expires_at=get_current_datetime() + self._lock_timeout)
                )
                await session.commit()
        return run_ids


class RunWorker:
    def __init__(
        self,
        queue: asyncio.Queue,
    ) -> None:
        self._queue = queue

    async def start(self):
        while True:
            item = await self._queue.get()
            await self.process(item)

    async def process(self, run_id: uuid.UUID):
        logger.debug("Processing run %s", run_id)
        async with get_session_ctx() as session:
            res = await session.execute(select(RunModel).where(RunModel.id == run_id))
            run_model = res.scalar_one()
            # Do some work ...
            await asyncio.sleep(10)
            run_model.lock_expires_at = None
            run_model.last_processed_at = get_current_datetime()
            run_model.status = RunStatus.DONE
            await session.commit()
        logger.debug("Processed run %s", run_id)
