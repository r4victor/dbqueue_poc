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
from dbqueue_poc.models import PlacementGroupModel
from dbqueue_poc.schemas import PlacementGroupStatus
from dbqueue_poc.services.locking import get_locker
from dbqueue_poc.utils.common import get_current_datetime
from dbqueue_poc.utils.logging import get_logger

logger = get_logger(__name__)


class PlacementGroupPipeline(Pipeline):
    """
    An example of the simplest pipeline that only needs to lock
    and update one resource type (`PlacementGroupModel`).

    Highlights:
        * A fetcher fetches items for processing from the DB via
          SELECT FOR UPDATE SKIP LOCKED on Postgres and in-memory locks on SQLite,
          locks the items in the DB, and puts the items into in-memory queue.
        * Workers get items from the queue, process them, update and unlock in the DB.
        * Stale locked items are picked up by the pipeline when `lock_expires_at` is due.
        * `lock_token` prevents stale workers to update stale locked items.
        * A hearbeater tracks all items currently in the pipeline (in the queue or in processing)
          and renews `lock_expires_at` before it's due. This allows setting low `lock_expires_at`,
          thus picking up stale locked items quickly.
    """

    def __init__(
        self,
        workers_num: int = 10,
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
        self.__heartbeater = Heartbeater[PlacementGroupModel](
            model_type=PlacementGroupModel,
            lock_timeout=self._lock_timeout,
            heartbeat_trigger=self._heartbeat_trigger,
        )
        self.__fetcher = PlacementGroupFetcher(
            queue=self._queue,
            queue_desired_minsize=self._queue_desired_minsize,
            min_processing_interval=self._min_processing_interval,
            lock_timeout=self._lock_timeout,
            heartbeater=self._heartbeater,
        )
        self.__workers = [
            PlacementGroupWorker(queue=self._queue, heartbeater=self._heartbeater)
            for _ in range(self._workers_num)
        ]

    @property
    def hint_fetch_model_name(self) -> str:
        return PlacementGroupModel.__name__

    @property
    def _heartbeater(self) -> Heartbeater:
        return self.__heartbeater

    @property
    def _fetcher(self) -> Fetcher:
        return self.__fetcher

    @property
    def _workers(self) -> Sequence["PlacementGroupWorker"]:
        return self.__workers


class PlacementGroupFetcher(Fetcher):
    def __init__(
        self,
        queue: asyncio.Queue[PipelineItem],
        queue_desired_minsize: int,
        min_processing_interval: timedelta,
        lock_timeout: timedelta,
        heartbeater: Heartbeater[PlacementGroupModel],
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
        placement_group_lock, _ = get_locker(get_db().dialect_name).get_lockset(
            PlacementGroupModel.__tablename__
        )
        async with placement_group_lock:
            async with get_session_ctx() as session:
                now = get_current_datetime()
                res = await session.execute(
                    select(PlacementGroupModel)
                    .where(
                        PlacementGroupModel.status.not_in(
                            PlacementGroupStatus.finished_statuses()
                        ),
                        or_(
                            PlacementGroupModel.last_processed_at
                            <= now - self._min_processing_interval,
                            PlacementGroupModel.last_processed_at
                            == PlacementGroupModel.submitted_at,
                        ),
                        or_(
                            PlacementGroupModel.lock_expires_at.is_(None),
                            PlacementGroupModel.lock_expires_at < now,
                        ),
                        or_(
                            PlacementGroupModel.lock_owner.is_(None),
                            PlacementGroupModel.lock_owner == PlacementGroupPipeline.__name__,
                        ),
                    )
                    .order_by(PlacementGroupModel.last_processed_at.asc())
                    .limit(limit)
                    .with_for_update(skip_locked=True, key_share=True)
                    .options(
                        load_only(
                            PlacementGroupModel.id,
                            PlacementGroupModel.lock_token,
                            PlacementGroupModel.lock_expires_at,
                        )
                    )
                )
                placement_group_models = list(res.scalars().all())
                lock_expires_at = get_current_datetime() + self._lock_timeout
                lock_token = uuid.uuid4()
                for placement_group_model in placement_group_models:
                    placement_group_model.lock_expires_at = lock_expires_at
                    placement_group_model.lock_token = lock_token
                    placement_group_model.lock_owner = PlacementGroupPipeline.__name__
                await session.commit()
        return [cast(PipelineItem, r) for r in placement_group_models]


class PlacementGroupWorker(Worker):
    def __init__(
        self,
        queue: asyncio.Queue[PipelineItem],
        heartbeater: Heartbeater[PlacementGroupModel],
    ) -> None:
        super().__init__(
            queue=queue,
            heartbeater=heartbeater,
        )

    async def process(self, item: PipelineItem):
        logger.debug("Processing placement group %s", item.id)
        async with get_session_ctx() as session:
            res = await session.execute(
                select(PlacementGroupModel).where(
                    PlacementGroupModel.id == item.id,
                    PlacementGroupModel.lock_token == item.lock_token,
                )
            )
            placement_group_model = res.scalar_one_or_none()
            if placement_group_model is None:
                logger.warning(
                    "Failed to process placement group: lock_token mismatch."
                    " The placement group is expected to be processed and updated on another fetch iteration."
                )
                return

        # Do some work ...
        await asyncio.sleep(30)

        async with get_session_ctx() as session:
            res = await session.execute(
                update(PlacementGroupModel)
                .where(
                    PlacementGroupModel.id == placement_group_model.id,
                    PlacementGroupModel.lock_token == placement_group_model.lock_token,
                )
                .values(
                    lock_expires_at=None,
                    lock_token=None,
                    lock_owner=None,
                    last_processed_at=get_current_datetime(),
                    status=PlacementGroupStatus.TERMINATED,
                )
            )
            if res.rowcount == 0:  # pyright: ignore[reportAttributeAccessIssue]
                logger.warning(
                    "Failed to update the placement group after processing: lock_token changed."
                    " The placement group is expected to be processed and updated on another fetch iteration."
                )
                return
        logger.debug("Processed placement group %s", item.id)
