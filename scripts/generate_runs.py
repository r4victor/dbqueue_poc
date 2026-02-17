#!/usr/bin/env python3
import argparse
import asyncio
import random
import time
import uuid
from datetime import timedelta

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dbqueue_poc import settings
from dbqueue_poc.models import RunModel
from dbqueue_poc.schemas import RunStatus
from dbqueue_poc.utils.common import get_current_datetime

ACTIVE_RUN_STATUSES = [
    RunStatus.PENDING,
    RunStatus.SUBMITTED,
    RunStatus.PROVISIONING,
    RunStatus.RUNNING,
    RunStatus.TERMINATING,
]
FINISHED_RUN_STATUSES = list(RunStatus.finished_statuses())
RUN_PRIORITY_MIN = 0
RUN_PRIORITY_MAX = 100
PROGRESS_EVERY = 5000


def _build_run_row(
    index: int,
    now,
    rng: random.Random,
    *,
    active_ratio: float,
    blocked_ratio: float,
    lookback_seconds: int,
    name_prefix: str,
) -> dict:
    if rng.random() < active_ratio:
        status = rng.choice(ACTIVE_RUN_STATUSES)
    else:
        status = rng.choice(FINISHED_RUN_STATUSES)

    submitted_delta = rng.randint(0, lookback_seconds)
    submitted_at = now - timedelta(seconds=submitted_delta)

    # Keep many rows immediately fetchable by pipelines.
    if rng.random() < 0.6:
        last_processed_at = submitted_at
    else:
        processing_delta = rng.randint(5, max(5, lookback_seconds))
        last_processed_at = now - timedelta(seconds=processing_delta)

    lock_expires_at = None
    lock_owner = None
    lock_token = None

    # Create a subset of blocked active rows that fetchers should skip.
    if status in ACTIVE_RUN_STATUSES and rng.random() < blocked_ratio:
        lock_expires_at = now + timedelta(seconds=rng.randint(30, 1800))
        lock_owner = "OtherPipeline"
        lock_token = uuid.uuid4()

    return {
        "id": uuid.uuid4(),
        "name": f"{name_prefix}-{index}",
        "submitted_at": submitted_at,
        "last_processed_at": last_processed_at,
        "status": status,
        "deleted": False,
        "lock_expires_at": lock_expires_at,
        "lock_token": lock_token,
        "lock_owner": lock_owner,
        "run_spec": f'{{"seed_index": {index}}}',
        "priority": rng.randint(RUN_PRIORITY_MIN, RUN_PRIORITY_MAX),
    }


async def _generate_runs(args: argparse.Namespace) -> int:
    engine = create_async_engine(args.database_url, echo=False)
    session_maker = async_sessionmaker(bind=engine, expire_on_commit=False)
    rng = random.Random(args.seed)
    created = 0
    start = time.perf_counter()
    try:
        async with session_maker() as session:
            while created < args.count:
                batch_size = min(args.batch_size, args.count - created)
                now = get_current_datetime()
                rows = [
                    _build_run_row(
                        created + i,
                        now,
                        rng,
                        active_ratio=args.active_ratio,
                        blocked_ratio=args.blocked_ratio,
                        lookback_seconds=args.lookback_seconds,
                        name_prefix=args.name_prefix,
                    )
                    for i in range(batch_size)
                ]
                await session.execute(insert(RunModel), rows)
                await session.commit()
                created += batch_size

                if created % PROGRESS_EVERY == 0 or created == args.count:
                    elapsed = max(0.001, time.perf_counter() - start)
                    print(f"Inserted {created}/{args.count} runs ({created / elapsed:.1f} rows/s)")
    finally:
        await engine.dispose()

    elapsed = max(0.001, time.perf_counter() - start)
    print(
        "Done:"
        f" inserted={created}, elapsed={elapsed:.2f}s, throughput={created / elapsed:.1f} rows/s"
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bulk-generate runs via direct DB batch inserts for load/index testing.",
    )
    parser.add_argument("count", type=int, help="Total number of runs to insert")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Rows per INSERT batch (default: %(default)s)",
    )
    parser.add_argument(
        "--database-url",
        default=settings.DATABASE_URL,
        help="Database URL (default: DBQUEUE_DATABASE_URL / settings)",
    )
    parser.add_argument(
        "--active-ratio",
        type=float,
        default=0.05,
        help="Fraction of rows in non-finished statuses (default: %(default)s)",
    )
    parser.add_argument(
        "--blocked-ratio",
        type=float,
        default=0.10,
        help=(
            "Fraction of active rows pre-locked by another pipeline "
            "to simulate non-fetchable rows (default: %(default)s)"
        ),
    )
    parser.add_argument(
        "--lookback-seconds",
        type=int,
        default=86400,
        help="Timestamp spread window for submitted/processed times (default: %(default)s)",
    )
    parser.add_argument(
        "--name-prefix",
        default="seed-run",
        help="Prefix for generated run names (default: %(default)s)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible data generation (default: %(default)s)",
    )
    return parser


def main() -> int:
    return asyncio.run(_generate_runs(_build_parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
