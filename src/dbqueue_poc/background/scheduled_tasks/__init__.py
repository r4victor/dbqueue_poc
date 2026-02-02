from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from dbqueue_poc.background.scheduled_tasks.process_events import delete_events


def start_scheduled_tasks() -> AsyncIOScheduler:
    """
    Start periodic tasks triggered by apscheduler at specific times/intervals.
    Suitable for tasks that run infrequently and don't need to lock rows for a long time.
    """
    scheduler = AsyncIOScheduler()
    scheduler.add_job(delete_events, IntervalTrigger(minutes=1), max_instances=1)
    scheduler.start()
    return scheduler
