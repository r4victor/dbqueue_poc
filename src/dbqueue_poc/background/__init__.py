from apscheduler.schedulers.asyncio import AsyncIOScheduler


def start_background_tasks() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()
    scheduler.start()
    return scheduler
