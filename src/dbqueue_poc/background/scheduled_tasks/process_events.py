from datetime import timedelta

from sqlalchemy import delete

from dbqueue_poc.db import get_session_ctx
from dbqueue_poc.models import EventModel
from dbqueue_poc.utils.common import get_current_datetime


EVENTS_TTL_SECONDS = 3600

async def delete_events():
    cutoff = get_current_datetime() - timedelta(seconds=EVENTS_TTL_SECONDS)
    stmt = delete(EventModel).where(EventModel.recorded_at < cutoff)
    async with get_session_ctx() as session:
        await session.execute(stmt)
