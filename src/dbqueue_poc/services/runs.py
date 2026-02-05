import random
import string
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from dbqueue_poc.models import RunModel
from dbqueue_poc.schemas import CreateRunRequest, RunStatus
from dbqueue_poc.services.pipeline import PipelineHinter
from dbqueue_poc.utils.common import get_current_datetime


async def create_run(
    session: AsyncSession,
    create_run_request: CreateRunRequest,
    pipeline_hinter: PipelineHinter,
) -> uuid.UUID:
    name = create_run_request.name
    if name is None:
        name = "".join(random.choice(string.ascii_lowercase) for _ in range(8))
    now = get_current_datetime()
    run_model = RunModel(
        name=name,
        status=RunStatus.SUBMITTED,
        run_spec="some_random_text",
        submitted_at=now,
        last_processed_at=now,
    )
    session.add(run_model)
    await session.commit()
    pipeline_hinter.hint_fetch(RunModel.__name__)
    return run_model.id
