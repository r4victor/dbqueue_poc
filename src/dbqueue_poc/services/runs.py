import random
import string
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from dbqueue_poc.models import RunModel
from dbqueue_poc.schemas import CreateRunRequest, RunStatus


async def create_run(session: AsyncSession, create_run_request: CreateRunRequest) -> uuid.UUID:
    name = create_run_request.name
    if name is None:
        name = "".join(random.choice(string.ascii_lowercase) for _ in range(8))
    run_model = RunModel(
        name=name,
        status=RunStatus.SUBMITTED,
        run_spec="some_random_text",
    )
    session.add(run_model)
    await session.commit()
    return run_model.id
