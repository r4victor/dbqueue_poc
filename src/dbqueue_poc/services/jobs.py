import random
import string
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from dbqueue_poc.models import JobModel
from dbqueue_poc.schemas import CreateJobRequest, JobStatus


async def create_job(session: AsyncSession, create_job_request: CreateJobRequest) -> uuid.UUID:
    name = create_job_request.name
    if name is None:
        name = "".join(random.choice(string.ascii_lowercase) for _ in range(8))
    job_model = JobModel(
        run_id=create_job_request.run_id,
        name=name,
        status=JobStatus.SUBMITTED,
    )
    session.add(job_model)
    await session.commit()
    return job_model.id
