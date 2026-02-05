import random
import string
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from dbqueue_poc.models import JobModel
from dbqueue_poc.schemas import CreateJobRequest, JobStatus
from dbqueue_poc.services.pipeline import PipelineHinter
from dbqueue_poc.utils.common import get_current_datetime


async def create_job(
    session: AsyncSession,
    create_job_request: CreateJobRequest,
    pipeline_hinter: PipelineHinter,
) -> uuid.UUID:
    name = create_job_request.name
    if name is None:
        name = "".join(random.choice(string.ascii_lowercase) for _ in range(8))
    now = get_current_datetime()
    job_model = JobModel(
        run_id=create_job_request.run_id,
        name=name,
        status=JobStatus.SUBMITTED,
        submitted_at=now,
        last_processed_at=now,
    )
    session.add(job_model)
    await session.commit()
    pipeline_hinter.hint_fetch(JobModel.__name__)
    return job_model.id
