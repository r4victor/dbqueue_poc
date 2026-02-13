import random
import string
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from dbqueue_poc.models import PlacementGroupModel
from dbqueue_poc.schemas import CreatePlacementGroupRequest, PlacementGroupStatus
from dbqueue_poc.services.pipeline import PipelineHinterProtocol
from dbqueue_poc.utils.common import get_current_datetime


async def create_placement_group(
    session: AsyncSession,
    create_placement_group_request: CreatePlacementGroupRequest,
    pipeline_hinter: PipelineHinterProtocol,
) -> uuid.UUID:
    name = create_placement_group_request.name
    if name is None:
        name = "".join(random.choice(string.ascii_lowercase) for _ in range(8))
    now = get_current_datetime()
    placement_group_model = PlacementGroupModel(
        name=name,
        status=PlacementGroupStatus.ACTIVE,
        submitted_at=now,
        last_processed_at=now,
    )
    session.add(placement_group_model)
    await session.commit()
    pipeline_hinter.hint_fetch(PlacementGroupModel.__name__)
    return placement_group_model.id
