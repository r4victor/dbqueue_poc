import random
import string
import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from dbqueue_poc.models import PlacementGroupModel
from dbqueue_poc.schemas import CreatePlacementGroupRequest, PlacementGroupStatus


async def create_placement_group(
    session: AsyncSession, create_placement_group_request: CreatePlacementGroupRequest
) -> uuid.UUID:
    name = create_placement_group_request.name
    if name is None:
        name = "".join(random.choice(string.ascii_lowercase) for _ in range(8))
    placement_group_model = PlacementGroupModel(
        name=name,
        status=PlacementGroupStatus.ACTIVE,
    )
    session.add(placement_group_model)
    await session.commit()
    return placement_group_model.id
