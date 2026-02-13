import asyncio
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Request, Response
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from dbqueue_poc.background.pipeline_tasks import PipelineHinter, start_pipeline_tasks
from dbqueue_poc.background.scheduled_tasks import start_scheduled_tasks
from dbqueue_poc.db import get_db, get_session, migrate
from dbqueue_poc.schemas import CreateJobRequest, CreatePlacementGroupRequest, CreateRunRequest
from dbqueue_poc.services import jobs, placement_groups, runs
from dbqueue_poc.services.pipelines import PipelineHinterProtocol
from dbqueue_poc.utils.logging import configure_logging, get_logger

logger = get_logger(__name__)


def create_app() -> FastAPI:
    app = FastAPI(
        docs_url="/api/docs",
        lifespan=lifespan,
    )
    return app


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    await migrate()
    scheduler = start_scheduled_tasks()
    pipeline_manager = start_pipeline_tasks()
    app.state.pipeline_manager = pipeline_manager
    logger.info("dbqueue server running")
    yield
    pipeline_manager.shutdown()
    scheduler.shutdown()
    await pipeline_manager.drain()
    await get_db().engine.dispose()
    # Let checked-out DB connections close as dispose() only closes checked-in connections
    await asyncio.sleep(3)


def get_pipeline_hinter(request: Request) -> PipelineHinterProtocol:
    hinter: PipelineHinter = request.app.state.pipeline_manager.hinter
    return hinter


def register_routes(app: FastAPI, ui: bool = True):
    @app.get("/")
    async def index():
        return Response(content="Hello!")

    @app.get("/healthcheck")
    async def healthcheck():
        return JSONResponse(content={"status": "running"})

    @app.post("/runs/create")
    async def create_run(
        body: CreateRunRequest,
        session: AsyncSession = Depends(get_session),
        pipeline_hinter: PipelineHinterProtocol = Depends(get_pipeline_hinter),
    ):
        run_id = await runs.create_run(
            session=session,
            create_run_request=body,
            pipeline_hinter=pipeline_hinter,
        )
        return {"id": run_id}

    @app.post("/jobs/create")
    async def create_job(
        body: CreateJobRequest,
        session: AsyncSession = Depends(get_session),
        pipeline_hinter: PipelineHinterProtocol = Depends(get_pipeline_hinter),
    ):
        job_id = await jobs.create_job(
            session=session,
            create_job_request=body,
            pipeline_hinter=pipeline_hinter,
        )
        return {"id": job_id}

    @app.post("/placement-groups/create")
    async def create_placement_group(
        body: CreatePlacementGroupRequest,
        session: AsyncSession = Depends(get_session),
        pipeline_hinter: PipelineHinterProtocol = Depends(get_pipeline_hinter),
    ):
        placement_group_id = await placement_groups.create_placement_group(
            session=session,
            create_placement_group_request=body,
            pipeline_hinter=pipeline_hinter,
        )
        return {"id": placement_group_id}
