import asyncio
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Response
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from dbqueue_poc.background.pipeline_tasks import start_pipeline_tasks
from dbqueue_poc.background.scheduled_tasks import start_scheduled_tasks
from dbqueue_poc.db import get_db, get_session, migrate
from dbqueue_poc.schemas import CreateJobRequest, CreateRunRequest
from dbqueue_poc.services import jobs, runs
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
    logger.info("dbqueue server running")
    yield
    pipeline_manager.shutdown()
    scheduler.shutdown()
    await get_db().engine.dispose()
    # Let checked-out DB connections close as dispose() only closes checked-in connections
    await asyncio.sleep(3)


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
    ):
        await runs.create_run(session=session, create_run_request=body)

    @app.post("/jobs/create")
    async def create_job(
        body: CreateJobRequest,
        session: AsyncSession = Depends(get_session),
    ):
        await jobs.create_job(session=session, create_job_request=body)
