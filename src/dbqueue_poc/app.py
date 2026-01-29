import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse

from dbqueue_poc.db import get_db, migrate
from dbqueue_poc.background import start_background_tasks


def create_app() -> FastAPI:
    app = FastAPI(
        docs_url="/api/docs",
        lifespan=lifespan,
    )
    return app


@asynccontextmanager
async def lifespan(app: FastAPI):
    await migrate()
    scheduler = start_background_tasks()
    yield
    if scheduler is not None:
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
