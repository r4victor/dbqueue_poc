import os

from pathlib import Path


STATE_DIR_PATH = Path(
    os.getenv(
        "DBQUEUE_STATE_DIR",
        Path(__file__).parent.parent.parent / ".state",
    )
).resolve()
STATE_DIR_PATH.mkdir(parents=True, exist_ok=True)
DATABASE_URL = os.getenv(
    "DBQUEUE_DATABASE_URL",
    f"sqlite+aiosqlite:///{str(STATE_DIR_PATH.absolute())}/sqlite.db",
)
DB_POOL_SIZE = int(os.getenv("DBQUEUE_DB_POOL_SIZE", 20))
DB_MAX_OVERFLOW = int(os.getenv("DBQUEUE_DB_MAX_OVERFLOW", 20))
SQL_ECHO_ENABLED = os.getenv("DBQUEUE_SQL_ECHO_ENABLED") is not None
ALEMBIC_MIGRATIONS_LOCATION = os.getenv(
    "DBQUEUE_ALEMBIC_MIGRATIONS_LOCATION",
    "dbqueue_poc:migrations",
)
