from datetime import datetime, timezone


def get_current_datetime() -> datetime:
    return datetime.now(tz=timezone.utc)
