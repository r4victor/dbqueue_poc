import logging
import sys

from dbqueue_poc import settings


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def configure_logging():
    root_logger = logging.getLogger(None)
    root_logger.setLevel(settings.ROOT_LOG_LEVEL)
    formatter = logging.Formatter(
        fmt="%(levelname)s %(asctime)s.%(msecs)03d %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    dbqueue_logger = logging.getLogger("dbqueue_poc")
    dbqueue_logger.setLevel(settings.LOG_LEVEL)
