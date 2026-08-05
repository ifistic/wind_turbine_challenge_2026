"""
Logging setup for the pipeline: writes to both the console and
logs/pipeline.log (rotating, so it doesn't grow unbounded across runs).

Usage:
    from logging_config import get_logger
    logger = get_logger(__name__)
    logger.info("...")
"""
import logging
import logging.handlers
from pathlib import Path

from config import LOGGING, PROJECT_ROOT

_configured = False


def _configure_root_logger() -> None:
    global _configured
    if _configured:
        return

    log_path = PROJECT_ROOT / LOGGING.log_file
    log_path.parent.mkdir(parents=True, exist_ok=True)

    level = getattr(logging, LOGGING.level.upper(), logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)

    file_handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=3
    )
    file_handler.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(console_handler)
    root.addHandler(file_handler)

    # Spark's own logger is extremely verbose at INFO; keep it quieter
    # regardless of the pipeline's configured level, unless the person
    # explicitly asked for DEBUG (in which case they want everything).
    if level > logging.DEBUG:
        logging.getLogger("py4j").setLevel(logging.WARNING)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    _configure_root_logger()
    return logging.getLogger(name)