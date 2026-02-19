import sys
from loguru import logger
from dbt_gdpr_anonymizer.config.settings import LoggingConfig


def setup_logger(cfg: LoggingConfig) -> None:
    """Configure Loguru with structured formatting and file rotation."""

    logger.remove()  # Remove default handler

    # Console handler
    logger.add(
        sys.stderr,
        format=cfg.format,
        level=cfg.level,
        colorize=True,
    )

    # File handler
    logger.add(
        cfg.file_path,
        format=cfg.format,
        level=cfg.level,
        rotation=cfg.rotation,
        retention=cfg.retention,
        compression=cfg.compression,
    )
