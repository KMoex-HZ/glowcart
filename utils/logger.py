import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """
    Initializes or retrieves a logger instance with a standardized stream handler.
    Prevents handler duplication and ensures consistent log formatting across the platform.
    """
    logger = logging.getLogger(name)

    # Check for existing handlers to avoid duplicate log entries in shared environments
    if logger.handlers:
        return logger

    # Set base logging level to INFO
    logger.setLevel(logging.INFO)

    # Use sys.stdout instead of default stderr for better compatibility with Airflow/Docker logs
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)

    # Define a clean, structured log format for easier parsing
    formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger