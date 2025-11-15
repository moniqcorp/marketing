"""
Logging configuration
"""

import os
import sys
import logging
from datetime import datetime


def setup_logging(level=logging.INFO, log_file=None):
    """
    Setup logging configuration

    Args:
        level: Logging level (default: INFO)
        log_file: Optional log file path (auto-generated if True)
    """
    # Create formatters
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Setup handlers
    handlers = []

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(level)
    handlers.append(console_handler)

    # File handler (if requested)
    if log_file:
        if log_file is True:
            # Auto-generate log file name
            log_dir = 'logs'
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(
                log_dir,
                f"crawler_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            )

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(level)
        handlers.append(file_handler)

    # Configure root logger
    logging.basicConfig(
        level=level,
        handlers=handlers
    )

    return logging.getLogger(__name__)
