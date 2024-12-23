import logging

def configure_logging(level=logging.INFO):
    logging.basicConfig(level=level)
    logger = logging.getLogger(__name__)
    logger.setLevel(level)  # Ensure the logger level is set
    return logger