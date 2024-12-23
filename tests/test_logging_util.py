# test_logging_util.py
from utils.logging_util import configure_logging

def test_configure_logging():
    logger = configure_logging()
    assert logger is not None
    assert logger.level == 20  # INFO level