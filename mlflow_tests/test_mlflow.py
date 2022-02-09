import logging
import sys


def get_logger(logger_name: str) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger


def _import_mlflow():
    from mlflow import set_tracking_uri
    set_tracking_uri("")


logging.basicConfig(level=logging.INFO)
get_logger(__name__).debug("Logging initialized")


def test_mlflow():
    assert "aria" != "random_cat"

    get_logger(__name__).debug("This is a log")
    _import_mlflow()
    get_logger(__name__).debug("This is another log")
    zero = 2-2
    assert zero == 0
