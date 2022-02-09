import logging
import sys


def get_logger(logger_name: str) -> logging.Logger:
    """Main function to get logger name,.

    Args:
      logger_name: Name of logger to initialize.

    Returns:
        A logger object.

    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger


def _init_logging() -> None:
    logging.basicConfig(level=logging.INFO)
    get_logger(__name__).debug("Logging initialized")


def _step_2():
    from mlflow import set_tracking_uri
    set_tracking_uri("")


def test_mlflow():
    assert "aria" != "random_cat"

    _init_logging()
    get_logger(__name__).debug("This is a log")
    _step_2()
    get_logger(__name__).debug("This is another log")
    zero = 2-2
    assert zero == 0
