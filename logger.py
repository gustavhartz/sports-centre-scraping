import logging
import datetime
import os

LOGGER_NAME = "web_scraper"


def setup_logging(path=None):
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.DEBUG)  # Set the lowest level to capture all messages

    # Create file handler which logs even debug messages
    time = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    if path != None:
        os.makedirs(path, exist_ok=True)
    else:
        path = "."
    fh = logging.FileHandler(f"{path}/{LOGGER_NAME}-{time}.log")
    fh.setLevel(logging.DEBUG)

    # Create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def get_logger():
    return logging.getLogger(LOGGER_NAME)
