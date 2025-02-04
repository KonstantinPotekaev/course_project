import logging
import logging.config
import os


LOGGING_SECTION = "Logging"
DEBUG = "Debug"
INFO = "Info"

ROOT_SECTION = "root"
HANDLERS_SECTION = "handlers"
LOGGERS_SECTION = "loggers"
FILE_SECTION = "file"
LEVEL_SUBSECTION = "level"

LOG_LEVEL_ENV = "SRV_LOG_LEVEL"

LOG_FORMAT = "%(asctime)s [%(levelname)s] : <%(name)s> %(message)s"
LOG_LEVEL_STRINGS = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")


def create_logger(conf_dict: dict):
    log_level = (os.getenv(LOG_LEVEL_ENV, INFO)).upper()
    if log_level:
        conf_dict[LOGGING_SECTION][ROOT_SECTION][LEVEL_SUBSECTION] = log_level

    logging.config.dictConfig(conf_dict[LOGGING_SECTION])

