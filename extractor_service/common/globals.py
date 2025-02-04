import logging
import logging.config
from typing import Optional

from extractor_service.common.struct.process_logger import ProcessLogger, QueueHandler
from extractor_service.common.struct.resource_manager import ResourceManager
from utils import ut_logging

service_logger: Optional[logging.Logger] = None
service_config = {}

resource_manager = ResourceManager()


def init_service_config(config: dict):
    global service_config
    service_config = config


def init_service_logger(service_name, conf_dict):
    global service_logger

    root_logger = logging.getLogger()
    root_level = root_logger.level

    if root_level == logging.INFO:
        # отключаем логирование в библиотеках в режиме INFO
        loggers_section = conf_dict[ut_logging.LOGGING_SECTION][ut_logging.LOGGERS_SECTION]

        loggers_section['speechbrain.pretrained.fetching'][ut_logging.LEVEL_SUBSECTION] = "ERROR"
        loggers_section['faster_whisper'][ut_logging.LEVEL_SUBSECTION] = "ERROR"
        loggers_section['uvicorn.access'][ut_logging.LEVEL_SUBSECTION] = "ERROR"
        loggers_section['gensim.utils'][ut_logging.LEVEL_SUBSECTION] = "ERROR"

        logging.config.dictConfig(conf_dict[ut_logging.LOGGING_SECTION])

    process_logger = ProcessLogger(service_name, conf_dict)

    root_logger.handlers.clear()
    qh = QueueHandler(process_logger.queue)
    root_logger.addHandler(qh)

    # выставляем хэндлер по-умолчанию для логеров из конфигурации
    loggers = conf_dict[ut_logging.LOGGING_SECTION][ut_logging.LOGGERS_SECTION]
    for logger_name in loggers:
        conf_logger = logging.getLogger(logger_name)
        conf_logger.handlers.clear()
        conf_logger.addHandler(qh)

    # выставим уровень логирования
    root_logger.setLevel(root_level)

    service_logger = logging.getLogger(service_name)
