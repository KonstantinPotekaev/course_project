import asyncio
import logging.config
import os
import random
import time
from functools import wraps
from typing import Optional, Union, Type, Tuple, AsyncGenerator

import utils.ut_logging as ut_logging

SRV_LOG_LEVEL_ENV = "SRV_LOG_LEVEL"


async def async_grouper(content_gen: AsyncGenerator, batch_size: Optional[int] = -1) -> AsyncGenerator:
    cur_batch = []
    async for content_info in content_gen:
        cur_batch.append(content_info)
        if 0 < batch_size <= len(cur_batch):
            yield cur_batch
            cur_batch = []

    if cur_batch:
        yield cur_batch


def retry(exceptions: Union[Type[Exception], Tuple[Type[Exception]]] = Exception,
          tries: int = -1,
          delay: float = 0,
          max_delay: Optional[float] = None,
          backoff: float = 1,
          jitter: Union[float, Tuple[float, float]] = 0):
    """
    Выполнить функцию и повторить в случае возникновения исключений

    :param exceptions:  исключение или кортеж исключений, которые будут перехватываться
    :param tries:       максимальное число попыток перезапуска
    :param delay:       начальная задержка между попытками
    :param max_delay:   максимальная задержка между попытками
    :param backoff:     мультиплиептор, на который умножатся delay при очередной попытке перезапуска
    :param jitter:      доп. прибавка в секундах к задержке между попытками
                        фиксированный или случайный в интервале (min, max)
    :returns: результат исполнения функции или исключение
    """
    logger = logging.getLogger("prediction_utils.common.retryer")

    def wrapper(func):
        def update_delay(old_delay: float) -> float:
            new_delay = old_delay * backoff

            if isinstance(jitter, tuple):
                new_delay += random.uniform(*jitter)
            else:
                new_delay += jitter

            if max_delay is not None:
                new_delay = min(new_delay, max_delay)
            return new_delay

        @wraps(func)
        async def async_wrapped(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries:
                try:
                    return await func(*args, **kwargs)
                except exceptions:
                    _tries -= 1
                    if _tries == 0:
                        raise

                logger.debug(f"retry: {func}")
                time.sleep(_delay)
                _delay = update_delay(_delay)

        @wraps(func)
        def wrapped(*args, **kwargs):
            _tries, _delay = tries, delay
            while _tries:
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    _tries -= 1
                    if _tries == 0:
                        raise

                logger.debug(f"retry: {func}")
                time.sleep(_delay)
                _delay = update_delay(_delay)

        if asyncio.iscoroutinefunction(func):
            return async_wrapped
        return wrapped

    return wrapper


def set_logging(config: dict):
    log_level = os.getenv(SRV_LOG_LEVEL_ENV, None)
    if log_level:
        config[ut_logging.LOGGING_SECTION][ut_logging.ROOT_SECTION][
            ut_logging.LEVEL_SUBSECTION
        ] = log_level.upper()
    logging.config.dictConfig(config[ut_logging.LOGGING_SECTION])
