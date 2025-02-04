import asyncio
import logging
import logging.config
import os
import signal
import sys
from argparse import ArgumentParser
from functools import wraps
from typing import Coroutine, Callable, Dict, Optional

import utils.common as ut_common
import utils.exit_codes as ut_codes
from utils.aes_utils.common import set_logging


def _handle_sigint(signum, frame):
    msg = "Catch SIGINT signal"
    raise SystemExit(msg)


signal.signal(signal.SIGINT, _handle_sigint)


def get_service_args(service_name: str):
    parser = ArgumentParser(description=f"Subscriber service '{service_name}'")
    parser.add_argument(
        "--config",
        default=os.path.join(
            os.path.abspath(os.path.dirname(sys.argv[0])), "config.yml"
        ),
        help="Path to service config",
    )
    return parser.parse_args()


def graceful_stop(func, on_stop: Coroutine):
    @wraps(func)
    async def wrapped(config):
        try:
            await func(config)
        except (SystemExit, Exception):
            if on_stop is not None:
                await on_stop
            raise
    return wrapped


def run_async_service(service_name,
                      main_coro: Callable[[Dict], Coroutine],
                      on_stop: Optional[Coroutine] = None):
    """Запуск асинхронного сервиса-подписчика"""
    ret_code = ut_codes.STATUS_OK

    # запросим аргументы командной строки
    args = get_service_args(service_name)

    # Настроить логирование
    conf_dict = ut_common.load_config(args.config)
    set_logging(conf_dict)

    _service_logger = logging.getLogger(service_name)
    _service_logger.info("Start '%s' service", service_name)

    try:
        asyncio.run(
            graceful_stop(main_coro, on_stop)(conf_dict)
        )
    except SystemExit as sys_exit_ex:
        _service_logger.debug(sys_exit_ex)
    except Exception as ex:
        _service_logger.exception(ex)
        ret_code = ut_codes.UNKNOWN_ERROR
    finally:
        _service_logger.info("Stop '%s' service", service_name)
    return ret_code
