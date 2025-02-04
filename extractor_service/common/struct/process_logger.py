import logging
from logging.handlers import QueueHandler as LoggingQueueHandler
from multiprocessing import Process, get_context, current_process, Queue
from queue import Empty, Full   # noqa

from utils.aes_utils.common import set_logging


class QueueHandler(LoggingQueueHandler):

    def enqueue(self, record: logging.LogRecord) -> None:
        try:
            self.queue.put_nowait(record)
        except Full:
            pass


class ProcessLogger(Process):

    def __init__(self, name: str, config: dict):
        super().__init__(name=name, daemon=True)
        self.name = name
        self.config = config

        self._proc_ctx = get_context("forkserver")
        self._queue = Queue()

        self.start()

    @property
    def queue(self) -> Queue:
        return self._queue

    def run(self):
        set_logging(self.config)
        logger = logging.getLogger(self.name)

        proc = current_process()
        logger.debug(f"Start logger process: {proc.pid}")
        while True:
            message: logging.LogRecord = self._queue.get()
            logger.handle(message)
