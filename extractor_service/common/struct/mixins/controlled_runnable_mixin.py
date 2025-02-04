import asyncio
from abc import abstractmethod, ABC
from copy import copy
from datetime import datetime
from enum import Enum
from math import ceil
from multiprocessing import get_context, Process, Value
from threading import Thread
from time import sleep
from typing import Type, Tuple, Dict, Coroutine, Callable, List, Any
from uuid import uuid4

from utils.aes_utils.common import retry
from utils.aes_utils.models.base_model import BaseModel

from utils.status import StatusCodes

import extractor_service.common.globals as aes_globals
from extractor_service.common.env.tech.common import DROP_INACTIVE_MODEL_PERIOD
from extractor_service.common.struct.model.common import Status
from extractor_service.common.struct.queue import (
    ProcessJoinableQueue,
    ProcessQueue,
    BaseInQueueMsg,
    BaseOutQueueMsg,
    Command, BaseInData, BaseOutData
)


class InMsg(BaseInQueueMsg):
    ...


class OutMsg(BaseOutQueueMsg):
    ...


class BaseResources(BaseModel):
    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True


class ProcessStatus(Enum):
    STARTED = 0
    STOPPED = 1


class ProcessPool:
    def __init__(self, workers):
        self._workers = workers
        self._pool: List[Process]= []
        self._status = ProcessStatus.STOPPED

    def join(self):
        for proc_idx in range(len(self._pool)):
            proc = self._pool.pop()
            proc.join()
        self._status = ProcessStatus.STOPPED

    def started(self) -> bool:
        return self._status == ProcessStatus.STARTED

    def start(self, target: Callable, args: Tuple[Any] = ()):
        for i in range(self._workers):
            p = Process(target=target, args=args)
            p.start()
            self._pool.append(p)
            self._status = ProcessStatus.STARTED


class ControlledRunnableMixin(ABC):
    def __init__(self,
                 name: str,
                 in_msg_type: Type[InMsg] = InMsg,
                 out_msg_type: Type[OutMsg] = OutMsg,
                 replicas: int = 1,
                 lazy: bool = True):
        self._name = name
        self._replicas = replicas
        self._lazy = lazy

        self._in_msg_type = in_msg_type
        self._out_msg_type = out_msg_type

        self._logger = aes_globals.service_logger.getChild(f"tech.{name}")

        self._pool = ProcessPool(replicas)
        self._proc_ctx = get_context("forkserver")

        now_ts = ceil(datetime.now().timestamp())
        self._start_ts = now_ts
        self._last_msg_ts = Value('Q', now_ts, lock=True)

        self._max_inactivity_period_sec = DROP_INACTIVE_MODEL_PERIOD
        self._usage_check_period_sec = 10
        self._new_msg_check_period_sec = 1

        self._in_queue = ProcessJoinableQueue(data_type=in_msg_type, ctx=self._proc_ctx)
        self._out_queue = ProcessQueue(data_type=out_msg_type, ctx=self._proc_ctx)

    def _serializable_copy(self):
        obj = copy(self)
        del obj._pool
        del obj._proc_ctx
        return obj

    @property
    def started(self):
        return self._pool.started()

    @abstractmethod
    def handle_data(self,
                    resources: BaseResources,
                    task_data: BaseInData) -> BaseOutData:
        raise NotImplementedError

    def _init_resources(self) -> BaseResources:
        return BaseResources()

    def _stop_inactive_routine(self):
        while True:
            sleep(self._usage_check_period_sec)

            if not self.started:
                return

            now_ts = datetime.now().timestamp()
            inactivity_period = now_ts - self._last_msg_ts.value
            if inactivity_period > self._max_inactivity_period_sec:
                self._logger.debug("Inactivity period exceeded: %d > %d",
                                   inactivity_period, self._max_inactivity_period_sec)
                self.pause()
                return

    def _check_new_message_routine(self):
        while True:
            sleep(self._new_msg_check_period_sec)

            if self.started:
                return

            if not self._in_queue.empty():
                self._logger.debug("New msg received. Start process...")
                self._update_last_msg_dt()
                self.start()
                return

    def _start_inactivity_control(self):
        if not self.started:
            return

        self._logger.debug("Start inactivity control...")
        thread = Thread(target=self._stop_inactive_routine, daemon=True)
        thread.start()

    def _start_new_msg_checker(self):
        if self.started:
            return

        self._logger.debug("Start new msg checker...")
        thread = Thread(target=self._check_new_message_routine, daemon=True)
        thread.start()

    def _update_last_msg_dt(self):
        self._last_msg_ts.value = ceil(datetime.now().timestamp())

    def _on_stop(self):
        pass

    def _process_routine(self):
        # TODO: exception check

        resources = self._init_resources()
        while True:
            task: InMsg = self._in_queue.get()
            if task.cmd == Command.STOP:
                self._logger.debug("Stop task received")
                self._on_stop()
                break

            if task.data is None:
                self._logger.warning(f"Task with no data: {task}")
                continue

            # обновляем время последнего обращения
            self._update_last_msg_dt()

            data = task.data
            try:
                out_data = self.handle_data(resources, data)
            except Exception:
                self._logger.exception("Error [handle data]")
                status = Status.make_status(status=StatusCodes.INTERNAL_ERROR,
                                            message="Error while processing task")
                self._out_queue.put(
                    self._out_msg_type(uuid=task.uuid, status=status)
                )
            else:
                self._out_queue.put(
                    self._out_msg_type(uuid=task.uuid, data=out_data)
                )

    @staticmethod
    @retry(delay=0.1, max_delay=5, jitter=(0.1, 1), backoff=2)
    def main_process_routine(serialized_self: "ControlledRunnableMixin"):
        try:
            serialized_self._process_routine()
        except Exception as ex:
            serialized_self._logger.exception(ex)
            raise

    def _start_model_routine(self):
        self._logger.info(f"Start model (replicas={self._replicas})...")
        self._pool.start(target=self.main_process_routine,
                         args=(self._serializable_copy(),))

    def start(self) -> Tuple[ProcessJoinableQueue[InMsg], ProcessQueue[OutMsg]]:
        if self.started:
            return self._in_queue, self._out_queue

        if self._lazy:
            # характеристика актуальна только только для первого запуска
            self._lazy = False
            self._start_new_msg_checker()
            return self._in_queue, self._out_queue

        self._start_model_routine()
        self._start_inactivity_control()
        return self._in_queue, self._out_queue

    def stop(self):
        if not self.started:
            return

        self._logger.debug("Stop model...")
        with self._in_queue.put_lock:
            for _ in range(self._replicas):
                self._in_queue.put_no_lock(self._in_msg_type(cmd=Command.STOP))
                self._in_queue.task_done()
            self._pool.join()
            self._logger.debug("Model has been stopped")

    def pause(self):
        if not self.started:
            return

        self._logger.info("Pause model...")
        self.stop()
        self._start_new_msg_checker()


class AsyncControlledRunnableMixin(ControlledRunnableMixin, ABC):

    def __init__(self,
                 name: str,
                 async_task_limit: int = 100,
                 in_msg_type: Type[InMsg] = InMsg,
                 out_msg_type: Type[OutMsg] = OutMsg,
                 replicas: int = 1,
                 lazy: bool = True):
        super().__init__(name=name,
                         in_msg_type=in_msg_type,
                         out_msg_type=out_msg_type,
                         replicas=replicas,
                         lazy=lazy)
        self._async_task_limit = async_task_limit

        self._async_task_access_lock = asyncio.Lock()
        self._async_tasks: Dict[str, asyncio.Task] = {}

    async def _add_task(self, task: asyncio.Task) -> str:
        async with self._async_task_access_lock:
            task_name = task.get_name()
            if task_name in self._async_tasks:
                new_task_name = str(uuid4())
                task.set_name(new_task_name)

                self._logger.debug("Task name already exists '%s'."
                                   " New name generated: '%s'",
                                   task_name, new_task_name)
                task_name = new_task_name

            self._logger.debug(f"Run task '%s'", task_name)
            self._async_tasks[task_name] = task
            return task_name

    async def _delete_task(self, task: asyncio.Task):
        task_name = task.get_name()

        async with self._async_task_access_lock:
            self._logger.debug(f"Delete task '{task_name}'")
            del self._async_tasks[task_name]

    @property
    async def running_task_count(self):
        async with self._async_task_access_lock:
            return len(self._async_tasks)

    async def _wait_tasks_to_complete(self):
        async with self._async_task_access_lock:
            if not self._async_tasks:
                return

            await asyncio.wait(self._async_tasks.values(),
                               return_when=asyncio.ALL_COMPLETED)

    async def _process_and_send_result(self, handle_coro: Coroutine, process_task_uuid: str):
        task = asyncio.current_task()
        task.set_name(process_task_uuid)

        # имя задачи может измениться в процессе вставки,
        # если задача с таким именем уже существует
        task_name = await self._add_task(task)

        # выполняем полезную работу
        try:
            result = await handle_coro
        except Exception:
            self._logger.exception("Error [handle task] (%s)", task_name)
            status = Status.make_status(status=StatusCodes.INTERNAL_ERROR,
                                        message="Error while processing task")

            out_msg = self._out_msg_type.construct(uuid=process_task_uuid, status=status)
            await self._out_queue.aput(out_msg)
        else:
            out_msg = self._out_msg_type.construct(uuid=process_task_uuid, data=result)
            await self._out_queue.aput(out_msg)
        finally:
            await self._delete_task(task)

    async def _run_async_task(self, task_data, task_uuid, resources):
        handle_coro = self.handle_data(resources, task_data)
        asyncio.create_task(
            self._process_and_send_result(handle_coro, task_uuid)
        )

    @abstractmethod
    async def handle_data(self,
                          resources: BaseResources,
                          task_data: BaseInData) -> BaseOutData:
        raise NotImplementedError

    async def _init_resources(self) -> BaseResources:
        pass

    async def _on_stop(self):
        pass

    async def _process_routine(self):
        # TODO: exception check

        resources = await self._init_resources()

        while True:
            running_task_count = await self.running_task_count
            if running_task_count >= self._async_task_limit:
                await asyncio.sleep(0.5)
                continue

            task: InMsg = await self._in_queue.aget()
            if task.cmd == Command.STOP:
                self._logger.debug("Stop task received")
                await self._wait_tasks_to_complete()
                await self._on_stop()
                break

            if task.data is None:
                self._logger.warning(f"Task with no data: {task}")
                continue

            # обновляем время последнего обращения
            self._update_last_msg_dt()

            # ставим задачу на асинхронную обработку
            await self._run_async_task(task.data, task.uuid, resources)

    @staticmethod
    @retry(delay=0.1, max_delay=5, jitter=(0.1, 1), backoff=2)
    def main_process_routine(serialized_self: "AsyncControlledRunnableMixin"):
        try:
            asyncio.run(serialized_self._process_routine())
        except Exception as ex:
            serialized_self._logger.exception(ex)
            raise
