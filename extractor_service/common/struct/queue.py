import asyncio
from enum import Enum
from multiprocessing import Lock
from multiprocessing.queues import JoinableQueue, Queue
from typing import Type, TypeVar, Generic, Optional
from uuid import uuid4


from queue import Empty, Full  # noqa

from extractor_service.common.env.resources import PROCESS_QUEUE_MAX_SIZE

from utils.aes_utils.models.base_message import Status
from utils.aes_utils.models.base_model import BaseModel
from utils.status import StatusCodes


class Command(str, Enum):
    PROCESS = "process"
    STOP = "stop"


BaseInData = TypeVar("BaseInData", bound=BaseModel)
BaseOutData = TypeVar("BaseOutData", bound=BaseModel)


class BaseInQueueMsg(BaseModel):
    uuid: str = str(uuid4())
    cmd: Command = Command.PROCESS
    data: Optional[BaseInData]


class BaseOutQueueMsg(BaseModel):
    uuid: str
    data: Optional[BaseOutData]
    status: Status = Status.make_status(status=StatusCodes.OK)


ModelType = TypeVar('ModelType')


class ProcessQueue(Generic[ModelType], Queue):

    def __init__(self,
                 data_type: Type[ModelType],
                 max_size: int = PROCESS_QUEUE_MAX_SIZE,
                 *,
                 ctx):
        super().__init__(max_size, ctx=ctx)
        self.data_type = data_type
        self._put_block = Lock()

    @property
    def put_lock(self):
        return self._put_block

    def get(self, block=True, timeout=None) -> ModelType:
        data = super().get(block, timeout)
        if isinstance(data, self.data_type):
            return data

        if not isinstance(data, dict):
            raise ValueError("Data must be Pydantic.BaseModel or dict")
        return self.data_type.construct(**data)

    async def aget(self) -> ModelType:
        while True:
            try:
                return self.get_nowait()
            except Empty:
                await asyncio.sleep(0.5)

    async def aput(self, msg: ModelType):
        while True:
            try:
                self.put_nowait(msg)
                break
            except Full:
                await asyncio.sleep(0.5)

    def put_no_lock(self, obj: ModelType, block=True, timeout=None):
        super().put(obj, block, timeout)

    def put(self, obj: ModelType, block=True, timeout=None):
        with self._put_block:
            super().put(obj, block, timeout)


class ProcessJoinableQueue(Generic[ModelType], JoinableQueue):

    def __init__(self,
                 data_type: Type[ModelType],
                 max_size: int = PROCESS_QUEUE_MAX_SIZE,
                 *,
                 ctx):
        super().__init__(max_size, ctx=ctx)
        self.data_type = data_type
        self._put_block = Lock()

    @property
    def put_lock(self):
        return self._put_block

    def get(self, block=True, timeout=None) -> ModelType:
        data = super().get(block, timeout)
        if isinstance(data, self.data_type):
            return data

        if not isinstance(data, dict):
            raise ValueError("Data must be Pydantic.BaseModel or dict")
        return self.data_type.construct(**data)

    def put_no_lock(self, obj: ModelType, block=True, timeout=None):
        super().put(obj, block, timeout)

    def put(self, obj: ModelType, block=True, timeout=None):
        with self._put_block:
            super().put(obj, block, timeout)

    async def aget(self) -> ModelType:
        while True:
            try:
                return self.get_nowait()
            except Empty:
                await asyncio.sleep(0.5)

    async def aput(self, msg: ModelType):   # noqa
        while True:
            try:
                self.put_nowait(msg)
                break
            except Full:
                await asyncio.sleep(0.5)
