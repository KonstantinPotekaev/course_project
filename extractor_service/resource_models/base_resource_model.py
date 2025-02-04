from abc import ABC
from typing import Type, TypeVar, Union, List
from uuid import uuid4

from extractor_service.common.struct.mixins.controlled_runnable_mixin import ControlledRunnableMixin, InMsg, OutMsg
from extractor_service.common.struct.queue import (
    ProcessQueue,
    BaseInQueueMsg,
    ProcessJoinableQueue,
    BaseOutQueueMsg,
    BaseInData,
    BaseOutData,
)
from utils.aes_utils.exceptions import TechHandleException
from utils.status import StatusCodes

BaseOutMsg = TypeVar("BaseOutMsg", bound=BaseOutQueueMsg)


class BaseProxyModel:
    def __init__(self,
                 in_queue: ProcessJoinableQueue,
                 out_queue: ProcessQueue,
                 msg_data_type: Type[InMsg]):
        self._in_queue = in_queue
        self._out_queue = out_queue
        self._msg_data_type = msg_data_type

    async def _send_task(self, msg: BaseInQueueMsg) -> BaseOutMsg:
        self._in_queue.put(msg)
        while True:
            out_msg = await self._out_queue.aget()

            if out_msg.uuid != msg.uuid:
                await self._out_queue.aput(out_msg)
                continue
            break

        self._in_queue.task_done()
        return out_msg

    async def request(self, data: Union[BaseInData, List[BaseInData]]) -> Union[BaseOutData, List[BaseOutData]]:
        out_msg: OutMsg = await self._send_task(
            self._msg_data_type.construct(uuid=str(uuid4()), data=data)
        )
        if out_msg.status.code != StatusCodes.OK.code:
            raise TechHandleException(status=out_msg.status)
        return out_msg.data


class BaseResourceModel(ControlledRunnableMixin, ABC):
    def __init__(self,
                 name: str,
                 proxy_type: Type[BaseProxyModel],
                 in_msg_type: Type[InMsg] = InMsg,
                 out_msg_type: Type[OutMsg] = OutMsg,
                 replicas: int = 1):
        super().__init__(name, in_msg_type, out_msg_type, replicas)

        self._proxy_model = proxy_type
        self._link_counter = 0

    @property
    def proxy(self) -> BaseProxyModel:
        self._link_counter += 1
        return self._proxy_model(self._in_queue,
                                 self._out_queue,
                                 self._in_msg_type)

    def unlink(self):
        self._link_counter -= 1
        if self._link_counter == 0:
            self.pause()
