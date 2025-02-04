from typing import Type

from utils.status import StatusCodes

import extractor_service.common.globals as aes_globals
from extractor_service.common.struct.mixins.controlled_runnable_mixin import AsyncControlledRunnableMixin
from extractor_service.common.struct.model.common import Status
from extractor_service.common.struct.queue import BaseInQueueMsg, BaseOutQueueMsg
from extractor_service.common.struct.resource_manager import ResourceManager
from extractor_service.resource_models.base_resource_model import BaseProxyModel


class InMsg(BaseInQueueMsg):
    ...


class OutMsg(BaseOutQueueMsg):
    status: Status = Status.make_status(status=StatusCodes.OK)


class BaseTechnology(AsyncControlledRunnableMixin):

    def __init__(self,
                 name: str,
                 resource_manager: ResourceManager,
                 proxy_type: Type[BaseProxyModel],
                 in_msg_type: Type[InMsg] = InMsg,
                 out_msg_type: Type[OutMsg] = OutMsg,
                 replicas: int = 1):
        super().__init__(name=name,
                         in_msg_type=in_msg_type,
                         out_msg_type=out_msg_type,
                         replicas=replicas)
        self._logger = aes_globals.service_logger.getChild(f"tech.{name}")
        self._resource_manager = resource_manager
        self._proxy_model = proxy_type

    @property
    def proxy(self) -> BaseProxyModel:
        return self._proxy_model(self._in_queue, self._out_queue, self._in_msg_type)
