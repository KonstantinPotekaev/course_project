from typing import Optional, List, Dict

from extractor_service.common.struct.language import LanguageEnum
from extractor_service.common.struct.mixins.controlled_runnable_mixin import BaseResources
from extractor_service.common.struct.model.common import BaseData
from extractor_service.common.struct.queue import BaseInQueueMsg, BaseOutQueueMsg
from extractor_service.extractor.expansion_detection import ExpansionDetector
from extractor_service.resource_models.base_resource_model import BaseResourceModel, BaseProxyModel


class InData(BaseData):
    text: str
    abbreviations: List[str]
    language: LanguageEnum


class OutData(BaseData):
    expansions: Dict[str, Dict[str, int]]


class InMsg(BaseInQueueMsg):
    data: Optional[InData]


class OutMsg(BaseOutQueueMsg):
    data: Optional[OutData]


class Proxy(BaseProxyModel):
    async def detect_expansions(self,
                                content_id: str,
                                abbreviations: List[str],
                                text: str,
                                language: LanguageEnum) -> OutData:
        return await self.request(
            InData(
                key_=content_id,
                text=text,
                abbreviations=abbreviations,
                language=language
            )
        )


class Resources(BaseResources):
    detector: ExpansionDetector


class ExpansionDetectorModel(BaseResourceModel):
    def __init__(self,
                 name: str,
                 replicas: int):
        super().__init__(name=name,
                         in_msg_type=InMsg,
                         out_msg_type=OutMsg,
                         proxy_type=Proxy,
                         replicas=replicas)

    def _init_resources(self) -> Resources:
        return Resources(detector=ExpansionDetector())

    def handle_data(self, resources: Resources, task_data: InData) -> OutData:
        expansions = resources.detector.detect(text=task_data.text,
                                               abbreviations=task_data.abbreviations,
                                               language=task_data.language)
        return OutData(key_=task_data.key_,
                       expansions=expansions)
