from typing import Optional, List

from extractor_service.common.struct.language import LanguageEnum
from extractor_service.common.struct.mixins.controlled_runnable_mixin import BaseResources
from extractor_service.common.struct.model.common import BaseData
from extractor_service.common.struct.queue import BaseInQueueMsg, BaseOutQueueMsg
from extractor_service.extractor.abbreviation_detection import AbbreviationDetector
from extractor_service.resource_models.base_resource_model import BaseResourceModel, BaseProxyModel


class InData(BaseData):
    text: str
    language: LanguageEnum


class OutData(BaseData):
    abbreviations: List[str]


class InMsg(BaseInQueueMsg):
    data: Optional[InData]


class OutMsg(BaseOutQueueMsg):
    data: Optional[OutData]


class Proxy(BaseProxyModel):
    async def detect_abbreviations(self,
                                   content_id: str,
                                   text: str,
                                   language: LanguageEnum) -> OutData:
        return await self.request(
            InData(
                key_=content_id,
                text=text,
                language=language
            )
        )


class Resources(BaseResources):
    detector: AbbreviationDetector


class AbbreviationDetectorModel(BaseResourceModel):
    def __init__(self,
                 name: str,
                 replicas: int):
        super().__init__(name=name,
                         in_msg_type=InMsg,
                         out_msg_type=OutMsg,
                         proxy_type=Proxy,
                         replicas=replicas)

    def _init_resources(self) -> Resources:
        return Resources(detector=AbbreviationDetector())

    def handle_data(self, resources: Resources, task_data: InData) -> OutData:
        abbreviations = resources.detector.detect(text=task_data.text,
                                                  language=task_data.language)
        return OutData(key_=task_data.key_,
                       abbreviations=abbreviations)
