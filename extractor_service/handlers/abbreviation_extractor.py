from time import time
from typing import List

import extractor_service.common.globals as aes_globals
from extractor_service.common.const.resources.tech_names import ABBREVIATION_EXTRACTION
from extractor_service.common.env.tech.abbreviation_extraction import ABBREVIATION_EXTRACTOR_MAX_MSG_DATA_BATCH_SIZE
from extractor_service.common.struct.model.common import S3ContainerInfo as InternalS3ContainerInfo
from extractor_service.common.struct.resource_manager import ResourceManager
from extractor_service.handlers.common import catch_internal_errors
from extractor_service.technologies.abbreviation_extraction.abbreviation_extraction import Proxy as Extractor
from utils.aes_utils.models.abbreviation_extractor import AbbreviationExtractionRequestMsg, S3ContainerInfo, \
    AbbreviationExtractionResponseMsg, AbbreviationExtractionResultsData
from utils.common import grouper


class AbbreviationsExtractorHandler:
    def __init__(self, resource_manager: ResourceManager):
        self._tech: Extractor = resource_manager.get_resource(ABBREVIATION_EXTRACTION)
        self._logger = aes_globals.service_logger.getChild('handlers.abbreviation_extractor')

    @staticmethod
    def _transform_containers(containers: List[S3ContainerInfo]) -> List[InternalS3ContainerInfo]:
        if not containers:
            return []

        default_bucket = "abbreviation_extractor"
        if isinstance(containers[0], S3ContainerInfo):
            internal_containers = []
            for item in containers:
                if not item.reply_bucket_name:
                    item.reply_bucket_name = default_bucket

                internal_containers.append(
                    InternalS3ContainerInfo.construct(**item.dict(by_alias=False))
                )
            return internal_containers
        else:
            raise ValueError()

    @catch_internal_errors
    async def __call__(self, msg: AbbreviationExtractionRequestMsg) -> List[AbbreviationExtractionResponseMsg]:
        self._logger.info(f"Msg: {msg}")
        t0 = time()

        data = self._transform_containers(msg.data.s3_object_containers)
        results = await self._tech.handle(data=data,
                                          language=msg.data.language)

        resp_msg_list = [
            AbbreviationExtractionResponseMsg(
                data=AbbreviationExtractionResultsData(s3_objects=data)
            )
            for data in grouper(results, ABBREVIATION_EXTRACTOR_MAX_MSG_DATA_BATCH_SIZE)
        ]
        t1 = time()
        self._logger.info(f"Done ({(t1 - t0):.2f} s)")
        return resp_msg_list

    async def func(self):
        await self()
