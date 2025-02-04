from time import time
from typing import List

import extractor_service.common.globals as aes_globals

from extractor_service.common.const.resources.tech_names import ABBREVIATION_EXTRACTION
from extractor_service.common.struct.resource_manager import ResourceManager
from extractor_service.handlers.common import catch_internal_errors
from extractor_service.common.struct.model.common import S3ContainerInfo as InternalS3ContainerInfo
from extractor_service.technologies.abbreviation_extraction.abbreviation_extraction import Proxy as Extractor
from utils.aes_utils.models.abbreviation_extractor import AbbreviationExtractionRequestMsg, S3ContainerInfo


class AbbreviationsExtractorHandler:
    def __init__(self, resource_manager: ResourceManager):
        self._tech: Extractor = resource_manager.get_resource(ABBREVIATION_EXTRACTION)
        self._logger = aes_globals.service_logger.getChild('handlers.transcription')

    @staticmethod
    def _transform_containers(containers: List[S3ContainerInfo]) -> List[InternalS3ContainerInfo]:
        if not containers:
            return []

        default_bucket = "abbreviation_extractor"
        if isinstance(containers[0], S3ContainerInfo):
            internal_containers = []
            for item in containers:
                if not item.reply_bucket_id:
                    item.reply_bucket_id = default_bucket

                internal_containers.append(
                    InternalS3ContainerInfo.construct(**item.dict(by_alias=False))
                )
            return internal_containers
        else:
            raise ValueError()

    @catch_internal_errors
    async def __call__(self, msg: AbbreviationExtractionRequestMsg):
        self._logger.info(f"Msg: {msg}")
        t0 = time()

        data = self._transform_containers(msg.data.s3_object_containers)
        results = await self._tech.handle(data=data,
                                          language=msg.data.language)

        print(results, flush=True)

        # resp_msg_list = [
        #     TranscriptionResponseMsg(
        #         data=TranscriptionResultsData(ds_node=msg_data.ds_node,
        #                                       ds_objects=data)
        #     )
        #     for data in grouper(results, TRANSCR_MAX_MSG_DATA_BATCH_SIZE)
        # ]
        # await self._reply_client.ack_msg_batch(msg, resp_msg_list)
        t1 = time()
        self._logger.info(f"Done ({(t1 - t0):.2f} s)")

    async def func(self):
        await self()
