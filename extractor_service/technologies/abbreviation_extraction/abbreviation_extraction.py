from functools import partial
from typing import Optional, List

from extractor_service.common.const.resources.model_names import ABBREVIATION_DETECTOR, EXPANSION_DETECTOR
from extractor_service.common.struct.content_loader import S3ContentsLoader
from extractor_service.common.struct.mixins.controlled_runnable_mixin import BaseResources
from extractor_service.common.struct.model.abbreviation_extractor import AbbreviationExtractorRequestData, \
    AbbreviationExtractorS3Result
from extractor_service.common.struct.model.common import S3ContainerInfo
from extractor_service.common.struct.pipeline import Pipeline, PipelineStep
from extractor_service.common.struct.queue import BaseInQueueMsg, BaseOutQueueMsg
from extractor_service.resource_models.base_resource_model import BaseProxyModel
from extractor_service.technologies.abbreviation_extraction.utils.abbreviation_extraction import extract
from extractor_service.technologies.abbreviation_extraction.utils.merge import merge_contents
from extractor_service.technologies.base_technology import BaseTechnology
from utils.aes_utils.models.abbreviation_extractor import AbbreviationExtractionResultsData


class InMsg(BaseInQueueMsg):
    data: Optional[AbbreviationExtractorRequestData]


class OutMsg(BaseOutQueueMsg):
    data: AbbreviationExtractionResultsData


class Resources(BaseResources):
    pipeline: Pipeline


class Proxy(BaseProxyModel):
    async def handle(self,
                     data: List[S3ContainerInfo],
                     language: str):
        return await self.request(
            AbbreviationExtractorRequestData(language=language, s3_containers=data)
        )


class AbbreviationExtractionTechnology(BaseTechnology):

    def __init__(self,
                 resource_manager,
                 name="abbreviation_extraction",
                 replicas: int = 1):
        super().__init__(name=name,
                         in_msg_type=InMsg,
                         out_msg_type=OutMsg,
                         proxy_type=Proxy,
                         resource_manager=resource_manager,
                         replicas=replicas)
        self._contents_loader: Optional[S3ContentsLoader] = None

    async def _on_stop(self):
        if self._contents_loader:
            await self._contents_loader.close()

        self._resource_manager.unlink(ABBREVIATION_DETECTOR)
        self._resource_manager.unlink(EXPANSION_DETECTOR)

    async def _init_resources(self):
        from extractor_service.resource_models.abbreviation_extraction.abbreviation_detector import \
            Proxy as AbbreviationDetectorProxy
        from extractor_service.resource_models.abbreviation_extraction.expansion_detector import \
            Proxy as ExpansionDetectorProxy

        self._contents_loader = S3ContentsLoader()

        abbreviation_detector: AbbreviationDetectorProxy = self._resource_manager.get_resource(ABBREVIATION_DETECTOR)
        expansion_detector: ExpansionDetectorProxy = self._resource_manager.get_resource(EXPANSION_DETECTOR)

        attr_mapping = {"content_id": "key_"}
        content_download_step = PipelineStep(
            partial(self._contents_loader.get_contents_gen, merge_contents=False),
            attr_mapping=attr_mapping
        )
        container_transform_step = PipelineStep(merge_contents,
                                                attr_mapping=attr_mapping)
        abbreviation_extraction_step = PipelineStep(
            partial(extract,
                    abbreviation_detector_model=abbreviation_detector,
                    expansion_detector_model=expansion_detector),
            attr_mapping=attr_mapping
        )

        attr_mapping['bucket_name'] = 'reply_bucket_name'
        result_upload_step = PipelineStep(self._contents_loader.put_content,
                                          attr_mapping=attr_mapping)

        pipeline = Pipeline(initial_step=content_download_step,
                            in_item_type=S3ContainerInfo,
                            out_item_type=AbbreviationExtractorS3Result)
        pipeline.add_branch(
            container_transform_step,
            abbreviation_extraction_step,
            result_upload_step,
        )
        return Resources.construct(pipeline=pipeline)

    async def handle_data(self, resources: Resources, data: AbbreviationExtractorRequestData) -> List[AbbreviationExtractorS3Result]:
        self._logger.debug(f"Msg data: {data}")

        meta = {
            "language": data.language,
        }
        result = await resources.pipeline.start(data.s3_containers, meta=meta)

        self._logger.debug(f"Done")
        return result
