import uuid
from typing import List, Union, Optional

from pydantic import Field

from utils.aes_utils.models.base_message import BaseMsgBody, Status, BaseData
from utils.aes_utils.models.base_model import BaseModel
from utils.status import StatusCodes


# class S3Objects(BaseModel):
#     s3_keys: List[str]
#     bucket_id: str
#     reply_bucket_id: str
#
#
# class S3ObjectsProcessed(BaseModel):
#     s3_keys: List[str]
#     bucket_id: str
#     status: Status = Status.make_status(status=StatusCodes.OK)
#
#
# class AbbreviationExtractionResultData(BaseData):
#     objects: S3ObjectsProcessed
#
#
# class TranscriptionResponseMsg(BaseMsgBody):
#     """Сообщение с результатами транскрибации"""
#
#     data: AbbreviationExtractionResultData

class S3ObjectId(BaseModel):
    bucket_id: str
    object_id: str


class S3ContainerInfo(BaseData):
    container_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    s3_object: List[S3ObjectId]
    reply_bucket_id: Optional[str]
    status: Status = Status.make_status(status=StatusCodes.OK)


class S3ObjectContainersData(BaseData):
    s3_object_containers: List[S3ContainerInfo]


class AbbreviationExtractionRequestData(S3ObjectContainersData):
    language: str


class AbbreviationExtractionRequestMsg(BaseMsgBody):
    """Сообщение для запроса расшифровки аббревиатур объектов DS"""

    data: AbbreviationExtractionRequestData


class S3ObjectProcessed(BaseModel):
    container_id: str
    bucket_id: Optional[str]
    object_id: Optional[str]
    status: Status = Status.make_status(status=StatusCodes.OK)


class AbbreviationExtractionResultsData(BaseData):
    s3_objects: List[S3ObjectProcessed]


class AbbreviationExtractionResponseMsg(BaseMsgBody):
    data: AbbreviationExtractionResultsData
