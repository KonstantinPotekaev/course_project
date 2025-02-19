import uuid
from typing import List, Optional, Dict

from pydantic import Field

from utils.aes_utils.models.base_message import BaseMsgBody, Status, BaseData
from utils.aes_utils.models.base_model import BaseModel
from utils.status import StatusCodes


class S3ObjectId(BaseModel):
    bucket_name: str
    s3_key: str

    def __hash__(self):
        return hash(f"{self.bucket_name}:{self.s3_key}")


class S3ContainerInfo(BaseData):
    container_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    s3_object: List[S3ObjectId]
    user_data: Dict = Field(default_factory=dict)
    reply_bucket_name: Optional[str]
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
    bucket_name: Optional[str]
    s3_key: Optional[str]
    user_data: Dict = Field(default_factory=dict)
    status: Status = Status.make_status(status=StatusCodes.OK)


class AbbreviationExtractionResultsData(BaseData):
    s3_objects: List[S3ObjectProcessed]


class AbbreviationExtractionResponseMsg(BaseMsgBody):
    data: AbbreviationExtractionResultsData
