from io import BytesIO
from typing import List, Union, Optional, Dict

from pydantic import Field, validator

from extractor_service.common.struct.language import LanguageEnum
from extractor_service.common.struct.model.common import S3ContainerInfo, BaseData
from utils.aes_utils.models.base_model import to_pascal, BaseModel


class AbbreviationExtractorRequestData(BaseModel):
    language: Union[LanguageEnum, str] = Field(default=LanguageEnum.RUSSIAN)
    s3_containers: List[S3ContainerInfo]

    @validator('language', pre=True)
    def convert_language(cls, value):
        if isinstance(value, str):
            try:
                return LanguageEnum(value.lower())
            except ValueError as e:
                raise ValueError(f"Некорректное значение для language: {value}") from e
        return value

    class Config:
        arbitrary_types_allowed = True
        allow_population_by_field_name = True
        alias_generator = to_pascal


class ExpansionToSave(BaseData):
    file_data: BytesIO
    data_length: int


class CreatedS3Object(BaseData):
    bucket_id: Optional[str]
    s3_key: Optional[str]


class AbbreviationExtractorS3Result(CreatedS3Object):
    container_id: str

    class Config:
        arbitrary_types_allowed = True
        allow_population_by_field_name = True
        alias_generator = to_pascal
