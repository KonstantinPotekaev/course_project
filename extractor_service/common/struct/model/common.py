from typing import Optional, List, Dict, Any, Union

from pydantic import create_model
from pydantic.fields import FieldInfo

from utils.aes_utils.models.abbreviation_extractor import S3ObjectId
from utils.aes_utils.models.base_message import Status
from utils.aes_utils.models.base_model import BaseModel
from utils.status import StatusCodes

Content = Union[str, bytes, Dict]
ContentList = List[Content]


class BaseData(BaseModel):
    key_: str
    status: Status = Status.make_status(status=StatusCodes.OK)

    class Config:
        arbitrary_types_allowed = True
        allow_population_by_field_name = True
        alias_generator = None
        by_alias = False

    def merge(self, model: 'BaseData') -> 'BaseData':
        new_model = super().merge(model)
        new_model.status = model.status
        return new_model


class ExtendedBaseData(BaseData):
    class ExtendedBaseDataBase(BaseModel):
        class Config:
            arbitrary_types_allowed = True
            alias_generator = None
            by_alias = False

        def merge(self, model: BaseData) -> BaseData:
            new_model = super().merge(model)
            new_model.status = model.status
            return new_model

    @classmethod
    def construct(cls, _fields_set=None, **values: Any):
        fields = {name: (field.annotation, field.field_info)
                  for name, field in cls.__fields__.items()}
        fields.update(
            {
                name: (type(value), FieldInfo(None))
                for name, value in values.items()
                if name not in fields
            }
        )
        new_model = create_model("ExtendedBaseData",
                                 __module__=cls.__module__,
                                 __base__=cls.ExtendedBaseDataBase,
                                 **fields)
        return new_model.construct(_fields_set=_fields_set, **values)



class S3ContainerInfo(BaseData):
    container_id: str
    s3_object: List[S3ObjectId]
    user_data: Dict
    reply_bucket_name: Optional[str]

    def __init__(self, **data):
        if 'key_' not in data:
            data['key_'] = data['container_id']
        super().__init__(**data)

    @classmethod
    def construct(cls, _fields_set=None, **values: Any):
        if 'key_' not in values:
            values['key_'] = values['container_id']
        return super().construct(_fields_set=_fields_set, **values)


class LoadedContainer(BaseData):
    container_contents: Optional[ContentList]


class LoadedContent(BaseData):
    content: Content
