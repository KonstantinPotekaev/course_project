from __future__ import annotations
import datetime
from typing import Optional

from pydantic import validator

from utils.aes_utils.models.base_model import BaseModel
from utils.status import StatusCodes


class Status(BaseModel):
    """Статус обработки сообщения"""

    code: int
    message: str

    def __str__(self):
        return f"{self.code}: {self.message}"

    @classmethod
    def make_status(cls, status: StatusCodes, message: str = None) -> Status:
        code, description = status.value

        if message:
            message = f"{description}: {message}"
        else:
            message = description
        return Status(code=code, message=message)


class BaseData(BaseModel):
    status: Status = Status.make_status(status=StatusCodes.OK)


class BaseMsgBody(BaseModel):
    """Тело сообщения"""

    generated_utc: Optional[datetime.datetime]
    data: BaseData = BaseData()


class BaseInternalMsgBody(BaseMsgBody):
    """Вспомогательный тип для содержимого сообщений генерируемых внутренними сервисами"""

    @validator("generated_utc", pre=True, always=True)
    @classmethod
    def set_gen_datetime(cls, v):
        return v or datetime.datetime.now()
