from functools import wraps
from typing import Callable, Awaitable, Any

from utils.aes_utils.exceptions import BaseAesException, InternalErrorException
from utils.aes_utils.models.abbreviation_extractor import AbbreviationExtractionRequestMsg
from utils.aes_utils.models.base_message import Status
from utils.status import StatusCodes


def catch_internal_errors(func: Callable[[Any, AbbreviationExtractionRequestMsg], Awaitable[None]]):
    @wraps(func)
    async def wrapper(handler, msg: AbbreviationExtractionRequestMsg):
        try:
            return await func(handler, msg)
        except Exception as e:
            if isinstance(e, BaseAesException):
                raise InternalErrorException(status=Status(**e.status.dict(by_alias=True)))

            raise InternalErrorException(
                status=Status.make_status(
                    status=StatusCodes.INTERNAL_ERROR,
                    message="Unknown error")
            )

    return wrapper
