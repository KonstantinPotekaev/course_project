from functools import wraps
from typing import Callable, Awaitable, Any

from utils.aes_utils.exceptions import BaseAesException, InternalErrorException
from utils.aes_utils.models.abbreviation_extractor import AbbreviationExtractionRequestMsg
from utils.aes_utils.models.base_message import Status
from utils.status import StatusCodes

CONTENT = "content"
REPLY_BUCKET = "reply_bucket_id"
CONTENT_LEN = "content_len"
CONTENT_HASH = "content_hash"


# class BaseModelProvider(abc.ABC):
#
#     _loaded = False
#     _last_used = datetime.now()
#     _drop_models_task: Optional[Task] = None
#
#     @classmethod
#     @abstractmethod
#     def _load_models(cls):
#         raise NotImplementedError
#
#     @classmethod
#     @final
#     def load_models(cls):
#         cls._last_used = datetime.now()
#         if cls._loaded:
#             return
#
#         cls._load_models()
#         cls._loaded = True
#
#     @classmethod
#     @abstractmethod
#     def _drop_models(cls):
#         raise NotImplementedError
#
#     @classmethod
#     @final
#     def drop_models(cls):
#         if cls._drop_models_task is not None:
#             cls._drop_models_task.cancel()
#             cls._drop_models_task = None
#         cls._drop_models()
#         cls._loaded = False
#
#     @classmethod
#     async def _inactive_model_checking_loop(cls, max_inactive_period: int):
#         while True:
#             await asyncio.sleep(max_inactive_period)
#             if not cls._loaded:
#                 continue
#
#             inactive_period = (datetime.now() - cls._last_used).seconds
#             if inactive_period > max_inactive_period:
#                 cls.drop_models()
#
#     @classmethod
#     def start_drop_models_task(cls, inactive_period: int):
#         if cls._drop_models_task is None:
#             cls._drop_models_task = asyncio.create_task(
#                 cls._inactive_model_checking_loop(inactive_period)
#             )
#
#
# class HandlerInterface(metaclass=ABCMeta):
#     async def __aenter__(self):
#         return self
#
#     async def __aexit__(self, exc_type, exc, tb): ...
#
#     @classmethod
#     def __subclasshook__(cls, subclass):
#         return (hasattr(subclass, 'handle')
#                 and callable(subclass.handle)
#                 or NotImplemented)
#
#     @abstractmethod
#     async def handle(self):
#         raise NotImplementedError
#
#
# class DropInactiveModels:
#     """ Декоратор над классами, реализующими методы ModelManagerMixin,
#         позволяющий выгружать модели после заданного периода неактивности
#         и загружать по их необходимости
#     """
#
#     def __init__(self, model_provider: Type[BaseModelProvider], inactive_period: int):
#         """ Инициализация
#
#         :param inactive_period: период неактивности, после которого модели должны быть выгружены
#         :param model_provider:  провайдер моделей
#         """
#         self._model_provider = model_provider
#         self._inactive_period = inactive_period
#
#     def handle_dec(self, decorated_method):
#         @wraps(decorated_method)
#         async def method_wrapper(self_: HandlerInterface):
#             self._model_provider.load_models()
#             self._model_provider.start_drop_models_task(self._inactive_period)
#             await decorated_method(self_)
#         return method_wrapper
#
#     def __call__(self, cls: HandlerInterface):
#         cls.handle = self.handle_dec(cls.handle)
#         return cls


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
