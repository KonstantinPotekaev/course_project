from asyncio import Condition, Lock
from contextlib import asynccontextmanager, AsyncExitStack
from functools import wraps
from inspect import ismethod
from io import BytesIO
from typing import Optional, Union, List, Dict

import aioboto3
from botocore.exceptions import ClientError

from extractor_service.common.env.general import S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT_URL
from extractor_service.common.func.misc import S3ContentType
from utils.aes_utils.exceptions import S3Exception
from utils.aes_utils.models.abbreviation_extractor import S3ObjectId
from utils.aes_utils.models.base_message import Status
from utils.status import StatusCodes


def not_activity(func):
    setattr(func, "activity", False)
    return func


class S3Storage:
    def __init__(self):
        self._exit_stack = AsyncExitStack()
        self._session = aioboto3.Session()
        self._client = None

        self._active_cond = Condition()
        self._active_req_count = 0
        self._update_lock = Lock()

        self._wrap_public_methods()

    @not_activity
    async def init(self):
        if self._client:
            return

        self._client = await self._exit_stack.enter_async_context(
            self._session.client(
                service_name="s3",
                endpoint_url=S3_ENDPOINT_URL,
                aws_access_key_id=S3_ACCESS_KEY,
                aws_secret_access_key=S3_SECRET_KEY
            )
        )

    def _wrap_public_methods(self):
        def _wrapper(func):
            @wraps(func)
            async def wrapped(*args, **kwargs):
                await self._increase_activity()
                try:
                    res = await func(*args, **kwargs)
                finally:
                    await self._decrease_activity()
                return res

            return wrapped

        for attr_name in dir(self):
            attr = getattr(self, attr_name)

            if not ismethod(attr):
                continue

            is_activity = (getattr(attr, "activity")
                           if hasattr(attr, "activity")
                           else True)
            if attr_name.startswith("_") or not is_activity:
                continue

            setattr(self, attr_name, _wrapper(attr))

    async def _increase_activity(self):
        async with self._update_lock:
            async with self._active_cond:
                if self._client is None:
                    await self.init()
                self._active_req_count += 1

    async def _decrease_activity(self):
        async with self._active_cond:
            self._active_req_count -= 1
            self._active_cond.notify_all()

    @asynccontextmanager
    async def _wait_activity_drop(self, min_activities: int = 0):
        async with self._active_cond:
            await self._active_cond.wait_for(
                lambda: self._active_req_count == min_activities
            )
            yield

    @asynccontextmanager
    async def _stop_activity(self, min_activities: int = 0):
        async with self._update_lock:
            async with self._wait_activity_drop(min_activities=min_activities):
                yield

    @not_activity
    async def close(self):
        async with self._stop_activity():
            if self._client:
                await self._client.__aexit__(None, None, None)  # Выход из контекста
                self._client = None

    @not_activity
    async def reinit(self):
        old_client = self._client
        async with self._stop_activity():
            if old_client is not self._client:
                return
            if self._client:
                close_method = getattr(self._client, "close", None)
                if callable(close_method):
                    await close_method()
                self._client = None
            await self.init()

    @not_activity
    async def update_token(self, current_activities: int = 0):
        pass

    async def get_s3_object(self,
                            bucket_name: str,
                            object_key: str,
                            encoding: str = "utf-8") -> Optional[Union[bytes, str, dict]]:
        try:
            resp = await self._client.get_object(
                Bucket=bucket_name,
                Key=object_key
            )
        except ClientError as e:
            error_message = e.response["Error"].get("Message", "Unknown error from S3")
            raise S3Exception(status=Status.make_status(status=StatusCodes.DB_ERROR, message=error_message))

        async with resp["Body"] as stream:
            raw_data = await stream.read()

        text_data = raw_data.decode(encoding)
        return text_data

    async def put_s3_object(self,
                            bucket_name: str,
                            object_id: str,
                            data: BytesIO,
                            data_length: int,
                            file_type: S3ContentType) -> Optional[str]:

        try:
            await self._client.put_object(
                Bucket=bucket_name,
                Key=object_id,
                Body=data.getvalue(),
                ContentType=file_type.value,
                ContentLength=data_length
            )
            return object_id
        except ClientError as e:
            error_message = e.response["Error"].get("Message", "Unknown error from S3")
            raise S3Exception(status=Status.make_status(status=StatusCodes.DB_ERROR, message=error_message))

    async def get_s3_objects(self,
                             objects: List[S3ObjectId],
                             encoding: str = "utf-8") -> Dict[S3ObjectId, Optional[Union[bytes, str, dict]]]:
        results = {}
        for s3_object in objects:
            content = await self.get_s3_object(bucket_name=s3_object.bucket_name,
                                               object_key=s3_object.s3_key,
                                               encoding=encoding)
            results[s3_object] = content
        return results
