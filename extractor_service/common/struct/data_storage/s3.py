import asyncio
import json
import uuid
from asyncio import Condition, Lock
from contextlib import asynccontextmanager, AsyncExitStack
from functools import wraps
from inspect import ismethod
from io import BytesIO
from typing import Optional, Union, BinaryIO, List, Dict

import aioboto3

from extractor_service.common.env.general import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
from extractor_service.common.func.misc import S3ContentType
from utils.aes_utils.models.abbreviation_extractor import S3ObjectId


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
                region_name=AWS_REGION,
                endpoint_url="http://localhost:9000",  # Или S3_ENDPOINT_URL
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
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
                            bucket_id: str,
                            object_key: str,
                            encoding: str = "utf-8") -> Optional[Union[bytes, str, dict]]:

        try:
            resp = await self._client.get_object(
                Bucket=bucket_id,
                Key=object_key
            )
        except self._client.exceptions.NoSuchKey:
            return None
        except Exception as ex:
            return None

        async with resp["Body"] as stream:
            raw_data = await stream.read()

        text_data = raw_data.decode(encoding)
        return text_data

    async def put_s3_object(self,
                            bucket_id: str,
                            object_id: str,
                            data: Union[bytes, str, dict, BinaryIO],
                            data_length: int,
                            s3_content_type: Optional[S3ContentType] = None) -> Optional[str]:
        assert self._client, "S3 client must be initialized"

        body = data
        auto_content_type = "application/json"

        # if isinstance(data, (dict, list)):
        #     body = json.dumps(data).encode("utf-8")
        #     auto_content_type = "application/json"
        # elif isinstance(data, str):
        #     body = data.encode("utf-8")
        #     auto_content_type = "text/plain"
        # elif isinstance(data, bytes):
        #     auto_content_type = "application/octet-stream"
        # elif hasattr(data, "read"):
        #     body = data
        #     auto_content_type = "application/octet-stream"
        # else:
        #     raise TypeError(f"Unsupported data type: {type(data)}")

        final_content_type = s3_content_type.value if s3_content_type else auto_content_type

        try:
            await self._client.put_object(
                Bucket=bucket_id,
                Key=object_id,
                Body=body,
                ContentType=final_content_type,
                ContentLength=data_length
            )
            return object_id
        except Exception as ex:
            return None

    async def get_s3_objects(self,
                             objects: List[S3ObjectId],
                             encoding: str = "utf-8") -> Dict[S3ObjectId, Optional[Union[bytes, str, dict]]]:
        results = {}
        for s3_object in objects:
            content = await self.get_s3_object(bucket_id=s3_object.bucket_id,
                                               object_key=s3_object.object_id,
                                               encoding=encoding)
            results[s3_object] = content
        return results
