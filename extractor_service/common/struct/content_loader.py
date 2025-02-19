import asyncio
from abc import abstractmethod, ABC
from asyncio import QueueEmpty, Task
from io import BytesIO
from itertools import chain
from typing import AsyncGenerator, List, Optional, Generator, Dict

import extractor_service.common.globals as aes_globals
from extractor_service.common.env.general import CONTENTS_FETCH_THREADS, CONTENTS_BATCH_SIZE
from extractor_service.common.func.misc import S3ContentType
from extractor_service.common.struct.data_storage.s3 import S3Storage
from extractor_service.common.struct.model.abbreviation_extractor import CreatedS3Object
from extractor_service.common.struct.model.common import LoadedContainer, BaseData, S3ContainerInfo, ContentList
from utils.aes_utils.models.abbreviation_extractor import S3ObjectId
from utils.aes_utils.models.base_message import Status
from utils.common import grouper
from utils.status import StatusCodes


class ContentsLoader(ABC):
    @abstractmethod
    async def get_contents(self, *args, **kwargs):
        raise NotImplementedError

    @staticmethod
    def _queue_ready_items_gen(queue: asyncio.Queue) -> Generator:
        while True:
            try:
                yield queue.get_nowait()
            except QueueEmpty:
                return

    @staticmethod
    async def wait_and_collect(tasks: List[Task], queue: asyncio.Queue) -> List:
        await asyncio.gather(*tasks)
        return [await queue.get() for _ in range(queue.qsize())]

    async def gen_as_ready(self, tasks: List[Task], queue: asyncio.Queue) -> AsyncGenerator:
        while tasks:
            for item in self._queue_ready_items_gen(queue):
                yield item

            _, tasks = await asyncio.wait(tasks, timeout=0.5)

        # оставшиеся результаты
        for item in self._queue_ready_items_gen(queue):
            yield item

    @staticmethod
    async def _put_with_status(queue: asyncio.Queue,
                               data: BaseData,
                               status: Status):
        res_item = LoadedContainer.construct(key_=data.key_, status=status)
        await queue.put(res_item)

    async def _put_all_with_status(self,
                                   queue: asyncio.Queue,
                                   data: List[BaseData],
                                   status: Status):
        for item in data:
            await self._put_with_status(queue=queue,
                                        data=item,
                                        status=status)

    async def close(self):
        pass


class S3ContentsLoader(ContentsLoader):
    def __init__(self):
        self._logger = aes_globals.service_logger.getChild("content_loader.s3")

        self._s3_client = S3Storage()

        self._semaphore = asyncio.Semaphore(CONTENTS_FETCH_THREADS)
        self._access_lock = asyncio.Lock()

    async def close(self):
        """Закрываем соединение с S3."""
        if not self._s3_client:
            return
        await self._s3_client.close()

    async def _update_s3_client(self):
        await self._s3_client.reinit()

    def _assemble_content(self,
                          container_info: S3ContainerInfo,
                          contents: Dict[S3ObjectId, Optional[str]]) -> Optional[ContentList]:
        if not contents:
            return None

        content_list = []
        for s3_object in container_info.s3_object:
            content = contents.get(s3_object)
            if content is None:
                message = f"Contents for object '{s3_object.bucket_name}:{s3_object.s3_key}' wasn't found in S3"
                self._logger.debug(message)
                return None
            content_list.append(content)

        # if merge_contents and content_type != S3ContentType.JSON:
        #     merged_content = _merge_contents(content_list, content_type)
        #     content_list = [merged_content]
        return content_list

    async def _fetch_and_parse_with_semaphore(self,
                                              container_batch: List[S3ContainerInfo],
                                              queue: asyncio.Queue,
                                              content_type: S3ContentType,
                                              merge_contents: bool):

        async with self._semaphore:
            await self.fetch_and_parse(container_batch, queue, content_type, merge_contents)

    @staticmethod
    def _to_flat_object_list(data: List[S3ContainerInfo]) -> List[S3ObjectId]:
        return list(chain.from_iterable(
            (container.s3_object for container in data)
        ))

    async def fetch_and_parse(self,
                              container_batch: List[S3ContainerInfo],
                              queue: asyncio.Queue,
                              content_type: S3ContentType,
                              merge_contents: bool):
        s3_objects = self._to_flat_object_list(container_batch)

        retry_limit = 3
        contents = None
        while retry_limit:
            try:
                contents = await self._s3_client.get_s3_objects(objects=s3_objects)
                break
            except Exception as ex:
                self._logger.warning(f"Failed to get S3 objects: {ex}, retry...")
                await asyncio.sleep(1)
                retry_limit -= 1

        for container in container_batch:
            container_contents = self._assemble_content(container_info=container,
                                                        contents=contents)
            if container_contents is None:
                message = f"Contents for s3_object '{container.container_id}' wasn't found in S3"
                self._logger.debug(message)

                status = Status.make_status(status=StatusCodes.CONTENT_NOT_FOUND,
                                            message=message)
                await self._put_with_status(queue=queue, data=container, status=status)
                continue

            await queue.put(
                LoadedContainer.construct(key_=container.key_,
                                          container_contents=container_contents)
            )

    async def get_contents(self,
                           data: List[S3ContainerInfo],
                           content_type: S3ContentType = S3ContentType.TEXT,
                           merge_contents: bool = False) -> List[LoadedContainer]:
        aes_globals.service_logger.debug("Request S3 contents sequentially ...")

        result_queue = asyncio.Queue()
        tasks = [
            asyncio.create_task(
                self._fetch_and_parse_with_semaphore(container_batch=batch,
                                                     queue=result_queue,
                                                     content_type=content_type,
                                                     merge_contents=merge_contents)
            )
            for batch in grouper(data, CONTENTS_BATCH_SIZE)
        ]

        return await self.wait_and_collect(tasks, result_queue)

    async def get_contents_gen(self,
                               data: List[S3ContainerInfo],
                               content_type: S3ContentType = S3ContentType.TEXT,
                               merge_contents: bool = False) -> AsyncGenerator[LoadedContainer, None]:
        aes_globals.service_logger.debug("Request S3 contents sequentially ...")

        result_queue = asyncio.Queue()
        tasks = [
            asyncio.create_task(
                self._fetch_and_parse_with_semaphore(container_batch=batch,
                                                     queue=result_queue,
                                                     content_type=content_type,
                                                     merge_contents=merge_contents)
            )
            for batch in grouper(data, CONTENTS_BATCH_SIZE)
        ]

        async for item in self.gen_as_ready(tasks, result_queue):
            yield item

    async def put_content(self,
                          content_id: str,
                          file_data: BytesIO,
                          bucket_name: str,
                          data_length: int,
                          file_type: S3ContentType) -> CreatedS3Object:
        aes_globals.service_logger.debug("Create S3 contents sequentially ...")

        retry_limit = 3
        while retry_limit:
            try:
                object_id = await self._s3_client.put_s3_object(data=file_data,
                                                                object_id=content_id,
                                                                bucket_name=bucket_name,
                                                                data_length=data_length,
                                                                file_type=file_type)
                return CreatedS3Object.construct(key_=content_id, bucket_name=bucket_name, s3_key=object_id)
            except Exception as ex:
                self._logger.warning(f"Failed to put S3 object: {ex}, retry...")
                await asyncio.sleep(1)
                retry_limit -= 1
        status = Status.make_status(status=StatusCodes.CONNECTION_ERROR,
                                    message="Can't push content to S3")
        return CreatedS3Object.construct(key_=content_id, status=status)
