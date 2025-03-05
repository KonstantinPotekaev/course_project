import json
import logging
import os
import uuid
from concurrent import futures
from hashlib import sha256
from pathlib import Path
from threading import Lock
from typing import List, Generator, Iterator, Literal, Union, Optional

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from web.common.const.general import FILE_PATH, S3_KEY

COMMON_DIR = Path(__file__).parent.parent


def response_message(msg: str, ext_mes: str = "") -> str:
    """Вспомогательная функция для формирования сообщений об ошибках."""
    if ext_mes:
        return f"{msg}; {ext_mes}"
    return msg


class S3StorageProvider:
    """ Предоставляет методы для загрузки документов в хранилище s3 """

    def __init__(self, host: str, access_key: str, secret_key: str):
        """
        Инициализирует клиент для взаимодействия с S3-хранилищем.

        :param host:       Адрес S3-сервера
        :param access_key: AWS/MinIO Access Key
        :param secret_key: AWS/MinIO Secret Key
        """
        self._logger = logging.getLogger("aes_web.s3_provider")
        self._host = host
        self._access_key = access_key
        self._secret_key = secret_key

        self._lock = Lock()

        try:
            self._s3_client: BaseClient = self._get_s3_client()
        except Exception as e:
            self._logger.info(f"Host '{self._host}' is unreachable")
            raise ConnectionError(f"Host '{self._host}' is unreachable") from e

    @property
    def s3_client(self) -> BaseClient:
        return self._s3_client

    @staticmethod
    def get_data_hash(data: bytes) -> str:
        data_hash = sha256()
        data_hash.update(data)
        return data_hash.hexdigest()

    def _get_s3_client(self) -> BaseClient:
        """Создаёт Boto3 S3-клиент для работы с хранилищем"""
        return boto3.client(
            "s3",
            endpoint_url=self._host,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key
        )

    def get_object(self,
                   s3_key: str,
                   bucket: str,
                   content_type: Literal["bytes", "text", "json"] = "bytes"
                   ) -> Optional[Union[bytes, str, dict]]:
        """
        Получение объекта из S3.

        :param s3_key: Ключ (Key) объекта в бакете
        :param bucket: Имя бакета (Bucket)
        :param content_type: Тип контента ("bytes", "text" или "json")
        :return: Контент файла либо None (если объект не найден)
        :raises ConnectionError: при ошибке подключения
        """
        try:
            resp = self._s3_client.get_object(Bucket=bucket, Key=s3_key)
            status_code = resp["ResponseMetadata"]["HTTPStatusCode"]
            if status_code != 200:
                raise ConnectionError(
                    response_message(
                        f"Error: get object {bucket}:{s3_key}, code={status_code}"
                    )
                )

            raw_data = resp["Body"].read()
            if content_type == "text":
                return raw_data.decode("utf-8")
            elif content_type == "json":
                return json.loads(raw_data)
            else:
                return raw_data
        except ClientError as e:
            code = e.response["Error"]["Code"]
            raise ConnectionError(
                response_message(
                    f"S3 get_object failed ({code})",
                    f"Object: {bucket}/{s3_key}"
                )
            )

    def load_object(self, file_path: Path, bucket: str) -> dict:
        """
        Загрузка одного файла в S3, добавляя uuid к имени файла (чтобы избегать коллизий).
        Возвращает словарь с информацией {FILE_PATH, S3_KEY}
        """
        s3_key = f"{uuid.uuid4().hex}_{file_path.name}"
        self.create_bucket(bucket)
        try:
            with file_path.open("rb") as file_obj:
                resp = self._s3_client.put_object(
                    Bucket=bucket,
                    Key=s3_key,
                    Body=file_obj
                )
            status_code = resp["ResponseMetadata"]["HTTPStatusCode"]
            if status_code not in (200, 201, 204):
                raise ConnectionError(
                    response_message(
                        f"Can't upload file: {file_path}",
                        f"Status code: {status_code}"
                    )
                )

            return {
                FILE_PATH: file_path,
                S3_KEY: s3_key
            }
        except ClientError as e:
            code = e.response["Error"]["Code"]
            raise ConnectionError(
                response_message(
                    f"S3 put_object failed ({code})",
                    f"Can't upload file: {file_path}"
                )
            )

    def load_objects(self,
                     file_path_list: Iterator[Path],
                     bucket: str,
                     thread_count: int = 10) -> List[dict]:
        """
        Параллельная загрузка нескольких файлов через ThreadPoolExecutor.
        """
        with futures.ThreadPoolExecutor(max_workers=thread_count) as executor:
            tasks_pool = [
                executor.submit(self.load_object, file_path=fp, bucket=bucket)
                for fp in file_path_list
            ]
            s3_objects = []
            for task in futures.as_completed(tasks_pool):
                try:
                    s3_objects.append(task.result())
                except Exception:
                    raise
            return s3_objects

    @staticmethod
    def _dir_files_gen(dir_path: Path) -> Generator[Path, None, None]:
        for current_dir, dirs, files in os.walk(dir_path):
            for file_name in files:
                yield Path(current_dir, file_name)

    def load_objects_from_dir(self,
                              dir_path: Path,
                              bucket: str,
                              thread_count: int = 10) -> List[dict]:
        """
        Загрузка всех файлов из директории (рекурсивно) в S3.
        """
        return self.load_objects(
            file_path_list=self._dir_files_gen(dir_path),
            bucket=bucket,
            thread_count=thread_count
        )

    def create_bucket(self, bucket: str):
        """
        Создаёт бакет (если не существует).
        """
        try:
            resp = self._s3_client.create_bucket(Bucket=bucket)
            status_code = resp["ResponseMetadata"]["HTTPStatusCode"]
            if status_code not in (200, 202):
                raise ConnectionError(
                    response_message(
                        f"Can't create bucket: '{bucket}'",
                        f"Status code: {status_code}"
                    )
                )
            self._logger.info(f"Bucket '{bucket}' was successfully created.")
        except self._s3_client.exceptions.BucketAlreadyExists:
            pass
        except self._s3_client.exceptions.BucketAlreadyOwnedByYou:
            pass
        except ClientError as e:
            code = e.response["Error"]["Code"]
            raise ConnectionError(
                response_message(
                    f"S3 create_bucket failed ({code})",
                    f"Can't create bucket: '{bucket}'"
                )
            )

    def delete_bucket(self, bucket: str):
        """
        Удаление бакета, даже если он не пустой.
        """
        self._logger.info("Deleting all objects in S3 bucket: %s", bucket)
        try:
            objects = self._s3_client.list_objects_v2(Bucket=bucket)

            if "Contents" in objects:
                delete_keys = [{"Key": obj["Key"]} for obj in objects["Contents"]]
                self._s3_client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": delete_keys}
                )
                self._logger.info("All objects deleted from bucket: %s", bucket)

            self._logger.info("Deleting bucket: %s", bucket)
            resp = self._s3_client.delete_bucket(Bucket=bucket)
            status_code = resp["ResponseMetadata"]["HTTPStatusCode"]
            if status_code != 204:
                raise ConnectionError(
                    response_message(
                        f"Can't delete bucket: '{bucket}'",
                        f"Status code: {status_code}"
                    )
                )
        except ClientError as e:
            code = e.response["Error"]["Code"]
            raise ConnectionError(
                response_message(
                    f"S3 delete_bucket failed ({code})",
                    f"Can't delete bucket: '{bucket}'"
                )
            )