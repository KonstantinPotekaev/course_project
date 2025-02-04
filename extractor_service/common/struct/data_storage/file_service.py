import json
import uuid
from typing import Optional

from extractor_service.common.struct.data_storage.s3 import s3_client
from extractor_service.common.env.general import S3_BUCKET_NAME
from extractor_service.common.globals import service_logger


class FileService:
    """Сервис для работы с файлами (S3, локальная ФС, …)."""

    def __init__(self, bucket_name: str = S3_BUCKET_NAME):
        self._bucket = bucket_name

    def create_presigned_url_for_put(self, filename: str, expires_in: int = 3600) -> (str, str):
        """
        Генерирует presigned URL для загрузки файла в S3.
        Возвращает кортеж (presigned_url, s3_key).
        """
        unique_id = str(uuid.uuid4())
        s3_key = f"uploads/{unique_id}_{filename}"

        url = s3_client.generate_presigned_url(
            ClientMethod='put_object',
            Params={
                'Bucket': self._bucket,
                'Key': s3_key
            },
            ExpiresIn=expires_in
        )
        return url, s3_key

    def create_presigned_url_for_get(self, s3_key: str, expires_in: int = 3600) -> str:
        """Возвращает presigned URL для чтения объекта."""
        try:
            url = s3_client.generate_presigned_url(
                ClientMethod='get_object',
                Params={'Bucket': self._bucket, 'Key': s3_key},
                ExpiresIn=expires_in
            )
            return url
        except Exception as e:
            service_logger.error(f"Error generating presigned GET URL: {e}")
            return ""

    def get_text_file_content(self, s3_key: str) -> Optional[str]:
        """
        Забирает содержимое текстового файла по s3_key из S3.
        Возвращает None, если возникла ошибка.
        """
        try:
            response = s3_client.get_object(Bucket=self._bucket, Key=s3_key)
            content = response["Body"].read().decode("utf-8", errors="ignore")
            return content
        except Exception as e:
            service_logger.error(f"Error reading s3://{self._bucket}/{s3_key}: {e}")
            return None

    def upload_json_to_s3(self, data: dict, prefix: str = "results") -> Optional[str]:
        """
        Сериализует словарь data в JSON и загружает в S3.
        Возвращает s3_key загруженного объекта, или None в случае ошибки.
        """
        s3_key = f"{prefix}/{uuid.uuid4()}.json"
        try:
            json_bytes = json.dumps(data).encode('utf-8')
            s3_client.put_object(
                Bucket=self._bucket,
                Key=s3_key,
                Body=json_bytes,
                ContentType='application/json'
            )
            return s3_key
        except Exception as e:
            service_logger.error(f"Error uploading JSON to s3://{self._bucket}/{s3_key}: {e}")
            return None
