from enum import Enum

from pydantic import BaseModel


class S3ContentType(str, Enum):
    JSON = "application/json"
    TEXT = "text/plain"
    OCTET_STREAM = "application/octet-stream"


class S3ObjectInfo(BaseModel):
    bucket_name: str
    object_key: str

    def __hash__(self):
        return hash(f"{self.bucket_name}:{self.object_key}")

