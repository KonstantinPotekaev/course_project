from enum import Enum

from pydantic import BaseModel


class S3ContentType(str, Enum):
    JSON = "application/json"
    TEXT = "text/plain"
    OCTET_STREAM = "application/octet-stream"
