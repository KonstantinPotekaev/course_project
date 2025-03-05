import os

SRV_LOG_LEVEL = os.getenv("SRV_LOG_LEVEL", "INFO").upper()

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")

# Переменные среды ограничения размеров очереди для subscribers
DEFAULT_SUB_PENDING_MSGS_LIMIT: int = int(os.getenv("DEFAULT_SUB_PENDING_MSGS_LIMIT", 512 * 1024))
DEFAULT_SUB_PENDING_BYTES_LIMIT: int = int(os.getenv("DEFAULT_SUB_PENDING_BYTES_LIMIT", 128 * 1024 * 1024))

CONTENTS_FETCH_THREADS = int(os.getenv("CONTENTS_FETCH_THREADS", 4))
CONTENTS_BATCH_SIZE = int(os.getenv("CONTENTS_BATCH_SIZE", 1))