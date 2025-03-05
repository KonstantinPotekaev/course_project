import os
from typing import Optional

from utils.common import parse_bool

CPU_LIMIT: Optional[float] = os.getenv('CPU_LIMIT')
MEMORY_LIMIT: Optional[int] = os.getenv('MEMORY_LIMIT')     # in bytes
USE_GPU: Optional[bool] = parse_bool(os.getenv('USE_GPU', False))
PROCESS_QUEUE_MAX_SIZE = int(os.getenv("PROCESS_QUEUE_MAX_SIZE", 1000))
