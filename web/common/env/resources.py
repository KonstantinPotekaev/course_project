import os

CHUNK_SIZE = os.getenv('CHUNK_SIZE', 1024)
THREADS = os.getenv('THREADS', 1)
API_TIMEOUT = os.getenv('API_TIMEOUT', 3600)
