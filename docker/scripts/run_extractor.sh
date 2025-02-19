#!/bin/bash

set -e

echo "Запуск backend (extractor_service)..."
docker run -d -p 8000:8000 --name my_extractor my_extractor
echo "Backend запущен!"
