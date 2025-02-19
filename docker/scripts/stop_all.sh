#!/bin/bash

echo "Остановка всех контейнеров..."
docker stop my_web_app my_extractor minio_s3 || true
docker rm my_web_app my_extractor minio_s3 || true
echo "Все контейнеры остановлены и удалены!"
