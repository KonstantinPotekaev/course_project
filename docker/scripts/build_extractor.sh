#!/bin/bash

set -e

echo "Сборка backend (extractor_service)..."
docker build -t my_extractor -f docker/dockerfiles/Dockerfile.service .
echo "Образ my_extractor готов!"
