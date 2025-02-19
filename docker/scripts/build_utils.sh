#!/bin/bash

set -e

echo "Сборка my_utils..."
docker build -t my_utils -f docker/dockerfiles/Dockerfile.utils .
echo "Образ my_utils готов!"
