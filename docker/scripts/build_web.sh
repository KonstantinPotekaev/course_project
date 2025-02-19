#!/bin/bash

set -e

echo "Сборка frontend (Streamlit)..."
docker build -t my_web_app -f docker/dockerfiles/Dockerfile.web .
echo "Образ my_web_app готов!"
