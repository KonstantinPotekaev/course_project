#!/bin/bash

echo "Полная очистка Docker..."

docker stop $(docker ps -aq) || true
docker rm $(docker ps -aq) || true

docker rmi $(docker images -q) || true

docker volume rm $(docker volume ls -q) || true

echo "Docker очищен!"
