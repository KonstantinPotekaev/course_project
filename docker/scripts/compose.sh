#!/bin/bash

set -e

COMPOSE_FILE="docker/docker-compose.yml"

case "$1" in
  up)
    echo "Запуск всех сервисов (docker-compose up)..."
    docker compose -f "$COMPOSE_FILE" up -d
    ;;
  down)
    echo "Остановка всех сервисов (docker-compose down)..."
    docker compose -f "$COMPOSE_FILE" down
    ;;
  logs)
    echo "Логи всех сервисов..."
    docker compose -f "$COMPOSE_FILE" logs -f
    ;;
  restart)
    echo "Перезапуск всех сервисов..."
    docker compose -f "$COMPOSE_FILE" down
    docker compose -f "$COMPOSE_FILE" up -d
    ;;
  *)
    echo "Неверная команда. Используй: up, down, logs, restart"
    exit 1
    ;;
esac
