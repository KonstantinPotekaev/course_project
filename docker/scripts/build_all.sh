#!/bin/bash

set -e

echo "Сборка всех сервисов..."

bash "$(dirname "$0")/build_utils.sh"
bash "$(dirname "$0")/build_service.sh"
bash "$(dirname "$0")/build_web.sh"

echo "Все сервисы успешно собраны!"
