#!/bin/bash

set -e

echo "Запуск web-приложения..."
docker run -d -p 8501:8501 --name my_web_app my_web_app
echo "Web-приложение запущено!"
