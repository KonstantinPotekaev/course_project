FROM python:3.9-slim

# Создадим рабочую директорию
WORKDIR /app

RUN apt-get update

# Копируем requirements и устанавливаем зависимости
COPY requirements.txt ./
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Копируем ВСЮ папку extractor_service в контейнер
COPY ../ /app/extractor_service


# Предположим, что ваше приложение стартует так:
CMD ["python", "-m", "extractor_service/extractor_2_app"]
