from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI

from extractor_service.common.struct.data_storage.s3 import FileService

from extractor_service.common.models.abbreviation_extractor import (
    AbbreviationExtractorRequestMsg,
    PresignedUrlResponse,
    PresignedUrlRequest,
    AbbreviationExtractorRequestData, First
)

from extractor_service.common.run_service_app import run_service
from extractor_service.extractor.abbreviations_extractor import AbbreviationsExtractor
from extractor_service.extractor.utils.merge import merge_abbreviations_dicts

file_service: Optional[FileService] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global file_service

    run_service("abbreviations_extractor_back")
    file_service = FileService()
    yield
    file_service = None


app = FastAPI(lifespan=lifespan)


@app.get("/health")
def healthcheck():
    return {"status": "ok"}


@app.post("/extract")
async def extract_abbreviations_from_files(request_data: First):
    """
    Обрабатываем файлы по одному, чтобы избежать большой нагрузки по памяти.
    Сливаем результаты в единый словарь.
    """
    # 1. Создаём экземпляр экстрактора, исходя из языка:
    extractor = AbbreviationsExtractor(request_data.data.language)

    # 2. Перебираем все s3_key, скачиваем текст, обрабатываем, и мерджим результат
    total_result = {}
    for s3_key in request_data.data.s3_keys:
        content = file_service.get_text_file_content(s3_key)
        if content is None:
            # на случай ошибки чтения
            continue

        # получаем словарь {abbr: {expansion: freq}}
        partial_result = extractor.extract(
            AbbreviationExtractorRequestMsg(data=AbbreviationExtractorRequestData(text=content)))

        # мерджим с общим словарём
        merge_abbreviations_dicts(total_result, partial_result.data.expansions)

    s3_key = file_service.upload_json_to_s3(total_result, prefix="results")
    if not s3_key:
        return {"error": "Failed to store the result in S3."}

    # 3. Генерируем presigned GET URL
    presigned_url = file_service.create_presigned_url_for_get(s3_key)

    return {
        "status": "ok",
        "s3_key": s3_key,
        "presigned_result_url": presigned_url
    }


@app.post("/presign_upload", response_model=PresignedUrlResponse)
def presign_upload(request: PresignedUrlRequest):
    """
    Генерация presigned URL для загрузки файлов в S3.
    """
    if not file_service:
        return PresignedUrlResponse(urls=[])

    results = []
    for fn in request.filenames:
        url, s3_key = file_service.create_presigned_url_for_put(fn)
        results.append({"filename": fn, "presigned_url": url, "s3_key": s3_key})

    return PresignedUrlResponse(urls=results)


@app.post("/presign_upload", response_model=PresignedUrlResponse)
def presign_upload(request: PresignedUrlRequest):
    if not file_service:
        return PresignedUrlResponse(urls=[])
    results = []
    for fn in request.filenames:
        url, s3_key = file_service.create_presigned_url_for_put(fn)
        results.append({"filename": fn, "presigned_url": url, "s3_key": s3_key})
    return PresignedUrlResponse(urls=results)
