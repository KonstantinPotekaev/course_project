import asyncio
import dataclasses
import io
import json
import shutil
import sys
import tempfile
import uuid
import zipfile
from enum import Enum
from pathlib import Path
from typing import List

import aiohttp
import streamlit as st

BASE_DIR = Path(__file__).absolute().parent.parent
sys.path.append(str(BASE_DIR))

from utils.aes_utils.models.abbreviation_extractor import (
    S3ObjectId,
    S3ContainerInfo,
    AbbreviationExtractionRequestMsg,
    AbbreviationExtractionRequestData,
    AbbreviationExtractionResponseMsg
)
from utils.status import StatusCodes

from web.common.const.general import (
    FILE_PATH,
    S3_KEY,
    EXTRACTED_S3_KEY,
    EXTRACTED_OBJECTS,
    INPUT_DIR,
    OUTPUT_DIR,
    S3_PROVIDER,
    S3_OBJECTS,
    EXTRACTING_OPTS,
    PAGE,
    IS_PROCESSING
)
from web.common.env.general import S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_ENDPOINT_URL, AES_HOST
from web.common.env.resources import CHUNK_SIZE, THREADS, API_TIMEOUT
from web.common.providers.data_storage_provider import S3StorageProvider
from web.common.utils import run_async, iter_grouper


class LanguageEnum(str, Enum):
    RUSSIAN = "ru"
    ENGLISH = "en"


@dataclasses.dataclass
class ExtractingOptions:
    Language: LanguageEnum


def get_unique_filename(filename: str) -> str:
    """
    Генерирует короткий уникальный суффикс и добавляет его к имени файла.
    Например, для "file.txt" получим "file_1a2b3c4d.txt".
    """
    unique_suffix = uuid.uuid4().hex[:8]
    path = Path(filename)
    return f"{path.stem}_{unique_suffix}{path.suffix}"


def save_contents(s3_provider: S3StorageProvider,
                  s3_object_list: List[dict],
                  out_dir: Path,
                  temp_bucket: str) -> None:
    """
    Сохраняет контент из S3 в локальные JSON-файлы.
    """
    for obj_group in s3_object_list:
        for s3_object in obj_group:
            s3_key = s3_object[EXTRACTED_S3_KEY]
            content = s3_provider.get_object(s3_key, temp_bucket, content_type="json")
            out_path = Path(out_dir) / f"{s3_object[FILE_PATH]}.json"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with out_path.open('w', encoding="utf-8") as file:
                json.dump(content, file, ensure_ascii=False, indent=4)


async def _extract_object_batch(host: str,
                                object_batch: List[dict],
                                temp_bucket: str,
                                extracting_opts: ExtractingOptions) -> List[dict]:
    """
    Асинхронно обрабатывает пакет объектов для извлечения сокращений.
    """
    extraction_results = {}
    s3_containers = []

    for object_info in object_batch:
        s3_key = object_info[S3_KEY]
        container = S3ContainerInfo(
            s3_object=[S3ObjectId(bucket_name=temp_bucket, s3_key=s3_key)],
            user_data={S3_KEY: s3_key, FILE_PATH: str(object_info[FILE_PATH])},
            reply_bucket_name=temp_bucket
        )
        s3_containers.append(container)
        extraction_results[s3_key] = object_info

    request_msg = AbbreviationExtractionRequestMsg(
        data=AbbreviationExtractionRequestData(
            s3_object_containers=s3_containers,
            **dataclasses.asdict(extracting_opts)
        )
    )

    async with aiohttp.ClientSession() as session:
        async with session.post(host, json=request_msg.dict(), timeout=API_TIMEOUT) as response:
            if response.status != 200:
                st.error(f"Error from {host}: {response.status} - {await response.text()}")
                return []
            result_list = await response.json()

    for result in result_list:
        resp_msg = AbbreviationExtractionResponseMsg.parse_obj(result)
        if resp_msg.data.status.code != StatusCodes.OK.code:
            st.error("Global status not OK: %s - %s",
                     result.data.status.code, result.data.status.message)
            return []
        for s3_obj_proc in resp_msg.data.s3_objects:
            s3_key = s3_obj_proc.user_data[S3_KEY]
            file_path = s3_obj_proc.user_data[FILE_PATH]
            if s3_obj_proc.status.code != StatusCodes.OK.code:
                st.error(f"Error handle: {Path(file_path).name}")
                extraction_results.pop(s3_key, None)
                continue
            extraction_results[s3_key][EXTRACTED_S3_KEY] = s3_obj_proc.s3_key

    return list(extraction_results.values())


async def extract_objects(host: str,
                          s3_objects: list,
                          chunk_size: int,
                          temp_bucket: str,
                          extracting_opts: ExtractingOptions,
                          threads: int = 1) -> List[dict]:
    """
    Обрабатывает объекты S3 порциями с асинхронными запросами.
    """
    object_batch_gen = iter_grouper(s3_objects, chunk_size)
    result = []
    for object_batch_groups in iter_grouper(object_batch_gen, threads):
        task_list = [
            asyncio.create_task(
                _extract_object_batch(
                    host=host,
                    object_batch=object_batch,
                    temp_bucket=temp_bucket,
                    extracting_opts=extracting_opts
                )
            )
            for object_batch in object_batch_groups
        ]
        for task in asyncio.as_completed(task_list):
            try:
                result.append(await task)
            except Exception as ex:
                st.error(f"Ошибка обработки: {ex}")
                raise
    return result


def save_uploaded_files(input_dir: Path, text_input: str, uploaded_files: list) -> None:
    """
    Сохраняет введённый текст и загруженные файлы во временную директорию.
    Каждый файл получает уникальное имя для предотвращения перезаписи.
    """
    if text_input:
        txt_file = input_dir / "input_text.txt"
        with open(txt_file, "w", encoding="utf-8") as f:
            f.write(text_input)

    for uf in uploaded_files:
        file_path = input_dir / get_unique_filename(uf.name)
        with open(file_path, "wb") as f:
            f.write(uf.getbuffer())


def clean_temp_dirs() -> None:
    """
    Удаляет временные директории, если они существуют.
    """
    input_dir = st.session_state.get(INPUT_DIR, "")
    output_dir = st.session_state.get(OUTPUT_DIR, "")
    if input_dir and Path(input_dir).exists():
        shutil.rmtree(input_dir, ignore_errors=True)
    if output_dir and Path(output_dir).exists():
        shutil.rmtree(output_dir, ignore_errors=True)


def page_main() -> None:
    """
    Главная страница: ввод текста, загрузка файлов и выбор языка.
    """
    st.title("Abbreviation Extractor")

    text_input = st.text_area(label="Введите текст (опционально):", height=300)
    uploaded_files = st.file_uploader(label="Загрузите файлы (.txt)",
                                      type=["txt"],
                                      accept_multiple_files=True)
    selected_language = st.selectbox(label="Выберите язык",
                                     options=list(LanguageEnum),
                                     format_func=lambda lang: lang.value)

    if IS_PROCESSING not in st.session_state:
        st.session_state[IS_PROCESSING] = False

    if st.button("Запустить расшифровку") and not st.session_state[IS_PROCESSING]:
        if not text_input and not uploaded_files:
            st.error("Нужно ввести текст или загрузить хотя бы один файл.")
            return

        # Создаем временные директории для входных и выходных данных
        input_dir = Path(tempfile.mkdtemp(prefix="aes_input_"))
        output_dir = Path(tempfile.mkdtemp(prefix="aes_output_"))
        st.session_state[INPUT_DIR] = str(input_dir)
        st.session_state[OUTPUT_DIR] = str(output_dir)

        # Сохраняем введенный текст и загруженные файлы с уникальными именами
        save_uploaded_files(input_dir, text_input, uploaded_files)
        st.info("Загрузка файлов в S3 и расшифровка...")

        try:
            s3_provider = S3StorageProvider(host=S3_ENDPOINT_URL,
                                            access_key=S3_ACCESS_KEY,
                                            secret_key=S3_SECRET_KEY)
            st.session_state[S3_PROVIDER] = s3_provider

            s3_objects = s3_provider.load_objects_from_dir(dir_path=input_dir,
                                                           bucket=S3_BUCKET)
            # Приводим пути к относительным, относительно input_dir
            for s3_object in s3_objects:
                s3_object[FILE_PATH] = s3_object[FILE_PATH].relative_to(input_dir)

            extracting_opts = ExtractingOptions(Language=selected_language)
            st.session_state[IS_PROCESSING] = True
            st.session_state[S3_OBJECTS] = s3_objects
            st.session_state[EXTRACTING_OPTS] = extracting_opts

            st.session_state[PAGE] = "results"
            st.rerun()
        except Exception as e:
            st.error(e)


def page_result() -> None:
    """
    Страница отображения результатов: вывод JSON-файлов и формирование ZIP-архива.
    """
    st.title("Abbreviation Extractor Results")

    if EXTRACTED_OBJECTS not in st.session_state:
        extracted_objects = run_async(extract_objects(
            host=AES_HOST,
            s3_objects=st.session_state[S3_OBJECTS],
            chunk_size=CHUNK_SIZE,
            temp_bucket=S3_BUCKET,
            extracting_opts=st.session_state[EXTRACTING_OPTS],
            threads=THREADS
        ))
        st.session_state[EXTRACTED_OBJECTS] = extracted_objects

        save_contents(s3_provider=st.session_state[S3_PROVIDER],
                      s3_object_list=extracted_objects,
                      out_dir=st.session_state[OUTPUT_DIR],
                      temp_bucket=S3_BUCKET)

    output_dir = Path(st.session_state.get(OUTPUT_DIR, ""))
    if not output_dir or not output_dir.exists():
        st.error("Папка с результатами не найдена.")
        return

    result_files = sorted(output_dir.glob("*.json"))
    if not result_files:
        st.warning("Нет файлов с результатами (.json).")
        return

    files_map = {}
    for file_path in result_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            st.error(f"Ошибка чтения файла {file_path.name}: {e}")
            continue
        files_map[file_path.name] = {"path": file_path, "data": data}

    if not files_map:
        st.warning("Нет корректных файлов с результатами.")
        return

    selected_file = st.selectbox("Выберите файл для просмотра:", list(files_map.keys()))
    file_info = files_map[selected_file]
    st.subheader(f"Результаты расшифровки: {selected_file}")
    st.json(file_info["data"])

    # Формирование ZIP-архива с результатами
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zipf:
        for info in files_map.values():
            zipf.write(info["path"], arcname=info["path"].name)
    zip_buffer.seek(0)

    st.download_button(
        label="Скачать ZIP",
        data=zip_buffer,
        file_name="results.zip",
        mime="application/zip"
    )

    if st.button("Вернуться на главную"):
        clean_temp_dirs()
        st.session_state.clear()
        st.rerun()


def main() -> None:
    """
    Основная функция переключения страниц.
    """
    if PAGE not in st.session_state:
        st.session_state[PAGE] = "main"

    if st.session_state[PAGE] == "main":
        page_main()
    else:
        page_result()


if __name__ == "__main__":
    main()
