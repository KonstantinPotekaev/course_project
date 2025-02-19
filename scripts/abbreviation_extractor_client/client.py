import asyncio
import dataclasses
import json
import logging
import sys
from argparse import ArgumentParser
from pathlib import Path
from time import time
from typing import Generator, List, AsyncGenerator

import aiohttp

from scripts.abbreviation_extractor_client.const import EXTRACTED_S3_KEY
from scripts.abbreviation_extractor_client.parameter_manager import ConfigurationParams as conf
from scripts.abbreviation_extractor_client.parameter_manager import ParameterManager, LanguageEnum
from scripts.common.const import FILE_PATH, S3_KEY
from scripts.common.providers.s3_storage_provider import S3StorageProvider
from scripts.common.utils import iter_grouper, Statistics
from utils.aes_utils.models.abbreviation_extractor import S3ObjectId, S3ContainerInfo, AbbreviationExtractionRequestMsg, \
    AbbreviationExtractionRequestData, AbbreviationExtractionResponseMsg
from utils.status import StatusCodes

logging.basicConfig(level="INFO",
                    stream=sys.stdout,
                    format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ExtractingOptions:
    Language: LanguageEnum


def _get_abbrev_api(host: str) -> str:
    """
    Генерирует URL вида "{host}/abbrev/extract" для POST-запроса.
    """
    # Убедимся, что нет двойного слэша:
    return f"{host.rstrip('/')}:8080/api/abbrev/extract"


async def save_contents(s3_provider: S3StorageProvider,
                        s3_object_list: AsyncGenerator,
                        out_dir: Path,
                        temp_bucket: str):
    async for s3_object in s3_object_list:
        s3_key = s3_object[EXTRACTED_S3_KEY]
        content = s3_provider.get_object(s3_key, temp_bucket, content_type="json")

        # Сохранение контента в файл
        out_path = Path(out_dir) / f"{s3_object[FILE_PATH]}.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)

        with out_path.open('w', encoding="utf-8") as file:
            json.dump(content, file, ensure_ascii=False, indent=4)


async def _extract_object_batch(host: str,
                                object_batch: List[dict],
                                temp_bucket: str,
                                extracting_opts: ExtractingOptions) -> List[dict]:
    extraction_results = {}

    s3_containers = []
    for object_info in object_batch:
        s3_key = object_info[S3_KEY]
        container = S3ContainerInfo(s3_object=[S3ObjectId(bucket_name=temp_bucket, s3_key=s3_key)],
                                    user_data={S3_KEY: s3_key, FILE_PATH: str(object_info[FILE_PATH])},
                                    reply_bucket_name=temp_bucket)
        s3_containers.append(container)
        extraction_results[s3_key] = object_info

    request_msg = AbbreviationExtractionRequestMsg(
        data=AbbreviationExtractionRequestData(s3_object_containers=s3_containers,
                                               **dataclasses.asdict(extracting_opts))
    )

    files_to_extract = [str(object_info[FILE_PATH]) for object_info in object_batch]
    logger.debug(f"Extraction of: %s", files_to_extract)

    start_time = time()

    async with aiohttp.ClientSession() as session:
        url = _get_abbrev_api(host)
        async with session.post(url, json=request_msg.dict()) as response:
            if response.status != 200:
                error_text = await response.text()
                logger.warning(f"Error from {url}: {response.status} - {error_text}")
                return []


            result_list = await response.json()
    Statistics.add_statistic(files_to_extract, time() - start_time)

    for result in result_list:
        resp_msg = AbbreviationExtractionResponseMsg.parse_obj(result)

        if resp_msg.data.status.code != StatusCodes.OK.code:
            logger.warning("Global status not OK: %s - %s",
                           result.data.status.code, result.data.status.message)
            return []


        for s3_obj_proc in resp_msg.data.s3_objects:
            s3_key = s3_obj_proc.user_data[S3_KEY]
            file_path = s3_obj_proc.user_data[FILE_PATH]

            if s3_obj_proc.status.code != StatusCodes.OK.code:
                logger.warning(f"Error handle: {Path(file_path).name}")
                del extraction_results[s3_key]
                continue

            extraction_results[s3_key][EXTRACTED_S3_KEY] = s3_obj_proc.s3_key

    return list(extraction_results.values())


async def extract_objects(host: str,
                          s3_objects: list,
                          chunk_size: int,
                          temp_bucket: str,
                          extracting_opts: ExtractingOptions,
                          threads: int = 1) -> Generator[dict, None, None]:
    object_batch_gen = iter_grouper(s3_objects, chunk_size)
    for object_batch_groups in iter_grouper(object_batch_gen, threads):
        task_list = []
        for object_batch in object_batch_groups:
            task = asyncio.create_task(
                _extract_object_batch(host=host,
                                      object_batch=object_batch,
                                      temp_bucket=temp_bucket,
                                      extracting_opts=extracting_opts)
            )
            task_list.append(task)

        for task in asyncio.as_completed(task_list):
            try:
                results = await task
                for transcribed_object in results:
                    yield transcribed_object
            except Exception as ex:
                logger.info(ex)


def parse_args():
    parser = ArgumentParser()
    parser.add_argument("-s", "--host", type=str, required=False, help="Host server address with AES")
    parser.add_argument("-akey", "--access-key", type=str, required=False, help="AWS/MinIO Access Key")
    parser.add_argument("-skey", "--secret-key", type=str, required=False, help="AWS/MinIO Secret Key")
    parser.add_argument("-i", "--in-dir", type=Path, required=False, help="Directory with files to load")
    parser.add_argument("-o", "--out-dir", type=Path, required=False, help="Output directory")
    parser.add_argument("-cfg", "--config", type=Path, required=False, help="Path to the configuration JSON file")
    parser.add_argument("-t", "--threads", type=int, required=False, help="Thread number to use (default=1)")
    parser.add_argument("-c", "--chunk-size", type=int, required=False,
                        help="Number of files in chunk that will be sent to DAS (default=1)")
    parser.add_argument("-b", "--bucket", type=str, required=False,
                        help="Название тестового бакета в S3,"
                             " который используется для передачи файлов в сервисы AES")
    parser.add_argument("-l", "--language",
                        type=LanguageEnum,
                        choices=list(LanguageEnum),
                        required=False,
                        help="Select language (default: RUSSIAN)")
    return parser.parse_args()


async def main():
    args_config = ParameterManager.from_args(parse_args())

    json_config = None
    if args_config[conf.CONFIG]:
        json_config = ParameterManager.from_json(args_config[conf.CONFIG])
    config = ParameterManager.merge_configs(args_config=args_config, json_config=json_config, check_requirements=True)

    logger.info("Configure connection to AES server...")

    s3_provider = S3StorageProvider(host=config[conf.HOST],
                                    access_key=config[conf.ACCESS_KEY],
                                    secret_key=config[conf.SECRET_KEY])
    try:
        logger.info("Loading files to S3...")
        s3_objects = s3_provider.load_objects_from_dir(dir_path=config[conf.IN_DIR],
                                                       bucket=config[conf.BUCKET])
        for s3_object in s3_objects:
            s3_object[FILE_PATH] = s3_object[FILE_PATH].relative_to(config[conf.IN_DIR])
        logger.info(f"Files loaded: {len(s3_objects)}\n")

        logger.info("Run extracting abbreviations...")
        extracting_opts = ExtractingOptions(Language=config[conf.LANGUAGE])
        extracting_start_time = time()
        extracted_objects = extract_objects(host=config[conf.HOST],
                                            s3_objects=s3_objects,
                                            extracting_opts=extracting_opts,
                                            chunk_size=config[conf.CHUNK_SIZE],
                                            threads=config[conf.THREADS],
                                            temp_bucket=config[conf.BUCKET])

        await save_contents(s3_provider=s3_provider,
                            s3_object_list=extracted_objects,
                            out_dir=config[conf.OUT_DIR],
                            temp_bucket=config[conf.BUCKET])

        Statistics.save_statistics(config[conf.OUT_DIR])
        logger.info(f"Result are saved to '{config[conf.OUT_DIR]}'")
        logger.info(f"Done (Time: {(time() - extracting_start_time):.2f}s)")
    finally:
        s3_provider.delete_bucket(config[conf.BUCKET])


if __name__ == '__main__':
    asyncio.run(main())
