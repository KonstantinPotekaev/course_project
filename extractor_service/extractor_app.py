from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

import extractor_service.common.const.resources.model_names as model_names
import extractor_service.common.const.resources.tech_names as tech_names
import extractor_service.common.globals as aes_globals
import extractor_service.resource_models as rcm
import extractor_service.technologies as tech
from extractor_service.common.env.tech.abbreviation_extraction import ABBREVIATION_DETECTOR_REPLICAS, \
    EXPANSION_DETECTOR_REPLICAS, ABBREVIATION_DETECTION_TECH_REPLICAS
from route import router
from utils.aes_utils.async_service_app import run_async_service
from utils.ut_logging import LOGGING_SECTION

SERVICE_NAME = "extractor_service"

app = None


def start_resource_manager():
    aes_globals.resource_manager.register_resources(
        (model_names.ABBREVIATION_DETECTOR,
         rcm.AbbreviationDetectorModel("abbreviation_detector", replicas=ABBREVIATION_DETECTOR_REPLICAS)),
        (model_names.EXPANSION_DETECTOR,
         rcm.ExpansionDetectorModel("expansion_detector", replicas=EXPANSION_DETECTOR_REPLICAS)),
    )

    aes_globals.resource_manager.register_resources(
        (tech_names.ABBREVIATION_EXTRACTION, tech.AbbreviationExtractionTechnology(aes_globals.resource_manager,
                                                                                   replicas=ABBREVIATION_DETECTION_TECH_REPLICAS)),
    )

    aes_globals.resource_manager.start()


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield  # Здесь можно добавить код инициализации перед запуском
    aes_globals.service_logger.info("Shutting down resource manager...")
    aes_globals.resource_manager.stop()


async def main(config: dict):
    aes_globals.init_service_config(config)
    aes_globals.init_service_logger(SERVICE_NAME, config)

    start_resource_manager()

    global app
    app = FastAPI(title=SERVICE_NAME, lifespan=lifespan)
    app.include_router(router, prefix="/api")

    config_uvicorn = uvicorn.Config(app=app,
                                    host="0.0.0.0",
                                    port=8080,
                                    log_config=config[LOGGING_SECTION])

    await uvicorn.Server(config_uvicorn).serve()


async def on_stop_callback():
    aes_globals.resource_manager.stop()


if __name__ == "__main__":
    run_async_service(service_name=SERVICE_NAME,
                      main_coro=main,
                      on_stop=on_stop_callback())
