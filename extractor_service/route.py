from fastapi import APIRouter
from utils.aes_utils.models.abbreviation_extractor import (
    AbbreviationExtractionRequestMsg,
)
import extractor_service.common.globals as aes_globals
import extractor_service.handlers as hdl

router = APIRouter()


@router.post("/abbrev/extract")
async def handle_abbrev_extract(req: AbbreviationExtractionRequestMsg):
    handler = hdl.AbbreviationsExtractorHandler(aes_globals.resource_manager)

    return await handler(req)
