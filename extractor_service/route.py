from fastapi import APIRouter
from utils.aes_utils.models.abbreviation_extractor import (
    AbbreviationExtractionRequestMsg, AbbreviationExtractionRequestData,
)
import extractor_service.common.globals as aes_globals
import extractor_service.handlers as hdl

router = APIRouter()


@router.post("/abbrev/extract")
async def handle_abbrev_extract(req: AbbreviationExtractionRequestMsg):
    handler = hdl.AbbreviationsExtractorHandler(aes_globals.resource_manager)

    res = await handler(req)

    # 4. Возвращаем результат наружу
    return {"status": "OK", "data": res}
