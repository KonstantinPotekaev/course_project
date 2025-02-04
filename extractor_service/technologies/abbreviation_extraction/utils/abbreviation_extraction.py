import json
from io import BytesIO
from typing import AsyncGenerator

from sympy import content

from extractor_service.common.struct.language import LanguageEnum
from extractor_service.common.struct.model.abbreviation_extractor import ExpansionToSave
from extractor_service.resource_models.abbreviation_extraction.abbreviation_detector import \
    Proxy as AbbreviationDetectorModel
from extractor_service.resource_models.abbreviation_extraction.expansion_detector import Proxy as ExpansionDetectorModel


async def extract(
        content_id: str,
        text: str,
        abbreviation_detector_model: AbbreviationDetectorModel,
        expansion_detector_model: ExpansionDetectorModel,
        language: LanguageEnum.RUSSIAN) -> AsyncGenerator[ExpansionToSave, None]:
    abbreviations = (await abbreviation_detector_model.detect_abbreviations(content_id=content_id,
                                                                            text=text,
                                                                            language=language)).abbreviations
    expansions = (await expansion_detector_model.detect_expansions(content_id=content_id,
                                                                   text=text,
                                                                   language=language,
                                                                   abbreviations=abbreviations)).expansions

    json_content = json.dumps(expansions, ensure_ascii=False, indent=2)
    byte_file_content = json_content.encode("utf-8")

    yield ExpansionToSave(
        key_=content_id,
        file_data=BytesIO(byte_file_content),
        data_length=len(byte_file_content),
    )
