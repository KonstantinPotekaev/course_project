from __future__ import annotations

from typing import List

from extractor_service.common.struct.language import LanguageEnum
from extractor_service.extractor.languages.language_facture import get_language_instance


class AbbreviationDetector:
    def detect(self, text: str, language: LanguageEnum) -> List[str]:
        return get_language_instance(language).find_abbreviations(text)
