from __future__ import annotations

from typing import List, Dict

from extractor_service.common.struct.language import LanguageEnum
from extractor_service.common.struct.word import WordList
from extractor_service.extractor.languages.language_facture import get_language_instance


class ExpansionDetector:

    def detect(self,
               text: str,
               abbreviations: List[str],
               language: LanguageEnum) -> Dict[str, Dict[str, int]]:
        detected_expansions = {}

        language_class = get_language_instance(language)

        abbreviations.sort()
        words = language_class.get_words_from_string(text)

        word_groups = language_class.get_word_groups_from_wordlist(words)
        word_groups = language_class.remove_single_length_groups(word_groups)

        word_list = [" ".join(group).lower() for group in word_groups]

        expansions_gen = language_class.find_expansion(abbreviations, word_groups, word_list)
        for abbr, expansion_words in expansions_gen:
            expansion_str = " ".join(language_class.normalize_words_form(expansion_words))
            freq = WordList.count_phrase_frequency(" ".join(expansion_words).lower(), word_list)

            detected_expansions.setdefault(abbr, {}).setdefault(expansion_str, 0)
            detected_expansions[abbr][expansion_str] += freq

        return detected_expansions
